// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.spark.PEP3101Parser
import com.microsoft.azure.synapse.ml.fabric.FabricClient
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails
import com.microsoft.azure.synapse.ml.param.{GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services.ServiceAuthHeaders
import com.microsoft.azure.synapse.ml.services.openai.{JsonParser => OpenAIJsonParser, _}
import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ExplicitComplexParamsReadable, ExplicitComplexParamsWritable, ParamInjections, Serializer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoders, Row, SparkSession, functions => F}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AesEncrypt, Attribute, EulerNumber, GenericRowWithSchema,
  LeafExpression, Literal, NamedLambdaVariable, Pi, SubqueryExpression, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, Project, Window}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import spray.json._

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.util.Collections
import java.util.concurrent.CancellationException
import scala.language.existentials

object AIPrompt extends ExplicitComplexParamsReadable[AIPrompt] {
  override protected def expectedClass: Class[AIPrompt] = classOf[AIPrompt]

  private val HeaderNamePattern = "[!#$%&'*+.^_`|~0-9A-Za-z-]+".r.pattern
  private val FirstLatin1HeaderCharacter = 0xa0
  private val LastLatin1HeaderCharacter = 0xff

  private def validateHeaderName(name: String): Unit = {
    require(name != null && HeaderNamePattern.matcher(name).matches(), "Invalid HTTP header name")
    require(!"Content-Length".equalsIgnoreCase(name) && !"Transfer-Encoding".equalsIgnoreCase(name),
      "HTTP framing headers Content-Length and Transfer-Encoding are managed by the client; omit them")
  }

  private def validateHeader(name: String, value: String): Unit = {
    validateHeaderName(name)
    require(value != null && value.forall(c =>
      c == '\t' || (c >= ' ' && c <= '~') ||
        (c >= FirstLatin1HeaderCharacter && c <= LastLatin1HeaderCharacter)),
      "Invalid HTTP header value")
  }

  private def validateCallerHeaders(headers: scala.collection.Map[String, String]): Unit =
    headers.keys.foreach(validateHeaderName)

  private def transportHeaders(prompt: AIPrompt, address: String,
                               resolution: ServiceAuthHeaders.Resolution): Seq[(String, String)] = {
    val headers = if (prompt.usingImplicitFabricEndpoint(address)) {
      OpenAIFabricHeaders.merge(Some(resolution.headers))
    } else resolution.headers
    val host = AIRequestAdapter.normalizedHost(new java.net.URI(address))
    val bearerKey = AIRequestAdapter.isOpenAIHost(host) ||
      (prompt.getApiType == "decisions" && !AIRequestAdapter.isAzureHost(host))
    val outgoing = headers.toSeq.filterNot { case (name, _) => HTTPRequestData.isFabricAuthMarker(name) }
      .filterNot(_._1.equalsIgnoreCase("Content-Type"))
      .map { case (name, value) =>
        if (bearerKey && resolution.usesSubscriptionKey && name.equalsIgnoreCase(prompt.subscriptionKeyHeaderName)) {
          prompt.aadHeaderName -> ("Bearer " + value)
        } else name -> value
      }
    val result = outgoing :+ ("Content-Type" -> "application/json")
    if (resolution.usesFabricFallback) result :+ (HTTPRequestData.FabricAuthMarkerHeader -> "true") else result
  }

  private def requestHeaders(prompt: AIPrompt, address: String,
                             string: ServiceParam[String] => Option[String],
                             map: ServiceParam[Map[String, String]] => Option[Map[String, String]],
                             includeFabricFallback: Boolean): Seq[(String, String)] = {
    val custom = map(prompt.customHeaders)
    val telemetry = map(prompt.telemHeaders)
    Seq(custom, telemetry).flatten.foreach(validateCallerHeaders)
    lazy val implicitAuth = if (includeFabricFallback && prompt.usingImplicitFabricEndpoint(address)) {
      Option(prompt.fabricAuthHeader)
    } else None
    val resolution = ServiceAuthHeaders.resolve(string(prompt.subscriptionKey), prompt.subscriptionKeyHeaderName,
      prompt.aadHeaderName, string(prompt.AADToken), string(prompt.CustomAuthHeader), custom,
      implicitAuth, telemetry, Some("application/json"))
    transportHeaders(prompt, address, resolution)
  }

  private[ai] def validateScalarHeaders(prompt: AIPrompt, address: String): Unit = {
    def scalar[T](param: ServiceParam[T]): Option[T] =
      prompt.get(param).orElse(prompt.getDefault(param)).flatMap(_.left.toOption).flatMap(Option(_))
    def column[T](param: ServiceParam[T]): Boolean =
      prompt.get(param).orElse(prompt.getDefault(param)).exists(_.isRight)
    // A higher-priority column can suppress a scalar fallback; only its rows can validate that choice.
    val conditionalAuth = Seq(prompt.subscriptionKey, prompt.AADToken, prompt.CustomAuthHeader)
      .takeWhile(param => !scalar(param).exists(ServiceAuthHeaders.nonBlank)).exists(column(_))
    val conditionalTelemetry = column(prompt.telemHeaders)
    requestHeaders(prompt, address, scalar[String], scalar[Map[String, String]], includeFabricFallback = false)
      .filter { case (name, _) =>
        if (name.equalsIgnoreCase(prompt.subscriptionKeyHeaderName) || name.equalsIgnoreCase(prompt.aadHeaderName)) {
          !conditionalAuth
        } else !conditionalTelemetry
      }.foreach { case (name, value) => validateHeader(name, value) }
  }

  private[ai] def hasConfiguredAuthentication(prompt: AIPrompt): Boolean = {
    def scalar[T](param: ServiceParam[T]): Option[T] =
      prompt.get(param).orElse(prompt.getDefault(param)).flatMap(_.left.toOption)
    val resolved = ServiceAuthHeaders.resolve(scalar(prompt.subscriptionKey), prompt.subscriptionKeyHeaderName,
      prompt.aadHeaderName, scalar(prompt.AADToken), scalar(prompt.CustomAuthHeader),
      scalar(prompt.customHeaders), None, None, None)
    resolved.headers.keys.exists(name => prompt.authenticationHeaderNames(name.toLowerCase(java.util.Locale.ROOT))) ||
      (prompt.isDefined(prompt.url) && prompt.usingImplicitFabricEndpoint(prompt.getUrl.stripSuffix("/") + "/"))
  }

  private val FirstRetryMs = 100
  private val SecondRetryMs = 500
  private val ThirdRetryMs = 1000
  private val RetryIntervals = Array(FirstRetryMs, SecondRetryMs, ThirdRetryMs)
  private[ai] val DefaultHttpPolicy = "synapseml-bounded-http-v1:" + RetryIntervals.mkString(",")
  private[ai] val DefaultHandler = UDFUtils.oldUdf(HandlingUtils.bounded(RetryIntervals: _*) _, StringType)
  private[ai] def usesBoundedHandler(stage: AIPrompt): Boolean =
    !stage.isSet(stage.handler) && stage.getDefault(stage.handler).contains(DefaultHandler)

  private val MessageSchema = Encoders.product[OpenAICompositeMessage].schema
  private val MessageType = ArrayType(MessageSchema, containsNull = true)

  private[ai] def createHttpTransformer(stage: AIPrompt): HTTPTransformer =
    new HTTPTransformer().setConcurrency(stage.getConcurrency).setTimeout(stage.getTimeout)
      .setConcurrentTimeout(stage.get(stage.concurrentTimeout).orElse(stage.getDefault(stage.concurrentTimeout)))
      .setHandler(stage.getHandler)

  private[ai] def withSession[T](spark: SparkSession)(action: => T): T = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    try action finally {
      previous match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  private[ai] def withSafeTemplate[T](prepare: => T): T = try {
    prepare
  } catch {
    // Parser and analyzer exceptions can contain the full prompt, including in their causes.
    case _: IllegalArgumentException | _: AnalysisException =>
      throw new IllegalArgumentException("Invalid prompt template; check braces, expressions and input columns")
  }

  private[ai] def bindTemplate(input: DataFrame, expression: Column): Column = {
    // A command's analyzed plan differs from the CommandResult used by subsequent Dataset operations.
    val base = input.select(input.columns.map(AIColumnPlan.column): _*)
    withSafeTemplate(base.select(expression).queryExecution.analyzed) match {
      case Project(Seq(resolved), child) if child == base.queryExecution.analyzed => new Column(resolved)
      case _ => throw new IllegalArgumentException(
        "Prompt templates require row-local expressions; materialize windows, aggregates and generators " +
          "into input columns")
    }
  }

  def loadOutput(spark: SparkSession, outputPath: String): DataFrame =
    AIPromptOutput.loadOutput(spark, outputPath)

  def getOutputStatus(spark: SparkSession, outputPath: String): AIPromptOutputStatus =
    AIPromptOutput.getOutputStatus(spark, outputPath)

  def getOutputStatusJson(spark: SparkSession, outputPath: String): String =
    getOutputStatus(spark, outputPath).toJson

  def releaseOutputOwner(spark: SparkSession, outputPath: String,
                         expectedOwnerToken: String, confirmStopped: Boolean): Unit =
    AIPromptOutput.releaseOutputOwner(spark, outputPath, expectedOwnerToken, confirmStopped)
}

private[ai] trait HasAIResponseSchema extends HasOpenAIResponseSchema {
  override protected def validateResponseSchemaValues(schema: Map[String, Any]): Unit = {
    AIJson.fromAny(schema)
  }

  override protected def pyParamInputType(p: Param[_]): Option[String] =
    AIPromptPython.paramInputType(p.name).orElse(super.pyParamInputType(p))

  override protected def pyResponseSchemaToJava: String =
    "self._java_obj.responseFormat().jsonDecode(self._ai_json({'left': schema})).left().get()"
}

/** Distributed synchronous inference. Uncommitted requests can repeat even with outputPath configured. */
class AIPrompt(override val uid: String) extends OpenAIPrompt(uid)
  with AIPromptParams with HasAIPromptRecovery with HasHandler
  with ExplicitComplexParamsWritable with HasAIResponseSchema {
  def this() = this(Identifiable.randomUID("AIPrompt"))

  override protected def createResponseFormatParam(name: String): ServiceParam[Map[String, Any]] =
    new AIJson.ObjectParam(this, name,
      "Response format for the completion. One of 'text', 'json_object', or 'json_schema'.", "response_format")

  setDefault(handler -> AIPrompt.DefaultHandler)
  ParamInjections.clearDefault(this, deploymentName)

  override protected def supportedApiTypes: Array[String] =
    AIRequestAdapter.SupportedApis.toArray

  override protected val subscriptionKeyHeaderName: String = "api-key"

  private[ai] lazy val authenticationHeaderNames: Set[String] =
    Set(subscriptionKeyHeaderName, aadHeaderName).map(_.toLowerCase(java.util.Locale.ROOT))

  override protected def hasExplicitPostProcessing: Boolean = isSet(postProcessing)

  override protected val useBoundedAttachments: Boolean = true

  def setModelCol(value: String): this.type = setVectorParam(model, value)
  def getModelCol: String = getVectorParam(model)
  override def setStoreCol(value: String): this.type = setVectorParam(store, value)
  override def getStoreCol: String = getVectorParam(store)
  def setResponseFormatCol(value: String): this.type = setVectorParam(responseFormat, value)
  def getResponseFormatCol: String = getVectorParam(responseFormat)

  override def setResponseFormat(value: Map[String, Any]): this.type = {
    val formatted = AIRequestAdapter.responseFormat(value, "chat_completions")
    setScalarParam(responseFormat, AIJson.orderedMap(formatted.fields.toSeq.map { case (key, entry) =>
      key -> AIJson.AnyFormat.read(entry)
    }))
  }

  override def copy(extra: ParamMap): AIPrompt = super.copy(extra).asInstanceOf[AIPrompt]

  override protected def validateForWrite(): Unit = {
    // Explicit persistence replaces OpenAIPrompt.write, including its parser validation.
    getParser
    require(getDefault(handler).contains(AIPrompt.DefaultHandler),
      "Saving AIPrompt requires the original handler default; use setHandler for a custom persisted handler. " +
        s"Loading a custom handler requires ${Serializer.LegacyObjectDeserializationConfig}=true; " +
        "enable this only for trusted artifacts.")
  }

  private def setImplicitFabricEndpoint(): Unit = {
    if (AIRequestAdapter.ImplicitFabricApis(getApiType) &&
      !isDefined(url) && !isDefined(customUrlRoot)) {
      fabricEndpoint.foreach(endpoint => setDefault(url -> endpoint))
    }
  }

  private def excludedGlobalDefaults(result: AIPrompt): Set[String] = {
    val overriddenModelDefaults = if (isSet(model)) Set(deploymentName.name)
      else if (isSet(deploymentName)) Set(model.name) else Set.empty[String]
    val allowed = AIRequestAdapter.allowedOptions(result.getApiType)
    val inactiveOptions = AIRequestAdapter.bodyOptions(result).filterNot(p => allowed(p.name)).map(_.name).toSet
    val nativeDefaults = if (AIRequestAdapter.NativeApis(result.getApiType)) {
      Set(model.name, deploymentName.name, url.name, subscriptionKey.name)
    } else Set.empty[String]
    val inactiveEndpointDefaults = if (result.isSet(customUrlRoot)) Set(url.name) else Set.empty[String]
    Set(apiType.name, apiVersion.name) ++ overriddenModelDefaults ++
      inactiveOptions ++ nativeDefaults ++ inactiveEndpointDefaults
  }

  private[ai] def effectiveCopy(): AIPrompt = {
    val result = copy(ParamMap.empty)
    if (!isSet(apiType)) {
      GlobalParams.getParam(apiType).filter(value => supportedApiTypes.contains(value.toString))
        .foreach(value => result.setDefault(result.apiType -> value.toString))
    }
    val excludedGlobals = excludedGlobalDefaults(result)
    params.filterNot(p => isSet(p) || excludedGlobals(p.name)).foreach { p =>
      GlobalParams.getParam(p).foreach { value =>
        result.setDefault(result.getParam(p.name).asInstanceOf[Param[Any]] -> value)
      }
    }
    result.setImplicitFabricEndpoint()
    AIRequestAdapter.defaultApiVersion(result).foreach(value => result.setDefault(result.apiVersion -> value))
    if (result.getApiType == "embeddings" && !isSet(deploymentName) && !isSet(model)) {
      GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey).foreach { v =>
        result.setDefault(result.deploymentName -> v)
      }
    }
    if (result.isSet(result.model)) ParamInjections.clearDefault(result, result.deploymentName)
    else if (result.isDefined(result.deploymentName)) ParamInjections.clearDefault(result, result.model)
    result
  }

  private[ai] def responseIdEnabled: Boolean = isSet(responseIdCol) ||
    (getApiType == "responses" && get(store).orElse(getDefault(store)).exists {
      case Left(enabled) => enabled
      case Right(_) => true
    })

  private def retainedMessagesName(schema: StructType): Option[String] =
    if (AIRequestAdapter.GenerativeApis(getApiType) && !getDropPrompt && !isSet(messagesCol)) {
      val outputs = Seq(getOutputCol, getErrorCol) ++ get(usageCol) ++ get(rawResponseCol) ++
        (if (responseIdEnabled) Seq(getResponseIdCol) else Seq.empty)
      Some(AIColumnPlan.unused(getMessagesCol, schema.fieldNames.toSeq ++ outputs))
    } else None

  private def plan(schema: StructType, parsed: Boolean): AIColumnPlan = {
    val responseType = if (parsed && AIRequestAdapter.GenerativeApis(getApiType)) {
      AIValueCodec.nullable(getParser.outputSchema)
    } else AIResponseCodec.outputType(getApiType)
    val outputs = Seq(StructField(getOutputCol, responseType), StructField(getErrorCol, ErrorUtils.ErrorSchema)) ++
      get(usageCol).map(StructField(_, UsageUtils.UsageStructType)) ++
      get(rawResponseCol).map(StructField(_, StringType)) ++
      (if (responseIdEnabled) Seq(StructField(getResponseIdCol, StringType)) else Seq.empty) ++
      retainedMessagesName(schema).map(StructField(_, AIPrompt.MessageType))
    AIColumnPlan(schema, outputs)
  }

  private[ai] def inferenceOutputSchema(schema: StructType): StructType = plan(schema, parsed = true).schema

  private[ai] def inferenceSchema(schema: StructType): StructType = {
    AIValueCodec.validateMapKeys(schema)
    val result = inferenceOutputSchema(schema)
    new AIRequestAdapter(this, schema).validate()
    result
  }

  override def transformSchema(schema: StructType): StructType = {
    val stage = effectiveCopy()
    val result = stage.inferenceSchema(schema)
    if (stage.isDefined(outputPath)) {
      AIPromptOutput.validateSettings(stage)
      AIPromptOutput.storageSchema(schema)
      stage.validateDurableInput(schema, SparkSession.active)
      AIPromptOutput.storageSchema(result)
    } else result
  }

  override def transform(dataset: Dataset[_]): DataFrame = AIPrompt.withSession(dataset.sparkSession) {
    logTransform({
      val stage = effectiveCopy()
      stage.inferenceSchema(dataset.schema)
      if (stage.isDefined(outputPath)) AIPromptOutput.transformEffective(stage, dataset.toDF())
      else {
        stage.validateFabricModel(dataset.schema)
        stage.infer(dataset)
      }
    }, dataset.columns.length)
  }

  private def error(code: String, message: String, status: Option[Row] = None): Row =
    Row(JsObject("code" -> JsString(code), "message" -> JsString(message)).compactPrint, status.orNull)

  private def attachmentColumns(schema: StructType): Seq[String] =
    getColumnTypes.toSeq.collect { case (name, "path") => name }
      .sortBy(name => (schema.fieldIndex(AIColumnPlan.resolve(schema, name)), name))

  private def generatedMessages(row: Row, prompt: String, indices: Map[String, Int], paths: Seq[String],
                                conf: Option[Configuration]): Seq[Row] = {
    val attachments = paths.map { name =>
      name -> row.getString(indices(name))
    }.toMap
    Option(createMessagesForRow(prompt, attachments, paths, conf)).map(_.map { message =>
      new GenericRowWithSchema(Array[Any](message.role, message.content, message.name.orNull),
        AIPrompt.MessageSchema): Row
    }).getOrElse(throw new AIMissingInputException("attachment input is missing"))
  }

  private def messages(row: Row, prompt: Option[String], schema: StructType,
                       indices: Map[String, Int], paths: Seq[String],
                       conf: Option[Configuration]): Option[Seq[Row]] = {
    if (isSet(messagesCol)) {
      val index = indices(getMessagesCol)
      val messageSchema = schema(index).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      Some(Option(row.get(index)).map(value => AIColumnPlan.arrayValues(value).iterator.map { message =>
        Option(message).map {
          case struct: Row => AIColumnPlan.withSchema(struct, messageSchema)
          case _ => throw new IllegalArgumentException("Expected a message struct")
        }.orNull
      }.toVector)
        .getOrElse(throw new AIMissingInputException("messages is null")))
    } else if (AIRequestAdapter.GenerativeApis(getApiType)) {
      val text = prompt.orElse(get(inputCol).flatMap(name =>
        Option(row.getString(indices(name)))))
        .getOrElse(throw new AIMissingInputException("prompt is null"))
      Some(generatedMessages(row, text, indices, paths, conf))
    } else None
  }

  private def buildRequest(row: Row, address: String, body: String, adapter: AIRequestAdapter,
                           toRow: HTTPRequestData => Row): Row = {
    val request = new HttpPost(address)
    def string(p: ServiceParam[String]): Option[String] = adapter.value(row, p).map(_.asInstanceOf[String])
    def map(p: ServiceParam[Map[String, String]]): Option[Map[String, String]] =
      adapter.value(row, p).map(_.asInstanceOf[scala.collection.Map[String, String]].toMap)
    AIPrompt.requestHeaders(this, address, string, map, includeFabricFallback = true)
      .foreach { case (name, value) =>
        AIPrompt.validateHeader(name, value)
        request.addHeader(name, value)
      }
    request.setEntity(new StringEntity(AIJson.validatedString(body), ContentType.APPLICATION_JSON))
    toRow(new HTTPRequestData(request))
  }

  private[ai] def fabricEndpoint: Option[String] =
    if (PlatformDetails.runningOnFabric()) Some(FabricClient.MLWorkloadEndpointOpenAI) else None

  private[ai] def fabricAuthHeader: String = FabricClient.getCognitiveMWCTokenAuthHeader

  private[ai] def usingImplicitFabricEndpoint(address: String): Boolean =
    AIRequestAdapter.ImplicitFabricApis(getApiType) &&
      !isSet(url) && !isSet(customUrlRoot) &&
      fabricEndpoint.exists(root => FabricClient.isEndpointUnder(address, root.stripSuffix("/") + "/"))

  private[ai] def assertFabricModelStatus(modelName: String): Unit =
    OpenAIFabricModelStatus.assertModelStatus(modelName)

  private[ai] def validateFabricModel(schema: StructType): Unit =
    new AIRequestAdapter(this, schema).validateFabricModel()

  private def preparePrompt(input: DataFrame, promptName: String, hiddenColumns: Seq[String],
                            interpolateFilenames: Boolean = true): DataFrame = {
    if (!isSet(promptTemplate)) input
    else {
      val filenames = if (!interpolateFilenames) Map.empty[String, Column] else {
        val filename = F.udf((path: String) => Option(path).map(_.trim).filter(_.nonEmpty).map { p =>
          try OpenAIAttachmentUtils.attachmentFilename(p)
          catch { case _: IllegalArgumentException => "attachment" }
        }.getOrElse(""))
        attachmentColumns(input.schema).map { name =>
          name -> filename(AIColumnPlan.column(AIColumnPlan.resolve(input.schema, name)))
        }.toMap
      }
      val expression = AIPrompt.withSafeTemplate(templateWithFilenames(filenames))
      val publicInput = if (hiddenColumns.isEmpty) input else input.drop(hiddenColumns: _*)
      // Bind against public columns without allowing the template to filter or expand rows.
      val scoped = AIPrompt.bindTemplate(publicInput, expression)
      AIPrompt.withSafeTemplate(input.withColumn(promptName, scoped))
    }
  }

  private def templateWithFilenames(filenames: Map[String, Column]): Column = {
    val tokens = new PEP3101Parser(AIJson.validatedString(getPromptTemplate)).parse.map { token =>
      token.expr match {
        case reference: UnresolvedAttribute if reference.nameParts.length == 1 =>
          filenames.collectFirst {
            case (name, filename) if AIColumnPlan.matches(name, reference.nameParts.head) => filename
          }.getOrElse(token)
        case _ => token
      }
    }
    F.concat(tokens: _*)
  }

  private def prepareRequests(prepared: DataFrame, promptName: String, requestName: String,
                              preparationName: String, messagesName: String,
                              retainGeneratedMessages: Boolean): DataFrame = {
    val inputSchema = prepared.schema
    val promptIndex = if (isSet(promptTemplate)) Some(inputSchema.fieldIndex(promptName)) else None
    val names = getColumnTypes.keys.toSeq ++ get(inputCol) ++
      (if (isSet(messagesCol)) Seq(getMessagesCol) else Seq.empty)
    val indices = names.map(name => name -> inputSchema.fieldIndex(AIColumnPlan.resolve(inputSchema, name))).toMap
    val paths = attachmentColumns(inputSchema)
    val attachmentConf = if (paths.nonEmpty) {
      Some(new SerializableConfiguration(prepared.sparkSession.sessionState.newHadoopConf()))
    } else None
    val adapter = new AIRequestAdapter(this, inputSchema)
    val preparedSchema = inputSchema.add(requestName, HTTPSchema.Request)
      .add(preparationName, ErrorUtils.ErrorSchema).add(messagesName, AIPrompt.MessageType)
    prepared.mapPartitions { rows =>
      val toRequest = HTTPRequestData.makeToRowConverter
      val conf = attachmentConf.map(_.value)
      rows.map { row =>
        val prompt = promptIndex.flatMap(index => Option(row.getString(index)))
        try {
          val inputMessages = messages(row, prompt, inputSchema, indices, paths, conf)
          val (address, body) = adapter.request(row, prompt, inputMessages)
          Row.fromSeq(row.toSeq ++ Seq(buildRequest(row, address, body, adapter, toRequest),
            Option.empty[Row].orNull,
            if (retainGeneratedMessages) inputMessages.orNull else Option.empty[Seq[Row]].orNull))
        } catch {
          case _: OpenAIAttachmentUtils.CancelledAttachmentReadException =>
            throw new CancellationException("Attachment read canceled")
          case _: OpenAIAttachmentUtils.AttachmentReadException =>
            Row.fromSeq(row.toSeq ++ Seq(Option.empty[Row].orNull,
              error("input_io_error", "Unable to read input attachment"), Option.empty[Seq[Row]].orNull))
          case e: AIMissingInputException =>
            Row.fromSeq(row.toSeq ++ Seq(Option.empty[Row].orNull, error("missing_input", e.getMessage),
              Option.empty[Seq[Row]].orNull))
          case e: IllegalArgumentException =>
            Row.fromSeq(row.toSeq ++ Seq(Option.empty[Row].orNull, error("invalid_request", e.getMessage),
              Option.empty[Seq[Row]].orNull))
        }
      }
    }(ExpressionEncoder(preparedSchema))
  }

  private def decodeResponse(response: HTTPResponseData, toStatus: StatusLineData => Row): Map[String, Any] = {
    val (raw, decoded) = try {
      val text = response.entity.map(e => StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(e.content)).toString).orNull
      text -> AIResponseCodec.decode(getApiType, text)
    } catch {
      case _: CharacterCodingException =>
        Option.empty[String].orNull -> AIResponseCodec.Decoded(None.orNull, None.orNull, None.orNull,
          Some("parse_error" -> "The provider response was not valid UTF-8"))
    }
    val exposed = get(rawResponseCol).map(_ -> raw).toMap[String, Any]
    val observations = exposed ++ get(usageCol).map(_ -> decoded.usage) ++
      (if (responseIdEnabled) Map(getResponseIdCol -> decoded.responseId) else Map.empty)
    if (response.statusLine.statusCode < 200 || response.statusLine.statusCode >= 300) {
      observations + (getErrorCol -> error("http_error", s"HTTP ${response.statusLine.statusCode}",
        Some(toStatus(response.statusLine))))
    } else {
      observations ++ Map(getOutputCol -> decoded.value) ++
        decoded.diagnostic.map { case (code, message) => getErrorCol -> error(code, message) }
    }
  }

  private def decodeRows(responseRows: DataFrame, outputPlan: AIColumnPlan, responseName: String,
                         preparationName: String, messagesName: String,
                         retainedMessages: Option[String]): DataFrame = {
    val responseIndex = responseRows.schema.fieldIndex(responseName)
    val prepIndex = responseRows.schema.fieldIndex(preparationName)
    val messagesIndex = responseRows.schema.fieldIndex(messagesName)
    responseRows.mapPartitions { rows =>
      val fromResponse = HTTPResponseData.makeFromRowConverter
      val toStatus = StatusLineData.makeToRowConverter
      rows.map { row =>
        val messages = retainedMessages.map(_ -> row.get(messagesIndex)).toMap
        val prepError = Option(row.getAs[Row](prepIndex))
        val values = if (prepError.isDefined) {
          Map(getErrorCol -> prepError.get)
        } else if (row.isNullAt(responseIndex)) {
          Map(getErrorCol -> error("transport_error", "No HTTP response was received"))
        } else {
          decodeResponse(fromResponse(row.getStruct(responseIndex)), toStatus)
        }
        outputPlan.assemble(row, messages ++ values)
      }
    }(ExpressionEncoder(outputPlan.schema))
  }

  private def postprocess(rawResult: DataFrame, outputPlan: AIColumnPlan): DataFrame = {
    if (AIRequestAdapter.GenerativeApis(getApiType) && getPostProcessing.nonEmpty) {
      val scratch = outputPlan.scratch(s"${uid}_unparsed")
      val parser = getParser
      val parsedType = AIValueCodec.nullable(parser.outputSchema)
      val parsed = rawResult.withColumn(scratch, AIColumnPlan.column(getOutputCol))
        .withColumn(getOutputCol, parser.parse(AIColumnPlan.column(scratch)).cast(parsedType))
      val invalid = parser match {
        case json: OpenAIJsonParser =>
          AIColumnPlan.column(getOutputCol).isNull || json.hasParseError(AIColumnPlan.column(scratch))
        case _ => AIColumnPlan.column(getOutputCol).isNull
      }
      val failure = F.struct(
        F.lit(error("parse_error", "Text does not match the configured postprocessing schema").getString(0))
          .as("response"),
        F.lit(Option.empty[String].orNull).cast(StatusLineData.schema).as("status"))
      parsed.withColumn(getErrorCol, F.when(AIColumnPlan.column(scratch).isNotNull &&
        invalid && AIColumnPlan.column(getErrorCol).isNull, failure)
        .otherwise(AIColumnPlan.column(getErrorCol))).drop(scratch)
    } else rawResult
  }

  private[ai] def infer(dataset: Dataset[_]): DataFrame = AIPrompt.withSession(dataset.sparkSession) {
    infer(dataset, AIPrompt.createHttpTransformer(this))
  }

  private[ai] def infer(dataset: Dataset[_], http: HTTPTransformer): DataFrame = {
    infer(dataset, Seq.empty, http)
  }

  private[ai] def infer(dataset: Dataset[_], hiddenColumns: Seq[String], http: HTTPTransformer): DataFrame = {
    AIPrompt.withSession(dataset.sparkSession) {
      val original = dataset.toDF()
      val outputPlan = plan(original.schema, parsed = false)
      val retainedMessages = retainedMessagesName(original.schema)
      val finalSchema = inferenceOutputSchema(original.schema)
      new AIRequestAdapter(this, original.drop(hiddenColumns: _*).schema).validate()
      val promptName = outputPlan.scratch(s"${uid}_prompt")
      val requestName = outputPlan.scratch(s"${uid}_request", promptName)
      val preparationName = outputPlan.scratch(s"${uid}_preparation", promptName, requestName)
      val messagesName = outputPlan.scratch(s"${uid}_generated_messages", promptName, requestName, preparationName)
      val responseName = outputPlan.scratch(s"${uid}_response", promptName, requestName, preparationName, messagesName)
      if (hiddenColumns.nonEmpty) validateDurableTemplate(original, promptName, hiddenColumns)
      val prepared = preparePrompt(original, promptName, hiddenColumns)
      // Preserve SQL map-key identity while retained input traverses the inference Row encoders.
      val protectedInput = AIValueCodec.encodeMapKeys(prepared)
      val retainedSchema = StructType(original.schema.fields.map(field => protectedInput.schema(field.name)))
      val protectedPlan = outputPlan.copy(input = retainedSchema)
      val requestRows = prepareRequests(protectedInput, promptName, requestName, preparationName, messagesName,
        retainedMessages.isDefined)
      http.setInputCol(requestName).setOutputCol(responseName)
      val responses = if (AIPrompt.usesBoundedHandler(this)) http.transformCancellable(requestRows)
        else http.transform(requestRows)
      val rawResult = decodeRows(responses, protectedPlan, responseName,
        preparationName, messagesName, retainedMessages)
      val processed = postprocess(rawResult, outputPlan)
      AIValueCodec.reconcileSchema(AIValueCodec.decodeMapKeys(processed, finalSchema), finalSchema)
    }
  }

  private[ai] def validateDurableInput(schema: StructType, session: => SparkSession): Unit = {
    AIDurableInput.validate(this, schema)
    if (isSet(promptTemplate)) {
      val input = session.createDataFrame(Collections.emptyList[Row](), schema)
      val outputPlan = plan(input.schema, parsed = false)
      validateDurableTemplate(input, outputPlan.scratch(s"${uid}_prompt"), Seq.empty)
    }
  }

  private def validateDurableTemplate(input: DataFrame, promptName: String,
                                      hiddenColumns: Seq[String]): Unit = if (isSet(promptTemplate)) {
    // Validate user expressions before inserting SynapseML's stable filename UDF.
    val prepared = preparePrompt(input, promptName, hiddenColumns, interpolateFilenames = false)
    val analyzed = AIPrompt.withSafeTemplate(prepared.queryExecution.analyzed)
    require(!analyzed.exists {
      case _: Window | _: Aggregate | _: Generate => true
      case _ => false
    }, "Durable prompt templates require row-local expressions; materialize windows, aggregates and generators " +
      "into input columns")
    // Input expressions are snapshotted once; inspect only the template projection for unstable functions.
    val expressions = analyzed.expressions
    require(!expressions.exists(SubqueryExpression.hasSubquery),
      "Durable prompt templates cannot contain SQL subqueries; materialize external values into input columns")
    val unstable = expressions.exists(_.exists {
      // Spark's deterministic flag does not account for AES's generated initialization vector.
      case _: UserDefinedExpression | _: AesEncrypt => true
      case expression if !expression.deterministic => true
      case _: Attribute | _: Literal | _: NamedLambdaVariable | _: Pi | _: EulerNumber => false
      case _: LeafExpression => true
      case _ => false
    })
    require(!unstable, "Durable prompt templates require stable built-in expressions over input columns; " +
      "materialize UDFs, encryption and runtime-dependent values into input columns")
  }

  private[ai] def validateDurableRows(snapshot: DataFrame): Unit = {
    if (isSet(promptTemplate)) {
      val outputPlan = plan(snapshot.schema, parsed = false)
      val promptName = outputPlan.scratch(s"${uid}_prompt")
      validateDurableTemplate(snapshot, promptName, Seq.empty)
      val prepared = preparePrompt(snapshot, promptName, Seq.empty)
      AIDurableInput.validateRows(this, prepared, Some(promptName))
    } else AIDurableInput.validateRows(this, snapshot)
  }

  override protected def pyParamSetter(p: Param[_]): String =
    AIPromptPython.paramSetter(super.pyParamSetter(p), p.name)

  override protected val pyHasExplicitPostProcessing: String = "self.isSet(self.postProcessing)"

  override protected def pythonClass(): String = AIPromptPython.nativeReader(super.pythonClass())

  override def pyAdditionalMethods: String = AIPromptPython.additionalMethods(super.pyAdditionalMethods, pyClassName)

  override def pyInitFunc(): String = AIPromptPython.initFunc(super.pyInitFunc())

  override protected def rParamArg[T](p: Param[T]): String = p match {
    case _: ServiceParam[_] => super.rParamArg(p)
    case _ => s"${p.name}=NULL"
  }
}
