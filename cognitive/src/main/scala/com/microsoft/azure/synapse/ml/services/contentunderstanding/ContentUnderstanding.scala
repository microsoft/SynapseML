// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData, HTTPSchema, SimpleHTTPTransformer}
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{
  CognitiveServicesBaseNoHandler, HasAPIVersion, HasCognitiveServiceInput, HasInternalJsonOutputParser}
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda}
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpPut, HttpRequestBase}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.TaskContext
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel}
import org.apache.spark.sql.functions.{coalesce, col, lit, struct, when}
import org.apache.spark.sql.types.{BinaryType, DataType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import spray.json._

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

object ContentUnderstanding extends ComplexParamsReadable[ContentUnderstanding]

/** Lazy, one-document-per-row Content Understanding analysis with resumable operation handles.
  *
  * Service failures are retained in the output struct and errorCol. A Running or NotStarted
  * response is not a completed result, even when the service has included an empty result object.
  */
class ContentUnderstanding(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasAPIVersion
  with ContentUnderstandingParams with ContentUnderstandingPersistence with ContentUnderstandingPython {

  import ContentUnderstandingProtocol._

  logClass(FeatureNames.AiServices.Form)
  setDefault(apiVersion -> Left(DefaultApiVersion))

  def this() = this(Identifiable.randomUID("ContentUnderstanding"))

  override def urlPath: String = AnalyzersPath.stripPrefix("/")

  override def setEndpoint(value: String): this.type = setUrl(endpointUrl(value))

  override def copy(extra: ParamMap): ContentUnderstanding = defaultCopy(extra)

  // OpenAI global URL/version/key settings and Fabric MWC tokens are not credentials for this resource.
  override private[ml] def transferGlobalParamsToParamMap(): Unit = ()

  override protected def getFabricFallbackAuthHeader(row: Row): Option[String] = None

  override protected def responseDataType: DataType =
    StructType(ContentUnderstandingResponse.schema.fields.map(_.copy(nullable = true)))

  private def authParams: Seq[ServiceParam[_]] =
    Seq(subscriptionKey, AADToken, CustomAuthHeader, customHeaders, telemHeaders)

  private def activeParams(poll: Boolean): Seq[ServiceParam[_]] = {
    val inputParams = if (poll) {
      Seq(operationLocation)
    } else {
      Seq(analyzerId, apiVersion, documentUrl, documentBytes, range, mimeType, documentName,
        modelDeployments, stringEncoding, processingLocation)
    }
    authParams ++ inputParams
  }

  override protected def getVectorParamMap: Map[String, String] = {
    val names = activeParams(getOperationMode == "poll").map(_.name).toSet
    super.getVectorParamMap.filter { case (name, _) => names(name) }
  }

  private def configured(param: ServiceParam[_]): Boolean = get(param).orElse(getDefault(param)).isDefined

  private def validateConfiguration(): Unit = {
    require(isSet(url), "Content Understanding requires an explicit endpoint. Call setEndpoint(resourceRoot).")
    validateEndpoint(getUrl)
    require(!isSet(customUrlRoot), "customUrlRoot is not supported; use setEndpoint.")
    require(getConcurrency > 0, "concurrency must be positive.")
    val maxTimeout = Int.MaxValue.toDouble / MillisecondsPerSecond
    require(java.lang.Double.isFinite(getTimeout) && getTimeout > 0 && getTimeout <= maxTimeout,
      "timeout must be positive, finite, and representable in milliseconds.")
    get(concurrentTimeout).foreach(value =>
      require(java.lang.Double.isFinite(value) && value > 0, "concurrentTimeout must be positive and finite."))
    require(Option(getOutputCol).exists(_.nonEmpty) && Option(getErrorCol).exists(_.nonEmpty) &&
      getOutputCol != getErrorCol,
      "outputCol and errorCol must be nonempty and distinct.")
  }

  private def settings: Settings = {
    validateConfiguration()
    Settings(getUrl, math.ceil(getTimeout * MillisecondsPerSecond).toInt,
      getMaxPollAttempts, getPollingDelay, getMaxResponseBytes)
  }

  private def expectedType(param: ServiceParam[_]): DataType = {
    if (param == documentBytes) {
      BinaryType
    } else if (Set(modelDeployments.name, customHeaders.name, telemHeaders.name)(param.name)) {
      MapType(StringType, StringType)
    } else {
      StringType
    }
  }

  private def matchesType(actual: DataType, expected: DataType): Boolean = (actual, expected) match {
    case (MapType(StringType, StringType, _), MapType(StringType, StringType, _)) => true
    case _ => actual == expected
  }

  private def validateValue(param: ServiceParam[_], value: Any): Unit = {
    require(Option(value).isDefined, s"${param.name} cannot be null.")
    val validType = (value, expectedType(param)) match {
      case (_: Array[Byte], BinaryType) => true
      case (values: Map[_, _], _: MapType) =>
        values.forall { case (key, item) => key.isInstanceOf[String] && item.isInstanceOf[String] }
      case (_: String, StringType) => true
      case _ => false
    }
    require(validType, s"${param.name} has an invalid value type.")
    if (!authParams.contains(param)) {
      validateInputValue(param, value)
    }
  }

  private def validateInputValue(param: ServiceParam[_], value: Any): Unit = {
    validateNonemptyValue(param, value)
    validateOptionValue(param, value)
  }

  private def validateNonemptyValue(param: ServiceParam[_], value: Any): Unit = {
    value match {
      case text: String => require(text.trim.nonEmpty, s"${param.name} cannot be empty.")
      case bytes: Array[Byte] => require(bytes.nonEmpty, "documentBytes cannot be empty.")
      case values: Map[_, _] =>
        require(values.forall { case (key, item) => key.toString.trim.nonEmpty && item.toString.trim.nonEmpty },
          s"${param.name} keys and values cannot be empty.")
      case _ =>
    }
  }

  private def validateDocumentUrl(value: String): Unit = {
    val uri = parseUri(value, "documentUrl")
    require(Set("http", "https")(Option(uri.getScheme).getOrElse("").toLowerCase(java.util.Locale.ROOT)) &&
      Option(uri.getHost).isDefined && Option(uri.getRawUserInfo).isEmpty && Option(uri.getRawFragment).isEmpty,
      "documentUrl must be an absolute HTTP(S) URL without user information or a fragment.")
  }

  private def validateOptionValue(param: ServiceParam[_], value: Any): Unit = {
    param.name match {
      case "analyzerId" => validateAnalyzerId(value.asInstanceOf[String])
      case "apiVersion" =>
        require(validApiVersion(value.asInstanceOf[String]), "apiVersion must be YYYY-MM-DD or YYYY-MM-DD-preview.")
      case "documentUrl" => validateDocumentUrl(value.asInstanceOf[String])
      case "operationLocation" => validateOperationLocation(getUrl, value.asInstanceOf[String])
      case "stringEncoding" =>
        require(Set("codePoint", "utf16", "utf8")(value.asInstanceOf[String]), "Unsupported stringEncoding.")
      case "processingLocation" =>
        require(Set("geography", "dataZone", "global")(value.asInstanceOf[String]), "Unsupported processingLocation.")
      case _ =>
    }
  }

  private def validateParamSchema(param: ServiceParam[_], schema: StructType): Unit = {
    get(param).orElse(getDefault(param)).foreach {
      case Left(value) => validateValue(param, value)
      case Right(name) =>
        require(Option(name).exists(_.nonEmpty), s"${param.name} column name cannot be empty.")
        val field = schema.fields.find(_.name == name)
        require(field.isDefined, s"The column configured for ${param.name} is missing.")
        require(matchesType(field.get.dataType, expectedType(param)),
          s"The column configured for ${param.name} must have type ${expectedType(param).simpleString}.")
    }
  }

  private[contentunderstanding] def validateInputSchema(schema: StructType): Unit = {
    validateConfiguration()
    val poll = getOperationMode == "poll"
    if (poll) {
      require(configured(operationLocation), "poll mode requires operationLocation or operationLocationCol.")
    } else {
      require(configured(documentUrl) != configured(documentBytes),
        "Configure exactly one of documentUrl and documentBytes, using a scalar value or a column.")
    }
    require(!schema.fieldNames.contains(getOutputCol) && !schema.fieldNames.contains(getErrorCol),
      "outputCol and errorCol must not overwrite input columns.")
    activeParams(poll).foreach(validateParamSchema(_, schema))
  }

  private def validateRow(row: Row, poll: Boolean): Unit = {
    validateConfiguration()
    activeParams(poll).foreach { param =>
      getValueAnyOpt(row, param).foreach(validateValue(param, _))
    }
    if (poll) {
      require(getValueOpt(row, operationLocation).isDefined, "operationLocation cannot be null.")
    } else {
      require(configured(documentUrl) != configured(documentBytes),
        "Configure exactly one of documentUrl and documentBytes.")
      require(getValueOpt(row, documentUrl).isDefined || getValueOpt(row, documentBytes).isDefined,
        "The selected document input cannot be null.")
      require(getValueOpt(row, analyzerId).isDefined && getValueOpt(row, apiVersion).isDefined,
        "analyzerId and apiVersion cannot be null.")
    }
  }

  private def requestBody(row: Row): String = {
    val source = getValueOpt(row, documentUrl).map(value => "url" -> JsString(value))
      .orElse(getValueOpt(row, documentBytes).map(value => "data" -> JsString(Base64.getEncoder.encodeToString(value))))
      .getOrElse(throw new IllegalArgumentException("Document input cannot be null."))
    val optionalInput = Seq(documentName -> "name", mimeType -> "mimeType", range -> "range")
      .flatMap { case (param, name) => getValueOpt(row, param).map(value => name -> JsString(value)) }
    val input = JsObject((optionalInput :+ source).toMap)
    val models = getValueOpt(row, modelDeployments).map(values =>
      "modelDeployments" -> JsObject(values.map { case (name, deployment) => name -> JsString(deployment) }))
    canonicalJson(JsObject(Map[String, JsValue]("inputs" -> JsArray(input)) ++ models))
  }

  private def buildUrl(path: String, query: Seq[(String, String)]): String = {
    val builder = new URIBuilder(getUrl).setPath(path)
    query.sortBy(_._1).foreach { case (name, value) => builder.setParameter(name, value) }
    builder.build().toString
  }

  private def analyzeUrl(row: Row): String = {
    val query = Seq("api-version" -> getValue(row, apiVersion)) ++
      Seq(stringEncoding, processingLocation).flatMap(param =>
        getValueOpt(row, param).map(value => param.payloadName -> value))
    buildUrl(s"$AnalyzersPath/${getValue(row, analyzerId)}:analyze", query)
  }

  override protected def prepareUrl: Row => String = analyzeUrl _

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = row =>
    Some(new StringEntity(requestBody(row), ContentType.APPLICATION_JSON))

  private def prepareRequest(row: Row, poll: Boolean): HttpRequestBase = {
    validateRow(row, poll)
    val request = if (poll) {
      new HttpGet(validateOperationLocation(getUrl, getValue(row, operationLocation)))
    } else {
      val post = new HttpPost(analyzeUrl(row))
      post.setEntity(new StringEntity(requestBody(row), ContentType.APPLICATION_JSON))
      post
    }
    addHeaders(request, row)
    request
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] =
    row => Some(prepareRequest(row, getOperationMode == "poll"))

  override protected def handlingFunc(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData = {
    // The inherited client has automatic retries. Use a no-retry client, still scheduled by the shared HTTP pipeline.
    val response = execute(settings, request, getOperationMode)
    // Keep service failures in the output rather than letting SimpleHTTPTransformer discard the response.
    HTTPSchema.stringToResponse(response.toJson.compactPrint, org.apache.http.HttpStatus.SC_OK, "OK")
  }

  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"

  private def addServiceErrors(frame: DataFrame): DataFrame = {
    val output = col(quoted(getOutputCol))
    val serviceError = struct(output.getField("error").alias("response"),
      struct(struct(lit("HTTP").alias("protocol"), lit(1).alias("major"), lit(1).alias("minor"))
        .alias("protocolVersion"), coalesce(output.getField("httpStatus"), lit(0)).alias("statusCode"),
        output.getField("status").alias("reasonPhrase")).alias("status"))
    frame.withColumn(getErrorCol,
      when(output.getField("error").isNotNull, serviceError).otherwise(col(quoted(getErrorCol))))
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    validateInputSchema(schema)
    val reserved = schema.fieldNames.toSet ++ Set(getOutputCol, getErrorCol)
    val inputColumn = DatasetExtensions.findUnusedColumnName("contentUnderstandingInput")(reserved)
    val resultColumn = DatasetExtensions.findUnusedColumnName("contentUnderstandingResult")(reserved + inputColumn)
    val errorColumn = DatasetExtensions.findUnusedColumnName("contentUnderstandingError")(
      reserved ++ Set(inputColumn, resultColumn))
    val columns = getVectorParamMap.values.toSeq.distinct.map(name => col(quoted(name)).alias(name))
    val inputs = if (columns.nonEmpty) columns else Seq(lit(false).alias("placeholder"))
    NamespaceInjections.pipelineModel(Array(
      Lambda(_.withColumn(inputColumn, struct(inputs: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(inputColumn)
        .setOutputCol(resultColumn)
        .setErrorCol(errorColumn)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc _)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setTimeout(getTimeout),
      Lambda(frame => addServiceErrors(frame.withColumnRenamed(resultColumn, getOutputCol)
        .withColumnRenamed(errorColumn, getErrorCol))),
      new DropColumns().setCol(inputColumn)))
  }

  private[contentunderstanding] def requestFingerprint(row: Row): String = {
    validateRow(row, poll = false)
    val bytes = (analyzeUrl(row) + "\n" + requestBody(row)).getBytes(StandardCharsets.UTF_8)
    MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 0xff}%02x").mkString
  }

  private[contentunderstanding] def submitOne(row: Row): ContentUnderstandingResponse =
    execute(settings, new HTTPRequestData(prepareRequest(row, poll = false)), "submit")

  private[contentunderstanding] def pollOne(row: Row, location: String): ContentUnderstandingResponse = {
    val config = settings
    val request = new HttpGet(validateOperationLocation(getUrl, location))
    authParams.foreach(param => getValueAnyOpt(row, param).foreach(validateValue(param, _)))
    addHeaders(request, row)
    execute(config, new HTTPRequestData(request), "poll", failOnClientError = true)
  }

  private def validateManagement(): Unit = {
    require(Option(TaskContext.get()).isEmpty, "Analyzer management is driver-only.")
    validateConfiguration()
    (authParams ++ Seq(analyzerId, apiVersion)).foreach { param =>
      require(get(param).orElse(getDefault(param)).forall(_.isLeft),
        s"Analyzer management requires scalar ${param.name}, not a column.")
      getValueAnyOpt(Row.empty, param).foreach(validateValue(param, _))
    }
  }

  private def analyzerRequest: HTTPRequestData = {
    val request = new HttpGet(buildUrl(s"$AnalyzersPath/$getAnalyzerId", Seq("api-version" -> getApiVersion)))
    addHeaders(request, Row.empty)
    new HTTPRequestData(request)
  }

  /** Provision a custom analyzer explicitly. Resource defaults are never changed.
    * Throws ContentUnderstandingException with the response on service failure or poll exhaustion.
    */
  def createAnalyzer(definitionJson: String, allowReplace: Boolean): String = {
    validateManagement()
    require(Option(definitionJson).exists(_.trim.nonEmpty), "definitionJson must be a nonempty analyzer JSON object.")
    val definition = try {
      definitionJson.parseJson
    } catch {
      case _: JsonParser.ParsingException => throw new IllegalArgumentException("definitionJson must be valid JSON.")
    }
    require(definition.isInstanceOf[JsObject], "definitionJson must be an analyzer JSON object.")
    val request = new HttpPut(buildUrl(s"$AnalyzersPath/$getAnalyzerId",
      Seq("api-version" -> getApiVersion, "allowReplace" -> allowReplace.toString)))
    request.setEntity(new StringEntity(definitionJson, ContentType.APPLICATION_JSON))
    addHeaders(request, Row.empty)
    ContentUnderstandingProtocol.createAnalyzer(settings, new HTTPRequestData(request), analyzerRequest, getAnalyzerId)
  }

  def getAnalyzer(): String = {
    validateManagement()
    ContentUnderstandingProtocol.getAnalyzer(settings, analyzerRequest)
  }
}
