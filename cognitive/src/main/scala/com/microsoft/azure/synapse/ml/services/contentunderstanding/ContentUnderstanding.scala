// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.codegen.CodegenConfig
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.{
  HTTPOutputParser, HTTPRequestData, HTTPResponseData, HTTPSchema, SimpleHTTPTransformer}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{
  CognitiveServicesBaseNoHandler, HasAPIVersion, HasCognitiveServiceInput, HasInternalJsonOutputParser,
  ServiceAuthHeaders}
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda}
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpPut, HttpRequestBase}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.TaskContext
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, NamespaceInjections, PipelineModel}
import org.apache.spark.sql.functions.{coalesce, col, lit, struct, udf, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json._

import java.util.Base64
import scala.util.control.NonFatal

object ContentUnderstanding extends ComplexParamsReadable[ContentUnderstanding]

/** Lazy, one-document-per-row Content Understanding analysis with resumable operation handles.
  *
  * Service failures are retained in the output struct and errorCol. A Running or NotStarted
  * response is not a completed result, even when the service has included an empty result object.
  *
  * transform and getInternalTransformer deliberately build a snapshot-bound pipeline instead of the
  * base pipeline. Its partition handler owns request settings and shared-stop state; the inherited
  * handlingFunc is unused and fails closed to prevent execution outside that snapshot.
  */
class ContentUnderstanding(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasAPIVersion
  with ContentUnderstandingParams with ContentUnderstandingPython {

  import ContentUnderstandingProtocol._

  private[contentunderstanding] type ColumnResolver = (String, String) => Boolean

  logClass(FeatureNames.AiServices.ContentUnderstanding)
  setDefault(apiVersion -> Left(DefaultApiVersion))

  def this() = this(Identifiable.randomUID("ContentUnderstanding"))

  override def urlPath: String = AnalyzersPath.stripPrefix("/")

  override def setEndpoint(value: String): this.type = setUrl(endpointUrl(value))

  override def copy(extra: ParamMap): ContentUnderstanding = defaultCopy(extra)

  override protected def saveDefaultParamsAsExplicit: Boolean = false

  override def transform(dataset: Dataset[_]): DataFrame = {
    transferGlobalParamsToParamMap()
    val resolver = DatasetExtensions.resolverFor(dataset)
    logTransform[DataFrame](buildInternalTransformer(dataset.schema, resolver).transform(dataset),
      dataset.columns.length)
  }

  // OpenAI global URL/version/key settings and Fabric MWC tokens are not credentials for this resource.
  override private[ml] def transferGlobalParamsToParamMap(): Unit = ()

  override protected def getFabricFallbackAuthHeader(row: Row): Option[String] = None

  override protected def responseDataType: DataType =
    ContentUnderstandingOutputParser.ResponseSchema

  private def authParams: Seq[ServiceParam[_]] =
    Seq(subscriptionKey, AADToken, CustomAuthHeader, customHeaders, telemHeaders)

  private def activeParams(poll: Boolean): Seq[ServiceParam[_]] = {
    val inputParams = if (poll) {
      Seq(operationLocation)
    } else {
      Seq(analyzerId, apiVersion, documentUrl, documentBytes, range, mimeType, documentName,
        modelDeployments, stringEncoding, processingLocation, allowInputTruncation)
    }
    authParams ++ inputParams
  }

  override protected def getVectorParamMap: Map[String, String] = {
    val names = activeParams(getOperationMode == "poll").map(_.name).toSet
    super.getVectorParamMap.filter { case (name, _) => names(name) }
  }

  private def configured(param: ServiceParam[_]): Boolean = get(param).orElse(getDefault(param)).isDefined

  private def sameColumnName(left: String,
                             right: String,
                             resolver: ColumnResolver): Boolean = resolver(left, right)

  private def validateConfiguration(resolver: ColumnResolver): Unit = {
    require(isSet(url), "Content Understanding requires an explicit endpoint. Call setEndpoint(resourceRoot).")
    validateEndpoint(getUrl)
    require(!isSet(customUrlRoot), "customUrlRoot is not supported; use setEndpoint.")
    require(getConcurrency > 0, "concurrency must be positive.")
    val maxTimeout = Int.MaxValue.toDouble / MillisecondsPerSecond
    require(java.lang.Double.isFinite(getTimeout) && getTimeout > 0 && getTimeout <= maxTimeout,
      "timeout must be positive, finite, and representable in milliseconds.")
    // A shared async wait starts before queued operations run, so it cannot preserve per-operation handles.
    require(get(concurrentTimeout).isEmpty,
      "concurrentTimeout is not supported; maxOperationDuration bounds each operation, " +
        "excluding opted-in analysis 429 retries.")
    require(Option(getOutputCol).exists(_.nonEmpty) && Option(getErrorCol).exists(_.nonEmpty) &&
      !sameColumnName(getOutputCol, getErrorCol, resolver),
      "outputCol and errorCol must be nonempty and distinct.")
  }

  private def settings(resolver: ColumnResolver = SQLConf.get.resolver): Settings = {
    validateConfiguration(resolver)
    def scalarValue[T](param: ServiceParam[T]): Option[T] =
      get(param).orElse(getDefault(param)).flatMap(_.left.toOption)
    val scalarAuthHeaders = ServiceAuthHeaders.resolve(
      scalarValue(subscriptionKey),
      subscriptionKeyHeaderName,
      aadHeaderName,
      scalarValue(AADToken),
      scalarValue(CustomAuthHeader),
      scalarValue(customHeaders),
      None,
      scalarValue(telemHeaders),
      None).headers
      .filter { case (name, _) =>
        Set("authorization", "ocp-apim-subscription-key")(name.toLowerCase(java.util.Locale.ROOT))
      }
    Settings(getUrl, math.ceil(getTimeout * MillisecondsPerSecond).toInt,
      getMaxPollAttempts, getPollingDelay, getMaxResponseBytes, getMaxOperationDuration,
      sharedAnalyzer = getOperationMode != "poll" && isScalar(analyzerId) && isScalar(apiVersion),
      sharedModels = getOperationMode != "poll" && isScalar(analyzerId) && isScalar(modelDeployments) &&
        isScalar(apiVersion),
      scalarAuthFingerprints = scalarAuthHeaders.map { case (name, value) =>
        FailureClassifier.authHeaderFingerprint(name, value)
      }.toSet,
      allAuthScalar = authParams.forall(isScalar),
      retryThrottledRequests = getRetryThrottledRequests)
  }

  private def isScalar(param: ServiceParam[_]): Boolean =
    get(param).orElse(getDefault(param)).forall(_.isLeft)

  private def isColumn(param: ServiceParam[_]): Boolean =
    get(param).orElse(getDefault(param)).exists(_.isRight)

  private def rowUsesColumnAuth(row: Row): Boolean = {
    val explicitCredential = Seq(subscriptionKey, AADToken, CustomAuthHeader)
      .find(param => getValueOpt(row, param).exists(ServiceAuthHeaders.nonBlank))
    explicitCredential.map(isColumn).getOrElse {
      val embeddedCredential = getValueOpt(row, customHeaders).exists(_.exists { case (name, value) =>
        (name.equalsIgnoreCase(subscriptionKeyHeaderName) || name.equalsIgnoreCase(aadHeaderName)) &&
          ServiceAuthHeaders.nonBlank(value)
      })
      embeddedCredential && isColumn(customHeaders)
    }
  }

  private def expectedType(param: ServiceParam[_]): DataType = {
    if (param == documentBytes) {
      BinaryType
    } else if (param == allowInputTruncation) {
      BooleanType
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
      case (_: Boolean, BooleanType) => true
      case (values: Map[_, _], _: MapType) =>
        values.forall { case (key, item) => key.isInstanceOf[String] && item.isInstanceOf[String] }
      case (_: String, StringType) => true
      case _ => false
    }
    require(validType, s"${param.name} has an invalid value type.")
    if (authParams.contains(param)) {
      ContentUnderstandingHeaderValidation.validateSetting(param.name, value)
    } else {
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
      case bytes: Array[Byte] =>
        require(bytes.nonEmpty, "documentBytes cannot be empty.")
        require(bytes.length <= getMaxDocumentBytes, "documentBytes exceeds maxDocumentBytes.")
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

  private def validateParamSchema(param: ServiceParam[_], schema: StructType, resolver: ColumnResolver): Unit = {
    get(param).orElse(getDefault(param)).foreach {
      case Left(value) => validateValue(param, value)
      case Right(name) =>
        require(Option(name).exists(_.nonEmpty), s"${param.name} column name cannot be empty.")
        val field = schema.fields.find(_.name == name)
        require(field.isDefined, s"The column configured for ${param.name} is missing.")
        require(schema.fields.count(field => resolver(field.name, name)) == 1,
          s"The column configured for ${param.name} is ambiguous.")
        require(matchesType(field.get.dataType, expectedType(param)),
          s"The column configured for ${param.name} must have type ${expectedType(param).simpleString}.")
    }
  }

  private[contentunderstanding] def validateInputSchema(
      schema: StructType,
      resolver: ColumnResolver = SQLConf.get.resolver): Unit = {
    validateConfiguration(resolver)
    val poll = getOperationMode == "poll"
    if (poll) {
      require(configured(operationLocation), "poll mode requires operationLocation or operationLocationCol.")
    } else {
      require(configured(documentUrl) != configured(documentBytes),
        "Configure exactly one of documentUrl and documentBytes, using a scalar value or a column.")
    }
    activeParams(poll).foreach(validateParamSchema(_, schema, resolver))
  }

  // Scalar configuration is validated on the driver; only column-sourced values are checked per row.
  private def validateRow(row: Row, poll: Boolean): Unit = {
    activeParams(poll).foreach { param =>
      if (isColumn(param)) {
        getValueAnyOpt(row, param).foreach(validateValue(param, _))
      }
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

  private def rowError(row: Row): Option[String] = {
    try {
      validateRow(row, getOperationMode == "poll")
      None
    } catch {
      case error: IllegalArgumentException => Some(diagnostic("InvalidInput", error.getMessage))
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
        getValueOpt(row, param).map(value => param.payloadName -> value)) ++
      getValueOpt(row, allowInputTruncation)
        .map(value => allowInputTruncation.payloadName -> allowInputTruncation.toValueString(value))
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
    addHeaders(request, row, addContentType = !poll)
    if (rowUsesColumnAuth(row)) {
      // Reuse the wire-stripped internal marker channel for CU provenance, not Fabric authentication.
      request.addHeader(HTTPRequestData.FabricAuthMarkerHeader, RowAuthColumnMarker)
    }
    request
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] =
    row => Option(row).map(prepareRequest(_, getOperationMode == "poll"))

  override protected def handlingFunc(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData =
    throw new UnsupportedOperationException(
      "ContentUnderstanding uses the snapshot-bound partition handler in buildInternalTransformer.")

  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"

  private def addServiceErrors(frame: DataFrame, outputColumn: String, errorColumn: String): DataFrame = {
    val output = col(quoted(outputColumn))
    val serviceError = struct(output.getField("error").alias("response"),
      struct(struct(lit("HTTP").alias("protocol"), lit(1).alias("major"), lit(1).alias("minor"))
        .alias("protocolVersion"), coalesce(output.getField("httpStatus"), lit(0)).alias("statusCode"),
        output.getField("status").alias("reasonPhrase")).alias("status"))
    frame.withColumn(errorColumn,
      when(output.getField("error").isNotNull, serviceError).otherwise(col(quoted(errorColumn))))
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel =
    buildInternalTransformer(schema, SQLConf.get.resolver)

  private def inputTransformer(schema: StructType, inputColumn: String, validationColumn: String): Lambda = {
    val names = getVectorParamMap.values.toSeq.distinct
    val columns = names.map(name => col(quoted(name)).alias(name))
    val inputs = if (columns.nonEmpty) columns else Seq(lit(false).alias("placeholder"))
    val fields = if (names.nonEmpty) names.map(schema(_))
      else Seq(StructField("placeholder", BooleanType, nullable = false))
    val inputType = StructType(fields)
    val validate = udf((row: Row) => rowError(row))
    Lambda(_.withColumn(inputColumn, struct(inputs: _*))
      .withColumn(validationColumn, validate(col(inputColumn)))
      .withColumn(inputColumn, when(col(validationColumn).isNull, col(inputColumn))))
      .setTransformSchema((input: StructType) =>
        input.add(inputColumn, inputType, nullable = true).add(validationColumn, StringType, nullable = true))
  }

  private def outputTransformer(resultColumn: String, errorColumn: String, validationColumn: String): Lambda = {
    val invalidResponse = struct(lit(None.orNull).cast(StringType).alias("operationLocation"),
      lit(None.orNull).cast(StringType).alias("id"), lit("Failed").alias("status"),
      lit(0).alias("httpStatus"), lit("").alias("rawResponse"), col(validationColumn).alias("error"))
    Lambda(frame => {
      val result = frame.withColumn(resultColumn,
        when(col(validationColumn).isNotNull, invalidResponse).otherwise(col(quoted(resultColumn))))
      addServiceErrors(result, resultColumn, errorColumn)
        .withColumnRenamed(resultColumn, getOutputCol)
        .withColumnRenamed(errorColumn, getErrorCol)
    }).setTransformSchema((input: StructType) => StructType(input.fields.map { field =>
      if (field.name == resultColumn) field.copy(name = getOutputCol)
      else if (field.name == errorColumn) field.copy(name = getErrorCol)
      else field
    }))
  }

  private def buildInternalTransformer(schema: StructType, resolver: ColumnResolver): PipelineModel = {
    val snapshot = copy(ParamMap.empty)
    snapshot.validateInputSchema(schema, resolver)
    require(!schema.fieldNames.exists(name => snapshot.sameColumnName(name, snapshot.getOutputCol, resolver)) &&
      !schema.fieldNames.exists(name => snapshot.sameColumnName(name, snapshot.getErrorCol, resolver)),
      "outputCol and errorCol must not overwrite input columns.")
    val reserved = schema.fieldNames.toSet ++ Set(snapshot.getOutputCol, snapshot.getErrorCol)
    val internalResolver: ColumnResolver = (left, right) => left.equalsIgnoreCase(right)
    val inputColumn = DatasetExtensions.findUnusedColumnName("contentUnderstandingInput", reserved, internalResolver)
    val resultColumn = DatasetExtensions.findUnusedColumnName(
      "contentUnderstandingResult", reserved + inputColumn, internalResolver)
    val errorColumn = DatasetExtensions.findUnusedColumnName(
      "contentUnderstandingError", reserved ++ Set(inputColumn, resultColumn), internalResolver)
    val validationColumn = DatasetExtensions.findUnusedColumnName(
      "contentUnderstandingValidation", reserved ++ Set(inputColumn, resultColumn, errorColumn), internalResolver)
    NamespaceInjections.pipelineModel(Array(
      snapshot.inputTransformer(schema, inputColumn, validationColumn),
      new SimpleHTTPTransformer()
        .setInputCol(inputColumn)
        .setOutputCol(resultColumn)
        .setErrorCol(errorColumn)
        .setInputParser(snapshot.getInternalInputParser(schema))
        .setOutputParser(snapshot.getInternalOutputParser(schema))
        .setHandler(rowHandler(snapshot.settings(resolver), snapshot.getOperationMode))
        .setConcurrency(snapshot.getConcurrency)
        .setTimeout(snapshot.getTimeout),
      snapshot.outputTransformer(resultColumn, errorColumn, validationColumn),
      new DropColumns().setCols(Array(inputColumn, validationColumn))))
  }

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser =
    new ContentUnderstandingOutputParser()

  private def validateManagement(): Unit = {
    require(Option(TaskContext.get()).isEmpty, "Analyzer management is driver-only.")
    validateConfiguration(SQLConf.get.resolver)
    (authParams ++ Seq(analyzerId, apiVersion)).foreach { param =>
      require(get(param).orElse(getDefault(param)).forall(_.isLeft),
        s"Analyzer management requires scalar ${param.name}, not a column.")
      getValueAnyOpt(Row.empty, param).foreach(validateValue(param, _))
    }
  }

  private def analyzerRequest: HTTPRequestData = {
    val request = new HttpGet(buildUrl(s"$AnalyzersPath/$getAnalyzerId", Seq("api-version" -> getApiVersion)))
    addHeaders(request, Row.empty, addContentType = false)
    new HTTPRequestData(request)
  }

  private def logManagement[T](verb: String)(body: => T): T = {
    val start = System.nanoTime()
    def record(outcome: String, errorType: Option[String]): Unit = ContentUnderstandingLogging.safely {
      // Service and validation exceptions can contain user data; never log their messages or causes.
      val payload = getPayload(verb, None, Some((System.nanoTime() - start) / 1e9), None) +
        ("outcome" -> outcome) ++ errorType.map("errorType" -> _).toMap
      logBase(payload, None)
    }
    try {
      val result = body
      record("succeeded", None)
      result
    } catch {
      case error: InterruptedException =>
        record("failed", Some(error.getClass.getName))
        throw error
      case NonFatal(error) =>
        record("failed", Some(error.getClass.getName))
        throw error
    }
  }

  /** Provision a custom analyzer explicitly. Resource defaults are never changed.
    * Throws ContentUnderstandingException with the response on service failure or poll exhaustion.
    */
  def createAnalyzer(definitionJson: String, allowReplace: Boolean): String = logManagement("createAnalyzer") {
    validateManagement()
    require(isSet(analyzerId), "createAnalyzer requires an explicit analyzerId.")
    require(Option(definitionJson).exists(_.trim.nonEmpty), "definitionJson must be a nonempty analyzer JSON object.")
    require(ContentUnderstandingResponseParsing.asObject(definitionJson).isDefined,
      "definitionJson must be a valid analyzer JSON object within the JSON depth and numeric-token limits.")
    val request = new HttpPut(buildUrl(s"$AnalyzersPath/$getAnalyzerId",
      Seq("api-version" -> getApiVersion, "allowReplace" -> allowReplace.toString)))
    request.setEntity(new StringEntity(definitionJson, ContentType.APPLICATION_JSON))
    addHeaders(request, Row.empty)
    ContentUnderstandingProtocol.createAnalyzer(
      settings(), new HTTPRequestData(request), analyzerRequest, getAnalyzerId)
  }

  def getAnalyzer(): String = logManagement("getAnalyzer") {
    validateManagement()
    ContentUnderstandingProtocol.getAnalyzer(settings(), analyzerRequest)
  }
}

private[contentunderstanding] object ContentUnderstandingOutputParser
  extends ComplexParamsReadable[ContentUnderstandingOutputParser] {
  val ResponseSchema: StructType =
    StructType(ContentUnderstandingResponse.schema.fields.map(_.copy(nullable = true)))
}

private[contentunderstanding] class ContentUnderstandingOutputParser(override val uid: String)
  extends HTTPOutputParser with ComplexParamsWritable with SynapseMLLogging {

  logClass(FeatureNames.Core)

  override protected lazy val pyInternalWrapper = true
  override protected lazy val rInternalWrapper = true

  // Internal parsers have no public R function to generate.
  override def makeRFile(conf: CodegenConfig): Unit = ()

  def this() = this(Identifiable.randomUID("ContentUnderstandingOutputParser"))

  override def transform(dataset: Dataset[_]): DataFrame = logTransform[DataFrame]({
    val parseResponse = udf(new UDF1[String, Row] {
      override def call(body: String): Row = Option(body).map { text =>
        val response = ContentUnderstandingResponse.JsonFormat.read(text.parseJson)
        Row(response.operationLocation.orNull, response.id.orNull, response.status,
          response.httpStatus, response.rawResponse, response.error.orNull)
      }.orNull
    }, ContentUnderstandingOutputParser.ResponseSchema)
    dataset.toDF().withColumn(getOutputCol,
      parseResponse(HTTPSchema.entity_to_string(col(s"${getInputCol}.entity"))))
  }, dataset.columns.length)

  override def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == HTTPSchema.Response)
    schema.add(getOutputCol, ContentUnderstandingOutputParser.ResponseSchema)
  }
}
