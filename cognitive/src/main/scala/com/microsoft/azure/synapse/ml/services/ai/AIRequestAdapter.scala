// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.param.{GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIChatCompletion, OpenAIEndpointUtils,
  OpenAIResponses, ResponseFormatUtils}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import spray.json._

import java.net.{URI, URISyntaxException, URLDecoder, URLEncoder}
import java.util.Locale

private[ai] class AIMissingInputException(message: String) extends IllegalArgumentException(message)

private[ai] object AIRequestAdapter {
  val SupportedApis: Vector[String] = Vector("chat_completions", "responses", "systemone", "decisions", "embeddings")
  val GenerativeApis: Set[String] = Set("chat_completions", "responses")
  val NativeApis: Set[String] = Set("systemone", "decisions")
  val VersionSelectableApis: Set[String] = GenerativeApis + "decisions"
  val ImplicitFabricApis: Set[String] = GenerativeApis + "embeddings"
  val DefaultDatedApiVersion: String = "2025-04-01-preview"

  private val DeploymentRoute = "(?i).*/openai/deployments/([^/]+)/(?:chat/completions|embeddings)$".r
  private val DeploymentBase = "(?i).*/openai/deployments/[^/]+$".r
  private val AzureHostSuffixes = Seq(".azure.com", ".azure.us", ".azure.cn")
  private val MaxPort = 65535

  def endpointUri(stage: AIPrompt): URI = {
    val base = stage.get(stage.customUrlRoot).getOrElse {
      require(stage.isDefined(stage.url), "Configure an endpoint with setUrl or setCustomUrlRoot")
      OpenAIEndpointUtils.stripTrailingSlashes(stage.getUrl)
    }
    val parameter = if (stage.isSet(stage.customUrlRoot)) stage.customUrlRoot else stage.url
    val uri = try new URI(AIJson.validatedText(base, parameter.name)) catch {
      case _: URISyntaxException => throw new IllegalArgumentException("Endpoint must be a valid absolute HTTP(S) URL")
    }
    require(Set("http", "https")(Option(uri.getScheme).getOrElse("").toLowerCase(Locale.ROOT)),
      "Endpoint must be an absolute HTTP(S) URL")
    require(normalizedHost(uri).nonEmpty,
      "Endpoint must use a valid DNS host name or IP address and port; host names with underscores are not supported")
    require(uri.getPort == -1 || (uri.getPort >= 1 && uri.getPort <= MaxPort),
      "Endpoint port must be omitted or between 1 and 65535")
    require(Option(uri.getUserInfo).isEmpty && Option(uri.getFragment).isEmpty,
      "Endpoint must not contain credentials or a fragment")
    if (!stage.isSet(stage.customUrlRoot) && isOpenAIHost(normalizedHost(uri)) &&
      routePath(uri).isEmpty && Option(uri.getRawQuery).isEmpty) {
      new URI(base + "/v1")
    } else uri
  }

  def normalizedHost(uri: URI): String =
    Option(uri.getHost).getOrElse("").stripSuffix(".").toLowerCase(Locale.ROOT)

  def isOpenAIHost(host: String): Boolean =
    host == "api.openai.com" || host.endsWith(".api.openai.com")

  private def isAzureHost(host: String): Boolean = AzureHostSuffixes.exists(host.endsWith)

  private def routePath(uri: URI): String =
    OpenAIEndpointUtils.stripTrailingSlashes(Option(uri.getRawPath).getOrElse("")).toLowerCase(Locale.ROOT)

  def isV1(uri: URI): Boolean = {
    val path = routePath(uri)
    !DeploymentRoute.pattern.matcher(path).matches() && !DeploymentBase.pattern.matcher(path).matches() &&
      (OpenAIEndpointUtils.isV1BaseUrl(uri.toString) ||
        Seq("/v1/chat/completions", "/v1/responses", "/v1/embeddings", "/v1/decisions").exists(path.endsWith))
  }

  def usesDatedApiVersion(stage: AIPrompt): Boolean =
    !NativeApis(stage.getApiType) && !stage.isSet(stage.customUrlRoot) && !isV1(endpointUri(stage))

  def defaultApiVersion(stage: AIPrompt): Option[Either[String, String]] =
    if (!stage.isSet(stage.apiVersion) && stage.getApiType == "decisions") {
      stage.getDefault(stage.apiVersion).orElse(Some(Left("v1")))
    } else if (!stage.isSet(stage.apiVersion) && stage.isDefined(stage.url) && usesDatedApiVersion(stage)) {
      GlobalParams.getParam(stage.apiVersion).orElse(stage.getDefault(stage.apiVersion))
        .orElse(Some(Left(DefaultDatedApiVersion)))
    } else None

  def bodyOptions(stage: AIPrompt): Seq[ServiceParam[_]] = Seq(
    stage.maxTokens, stage.maxCompletionTokens, stage.temperature, stage.topP, stage.user, stage.n,
    stage.stop, stage.echo, stage.cacheLevel, stage.presencePenalty, stage.frequencyPenalty,
    stage.bestOf, stage.logProbs, stage.seed, stage.verbosity, stage.reasoningEffort, stage.responseFormat,
    stage.store, stage.previousResponseId, stage.dimensions, stage.safetyIdentifier)

  def allowedOptions(api: String): Set[String] = api match {
    case "chat_completions" => Set("maxTokens", "maxCompletionTokens", "temperature", "topP", "user", "n",
      "stop", "presencePenalty", "frequencyPenalty", "seed", "verbosity", "reasoningEffort", "responseFormat")
    case "responses" => Set("maxTokens", "maxCompletionTokens", "temperature", "topP", "user",
      "verbosity", "reasoningEffort", "responseFormat", "store", "previousResponseId")
    case "embeddings" => Set("user", "dimensions")
    case "decisions" => Set("safetyIdentifier")
    case "systemone" => Set.empty[String]
    case _ => throw new IllegalArgumentException("Unsupported apiType")
  }

  def responseFormat(value: Any, api: String): JsObject = {
    val obj = value match {
      case text: String => AIJson.objectString(text)
      case other => AIJson.obj(AIJson.fromAny(other))
    }
    obj.fields.get("type").foreach(v =>
      require(v.isInstanceOf[JsString], "responseFormat.type must be a string"))
    obj.fields.get("json_schema").foreach(v =>
      require(v.isInstanceOf[JsObject], "responseFormat.json_schema must be an object"))
    val schemas = Seq(obj) ++ obj.fields.get("json_schema").collect { case nested: JsObject => nested }
    schemas.foreach(_.fields.get("name").foreach(v =>
      require(v.isInstanceOf[JsString], "responseFormat schema name must be a string")))
    val fields = AIJson.orderedMap(obj.fields.toSeq.map { case (key, entry) =>
      key -> AIJson.AnyFormat.read(entry)
    })
    val normalized = AIJson.obj(AIJson.fromAny(ResponseFormatUtils.normalize(fields)))
    val preserved = preserveFormatFields(obj, normalized)
    val formatted = if (api == "chat_completions" && preserved.fields.get("type").contains(JsString("json_schema"))) {
      JsObject("type" -> JsString("json_schema"), "json_schema" -> JsObject(preserved.fields - "type"))
    } else preserved
    AIJson.obj(AIJson.fromAny(formatted))
  }

  private def preserveFormatFields(original: JsObject, normalized: JsObject): JsObject = {
    original.fields.get("type").collect { case JsString(value) => value.trim.toLowerCase(Locale.ROOT) } match {
      case Some("json_schema") =>
        original.fields.get("json_schema") match {
          case Some(nested: JsObject) =>
            require(original.fields.keySet.subsetOf(Set("type", "json_schema")),
              "Nested responseFormat.json_schema cannot be combined with other top-level format fields")
            require(!nested.fields.contains("type"), "Nested responseFormat.json_schema must not contain type")
            JsObject(nested.fields.updated("type", JsString("json_schema")))
          case _ => JsObject(original.fields ++ normalized.fields)
        }
      case Some("text") | Some("json_object") => JsObject(original.fields ++ normalized.fields)
      // Normalizing a bare envelope only adds type; avoid the legacy map's unordered intermediate.
      case None if original.fields.keySet == (normalized.fields.keySet - "type") =>
        JsObject(original.fields.updated("type", normalized.fields("type")))
      case _ => normalized
    }
  }
}

private[ai] final class AIRequestAdapter(stage: AIPrompt, schema: StructType) extends Serializable {
  private val api = stage.getApiType
  private val generative = AIRequestAdapter.GenerativeApis(api)
  private val native = AIRequestAdapter.NativeApis(api)
  private val versionSelectable = AIRequestAdapter.VersionSelectableApis(api)
  private val bodyOptions = AIRequestAdapter.bodyOptions(stage)
  private val allowed = AIRequestAdapter.allowedOptions(api)

  private def binding[T](param: ServiceParam[T]): Option[Either[T, String]] =
    stage.get(param).orElse(stage.getDefault(param))

  private val indices = stage.params.collect { case p: ServiceParam[_] => p }.flatMap { p =>
    binding(p).flatMap(_.right.toOption).map(name =>
      p.name -> schema.fieldIndex(AIColumnPlan.resolve(schema, name)))
  }.toMap
  private val inputIndex = stage.get(stage.stateCol).orElse(stage.get(stage.inputCol))
    .map(name => schema.fieldIndex(AIColumnPlan.resolve(schema, name)))

  def value(row: Row, param: ServiceParam[_]): Option[Any] = binding(param).flatMap {
    case Left(v) => Option(v)
    case Right(_) => Option(row.get(indices(param.name)))
  }

  private def scalar(param: ServiceParam[_]): Option[Any] =
    binding(param).flatMap(_.left.toOption).flatMap(Option(_))

  private def path(param: ServiceParam[_]): List[String] = (api, param.name) match {
    case ("responses", "maxTokens" | "maxCompletionTokens") => List("max_output_tokens")
    case ("chat_completions", "maxTokens" | "maxCompletionTokens") => List("max_completion_tokens")
    case ("responses", "responseFormat") => List("text", "format")
    case ("responses", "verbosity") => List("text", "verbosity")
    case ("responses", "reasoningEffort") => List("reasoning", "effort")
    case _ => List(param.payloadName)
  }

  private val owned = bodyOptions.filter(stage.isSet).map(path).toSet

  private def insert(base: JsObject, keys: List[String], value: JsValue): JsObject = keys match {
    case key :: Nil => JsObject(base.fields.updated(key, value))
    case key :: tail =>
      val child = base.fields.get(key).map(AIJson.obj).getOrElse(JsObject())
      JsObject(base.fields.updated(key, insert(child, tail, value)))
    case _ => base
  }

  private def encodeOption(param: ServiceParam[_], value: Any): JsValue = {
    val encoded = if (param.name == "responseFormat") AIRequestAdapter.responseFormat(value, api)
      else AIJson.fromAny(value)
    if (param.name == "dimensions") {
      require(encoded match {
        case JsNumber(n) => n.isWhole && n > 0
        case _ => false
      }, "dimensions must be a positive integer")
    }
    encoded
  }

  private val constantOptions = bodyOptions.filter(p => allowed(p.name)).foldLeft(JsObject()) { (body, param) =>
    scalar(param).map(v => insert(body, path(param), encodeOption(param, v))).getOrElse(body)
  }
  private val varyingOptions = bodyOptions.filter(p => allowed(p.name) && binding(p).exists(_.isRight))
  private val constantExtension = scalar(stage.extraBody).map(v => AIJson.obj(AIJson.fromAny(v)))
  private val constantQuestions = binding(stage.questions).flatMap(_.left.toOption).map(AIJson.fromAny)
  private val staticBody = varyingOptions.isEmpty && !binding(stage.extraBody).exists(_.isRight)
  private val constantBody = AIExtraBody.merge(constantOptions, constantExtension.getOrElse(JsObject()),
    owned, api, stage.isSet(stage.rawResponseCol))
  private val constantPrefix =
    if (constantBody.fields.isEmpty) "{" else constantBody.compactPrint.dropRight(1) + ","

  private def mergedOptions(row: Row): JsObject = {
    val perRow = varyingOptions.foldLeft(constantOptions) { (body, param) =>
      value(row, param).map(v => insert(body, path(param), encodeOption(param, v))).getOrElse(body)
    }
    val extension = constantExtension.getOrElse(value(row, stage.extraBody).map {
      case json: String => AIJson.objectString(json)
      case other => AIJson.obj(AIJson.fromAny(other))
    }.getOrElse(JsObject()))
    AIExtraBody.merge(perRow, extension, owned, api, stage.isSet(stage.rawResponseCol))
  }

  private def validateOptions(): Unit = {
    bodyOptions.filter(p => stage.isSet(p) && !allowed(p.name)).foreach { param =>
      throw new IllegalArgumentException(
        s"${param.name} is not supported by apiType='$api'; use a supported typed option or explicit extraBody")
    }
    require(!(stage.isDefined(stage.maxTokens) && stage.isDefined(stage.maxCompletionTokens)),
      "Cannot set both maxTokens and maxCompletionTokens")
    require(stage.getConcurrency > 0, "concurrency must be positive")
    require(stage.getTimeout >= 0.001 && !stage.getTimeout.isInfinity,
      "timeout must be finite and at least 0.001 seconds")
    stage.get(stage.concurrentTimeout).orElse(stage.getDefault(stage.concurrentTimeout)).foreach(v =>
      require(v >= 1e-9 && !v.isInfinity, "concurrentTimeout must be finite and at least 0.000000001 seconds"))
    stage.get(stage.fileSizeLimitMB).orElse(stage.getDefault(stage.fileSizeLimitMB)).foreach(v =>
      require(v > 0, "fileSizeLimitMB must be positive"))
    AIExtraBody.merge(constantOptions, constantExtension.getOrElse(JsObject()),
      owned, api, stage.isSet(stage.rawResponseCol))
  }

  private def validateCapabilities(): Unit = {
    if (!generative) {
      Seq(stage.systemPrompt, stage.columnTypes, stage.fileSizeLimitMB, stage.postProcessing,
        stage.postProcessingOptions, stage.messagesCol, stage.dropPrompt).filter(stage.isSet).foreach(p =>
        require(p == stage.messagesCol && api == "decisions",
          s"${p.name} is not supported by apiType='$api'"))
    }
    if (stage.isSet(stage.messagesCol)) validateMessageConstruction()
    validateNativeBindings()
    require(api == "embeddings" || !stage.isDefined(stage.text), "text/textCol requires apiType='embeddings'")
    require(api == "systemone" || !stage.isDefined(stage.stateCol), "stateCol requires apiType='systemone'")
    validateVersionBinding()
  }

  private def validateVersionBinding(): Unit = {
    require(!(stage.isSet(stage.apiVersion) && stage.isSet(stage.customUrlRoot)),
      "apiVersion is not supported with customUrlRoot; include the version in the complete URL query instead")
    if (stage.isSet(stage.apiVersion) && (native || v1) && !versionSelectable) {
      throw new IllegalArgumentException(s"apiVersion is not supported for '$api' on this endpoint")
    }
  }

  private def validateMessageConstruction(): Unit = {
    Seq(stage.systemPrompt, stage.fileSizeLimitMB, stage.dropPrompt, stage.inputMode)
      .filter(stage.isSet).foreach(p =>
        throw new IllegalArgumentException(s"${p.name} cannot be combined with messagesCol"))
    require(!stage.isSet(stage.columnTypes) || stage.getColumnTypes.isEmpty,
      "Non-empty columnTypes cannot be combined with messagesCol")
  }

  private def validateNativeBindings(): Unit = {
    if (!native) {
      require(!stage.isSet(stage.questions) && !stage.isSet(stage.stateCol) && !stage.isSet(stage.inputMode),
        s"questions/stateCol/inputMode require a native API, not '$api'")
    } else {
      require(stage.isDefined(stage.questions), s"questions is required for apiType='$api'")
      constantQuestions.foreach(NativeQuestions.validate(api, _))
    }
  }

  private def validateInputs(): Unit = {
    val inputs = Seq(
      stage.isSet(stage.promptTemplate), stage.isSet(stage.messagesCol), stage.isSet(stage.stateCol),
      stage.isSet(stage.inputCol), stage.isDefined(stage.text)).count(identity)
    require(inputs == 1, "Configure exactly one of promptTemplate, messagesCol, stateCol, inputCol, or text/textCol")
    Seq(stage.stateCol, stage.inputCol).filter(stage.isSet).foreach(checkStringColumn)
    stage.params.collect { case p: ServiceParam[_] => p }.foreach { p =>
      binding(p).flatMap(_.right.toOption).foreach { name =>
        validateServiceColumn(p.name, schema(AIColumnPlan.resolve(schema, name)).dataType)
      }
    }
    if (stage.isSet(stage.messagesCol)) {
      schema(AIColumnPlan.resolve(schema, stage.getMessagesCol)).dataType match {
        case ArrayType(message: StructType, _) => validateMessageSchema(message)
        case _ => throw new IllegalArgumentException("messagesCol must contain an array of message structs")
      }
    }
    AIColumnPlan.distinct(stage.getColumnTypes.keys.toSeq, "columnTypes keys")
    stage.getColumnTypes.foreach { case (name, kind) =>
      require(kind == "text" || kind == "path", "columnTypes supports only 'text' and 'path'")
      val field = schema(AIColumnPlan.resolve(schema, name))
      require(kind != "path" || field.dataType == StringType, "Path columns must have string type")
    }
    if (stage.isSet(stage.promptTemplate)) {
      val input = SparkSession.active.createDataFrame(java.util.Collections.emptyList[Row](), schema)
      val expression = AIPrompt.withSafeTemplate(Functions.template(AIJson.validatedString(stage.getPromptTemplate)))
      val rendered = AIPrompt.bindTemplate(input, expression)
      require(rendered.expr.dataType == StringType, "Prompt templates must produce strings")
    }
  }

  private def validateMessageSchema(dataType: DataType): Unit = dataType match {
    case struct: StructType =>
      require(struct.fieldNames.distinct.length == struct.length,
        "messagesCol must not contain duplicate protocol field names")
      struct.fields.foreach(field => validateMessageSchema(field.dataType))
    case ArrayType(element, _) => validateMessageSchema(element)
    case MapType(key, value, _) =>
      validateMessageSchema(key)
      validateMessageSchema(value)
    case _ => ()
  }

  private def jsonCompatible(dataType: DataType): Boolean = dataType match {
    case StringType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | NullType => true
    case _: DecimalType => true
    case ArrayType(element, _) => jsonCompatible(element)
    case MapType(StringType, value, _) => jsonCompatible(value)
    case _ => false
  }

  private def validateServiceColumn(name: String, dataType: DataType): Unit = {
    val integers = Set("maxTokens", "maxCompletionTokens", "n", "logProbs",
      "cacheLevel", "bestOf", "seed", "dimensions")
    val doubles = Set("temperature", "topP", "presencePenalty", "frequencyPenalty")
    val headerMaps = Set("customHeaders", "telemHeaders")
    val valid = if (integers(name)) dataType == IntegerType
    else if (doubles(name)) dataType == DoubleType
    else if (Set("store", "echo")(name)) dataType == BooleanType
    else if (headerMaps(name)) dataType match {
      case MapType(StringType, StringType, _) => true
      case _ => false
    }
    else if (name == "responseFormat") {
      dataType == StringType || (dataType.isInstanceOf[MapType] && jsonCompatible(dataType))
    }
    else dataType == StringType
    require(valid, s"${name}Col has an unsupported data type: ${dataType.simpleString}")
  }

  def validate(): Unit = {
    validateOptions()
    validateCapabilities()
    validateInputs()
    require(!(stage.isSet(stage.deploymentName) && stage.isSet(stage.model)),
      "Set model or deploymentName, not both; neither explicit binding may be silently ignored")
    require(stage.isDefined(stage.deploymentName) || stage.isDefined(stage.model),
      "Set deploymentName/model or the applicable operation-specific default")
    Seq(stage.deploymentName, stage.model).foreach { p =>
      scalar(p).foreach(v => require(v.isInstanceOf[String] && v.asInstanceOf[String].trim.nonEmpty,
        "Model/deployment must be a non-empty string"))
    }
    val scalarModel = if (versionColumn) None else scalar(selectedModelParam).map(_.asInstanceOf[String])
    AIPrompt.validateScalarHeaders(stage, endpoint(
      scalarModel.getOrElse("validation-model"),
      scalar(stage.apiVersion).map(_.toString),
      validateModel = scalarModel.isDefined))
  }

  private def checkStringColumn(param: Param[String]): Unit =
    require(schema(AIColumnPlan.resolve(schema, stage.getOrDefault(param))).dataType == StringType,
      s"${param.name} must reference a string column")

  private def selectedModelParam: ServiceParam[String] =
    if (stage.isSet(stage.deploymentName)) stage.deploymentName
    else if (stage.isSet(stage.model)) stage.model
    else if (stage.isDefined(stage.deploymentName)) stage.deploymentName else stage.model

  private def versionColumn: Boolean = versionSelectable && binding(stage.apiVersion).exists(_.isRight)

  def validateFabricModel(): Unit = {
    scalar(selectedModelParam).foreach { value =>
      val modelName = value.asInstanceOf[String]
      val address = endpoint(if (versionColumn) "validation-model" else modelName,
        scalar(stage.apiVersion).map(_.toString), validateModel = !versionColumn)
      if (stage.usingImplicitFabricEndpoint(address)) {
        stage.assertFabricModelStatus(modelName)
      }
    }
  }

  private def selectedModel(row: Row): String = {
    value(row, selectedModelParam) match {
      case Some(text: String) if text.trim.nonEmpty => text
      case _ => throw new AIMissingInputException("explicit model/deployment is missing for this row")
    }
  }

  private val uri = AIRequestAdapter.endpointUri(stage)
  private val base = uri.toString
  private val host = AIRequestAdapter.normalizedHost(uri)
  private val basePath = AIRequestAdapter.routePath(uri)
  private val v1 = AIRequestAdapter.isV1(uri)
  private val completeRoutes = Map("/providers/microsoft/v1/systemone" -> "systemone", "/v1/decisions" -> "decisions",
    "/chat/completions" -> "chat_completions", "/responses" -> "responses", "/embeddings" -> "embeddings")

  private def nativeRoute: String = api match {
      case "systemone" =>
        require(basePath.isEmpty || basePath.endsWith("/providers/microsoft/v1/systemone"),
          "systemone requires a Foundry resource endpoint or its complete System One route")
        "providers/microsoft/v1/systemone"
      case "decisions" =>
        require(!AIRequestAdapter.isAzureHost(host),
          "decisions requires a qualified OpenAI endpoint, not a Foundry endpoint")
        require(basePath.isEmpty || v1 || basePath.endsWith("/v1/decisions"),
          "decisions requires a /v1 endpoint")
        if (v1) "decisions" else "v1/decisions"
      case _ => throw new IllegalArgumentException("apiType does not select a native route")
  }

  private def generativeRoute(modelName: String): String = api match {
      case "responses" => if (v1) "responses" else "openai/responses"
      case "embeddings" =>
        if (v1) "embeddings"
        else s"openai/deployments/${deploymentSegment(modelName)}/embeddings"
      case "chat_completions" =>
        if (v1) "chat/completions"
        else if (host.endsWith(".services.ai.azure.com") &&
          selectedModelParam == stage.model) "models/chat/completions"
        else s"openai/deployments/${deploymentSegment(modelName)}/chat/completions"
      case _ => throw new IllegalArgumentException("apiType does not select a Chat, Responses or embeddings route")
  }

  private def deploymentSegment(modelName: String): String = {
    validateDeploymentSegment(modelName)
    URLEncoder.encode(AIJson.validatedText(modelName, selectedModelParam.name), "UTF-8").replace("+", "%20")
  }

  private def validateDeploymentSegment(modelName: String): Unit =
    require(modelName.trim.nonEmpty && !modelName.contains("%") &&
      !modelName.split("[/\\\\]", -1).exists(part => part == "." || part == ".."),
      "Deployment name must be non-empty and must not contain URL dot segments or percent signs")

  private def validateCompleteDeployment(modelName: String, validateModel: Boolean): Unit = {
    OpenAIEndpointUtils.stripTrailingSlashes(Option(uri.getRawPath).getOrElse("")) match {
      case AIRequestAdapter.DeploymentRoute(encoded) =>
        val decoded = URLDecoder.decode(encoded.replace("+", "%2B"), "UTF-8")
        validateDeploymentSegment(decoded)
        if (validateModel) require(decoded == modelName, "Endpoint route does not match deployment")
      case _ =>
    }
    if (basePath.endsWith("/models/chat/completions")) {
      require(selectedModelParam != stage.deploymentName,
        "The models endpoint requires model, not deploymentName")
    }
  }

  private def validateDeploymentBase(): Unit = if (!stage.isSet(stage.customUrlRoot)) {
    require(!AIRequestAdapter.DeploymentBase.pattern.matcher(basePath).matches(),
      "Configure setUrl with a resource endpoint or a complete API route, not an incomplete deployment path")
  }

  private def v1Endpoint: String = {
    val rawPath = OpenAIEndpointUtils.stripTrailingSlashes(Option(uri.getRawPath).getOrElse(""))
    val prefix = basePath match {
      case AIRequestAdapter.DeploymentRoute(_) => rawPath.take(basePath.lastIndexOf("/openai/deployments/"))
      case _ =>
        Seq("/models/chat/completions", "/openai/responses", "/chat/completions", "/responses")
          .find(basePath.endsWith).map(suffix => rawPath.dropRight(suffix.length)).getOrElse(rawPath)
    }
    val root = if (prefix.toLowerCase(Locale.ROOT).endsWith("/openai")) prefix else prefix + "/openai"
    val operation = if (api == "responses") "responses" else "chat/completions"
    base.dropRight(rawPath.length) + root + "/v1/" + operation
  }

  private def endpoint(modelName: String, version: Option[String], validateModel: Boolean = true): String = {
    if (api == "decisions") require(!AIRequestAdapter.isAzureHost(host),
      "decisions requires a qualified OpenAI endpoint, not a Foundry endpoint")
    require(api != "systemone" || !AIRequestAdapter.isOpenAIHost(host),
      "systemone requires a qualified Foundry endpoint, not an OpenAI endpoint")
    validateDeploymentBase()
    completeRoutes.foreach { case (suffix, expected) =>
      require(!basePath.endsWith(suffix) || api == expected, "Endpoint route does not match apiType")
    }
    validateCompleteDeployment(modelName, validateModel)
    if (stage.isSet(stage.customUrlRoot)) base else {
      require(Option(uri.getQuery).isEmpty, "Use customUrlRoot for a complete URL with query parameters")
      versionedEndpoint(modelName, version)
    }
  }

  private def versionedEndpoint(modelName: String, version: Option[String]): String = {
    val selectedVersion = if (stage.isSet(stage.apiVersion) || (!native && !v1)) version else None
    val selectV1 = versionSelectable && selectedVersion.exists(_.equalsIgnoreCase("v1"))
    val result = operationEndpoint(modelName, selectV1)
    val queryVersion = if (selectV1) None else selectedVersion.orElse {
      if (native || v1) None else Some(AIRequestAdapter.DefaultDatedApiVersion)
    }
    queryVersion.map(value => result + "?api-version=" +
      URLEncoder.encode(AIJson.validatedText(value, stage.apiVersion.name), "UTF-8")).getOrElse(result)
  }

  private def operationEndpoint(modelName: String, selectV1: Boolean): String =
    if (selectV1 && generative && !v1) v1Endpoint
    else if (completeRoutes.keys.exists(basePath.endsWith)) base
    else base + "/" + (if (native) nativeRoute else generativeRoute(modelName))

  private def input(row: Row, preparedText: Option[String]): JsValue = {
    val text = preparedText.orElse(inputIndex.flatMap(index => Option(row.getString(index))))
      .orElse(value(row, stage.text).map(_.asInstanceOf[String]))
    text match {
      case Some(v) if v.nonEmpty =>
        if (native && stage.getInputMode == "json") AIJson.parse(v) else JsString(v)
      case _ => throw new AIMissingInputException("input is null or empty")
    }
  }

  private def validateMessageType(row: Row): Unit = {
    val messageType = row.schema.fieldNames.indexOf("type")
    if (api != "chat_completions" && messageType >= 0 && !row.isNullAt(messageType)) {
      require(row.schema(messageType).dataType == StringType && row.getString(messageType) == "message",
        "Message type must be 'message'")
    }
  }

  private def protocolMessages(rows: Seq[Row]): Seq[Map[String, Any]] = {
    rows.foreach { row =>
      require(Option(row).isDefined, "Each message must be an object")
      val content = row.schema.fieldNames.indexOf("content")
      require(content >= 0 && content < row.length && !row.isNullAt(content),
        "Each message must contain non-null content")
      val supported = if (api == "chat_completions") Set("role", "content", "name")
        else Set("role", "content", "type")
      row.schema.fields.zipWithIndex.foreach { case (field, index) =>
        require(supported(field.name) || row.isNullAt(index),
          s"Message field '${field.name}' is not supported by apiType='$api'")
      }
      validateMessageType(row)
    }
    if (api == "chat_completions") {
      OpenAIChatCompletion.strictRequestMessages(rows).zip(rows).map { case (encoded, row) =>
        val name = row.schema.fieldNames.indexOf("name")
        if (name < 0 || row.isNullAt(name)) encoded
        else {
          require(row.schema(name).dataType == StringType, "Message name must be a string")
          encoded + ("name" -> row.getString(name))
        }
      }
    } else {
      require(rows.nonEmpty, "messages must not be empty")
      OpenAIResponses.requestMessages(rows)
    }
  }

  private def nativeEnvelope(row: Row, preparedText: Option[String],
                             messages: Option[Seq[Row]]): Map[String, JsValue] = {
    val questions = constantQuestions.getOrElse(value(row, stage.questions).map {
      case s: String => AIJson.parse(s)
      case other => AIJson.fromAny(other)
    }.getOrElse(throw new AIMissingInputException("questions is null")))
    if (constantQuestions.isEmpty) NativeQuestions.validate(api, questions)
    val content = if (api == "decisions" && messages.isDefined) {
      AIJson.fromAny(protocolMessages(messages.get))
    } else input(row, preparedText)
    val validated = if (api == "decisions") NativeQuestions.prepareDecisionsInput(content) else content
    Map("questions" -> questions, (if (api == "systemone") "state" else "input") -> validated)
  }

  private def requestVersion(row: Row): Option[String] = {
    val version = value(row, stage.apiVersion).map(_.toString)
    val requiresVersion = AIRequestAdapter.usesDatedApiVersion(stage) ||
      (versionSelectable && stage.isSet(stage.apiVersion))
    if (requiresVersion && binding(stage.apiVersion).isDefined && version.isEmpty) {
      throw new AIMissingInputException("apiVersion is null")
    }
    version
  }

  def request(row: Row, preparedText: Option[String], messages: Option[Seq[Row]]): (String, String) = {
    val modelName = selectedModel(row)
    val version = requestVersion(row)
    val body = if (staticBody) None else Some(mergedOptions(row))
    val envelope = api match {
      case "chat_completions" =>
        val mapped = protocolMessages(messages.get)
        Map("messages" -> AIJson.fromAny(mapped), "model" -> JsString(modelName))
      case "responses" =>
        val mapped = protocolMessages(messages.get)
        Map("input" -> AIJson.fromAny(mapped), "model" -> JsString(modelName))
      case "embeddings" =>
        Map("input" -> input(row, preparedText), "model" -> JsString(modelName))
      case "systemone" | "decisions" =>
        nativeEnvelope(row, preparedText, messages) + ("model" -> JsString(modelName))
      case _ => throw new IllegalArgumentException("Unsupported apiType")
    }
    val encoded = body.map(b => JsObject(b.fields ++ envelope).compactPrint)
      .getOrElse(constantPrefix + JsObject(envelope).compactPrint.drop(1))
    endpoint(modelName, version) -> encoded
  }
}
