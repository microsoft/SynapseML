// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.URLKey
import com.microsoft.azure.synapse.ml.param.{GlobalKey, GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services.{CognitiveServicesBaseNoHandler, HasCognitiveServiceInput,
  OpenAIApiVersion, OpenAISubscriptionKey, ServiceAuthHeaders}
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIApiTypeKey, OpenAIDeploymentNameKey,
  OpenAIEmbeddingDeploymentNameKey, OpenAIReasoningEffortKey, OpenAISeedKey, OpenAITemperatureKey,
  OpenAITopPKey, OpenAIVerbosityKey}
import org.apache.spark.ml.param.{Param, ParamMap, Params}

import java.net.URI
import scala.collection.JavaConverters._

/** Driver-JVM defaults, shared by Spark sessions in that JVM. Configure before constructing Spark actions. */
object AIDefaults {
  private val HttpsPort = 443
  private val HttpPort = 80
  private[ai] val LegacyKeys: Map[String, GlobalKey[_]] = Map(
    "endpoint" -> URLKey, "apiKey" -> OpenAISubscriptionKey,
    "model" -> AIFoundryModel, "deploymentName" -> OpenAIDeploymentNameKey,
    "apiVersion" -> OpenAIApiVersion, "apiType" -> OpenAIApiTypeKey,
    "embeddingDeploymentName" -> OpenAIEmbeddingDeploymentNameKey,
    "temperature" -> OpenAITemperatureKey, "topP" -> OpenAITopPKey, "seed" -> OpenAISeedKey,
    "verbosity" -> OpenAIVerbosityKey, "reasoningEffort" -> OpenAIReasoningEffortKey)

  private val ConnectionFields = Set("endpoint", "apiKey")
  private val ModelFields = ConnectionFields ++ Set("model", "deploymentName", "apiVersion")
  private val GenerationFields = ModelFields ++ Set("temperature", "topP", "verbosity", "reasoningEffort")
  private val ImplicitPromptFields = Set("apiKey", "temperature", "topP", "seed", "verbosity", "reasoningEffort")
  private[ai] val Fields: Map[String, Set[String]] = Map(
    "openai" -> LegacyKeys.keySet,
    "chat_completions" -> (GenerationFields + "seed"),
    "responses" -> GenerationFields,
    "embeddings" -> ModelFields,
    "systemone" -> (ConnectionFields + "model"),
    "decisions" -> (ConnectionFields ++ Set("model", "apiVersion")),
    "content_understanding" -> (ConnectionFields ++ Set("apiVersion", "analyzerId", "modelDeployments",
      "stringEncoding", "processingLocation", "allowInputTruncation")))

  private case class ScopedKey(api: String, field: String) extends GlobalKey[Any]

  /** openai is the exact legacy OpenAIDefaults store, not a second synchronized copy. */
  def forApi(api: String): AIDefaultsScope = {
    require(Fields.contains(api), "Unknown AI defaults API. Use: " + Fields.keys.toSeq.sorted.mkString(", "))
    new AIDefaultsScope(api)
  }

  def resetAll(): Unit = Fields.foreach { case (api, fields) => fields.foreach(reset(api, _)) }

  private[ai] def read[T](api: String, field: String): Option[T] = {
    val value = if (api == "openai") {
      GlobalParams.getGlobalParam(LegacyKeys(field).asInstanceOf[GlobalKey[Any]]).flatMap {
        case Left(v) => Some(v)
        case Right(_) => None
        case v => Some(v)
      }
    } else GlobalParams.getGlobalParam(ScopedKey(api, field))
    value.map(_.asInstanceOf[T])
  }

  private[ai] def write(api: String, field: String, value: Any): Unit = {
    if (api == "openai") {
      val stored = if (Set("endpoint", "apiType")(field)) value else Left(value)
      GlobalParams.setGlobalParam(LegacyKeys(field).asInstanceOf[GlobalKey[Any]], stored)
    } else GlobalParams.setGlobalParam(ScopedKey(api, field), value)
  }

  private[ai] def reset(api: String, field: String): Unit = {
    if (api == "openai") GlobalParams.resetGlobalParam(LegacyKeys(field).asInstanceOf[GlobalKey[Any]])
    else GlobalParams.resetGlobalParam(ScopedKey(api, field))
  }

  private def snapshot(api: String): Map[String, Any] =
    Fields(api).toSeq.flatMap(field => read[Any](api, field).map(field -> _)).toMap

  private[ai] def text(value: String, field: String): Unit =
    require(value != null && value.trim.nonEmpty && !value.exists(_.isControl), s"$field must be nonempty text")

  private[ai] def endpoint(value: String): URI = {
    text(value, "endpoint")
    val uri = try new URI(value) catch {
      case _: java.net.URISyntaxException => throw new IllegalArgumentException("Invalid endpoint URI")
    }
    val host = Option(uri.getHost).getOrElse("").toLowerCase(java.util.Locale.ROOT)
    val scheme = Option(uri.getScheme).getOrElse("").toLowerCase(java.util.Locale.ROOT)
    require(host.nonEmpty && uri.getRawUserInfo == null && uri.getRawFragment == null &&
      uri.getRawQuery == null && (uri.getPort == -1 || (uri.getPort > 0 && uri.getPort <= 65535)),
      "endpoint must have a host and valid port, without user information, query, or fragment")
    require(scheme == "https" || (scheme == "http" && Set("127.0.0.1", "[::1]", "::1")(host)),
      "endpoint must use HTTPS (HTTP is allowed only for literal loopback addresses)")
    uri
  }

  private def sameOrigin(left: String, right: String): Boolean = {
    def origin(value: String): (String, String, Int) = {
      val uri = new URI(value)
      val scheme = Option(uri.getScheme).getOrElse("").toLowerCase(java.util.Locale.ROOT)
      val port = if (uri.getPort != -1) uri.getPort else if (scheme == "https") HttpsPort else HttpPort
      (scheme, Option(uri.getHost).getOrElse("").toLowerCase(java.util.Locale.ROOT), port)
    }
    try origin(left) == origin(right) catch {
      case _: java.net.URISyntaxException => false
    }
  }

  private def explicitAuth(stage: HasCognitiveServiceInput): Boolean = {
    val headerAuth = stage.get(stage.customHeaders).exists {
      case Right(_) => false
      case Left(headers) => ServiceAuthHeaders.sanitizeHeaderMap(headers).exists { case (name, value) =>
        Set("authorization", "api-key", "ocp-apim-subscription-key")(name.toLowerCase(java.util.Locale.ROOT)) &&
          ServiceAuthHeaders.nonBlank(value)
      }
    }
    headerAuth || Seq(stage.subscriptionKey, stage.AADToken, stage.CustomAuthHeader).exists(stage.isSet)
  }

  private def authenticatedValues(stage: HasCognitiveServiceInput,
                                  values: Map[String, Any]): Map[String, Any] = {
    if (explicitAuth(stage)) values - "apiKey"
    else if (!values.contains("apiKey")) values
    else {
      require(values.contains("endpoint"), "An API-scoped apiKey requires an endpoint in the same defaults scope")
      val destination = stage.get(stage.customUrlRoot).orElse(stage.get(stage.url))
        .getOrElse(values("endpoint").asInstanceOf[String])
      if (!sameOrigin(values("endpoint").asInstanceOf[String], destination)) values - "apiKey"
      else values
    }
  }

  private def setValue(stage: Params, field: String, value: Any): Unit = {
    val name = field match {
      case "endpoint" => "url"
      case "apiKey" => "subscriptionKey"
      case other => other
    }
    val param: Param[_] = stage.getParam(name)
    val stored = param match {
      case _: ServiceParam[_] => Left(value)
      case _ => value
    }
    stage match {
      case prompt: AIPrompt if ImplicitPromptFields(field) => AIPrompt.setScopedDefault(prompt, name, stored)
      case _ => stage.set(param.asInstanceOf[Param[Any]], stored)
    }
  }

  private def overridden(stage: AIPrompt, field: String): Boolean = {
    val modelExplicit = stage.isSet(stage.model) || stage.isSet(stage.deploymentName)
    if (field == "endpoint") stage.isSet(stage.url) || stage.isSet(stage.customUrlRoot)
    else if (Set("model", "deploymentName")(field)) modelExplicit
    else if (field == "apiVersion") {
      stage.getApiType != "decisions" || stage.isSet(stage.customUrlRoot) || stage.isSet(stage.apiVersion)
    } else stage.isSet(stage.getParam(if (field == "apiKey") "subscriptionKey" else field))
  }

  /** Applies only to a disposable effective copy; the user's stage and its persisted params stay unchanged. */
  private[ai] def applyPrompt(stage: AIPrompt): Set[String] = {
    val values = snapshot(stage.getApiType)
    val selected = authenticatedValues(stage, values).filterNot { case (field, _) => overridden(stage, field) }
    selected.foreach { case (field, value) => setValue(stage, field, value) }
    // Neither an API-specific destination nor explicit authentication may pick up the legacy key.
    val credentials = if (explicitAuth(stage) || values.contains("endpoint") || values.contains("apiKey")) {
      Set("subscriptionKey")
    } else Set.empty[String]
    credentials ++ selected.keys.map {
      case "apiKey" => "subscriptionKey"
      case "endpoint" => "url"
      case field => field
    }
  }

  private[ai] def applyContent[T <: CognitiveServicesBaseNoHandler with HasCognitiveServiceInput](stage: T): T = {
    require(stage.getInternalServiceType == "contentunderstanding",
      "content_understanding defaults can only be applied to ContentUnderstanding")
    val result = stage.copy(ParamMap.empty).asInstanceOf[T]
    val values = authenticatedValues(result, snapshot("content_understanding"))
    values.foreach { case (field, value) =>
      val name = if (field == "apiKey") "subscriptionKey" else field
      if (field == "endpoint") {
        if (!result.isSet(result.url) && !result.isSet(result.customUrlRoot)) result.setEndpoint(value.toString)
      } else if (!result.isSet(result.getParam(name))) setValue(result, field, value)
    }
    result
  }
}

/** Typed scalar settings. Unsupported fields fail early instead of silently leaking into another API. */
final class AIDefaultsScope private[ai](val api: String) extends ContentUnderstandingDefaultParams {
  private def check(field: String): Unit =
    require(AIDefaults.Fields(api)(field), s"$field is not supported in the $api defaults scope")

  override protected def put(field: String, value: Any): this.type = {
    check(field)
    require(value != null, s"$field cannot be null; use its reset method")
    value match {
      case text: String => AIDefaults.text(text, field)
      case _ =>
    }
    AIDefaults.write(api, field, value)
    this
  }

  override protected def get[T](field: String): Option[T] = { check(field); AIDefaults.read[T](api, field) }
  override protected def clear(field: String): Unit = { check(field); AIDefaults.reset(api, field) }

  def reset(): Unit = AIDefaults.Fields(api).foreach(AIDefaults.reset(api, _))

  def setEndpoint(value: String): this.type = { AIDefaults.endpoint(value); put("endpoint", value) }
  def getEndpoint: Option[String] = get[String]("endpoint")
  def resetEndpoint(): Unit = clear("endpoint")

  def setApiKey(value: String): this.type = put("apiKey", value)
  def getApiKey: Option[String] = get[String]("apiKey")
  def resetApiKey(): Unit = clear("apiKey")

  private def checkModelBinding(other: String): Unit = {
    if (api != "openai" && AIDefaults.Fields(api)(other)) {
      require(AIDefaults.read[String](api, other).isEmpty,
        s"Reset $other before configuring the alternative model binding")
    }
  }

  def setModel(value: String): this.type = { checkModelBinding("deploymentName"); put("model", value) }
  def getModel: Option[String] = get[String]("model")
  def resetModel(): Unit = clear("model")

  def setDeploymentName(value: String): this.type = { checkModelBinding("model"); put("deploymentName", value) }
  def getDeploymentName: Option[String] = get[String]("deploymentName")
  def resetDeploymentName(): Unit = clear("deploymentName")

  def setApiVersion(value: String): this.type = put("apiVersion", value)
  def getApiVersion: Option[String] = get[String]("apiVersion")
  def resetApiVersion(): Unit = clear("apiVersion")

  def setApiType(value: String): this.type = {
    require(Set("responses", "chat_completions")(value), "apiType must be responses or chat_completions")
    put("apiType", value)
  }
  def getApiType: Option[String] = get[String]("apiType")
  def resetApiType(): Unit = clear("apiType")

  def setEmbeddingDeploymentName(value: String): this.type = put("embeddingDeploymentName", value)
  def getEmbeddingDeploymentName: Option[String] = get[String]("embeddingDeploymentName")
  def resetEmbeddingDeploymentName(): Unit = clear("embeddingDeploymentName")

  def setTemperature(value: Double): this.type = {
    require(value >= 0.0 && value <= 2.0, "temperature must be between 0 and 2")
    put("temperature", value)
  }
  def getTemperature: Option[Double] = get[Double]("temperature")
  def resetTemperature(): Unit = clear("temperature")

  def setTopP(value: Double): this.type = {
    require(value >= 0.0 && value <= 1.0, "topP must be between 0 and 1")
    put("topP", value)
  }
  def getTopP: Option[Double] = get[Double]("topP")
  def resetTopP(): Unit = clear("topP")

  def setSeed(value: Int): this.type = put("seed", value)
  def getSeed: Option[Int] = get[Int]("seed")
  def resetSeed(): Unit = clear("seed")

  def setVerbosity(value: String): this.type = put("verbosity", value)
  def getVerbosity: Option[String] = get[String]("verbosity")
  def resetVerbosity(): Unit = clear("verbosity")

  def setReasoningEffort(value: String): this.type = put("reasoningEffort", value)
  def getReasoningEffort: Option[String] = get[String]("reasoningEffort")
  def resetReasoningEffort(): Unit = clear("reasoningEffort")
}

private[ai] trait ContentUnderstandingDefaultParams {
  def api: String
  protected def put(field: String, value: Any): this.type
  protected def get[T](field: String): Option[T]
  protected def clear(field: String): Unit

  def setAnalyzerId(value: String): this.type = put("analyzerId", value)
  def getAnalyzerId: Option[String] = get[String]("analyzerId")
  def resetAnalyzerId(): Unit = clear("analyzerId")

  def setModelDeployments(value: Map[String, String]): this.type = {
    require(value != null, "modelDeployments cannot be null")
    value.foreach { case (model, deployment) =>
      AIDefaults.text(model, "modelDeployments model")
      AIDefaults.text(deployment, "modelDeployments deployment")
    }
    put("modelDeployments", value.toMap)
  }
  def setModelDeployments(value: java.util.Map[String, String]): this.type = {
    require(value != null, "modelDeployments cannot be null")
    setModelDeployments(value.asScala.toMap)
  }
  def getModelDeployments: Option[Map[String, String]] = get[Map[String, String]]("modelDeployments")
  def resetModelDeployments(): Unit = clear("modelDeployments")

  def setStringEncoding(value: String): this.type = put("stringEncoding", value)
  def getStringEncoding: Option[String] = get[String]("stringEncoding")
  def resetStringEncoding(): Unit = clear("stringEncoding")

  def setProcessingLocation(value: String): this.type = put("processingLocation", value)
  def getProcessingLocation: Option[String] = get[String]("processingLocation")
  def resetProcessingLocation(): Unit = clear("processingLocation")

  def setAllowInputTruncation(value: Boolean): this.type = put("allowInputTruncation", value)
  def getAllowInputTruncation: Option[Boolean] = get[Boolean]("allowInputTruncation")
  def resetAllowInputTruncation(): Unit = clear("allowInputTruncation")

  /** Explicitly snapshots Foundry CU defaults onto a copy; does not provision or alter a Foundry resource. */
  def applyTo[T <: CognitiveServicesBaseNoHandler with HasCognitiveServiceInput](stage: T): T = {
    require(api == "content_understanding", "applyTo is only needed for content_understanding defaults")
    AIDefaults.applyContent(stage)
  }
}
