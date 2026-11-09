// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._

import scala.language.existentials

// Scalar and column accessors are required for each generated Python ServiceParam.
// scalastyle:off number.of.methods
trait HasOpenAIModelParams extends HasOpenAITextParams {
  val promptCacheKey: ServiceParam[String] = new ServiceParam[String](
    this, "promptCacheKey", "Optional cache routing and accounting key. Does not guarantee a cache hit.",
    isRequired = false) {
    override val payloadName: String = "prompt_cache_key"
  }

  def getPromptCacheKey: String = getScalarParam(promptCacheKey)
  def setPromptCacheKey(v: String): this.type = setScalarParam(promptCacheKey, v)
  def getPromptCacheKeyCol: String = getVectorParam(promptCacheKey)
  def setPromptCacheKeyCol(v: String): this.type = setVectorParam(promptCacheKey, v)

  val promptCacheRetention: ServiceParam[String] = new ServiceParam[String](
    this, "promptCacheRetention",
    "Deprecated provider maximum-retention policy, independent of promptCacheOptions.ttl minimum lifetime. " +
      "Forwarded unchanged; prefer promptCacheOptions for new models.",
    isRequired = false) {
    override val payloadName: String = "prompt_cache_retention"
  }

  def getPromptCacheRetention: String = getScalarParam(promptCacheRetention)
  def setPromptCacheRetention(v: String): this.type = setScalarParam(promptCacheRetention, v)
  def getPromptCacheRetentionCol: String = getVectorParam(promptCacheRetention)
  def setPromptCacheRetentionCol(v: String): this.type = setVectorParam(promptCacheRetention, v)

  val safetyIdentifier: ServiceParam[String] = new ServiceParam[String](
    this, "safetyIdentifier", "Stable, non-identifying end-user identifier for provider safety monitoring.",
    isRequired = false) {
    override val payloadName: String = "safety_identifier"
  }

  def getSafetyIdentifier: String = getScalarParam(safetyIdentifier)
  def setSafetyIdentifier(v: String): this.type = setScalarParam(safetyIdentifier, v)
  def getSafetyIdentifierCol: String = getVectorParam(safetyIdentifier)
  def setSafetyIdentifierCol(v: String): this.type = setVectorParam(safetyIdentifier, v)

  val serviceTier: ServiceParam[String] = new ServiceParam[String](
    this, "serviceTier", "Requested processing tier. The provider determines availability and the effective tier.",
    isRequired = false) {
    override val payloadName: String = "service_tier"
  }

  def getServiceTier: String = getScalarParam(serviceTier)
  def setServiceTier(v: String): this.type = setScalarParam(serviceTier, v)
  def getServiceTierCol: String = getVectorParam(serviceTier)
  def setServiceTierCol(v: String): this.type = setVectorParam(serviceTier, v)

  val metadata: ServiceParam[Map[String, String]] = new ServiceParam[Map[String, String]](
    this, "metadata", "String key-value metadata attached to the generated response.", isRequired = false)

  def getMetadata: Map[String, String] = getScalarParam(metadata)
  def setMetadata(v: Map[String, String]): this.type = setScalarParam(metadata, v)
  def getMetadataCol: String = getVectorParam(metadata)
  def setMetadataCol(v: String): this.type = setVectorParam(metadata, v)

  val reasoning: ServiceParam[Map[String, Any]] = new ServiceParam[Map[String, Any]](
    this, "reasoning",
    "Responses reasoning configuration, including effort, context, mode and summary. " +
      "If reasoningEffort is also configured, its value must agree with reasoning.effort.",
    isRequired = false)

  def getReasoning: Map[String, Any] = getScalarParam(reasoning)
  def setReasoning(v: Map[String, Any]): this.type = setScalarParam(reasoning, v)
  def getReasoningCol: String = getVectorParam(reasoning)
  def setReasoningCol(v: String): this.type = setVectorParam(reasoning, v)

  val include: ServiceParam[Seq[String]] = new ServiceParam[Seq[String]](
    this, "include", "Additional Responses output data, such as message.output_text.logprobs " +
      "or reasoning.encrypted_content.",
    isRequired = false)

  def getInclude: Seq[String] = getScalarParam(include)
  def setInclude(v: Seq[String]): this.type = setScalarParam(include, v)
  def getIncludeCol: String = getVectorParam(include)
  def setIncludeCol(v: String): this.type = setVectorParam(include, v)

  val truncation: ServiceParam[String] = new ServiceParam[String](
    this, "truncation", "Responses input truncation policy, such as disabled or auto.", isRequired = false)

  def getTruncation: String = getScalarParam(truncation)
  def setTruncation(v: String): this.type = setScalarParam(truncation, v)
  def getTruncationCol: String = getVectorParam(truncation)
  def setTruncationCol(v: String): this.type = setVectorParam(truncation, v)

  val logitBias: ServiceParam[Map[String, Int]] = new ServiceParam[Map[String, Int]](
    this, "logitBias", "Chat token-ID-to-bias mapping. Model support and bias limits are provider-validated.",
    isRequired = false) {
    override val payloadName: String = "logit_bias"
  }

  def getLogitBias: Map[String, Int] = getScalarParam(logitBias)
  def setLogitBias(v: Map[String, Int]): this.type = setScalarParam(logitBias, v)
  def getLogitBiasCol: String = getVectorParam(logitBias)
  def setLogitBiasCol(v: String): this.type = setVectorParam(logitBias, v)

  val stopSequences: ServiceParam[Seq[String]] = new ServiceParam[Seq[String]](
    this, "stopSequences", "Chat stop sequences. Use either stop (one string) or stopSequences, not both.",
    isRequired = false) {
    override val payloadName: String = "stop_sequences"
  }

  def getStopSequences: Seq[String] = getScalarParam(stopSequences)
  def setStopSequences(v: Seq[String]): this.type = setScalarParam(stopSequences, v)
  def getStopSequencesCol: String = getVectorParam(stopSequences)
  def setStopSequencesCol(v: String): this.type = setVectorParam(stopSequences, v)

  val logProbsEnabled: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this, "logProbsEnabled",
    "Return output token log probabilities. Chat uses logprobs; Responses uses include=message.output_text.logprobs.",
    isRequired = false) {
    override val payloadName: String = "log_probs_enabled"
  }

  def getLogProbsEnabled: Boolean = getScalarParam(logProbsEnabled)
  def setLogProbsEnabled(v: Boolean): this.type = setScalarParam(logProbsEnabled, v)
  def getLogProbsEnabledCol: String = getVectorParam(logProbsEnabled)
  def setLogProbsEnabledCol(v: String): this.type = setVectorParam(logProbsEnabled, v)

  val topLogProbs: ServiceParam[Int] = new ServiceParam[Int](
    this, "topLogProbs", "Number of alternative tokens whose log probabilities to return; enables log probabilities.",
    isRequired = false) {
    override val payloadName: String = "top_logprobs"
  }

  def getTopLogProbs: Int = getScalarParam(topLogProbs)
  def setTopLogProbs(v: Int): this.type = setScalarParam(topLogProbs, v)
  def getTopLogProbsCol: String = getVectorParam(topLogProbs)
  def setTopLogProbsCol(v: String): this.type = setVectorParam(topLogProbs, v)

  override private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    val modelParams = Seq(promptCacheKey, promptCacheRetention, safetyIdentifier, serviceTier, metadata,
      reasoning, include, truncation, logitBias, stopSequences, logProbsEnabled, topLogProbs)
    super.getOptionalParams(r) ++ modelParams.flatMap { param =>
      getValueOpt(r, param).map(value => param.payloadName -> normalizeCacheOptionValue(value))
    }.toMap
  }

  protected def rejectUnsupportedParams(r: Row, api: String, unsupported: ServiceParam[_]*): Unit = {
    val configured = unsupported.filter(param => getValueOpt(r, param).isDefined).map(_.name)
    require(configured.isEmpty, s"${configured.mkString(", ")} not supported by $api.")
  }

  protected def resolveStopSequences(params: Map[String, Any]): Map[String, Any] = {
    require(!(params.contains("stop") && params.contains("stop_sequences")),
      "Cannot set both stop and stopSequences.")
    params.get("stop_sequences").map(value => (params - "stop_sequences").updated("stop", value)).getOrElse(params)
  }

  protected def resolveLogProbParams(params: Map[String, Any], responses: Boolean): Map[String, Any] = {
    require(!(params.contains("logprobs") && params.contains("top_logprobs")),
      "Cannot set both logProbs (legacy count) and topLogProbs.")
    val count = params.get("top_logprobs").orElse(params.get("logprobs"))
    val enabled = params.get("log_probs_enabled").map {
      case value: Boolean => value
      case _ => throw new IllegalArgumentException("logProbsEnabled must be boolean")
    }.orElse(count.map(_ => true))
    require(enabled.forall(identity) || count.isEmpty,
      "logProbsEnabled=false conflicts with logProbs or topLogProbs.")
    val base = params -- Seq("logprobs", "log_probs_enabled", "top_logprobs")
    val withCount = count.map(value => base.updated("top_logprobs", value)).getOrElse(base)
    if (responses) {
      val included = params.get("include").toSeq.flatMap {
        case values: Seq[_] => values
        case _ => throw new IllegalArgumentException("include must be an array")
      }
      require(enabled.forall(identity) || !included.contains("message.output_text.logprobs"),
        "logProbsEnabled=false conflicts with include=message.output_text.logprobs.")
      if (enabled.contains(true)) {
        withCount.updated("include", (included :+ "message.output_text.logprobs").distinct)
      } else {
        withCount
      }
    } else {
      enabled.map(value => withCount.updated("logprobs", value)).getOrElse(withCount)
    }
  }
}
// scalastyle:on number.of.methods
