// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.HasServiceParams
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._

import scala.collection.JavaConverters._

/** Modern fields supported by synchronous Responses API transforms.
  *
  * Every field is unset by default. Streaming, background lifecycle, compaction, automatic
  * tool execution, and other asynchronous orchestration surfaces are intentionally excluded.
  */
// scalastyle:off number.of.methods
trait HasOpenAIResponsesModernParams extends HasServiceParams {
  val instructions: ServiceParam[String] = stringParam(
    "instructions",
    "System or developer instructions. Resend them on every continuation turn.",
    "instructions")

  val truncation: ServiceParam[String] = stringParam(
    "truncation",
    "Responses truncation mode. Values are provider-defined and not closed by SynapseML.",
    "truncation")

  val metadata: ServiceParam[Map[String, String]] = new ServiceParam[Map[String, String]](
    this,
    "metadata",
    "Up to 16 provider-visible key/value pairs. Do not place secrets or PII here.",
    isRequired = false) {
    override val payloadName: String = "metadata"
  }

  val include: ServiceParam[Seq[String]] = new ServiceParam[Seq[String]](
    this,
    "include",
    "Additional response fields to include. Membership is provider-defined.",
    isRequired = false) {
    override val payloadName: String = "include"
  }

  val topLogprobs: ServiceParam[Int] = new ServiceParam[Int](
    this,
    "topLogprobs",
    "Number of top log probabilities to include, from 0 through 20.",
    isRequired = false) {
    override val payloadName: String = "top_logprobs"
  }

  val safetyIdentifier: ServiceParam[String] = stringParam(
    "safetyIdentifier",
    "Pseudonymous provider-visible end-user identifier of at most 64 characters.",
    "safety_identifier")

  val promptCacheKey: ServiceParam[String] = stringParam(
    "promptCacheKey",
    "Provider-visible prompt-prefix cache key. Prefer a per-row column for wide jobs.",
    "prompt_cache_key")

  val serviceTier: ServiceParam[String] = stringParam(
    "serviceTier",
    "OpenAI service tier. Azure OpenAI does not currently support this field.",
    "service_tier")

  val conversation: ServiceParam[String] = stringParam(
    "conversation",
    "Server-side conversation id. Concurrent partitions sharing one id are non-deterministic.",
    "conversation")

  val reasoningSummary: ServiceParam[String] = stringParam(
    "reasoningSummary",
    "Value emitted as reasoning.summary.",
    "reasoning_summary")

  val reasoningContext: ServiceParam[String] = stringParam(
    "reasoningContext",
    "Value emitted as reasoning.context. Azure support is endpoint-dependent.",
    "reasoning_context")

  val reasoningMode: ServiceParam[String] = stringParam(
    "reasoningMode",
    "Value emitted as reasoning.mode. Azure support is endpoint-dependent.",
    "reasoning_mode")

  private def stringParam(name: String, doc: String, wireName: String): ServiceParam[String] =
    new ServiceParam[String](this, name, doc, isRequired = false) {
      override val payloadName: String = wireName
    }

  def getInstructions: String = getScalarParam(instructions)

  def setInstructions(value: String): this.type = setNonBlank(instructions, value)

  def getInstructionsCol: String = getVectorParam(instructions)

  def setInstructionsCol(value: String): this.type = setVectorParam(instructions, value)

  def getTruncation: String = getScalarParam(truncation)

  def setTruncation(value: String): this.type = setNonBlank(truncation, value)

  def getTruncationCol: String = getVectorParam(truncation)

  def setTruncationCol(value: String): this.type = setVectorParam(truncation, value)

  def getMetadata: Map[String, String] = getScalarParam(metadata)

  private[openai] def getMetadataJava: java.util.Map[String, String] = getMetadata.asJava

  def setMetadata(value: Map[String, String]): this.type = {
    require(value != null, "metadata must not be null") //scalastyle:ignore null
    require(
      value.size <= OpenAIToolUtils.MaxMetadataEntries,
      s"metadata supports at most ${OpenAIToolUtils.MaxMetadataEntries} entries")
    value.foreach { case (key, itemValue) =>
      require(
        key != null && key.length <= OpenAIToolUtils.MaxMetadataKeyChars, //scalastyle:ignore null
        s"metadata key exceeds ${OpenAIToolUtils.MaxMetadataKeyChars} characters")
      require(
        itemValue != null && itemValue.length <= OpenAIToolUtils.MaxMetadataValueChars,
        s"metadata value for key '$key' exceeds " +
          s"${OpenAIToolUtils.MaxMetadataValueChars} characters") //scalastyle:ignore null
    }
    setScalarParam(metadata, value)
  }

  def setMetadata(value: java.util.Map[String, String]): this.type =
    setMetadata(value.asScala.toMap)

  def getInclude: Seq[String] = getScalarParam(include)

  private[openai] def getIncludeJava: java.util.List[String] = getInclude.asJava

  def setInclude(value: Seq[String]): this.type = {
    require(value != null && value.nonEmpty, "include must not be empty") //scalastyle:ignore null
    require(
      value.forall(item => Option(item).exists(_.trim.nonEmpty)),
      "include entries must be non-blank")
    setScalarParam(include, value)
  }

  def setInclude(value: java.util.List[String]): this.type =
    setInclude(value.asScala.toList)

  def getTopLogprobs: Int = getScalarParam(topLogprobs)

  def setTopLogprobs(value: Int): this.type = {
    require(
      value >= 0 && value <= OpenAIToolUtils.MaxTopLogprobs,
      s"topLogprobs must be between 0 and ${OpenAIToolUtils.MaxTopLogprobs}")
    setScalarParam(topLogprobs, value)
  }

  def getTopLogprobsCol: String = getVectorParam(topLogprobs)

  def setTopLogprobsCol(value: String): this.type = setVectorParam(topLogprobs, value)

  def getSafetyIdentifier: String = getScalarParam(safetyIdentifier)

  def setSafetyIdentifier(value: String): this.type = {
    require(
      Option(value).exists(item =>
        item.trim.nonEmpty && item.length <= OpenAIToolUtils.MaxSafetyIdentifierChars),
      s"safetyIdentifier must be 1-${OpenAIToolUtils.MaxSafetyIdentifierChars} characters")
    setScalarParam(safetyIdentifier, value)
  }

  def getSafetyIdentifierCol: String = getVectorParam(safetyIdentifier)

  def setSafetyIdentifierCol(value: String): this.type =
    setVectorParam(safetyIdentifier, value)

  def getPromptCacheKey: String = getScalarParam(promptCacheKey)

  def setPromptCacheKey(value: String): this.type = setNonBlank(promptCacheKey, value)

  def getPromptCacheKeyCol: String = getVectorParam(promptCacheKey)

  def setPromptCacheKeyCol(value: String): this.type = setVectorParam(promptCacheKey, value)

  def getServiceTier: String = getScalarParam(serviceTier)

  def setServiceTier(value: String): this.type = setNonBlank(serviceTier, value)

  def getServiceTierCol: String = getVectorParam(serviceTier)

  def setServiceTierCol(value: String): this.type = setVectorParam(serviceTier, value)

  def getConversation: String = getScalarParam(conversation)

  def setConversation(value: String): this.type = setNonBlank(conversation, value)

  def getConversationCol: String = getVectorParam(conversation)

  def setConversationCol(value: String): this.type = setVectorParam(conversation, value)

  def getReasoningSummary: String = getScalarParam(reasoningSummary)

  def setReasoningSummary(value: String): this.type = setNonBlank(reasoningSummary, value)

  def getReasoningSummaryCol: String = getVectorParam(reasoningSummary)

  def setReasoningSummaryCol(value: String): this.type =
    setVectorParam(reasoningSummary, value)

  def getReasoningContext: String = getScalarParam(reasoningContext)

  def setReasoningContext(value: String): this.type = setNonBlank(reasoningContext, value)

  def getReasoningContextCol: String = getVectorParam(reasoningContext)

  def setReasoningContextCol(value: String): this.type =
    setVectorParam(reasoningContext, value)

  def getReasoningMode: String = getScalarParam(reasoningMode)

  def setReasoningMode(value: String): this.type = setNonBlank(reasoningMode, value)

  def getReasoningModeCol: String = getVectorParam(reasoningMode)

  def setReasoningModeCol(value: String): this.type =
    setVectorParam(reasoningMode, value)

  private def setNonBlank(param: ServiceParam[String], value: String): this.type = {
    require(Option(value).exists(_.trim.nonEmpty), s"${param.name} must be non-blank")
    setScalarParam(param, value)
  }

  private[openai] def modernResponsesParams: Seq[ServiceParam[_]] = Seq(
    instructions,
    truncation,
    metadata,
    include,
    topLogprobs,
    safetyIdentifier,
    promptCacheKey,
    serviceTier,
    conversation
  )

  private[openai] def modernParamNames: Seq[String] =
    (modernResponsesParams ++ Seq(
      reasoningSummary,
      reasoningContext,
      reasoningMode
    )).map(_.name)

  private[openai] def mergeReasoningExtras(
      params: Map[String, Any],
      row: Row): Map[String, Any] = {
    val extras = Seq(
      "summary" -> getValueOpt(row, reasoningSummary),
      "context" -> getValueOpt(row, reasoningContext),
      "mode" -> getValueOpt(row, reasoningMode)
    ).collect { case (key, Some(value)) => key -> (value: Any) }

    if (extras.isEmpty) {
      params
    } else {
      val existing = params.get("reasoning").collect {
        case values: Map[_, _] => values.asInstanceOf[Map[String, Any]]
      }.getOrElse(Map.empty)
      params.updated("reasoning", existing ++ extras)
    }
  }
}
// scalastyle:on number.of.methods

trait HasResponsesInputParams extends Params {
  val functionCallOutputsCol: Param[String] = new Param[String](
    this,
    "functionCallOutputsCol",
    "ARRAY<STRUCT<call_id,output,status>> column serialized as function_call_output items.")

  def getFunctionCallOutputsCol: String = $(functionCallOutputsCol)

  def setFunctionCallOutputsCol(value: String): this.type =
    set(functionCallOutputsCol, value)

  private[openai] def hasFunctionCallOutputsCol: Boolean =
    isSet(functionCallOutputsCol)

  val inputItemsCol: Param[String] = new Param[String](
    this,
    "inputItemsCol",
    "Trusted STRING column containing a JSON array of raw Responses input items.")

  def getInputItemsCol: String = $(inputItemsCol)

  def setInputItemsCol(value: String): this.type = set(inputItemsCol, value)

  private[openai] def hasInputItemsCol: Boolean = isSet(inputItemsCol)
}
