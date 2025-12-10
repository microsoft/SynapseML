// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.codegen.GenerationUtils
import com.microsoft.azure.synapse.ml.fabric.{FabricClient, OpenAIFabricSetting}
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails
import com.microsoft.azure.synapse.ml.param.{GlobalKey, GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._

import scala.language.existentials

trait HasPromptInputs extends HasServiceParams {
  val prompt: ServiceParam[String] = new ServiceParam[String](
    this, "prompt", "The text to complete", isRequired = false)

  def getPrompt: String = getScalarParam(prompt)

  def setPrompt(v: String): this.type = setScalarParam(prompt, v)

  def getPromptCol: String = getVectorParam(prompt)

  def setPromptCol(v: String): this.type = setVectorParam(prompt, v)

  val batchPrompt: ServiceParam[Seq[String]] = new ServiceParam[Seq[String]](
    this, "batchPrompt", "Sequence of prompts to complete", isRequired = false)

  def getBatchPrompt: Seq[String] = getScalarParam(batchPrompt)

  def setBatchPrompt(v: Seq[String]): this.type = setScalarParam(batchPrompt, v)

  def getBatchPromptCol: String = getVectorParam(batchPrompt)

  def setBatchPromptCol(v: String): this.type = setVectorParam(batchPrompt, v)

}

trait HasMessagesInput extends Params {
  val messagesCol: Param[String] = new Param[String](
    this, "messagesCol", "The column messages to generate chat completions for," +
      " in the chat format. This column should have type Array(Struct(role: String, content: String)).")

  def getMessagesCol: String = $(messagesCol)

  def setMessagesCol(v: String): this.type = set(messagesCol, v)
}

case object OpenAIDeploymentNameKey extends GlobalKey[Either[String, String]]
case object OpenAIEmbeddingDeploymentNameKey extends GlobalKey[Either[String, String]]

trait HasOpenAISharedParams extends HasServiceParams with HasAPIVersion {

  val deploymentName = new ServiceParam[String](
    this, "deploymentName", "The name of the deployment", isRequired = false)

  GlobalParams.registerParam(deploymentName, OpenAIDeploymentNameKey)

  def getDeploymentName: String = getScalarParam(deploymentName)

  def setDeploymentName(v: String): this.type = setScalarParam(deploymentName, v)

  def getDeploymentNameCol: String = getVectorParam(deploymentName)

  def setDeploymentNameCol(v: String): this.type = setVectorParam(deploymentName, v)

  val user: ServiceParam[String] = new ServiceParam[String](
    this, "user",
    "The ID of the end-user, for use in tracking and rate-limiting.",
    isRequired = false)

  def getUser: String = getScalarParam(user)

  def setUser(v: String): this.type = setScalarParam(user, v)

  def getUserCol: String = getVectorParam(user)

  def setUserCol(v: String): this.type = setVectorParam(user, v)

}

trait HasOpenAIEmbeddingParams extends HasOpenAISharedParams with HasAPIVersion {

  val dimensions: ServiceParam[Int] = new ServiceParam[Int](
    this, "dimensions", "Number of dimensions for output embeddings.", isRequired = false)

  def getDimensions: Int = getScalarParam(dimensions)

  def setDimensions(value: Int): this.type = setScalarParam(dimensions, value)

  private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    Seq(
      dimensions
    ).flatMap(param =>
      getValueOpt(r, param).map(v => (GenerationUtils.camelToSnake(param.name), v))
    ).toMap
  }
}

case object OpenAITemperatureKey extends GlobalKey[Either[Double, String]]
case object OpenAISeedKey extends GlobalKey[Either[Int, String]]
case object OpenAITopPKey extends GlobalKey[Either[Double, String]]
case object OpenAIVerbosityKey extends GlobalKey[Either[String, String]]
case object OpenAIReasoningEffortKey extends GlobalKey[Either[String, String]]
case object OpenAIApiTypeKey extends GlobalKey[String]

// scalastyle:off number.of.methods
trait HasOpenAITextParams extends HasOpenAISharedParams {
  val maxTokens: ServiceParam[Int] = new ServiceParam[Int](
    this, "maxTokens",
    "The maximum number of tokens to generate. Has minimum of 0.",
    isRequired = false) {
    override val payloadName: String = "max_tokens"
  }

  def getMaxTokens: Int = getScalarParam(maxTokens)

  def setMaxTokens(v: Int): this.type = setScalarParam(maxTokens, v)

  def getMaxTokensCol: String = getVectorParam(maxTokens)

  def setMaxTokensCol(v: String): this.type = setVectorParam(maxTokens, v)

  val temperature: ServiceParam[Double] = new ServiceParam[Double](
    this, "temperature",
    "What sampling temperature to use. Higher values means the model will take more risks." +
      " Try 0.9 for more creative applications, and 0 (argmax sampling) for ones with a well-defined answer." +
      " We generally recommend using this or `top_p` but not both. Minimum of 0 and maximum of 2 allowed.",
    isRequired = false)

  GlobalParams.registerParam(temperature, OpenAITemperatureKey)

  def getTemperature: Double = getScalarParam(temperature)

  def setTemperature(v: Double): this.type = setScalarParam(temperature, v)

  def getTemperatureCol: String = getVectorParam(temperature)

  def setTemperatureCol(v: String): this.type = setVectorParam(temperature, v)

  val stop: ServiceParam[String] = new ServiceParam[String](
    this, "stop",
    "A sequence which indicates the end of the current document.",
    isRequired = false)

  def getStop: String = getScalarParam(stop)

  def setStop(v: String): this.type = setScalarParam(stop, v)

  def getStopCol: String = getVectorParam(stop)

  def setStopCol(v: String): this.type = setVectorParam(stop, v)

  val topP: ServiceParam[Double] = new ServiceParam[Double](
    this, "topP",
    "An alternative to sampling with temperature, called nucleus sampling, where the model considers the" +
      " results of the tokens with top_p probability mass." +
      " So 0.1 means only the tokens comprising the top 10 percent probability mass are considered." +
      " We generally recommend using this or `temperature` but not both." +
      " Minimum of 0 and maximum of 1 allowed.",
    isRequired = false) {
    override val payloadName: String = "top_p"
  }

  GlobalParams.registerParam(topP, OpenAITopPKey)

  def getTopP: Double = getScalarParam(topP)

  def setTopP(v: Double): this.type = setScalarParam(topP, v)

  def getTopPCol: String = getVectorParam(topP)

  def setTopPCol(v: String): this.type = setVectorParam(topP, v)

  val n: ServiceParam[Int] = new ServiceParam[Int](
    this, "n",
    "How many snippets to generate for each prompt. Minimum of 1 and maximum of 128 allowed.",
    isRequired = false)

  def getN: Int = getScalarParam(n)

  def setN(v: Int): this.type = setScalarParam(n, v)

  def getNCol: String = getVectorParam(n)

  def setNCol(v: String): this.type = setVectorParam(n, v)

  val logProbs: ServiceParam[Int] = new ServiceParam[Int](
    this, "logProbs",
    "Include the log probabilities on the `logprobs` most likely tokens, as well the chosen tokens." +
      " So for example, if `logprobs` is 10, the API will return a list of the 10 most likely tokens." +
      " If `logprobs` is 0, only the chosen tokens will have logprobs returned." +
      " Minimum of 0 and maximum of 100 allowed.",
    isRequired = false) {
    override val payloadName: String = "logprobs"
  }

  def getLogProbs: Int = getScalarParam(logProbs)

  def setLogProbs(v: Int): this.type = setScalarParam(logProbs, v)

  def getLogProbsCol: String = getVectorParam(logProbs)

  def setLogProbsCol(v: String): this.type = setVectorParam(logProbs, v)

  val echo: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this, "echo",
    "Echo back the prompt in addition to the completion",
    isRequired = false)

  def getEcho: Boolean = getScalarParam(echo)

  def setEcho(v: Boolean): this.type = setScalarParam(echo, v)

  def getEchoCol: String = getVectorParam(echo)

  def setEchoCol(v: String): this.type = setVectorParam(echo, v)

  val cacheLevel: ServiceParam[Int] = new ServiceParam[Int](
    this, "cacheLevel",
    "can be used to disable any server-side caching, 0=no cache, 1=prompt prefix enabled, 2=full cache",
    isRequired = false) {
    override val payloadName: String = "cache_level"
  }

  def getCacheLevel: Int = getScalarParam(cacheLevel)

  def setCacheLevel(v: Int): this.type = setScalarParam(cacheLevel, v)

  def getCacheLevelCol: String = getVectorParam(cacheLevel)

  def setCacheLevelCol(v: String): this.type = setVectorParam(cacheLevel, v)

  val presencePenalty: ServiceParam[Double] = new ServiceParam[Double](
    this, "presencePenalty",
    "How much to penalize new tokens based on their existing frequency in the text so far." +
      " Decreases the likelihood of the model to repeat the same line verbatim. Has minimum of -2 and maximum of 2.",
    isRequired = false) {
    override val payloadName: String = "presence_penalty"
  }

  def getPresencePenalty: Double = getScalarParam(presencePenalty)

  def setPresencePenalty(v: Double): this.type = setScalarParam(presencePenalty, v)

  def getPresencePenaltyCol: String = getVectorParam(presencePenalty)

  def setPresencePenaltyCol(v: String): this.type = setVectorParam(presencePenalty, v)

  val frequencyPenalty: ServiceParam[Double] = new ServiceParam[Double](
    this, "frequencyPenalty",
    "How much to penalize new tokens based on whether they appear in the text so far." +
      " Increases the likelihood of the model to talk about new topics.",
    isRequired = false) {
    override val payloadName: String = "frequency_penalty"
  }

  def getFrequencyPenalty: Double = getScalarParam(frequencyPenalty)

  def setFrequencyPenalty(v: Double): this.type = setScalarParam(frequencyPenalty, v)

  def getFrequencyPenaltyCol: String = getVectorParam(frequencyPenalty)

  def setFrequencyPenaltyCol(v: String): this.type = setVectorParam(frequencyPenalty, v)

  val bestOf: ServiceParam[Int] = new ServiceParam[Int](
    this, "bestOf",
    "How many generations to create server side, and display only the best." +
      " Will not stream intermediate progress if best_of > 1. Has maximum value of 128.",
    isRequired = false) {
    override val payloadName: String = "best_of"
  }

  def getBestOf: Int = getScalarParam(bestOf)

  def setBestOf(v: Int): this.type = setScalarParam(bestOf, v)

  def getBestOfCol: String = getVectorParam(bestOf)

  def setBestOfCol(v: String): this.type = setVectorParam(bestOf, v)

  val seed: ServiceParam[Int] = new ServiceParam[Int](
    this, "seed",
    "If specified, OpenAI will make a best effort to sample deterministically," +
      " such that repeated requests with the same seed and parameters should return the same result." +
      " Determinism is not guaranteed, and you should refer to the system_fingerprint response parameter" +
      " to monitor changes in the backend.",
    isRequired = false)

  GlobalParams.registerParam(seed, OpenAISeedKey)

  def getSeed: Int = getScalarParam(seed)
  def setSeed(v: Int): this.type = setScalarParam(seed, v)
  def getSeedCol: String = getVectorParam(seed)
  def setSeedCol(v: String): this.type = setVectorParam(seed, v)

  val verbosity: ServiceParam[String] = new ServiceParam[String](
    this, "verbosity",
    "Verbosity level hint for the model. Accepts 'low','medium','high' or any user-provided string.",
    isRequired = false) {
    override val payloadName: String = "verbosity"
  }

  GlobalParams.registerParam(verbosity, OpenAIVerbosityKey)

  def getVerbosity: String = getScalarParam(verbosity)
  def setVerbosity(v: String): this.type = setScalarParam(verbosity, v)
  def getVerbosityCol: String = getVectorParam(verbosity)
  def setVerbosityCol(v: String): this.type = setVectorParam(verbosity, v)

  val reasoningEffort: ServiceParam[String] = new ServiceParam[String](
    this, "reasoningEffort",
    "Reasoning effort hint for the model. Accepts 'minimal','low','medium','high' or any user string.",
    isRequired = false) {
    override val payloadName: String = "reasoning_effort"
  }

  GlobalParams.registerParam(reasoningEffort, OpenAIReasoningEffortKey)

  def getReasoningEffort: String = getScalarParam(reasoningEffort)
  def setReasoningEffort(v: String): this.type = setScalarParam(reasoningEffort, v)
  def getReasoningEffortCol: String = getVectorParam(reasoningEffort)
  def setReasoningEffortCol(v: String): this.type = setVectorParam(reasoningEffort, v)

  private[openai] val sharedTextParams: Seq[ServiceParam[_]] = Seq(
    maxTokens,
    temperature,
    topP,
    user,
    n,
    echo,
    stop,
    cacheLevel,
    presencePenalty,
    frequencyPenalty,
    bestOf,
    logProbs,
    seed,
    verbosity,
    reasoningEffort
  )

  private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    sharedTextParams.flatMap { param =>
      getValueOpt(r, param).map { value => param.payloadName -> value }
    }.toMap
  }
}
// scalastyle:on number.of.methods

trait HasRAIContentFilter {
  /**
   * Determines if the content in the output row was filtered by content safety
   * @param outputRow The output row from the API response
   * @return true if content was filtered, false otherwise
   */
  private[openai] def isContentFiltered(outputRow: Row): Boolean

  /**
   * Extracts the error/status reason from a filtered output row
   * @param outputRow The output row from the API response
   * @return The error reason (e.g., finish_reason or status)
   */
  private[openai] def getFilterReason(outputRow: Row): String
}

trait HasTextOutput {
  /**
   * Extract the text content from the output column for this specific API type
   */
  private[openai] def getOutputMessageText(outputColName: String): org.apache.spark.sql.Column

  /**
    * This one is used to convert messages from Rows
    * in both format of OpenAIMessage or OpenAICompositeMessage
    * to the format required by OpenAI API as a Map.
    *
    * @param messages
    * @return
    */
  private[openai] def encodeMessagesToMap(messages: Seq[Row]): Seq[Map[String, Any]] = {
    messages.map { row =>
      val role = row.getAs[String]("role")
      val contentField = row.schema.fieldIndex("content")
      val contentType = row.schema.fields(contentField).dataType

      val content = contentType.typeName match {
        case "string" =>
          // OpenAIMessage: content is a String
          row.getAs[String]("content")
        case "array" =>
          // OpenAICompositeMessage: content is Seq[Map[String, Any]]
          val rawContent = row.getAs[scala.collection.Seq[Map[String, Any]]]("content")
          rawContent.map(_.map { case (k, v) => k.toString -> v })
        case other =>
          throw new IllegalArgumentException(s"Unsupported content type: $other")
      }

      Map(
        "role" -> role,
        "content" -> content
      )
    }
  }
}


abstract class OpenAIServicesBase(override val uid: String) extends CognitiveServicesBase(uid: String)
  with HasOpenAISharedParams with OpenAIFabricSetting {
  setDefault(timeout -> 360.0)

  private def usingDefaultOpenAIEndpoint(): Boolean = {
    getUrl == FabricClient.MLWorkloadEndpointML + "/cognitive/openai/"
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    if (PlatformDetails.runningOnFabric() && usingDefaultOpenAIEndpoint) {
      assertModelStatus(getDeploymentName)
    }
    super.getInternalTransformer(schema)
  }
}
