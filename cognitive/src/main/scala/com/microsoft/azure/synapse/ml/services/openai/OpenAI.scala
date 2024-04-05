// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.codegen.GenerationUtils
import com.microsoft.azure.synapse.ml.fabric.{FabricClient, OpenAIFabricSetting, OpenAITokenLibrary}
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services._
import org.apache.spark.ml.PipelineModel
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

trait HasOpenAISharedParams extends HasServiceParams with HasAPIVersion {

  val deploymentName = new ServiceParam[String](
    this, "deploymentName", "The name of the deployment", isRequired = true)

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

  setDefault(apiVersion -> Left("2024-02-01"))

}

trait HasOpenAITextParams extends HasOpenAISharedParams {

  val maxTokens: ServiceParam[Int] = new ServiceParam[Int](
    this, "maxTokens",
    "The maximum number of tokens to generate. Has minimum of 0.",
    isRequired = false)

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
    isRequired = false)

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
    isRequired = false)

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
    isRequired = false)

  def getCacheLevel: Int = getScalarParam(cacheLevel)

  def setCacheLevel(v: Int): this.type = setScalarParam(cacheLevel, v)

  def getCacheLevelCol: String = getVectorParam(cacheLevel)

  def setCacheLevelCol(v: String): this.type = setVectorParam(cacheLevel, v)

  val presencePenalty: ServiceParam[Double] = new ServiceParam[Double](
    this, "presencePenalty",
    "How much to penalize new tokens based on their existing frequency in the text so far." +
      " Decreases the likelihood of the model to repeat the same line verbatim. Has minimum of -2 and maximum of 2.",
    isRequired = false)

  def getPresencePenalty: Double = getScalarParam(presencePenalty)

  def setPresencePenalty(v: Double): this.type = setScalarParam(presencePenalty, v)

  def getPresencePenaltyCol: String = getVectorParam(presencePenalty)

  def setPresencePenaltyCol(v: String): this.type = setVectorParam(presencePenalty, v)

  val frequencyPenalty: ServiceParam[Double] = new ServiceParam[Double](
    this, "frequencyPenalty",
    "How much to penalize new tokens based on whether they appear in the text so far." +
      " Increases the likelihood of the model to talk about new topics.",
    isRequired = false)

  def getFrequencyPenalty: Double = getScalarParam(frequencyPenalty)

  def setFrequencyPenalty(v: Double): this.type = setScalarParam(frequencyPenalty, v)

  def getFrequencyPenaltyCol: String = getVectorParam(frequencyPenalty)

  def setFrequencyPenaltyCol(v: String): this.type = setVectorParam(frequencyPenalty, v)

  val bestOf: ServiceParam[Int] = new ServiceParam[Int](
    this, "bestOf",
    "How many generations to create server side, and display only the best." +
      " Will not stream intermediate progress if best_of > 1. Has maximum value of 128.",
    isRequired = false)

  def getBestOf: Int = getScalarParam(bestOf)

  def setBestOf(v: Int): this.type = setScalarParam(bestOf, v)

  def getBestOfCol: String = getVectorParam(bestOf)

  def setBestOfCol(v: String): this.type = setVectorParam(bestOf, v)

  private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    Seq(
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
      bestOf
    ).flatMap(param =>
      getValueOpt(r, param).map(v => (GenerationUtils.camelToSnake(param.name), v))
    ).++(Seq(
      getValueOpt(r, logProbs).map(v => ("logprobs", v))
    ).flatten).toMap
  }
}

trait HasOpenAICognitiveServiceInput extends HasCognitiveServiceInput {
  override protected def getCustomAuthHeader(row: Row): Option[String] = {
    val providedCustomHeader = getValueOpt(row, CustomAuthHeader)
    if (providedCustomHeader.isEmpty && PlatformDetails.runningOnFabric()) {
      logInfo("Using Default OpenAI Token On Fabric")
      Option(OpenAITokenLibrary.getAuthHeader)
    } else {
      providedCustomHeader
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
      getModelStatus(getDeploymentName)
    }
    super.getInternalTransformer(schema)
  }
}
