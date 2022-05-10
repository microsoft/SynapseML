// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.codegen.{GenerationUtils, Wrappable}
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._
import AnyJsonFormat.anyFormat
import scala.language.existentials


trait HasSetServiceName extends Wrappable with HasURL {
  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def setServiceName(self, value):
      |    self._java_obj = self._java_obj.setServiceName(value)
      |    return self
      |""".stripMargin
  }

  def setServiceName(v: String): this.type = {
    setUrl(s"https://${v}.openai.azure.com/")
  }
}

trait HasPrompt extends HasServiceParams {
  val prompt: ServiceParam[String] = new ServiceParam[String](
    this, "prompt", "The text to complete", isRequired = true)

  def getPrompt: String = getScalarParam(prompt)

  def setPrompt(v: String): this.type = setScalarParam(prompt, v)

  def getPromptCol: String = getVectorParam(prompt)

  def setPromptCol(v: String): this.type = setVectorParam(prompt, v)
}

trait HasAPIVersion extends HasServiceParams {
  val apiVersion: ServiceParam[String] = new ServiceParam[String](
    this, "apiVersion", "version of the api", isRequired = true, isURLParam = true) {
    override val payloadName: String = "api-version"
  }

  def getApiVersion: String = getScalarParam(apiVersion)

  def setApiVersion(v: String): this.type = setScalarParam(apiVersion, v)

  def getApiVersionCol: String = getVectorParam(apiVersion)

  def setApiVersionCol(v: String): this.type = setVectorParam(apiVersion, v)

  setDefault(apiVersion -> Left("2022-03-01-preview"))
}

trait HasDeploymentName extends HasServiceParams {
  val deploymentName = new ServiceParam[String](
    this, "deploymentName", "The name of the deployment", isRequired = true)

  def getDeploymentName: String = getScalarParam(deploymentName)

  def setDeploymentName(v: String): this.type = setScalarParam(deploymentName, v)

  def getDeploymentNameCol: String = getVectorParam(deploymentName)

  def setDeploymentNameCol(v: String): this.type = setVectorParam(deploymentName, v)
}

trait HasMaxTokens extends HasServiceParams {

  val maxTokens: ServiceParam[Int] = new ServiceParam[Int](
    this, "maxTokens",
    "The maximum number of tokens to generate. Has minimum of 0.",
    isRequired = false)

  def getMaxTokens: Int = getScalarParam(maxTokens)

  def setMaxTokens(v: Int): this.type = setScalarParam(maxTokens, v)

  def getMaxTokensCol: String = getVectorParam(maxTokens)

  def setMaxTokensCol(v: String): this.type = setVectorParam(maxTokens, v)

}

trait HasOpenAIParams extends HasServiceParams
  with HasSetServiceName with HasPrompt with HasAPIVersion with HasDeploymentName with HasMaxTokens {

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

  val topP: ServiceParam[Double] = new ServiceParam[Double](
    this, "topP",
    "An alternative to sampling with temperature, called nucleus sampling, where the model considers the" +
      " results of the tokens with top_p probability mass." +
      " So 0.1 means only the tokens comprising the top 10% probability mass are considered." +
      " We generally recommend using this or `temperature` but not both." +
      " Minimum of 0 and maximum of 1 allowed.",
    isRequired = false)

  def getTopP: Double = getScalarParam(topP)

  def setTopP(v: Double): this.type = setScalarParam(topP, v)

  def getTopPCol: String = getVectorParam(topP)

  def setTopPCol(v: String): this.type = setVectorParam(topP, v)

  val user: ServiceParam[String] = new ServiceParam[String](
    this, "user",
    "The ID of the end-user, for use in tracking and rate-limiting.",
    isRequired = false)

  def getUser: String = getScalarParam(user)

  def setUser(v: String): this.type = setScalarParam(user, v)

  def getUserCol: String = getVectorParam(user)

  def setUserCol(v: String): this.type = setVectorParam(user, v)

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

  val model: ServiceParam[String] = new ServiceParam[String](
    this, "model",
    "The name of the model to use",
    isRequired = false)

  def getModel: String = getScalarParam(model)

  def setModel(v: String): this.type = setScalarParam(model, v)

  def getModelCol: String = getVectorParam(model)

  def setModelCol(v: String): this.type = setVectorParam(model, v)

  val echo: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this, "echo",
    "Echo back the prompt in addition to the completion",
    isRequired = false)

  def getEcho: Boolean = getScalarParam(echo)

  def setEcho(v: Boolean): this.type = setScalarParam(echo, v)

  def getEchoCol: String = getVectorParam(echo)

  def setEchoCol(v: String): this.type = setVectorParam(echo, v)

  val stop: ServiceParam[String] = new ServiceParam[String](
    this, "stop",
    "A sequence which indicates the end of the current document.",
    isRequired = false)

  def getStop: String = getScalarParam(stop)

  def setStop(v: String): this.type = setScalarParam(stop, v)

  def getStopCol: String = getVectorParam(stop)

  def setStopCol(v: String): this.type = setVectorParam(stop, v)

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
      " Decreases the model's likelihood to repeat the same line verbatim. Has minimum of -2 and maximum of 2.",
    isRequired = false)

  def getPresencePenalty: Double = getScalarParam(presencePenalty)

  def setPresencePenalty(v: Double): this.type = setScalarParam(presencePenalty, v)

  def getPresencePenaltyCol: String = getVectorParam(presencePenalty)

  def setPresencePenaltyCol(v: String): this.type = setVectorParam(presencePenalty, v)

  val frequencyPenalty: ServiceParam[Double] = new ServiceParam[Double](
    this, "frequencyPenalty",
    "How much to penalize new tokens based on whether they appear in the text so far." +
      " Increases the model's likelihood to talk about new topics.",
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

}

object OpenAICompletion extends ComplexParamsReadable[OpenAICompletion]

class OpenAICompletion(override val uid: String) extends CognitiveServicesBase(uid)
  with HasOpenAIParams with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("OpenAPICompletion"))

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/completions"
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = Seq(
        maxTokens,
        temperature,
        topP,
        user,
        n,
        model,
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

      getValueOpt(r, prompt).map { prompt =>
        val fullPayload = optionalParams.updated("prompt", prompt)
        new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
      }
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def responseDataType: DataType = CompletionResponse.schema

}
