// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

import com.microsoft.azure.synapse.ml.codegen.GenerationUtils
import com.microsoft.azure.synapse.ml.cognitive.{
  CognitiveServicesBase, HasCognitiveServiceInput,
  HasInternalJsonOutputParser
}
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.language.existentials

object OpenAICompletion extends ComplexParamsReadable[OpenAICompletion]

class OpenAICompletion(override val uid: String) extends CognitiveServicesBase(uid)
  with HasOpenAIParams with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging {
  logClass()

  def this() = this(Identifiable.randomUID("OpenAPICompletion"))

  def urlPath: String = ""

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

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

      getValueOpt(r, prompt)
        .map(prompt => getStringEntity(prompt, optionalParams))
        .orElse(getValueOpt(r, batchPrompt)
          .map(batchPrompt => getStringEntity(batchPrompt, optionalParams)))
        .orElse(getValueOpt(r, indexPrompt)
          .map(indexPrompt => getStringEntity(indexPrompt, optionalParams)))
        .orElse(getValueOpt(r, batchIndexPrompt)
          .map(batchIndexPrompt => getStringEntity(batchIndexPrompt, optionalParams)))
        .orElse(throw new IllegalArgumentException(
          "Please set one of prompt, batchPrompt, indexPrompt or batchIndexPrompt."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def responseDataType: DataType = CompletionResponse.schema

  private[this] def getStringEntity[A](prompt: A, optionalParams: Map[String, Any]): StringEntity = {
    val fullPayload = optionalParams.updated("prompt", prompt)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

}
