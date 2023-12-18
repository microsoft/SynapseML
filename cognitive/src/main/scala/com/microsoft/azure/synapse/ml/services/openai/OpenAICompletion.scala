// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.services.{CognitiveServicesBase,
  HasCognitiveServiceInput, HasInternalJsonOutputParser}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
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

class OpenAICompletion(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParams with HasPromptInputs with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAICompletion"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/completions"
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      getValueOpt(r, prompt)
        .map(prompt => getStringEntity(prompt, optionalParams))
        .orElse(getValueOpt(r, batchPrompt)
          .map(batchPrompt => getStringEntity(batchPrompt, optionalParams)))
        .orElse(throw new IllegalArgumentException(
          "Please set one of prompt, batchPrompt, indexPrompt or batchIndexPrompt."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def shouldSkip(row: Row): Boolean =
    super.shouldSkip(row) ||
      (emptyParamData(row, prompt) && emptyParamData(row, batchPrompt))

  override def responseDataType: DataType = CompletionResponse.schema

  private[this] def getStringEntity[A](prompt: A, optionalParams: Map[String, Any]): StringEntity = {
    val fullPayload = optionalParams.updated("prompt", prompt)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }
}

