// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.foundry

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services.openai._
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._

import scala.language.existentials

trait HasFoundryTextParamsExtended extends HasOpenAITextParamsExtended {
  val model = new ServiceParam[String](
    this, "model", "The name of the model", isRequired = true)

  GlobalParams.registerParam(model, OpenAIDeploymentNameKey)

  def getModel: String = getScalarParam(model)

  def setModel(v: String): this.type = setScalarParam(model, v)

  override val sharedTextParams: Seq[ServiceParam[_]] = Seq(
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
    responseFormat,
    model
  )
  }

object FoundryChatCompletion extends ComplexParamsReadable[FoundryChatCompletion]

class FoundryChatCompletion(override val uid: String) extends OpenAIChatCompletion
  with HasFoundryTextParamsExtended with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Foundry)

  def this() = this(Identifiable.randomUID("FoundryChatCompletion"))

  override private[ml] def internalServiceType: String = "foundry"

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.services.ai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}models/chat/completions"
  }

}

