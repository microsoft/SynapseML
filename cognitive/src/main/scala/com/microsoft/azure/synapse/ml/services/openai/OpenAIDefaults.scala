// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.{OpenAIApiVersion, OpenAISubscriptionKey}
import com.microsoft.azure.synapse.ml.io.http.URLKey
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel

object OpenAIDefaults {
  def setDeploymentName(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, Left(v))
  }

  def getDeploymentName: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAIDeploymentNameKey))
  }

  def resetDeploymentName(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
  }

  def setSubscriptionKey(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAISubscriptionKey, Left(v))
  }

  def getSubscriptionKey: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAISubscriptionKey))
  }

  def resetSubscriptionKey(): Unit = {
    GlobalParams.resetGlobalParam(OpenAISubscriptionKey)
  }

  def setTemperature(v: Double): Unit = {
    require(v >= 0.0 && v <= 2.0, s"Temperature must be between 0.0 and 2.0, got: $v")
    GlobalParams.setGlobalParam(OpenAITemperatureKey, Left(v))
  }

  def getTemperature: Option[Double] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAITemperatureKey))
  }

  def resetTemperature(): Unit = {
    GlobalParams.resetGlobalParam(OpenAITemperatureKey)
  }

  def setURL(v: String): Unit = {
    val url = if (v.endsWith("/")) v else v + "/"
    GlobalParams.setGlobalParam(URLKey, url)
  }

  def getURL: Option[String] = {
    GlobalParams.getGlobalParam(URLKey)
  }

  def resetURL(): Unit = {
    GlobalParams.resetGlobalParam(URLKey)
  }

  def setSeed(v: Int): Unit = {
    GlobalParams.setGlobalParam(OpenAISeedKey, Left(v))
  }

  def getSeed: Option[Int] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAISeedKey))
  }

  def resetSeed(): Unit = {
    GlobalParams.resetGlobalParam(OpenAISeedKey)
  }

  def setTopP(v: Double): Unit = {
    require(v >= 0.0 && v <= 1.0, s"TopP must be between 0.0 and 1.0, got: $v")
    GlobalParams.setGlobalParam(OpenAITopPKey, Left(v))
  }

  def getTopP: Option[Double] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAITopPKey))
  }

  def resetTopP(): Unit = {
    GlobalParams.resetGlobalParam(OpenAITopPKey)
  }

  def setApiVersion(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAIApiVersion, Left(v))
  }

  def getApiVersion: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAIApiVersion))
  }

  def resetApiVersion(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIApiVersion)
  }

  def setModel(v: String): Unit = {
    GlobalParams.setGlobalParam(AIFoundryModel, Left(v))
  }

  def getModel: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(AIFoundryModel))
  }

  def resetModel(): Unit = {
    GlobalParams.resetGlobalParam(AIFoundryModel)
  }

  def setEmbeddingDeploymentName(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left(v))
  }

  def getEmbeddingDeploymentName: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey))
  }

  def resetEmbeddingDeploymentName(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIEmbeddingDeploymentNameKey)
  }

  def setVerbosity(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAIVerbosityKey, Left(v))
  }

  def getVerbosity: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAIVerbosityKey))
  }

  def resetVerbosity(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIVerbosityKey)
  }

  def setReasoningEffort(v: String): Unit = {
    GlobalParams.setGlobalParam(OpenAIReasoningEffortKey, Left(v))
  }

  def getReasoningEffort: Option[String] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAIReasoningEffortKey))
  }

  def resetReasoningEffort(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIReasoningEffortKey)
  }

  def setApiType(v: String): Unit = {
    require(
      v == "responses" || v == "chat_completions",
      s"ApiType must be either 'responses' or 'chat_completions', got: $v"
    )
    GlobalParams.setGlobalParam(OpenAIApiTypeKey, v)
  }

  def getApiType: Option[String] = {
    GlobalParams.getGlobalParam(OpenAIApiTypeKey)
  }

  def resetApiType(): Unit = {
    GlobalParams.resetGlobalParam(OpenAIApiTypeKey)
  }

  private def extractLeft[T](optEither: Option[Either[T, String]]): Option[T] = {
    optEither match {
      case Some(Left(v)) => Some(v)
      case _ => None
    }
  }
}
