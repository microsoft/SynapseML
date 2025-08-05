// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAISubscriptionKey
import com.microsoft.azure.synapse.ml.io.http.URLKey

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

  private def extractLeft[T](optEither: Option[Either[T, String]]): Option[T] = {
    optEither match {
      case Some(Left(v)) => Some(v)
      case _ => None
    }
  }
}
