// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAISubscriptionKey

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
    GlobalParams.setGlobalParam(OpenAITemperatureKey, Left(v))
  }

  def getTemperature: Option[Double] = {
    extractLeft(GlobalParams.getGlobalParam(OpenAITemperatureKey))
  }

  def resetTemperature(): Unit = {
    GlobalParams.resetGlobalParam(OpenAITemperatureKey)
  }

  private def extractLeft[T](optEither: Option[Either[T, String]]): Option[T] = {
    optEither match {
      case Some(Left(v)) => Some(v)
      case _ => None
    }
  }
}
