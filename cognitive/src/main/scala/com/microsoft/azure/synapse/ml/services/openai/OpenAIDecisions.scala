// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.services.ai.AIPrompt
import org.apache.spark.ml.ExplicitComplexParamsReadable
import org.apache.spark.ml.util.Identifiable

object OpenAIDecisions extends ExplicitComplexParamsReadable[OpenAIDecisions] {
  override protected def expectedClass: Class[OpenAIDecisions] = classOf[OpenAIDecisions]
}

/** Native OpenAI Decisions requests; endpoint availability must be qualified separately. */
class OpenAIDecisions(override val uid: String) extends AIPrompt(uid) {
  def this() = this(Identifiable.randomUID("OpenAIDecisions"))

  setDefault(apiVersion -> Left("v1"))

  override protected def supportedApiTypes: Array[String] = Array("decisions")
  override protected def defaultApiType: String = "decisions"
}
