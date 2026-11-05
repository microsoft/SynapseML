// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.ExplicitComplexParamsReadable
import org.apache.spark.ml.util.Identifiable

object AISystemOne extends ExplicitComplexParamsReadable[AISystemOne] {
  override protected def expectedClass: Class[AISystemOne] = classOf[AISystemOne]
}

/** Native Microsoft System One requests; no chat emulation or default model. */
class AISystemOne(override val uid: String) extends AIPrompt(uid) {
  def this() = this(Identifiable.randomUID("AISystemOne"))

  override protected def supportedApiTypes: Array[String] = Array("systemone")
  override protected def defaultApiType: String = "systemone"
}
