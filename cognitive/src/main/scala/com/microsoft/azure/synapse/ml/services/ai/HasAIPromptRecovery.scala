// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[ai] trait HasAIPromptRecovery extends Params { self: AIPrompt =>
  def resume(spark: SparkSession): DataFrame = AIPrompt.withSession(spark) {
    logVerb("resume", AIPromptOutput.resume(this, spark))
  }

  private[ai] def restoreDurableOutputDefaults(bindings: Map[String, String]): Unit = {
    val roles = Set("outputCol", "errorCol", "usageCol", "rawResponseCol", "responseIdCol", "messagesCol")
    require(bindings.keySet.subsetOf(roles), "Manifest contains unsupported output bindings")
    bindings.foreach { case (name, saved) =>
      val param = getParam(name).asInstanceOf[Param[String]]
      if (isSet(param)) {
        require(getOrDefault(param) == saved, s"Explicit $name does not match the saved output binding")
      } else if (Set("usageCol", "rawResponseCol", "responseIdCol")(name)) set(param, saved)
      else setDefault(param -> saved)
    }
  }
}
