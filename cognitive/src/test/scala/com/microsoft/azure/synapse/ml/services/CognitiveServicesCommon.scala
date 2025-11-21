// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.Secrets

trait CognitiveKey {
  lazy val cognitiveKeyOption: Option[String] = {
    sys.env.get("COGNITIVE_API_KEY").orElse {
      scala.util.Try(Secrets.CognitiveApiKey).toOption
    }
  }

  lazy val cognitiveKey: String = cognitiveKeyOption.getOrElse("")
  lazy val cognitiveLoc = sys.env.getOrElse("COGNITIVE_API_LOC", "eastus")
}
