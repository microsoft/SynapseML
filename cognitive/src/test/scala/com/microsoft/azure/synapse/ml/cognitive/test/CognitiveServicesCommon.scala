package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.Secrets

trait CognitiveKey {
  lazy val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", Secrets.CognitiveApiKey)
}