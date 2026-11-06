// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets

// Shared OpenAI test configuration.
// Centralizes how tests get API keys, service names, and default deployments.
trait OpenAIAPIKey {
  // Prefer environment overrides to make CI/local runs configurable
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY_3", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = sys.env.getOrElse("OPENAI_SERVICE_NAME_3", "synapseml-openai-3")
  lazy val deploymentName: String = "gpt-6-luna"
}
