// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets

/**
 * Shared OpenAI test configuration.
 * Centralizes how tests get API keys, service names, and default deployments.
 */
trait OpenAIAPIKey {
  // Prefer environment overrides to make CI/local runs configurable
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY_2", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = sys.env.getOrElse("OPENAI_SERVICE_NAME_2", "synapseml-openai-2")
  // Standardized test deployments
  lazy val deploymentName4p1: String = "gpt-4.1-mini"
  lazy val deploymentName5: String = "gpt-5-mini"
  // Default deployment when GPT-5 features are not required
  lazy val deploymentName: String = deploymentName4p1
}

