// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.Secrets

trait CognitiveKey {
  private val cognitiveTokenResource = "https://cognitiveservices.azure.com/"

  lazy val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", Secrets.CognitiveApiKey)
  lazy val cognitiveLoc = sys.env.getOrElse("COGNITIVE_API_LOC", "centralus")
  lazy val cognitiveAADToken =
    sys.env.getOrElse("COGNITIVE_AAD_TOKEN", Secrets.getAccessToken(cognitiveTokenResource))
}
