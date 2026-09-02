// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.Secrets

trait CognitiveKey {
  private val cognitiveTokenResource = "https://cognitiveservices.azure.com/"

  lazy val cognitiveAADToken: String =
    sys.env.getOrElse("COGNITIVE_AAD_TOKEN", Secrets.getAccessToken(cognitiveTokenResource))

  lazy val cognitiveServiceName: String =
    sys.env.getOrElse("COGNITIVE_SERVICE_NAME", "mmlspark-cs")

  lazy val cognitiveResourceId: String =
    sys.env.getOrElse(
      "COGNITIVE_RESOURCE_ID",
      "/subscriptions/e342c2c0-f844-4b18-9208-52c8c234c30e/resourceGroups/" +
        "marhamil-mmlspark/providers/Microsoft.CognitiveServices/accounts/mmlspark-cs")

  lazy val speechAADToken: String =
    s"aad#$cognitiveResourceId#$cognitiveAADToken"

  protected implicit class CognitiveTestAuthOps[T <: CognitiveServicesBaseNoHandler](stage: T) {
    def setCognitiveTestAuth: T =
      setCognitiveTestAuth(cognitiveServiceName)

    def setCognitiveTestAuth(serviceName: String): T = {
      stage.setAADToken(cognitiveAADToken)
      stage.setCustomServiceName(serviceName)
      stage
    }
  }
}
