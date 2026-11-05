// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

private[ai] abstract class AIPromptTestBase extends TestBase {
  protected def path(): String = tmpDir.resolve(java.util.UUID.randomUUID().toString).toAbsolutePath.toString

  protected def withServer(test: AIPromptRecordingServer => Unit): Unit = {
    val server = new AIPromptRecordingServer
    try test(server) finally server.close()
  }

  protected def prompt(server: AIPromptRecordingServer): AIPrompt =
    new AIPrompt().setUrl(server.url + "/v1").setDeploymentName("local-model").setSubscriptionKey("synthetic-key")
      .setPromptTemplate("{text}").setOutputCol("answer").setErrorCol("error")
      .setUsageCol("usage").setRawResponseCol("raw")
}
