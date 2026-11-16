// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.{SparkSessionManagement, TestBase}

/** Delta suites require a caller-provided test runtime; absence is reported as a skip. */
private[ai] abstract class AIPromptOutputTestBase extends TestBase {
  override lazy val sparkProvider: SparkSessionManagement = AIPromptDeltaTestSession

  protected def path(): String = tmpDir.resolve(java.util.UUID.randomUUID().toString).toAbsolutePath.toString

  protected def delta(): Unit = {
    val available = AIPromptDeltaTestSession.hasDelta
    if (sys.env.get("SYNAPSEML_REQUIRE_DELTA_TESTS").contains("true")) {
      assert(available, "SYNAPSEML_REQUIRE_DELTA_TESTS=true requires Delta on the cognitive test classpath.")
    }
    assume(available, "This integration test requires a caller-provided Delta test classpath.")
  }

  protected def withServer(test: AIPromptRecoveryRecordingServer => Unit): Unit = {
    val server = new AIPromptRecoveryRecordingServer
    try test(server) finally server.close()
  }

  protected def prompt(path: String, server: AIPromptRecoveryRecordingServer): AIPrompt =
    AIPromptStorageTestConfig.configure(new AIPrompt(), path, server.url)

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestBase.stopSparkSession()
    sparkProvider.resetSparkSession(numCores = Some(2))
  }

  override def afterAll(): Unit = {
    try sparkProvider.stopSparkSession() finally super.afterAll()
  }
}
