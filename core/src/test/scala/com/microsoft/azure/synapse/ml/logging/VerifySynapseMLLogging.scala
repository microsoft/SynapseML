// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.SparkSession

class VerifySynapseMLLogging extends TestBase {

  test("RequiredLogFields stores uid, className, and method") {
    val fields = RequiredLogFields("test-uid-123", "TestClass", "testMethod")
    assert(fields.uid === "test-uid-123")
    assert(fields.className === "TestClass")
    assert(fields.method === "testMethod")
  }

  test("RequiredLogFields.toMap contains all required fields") {
    val fields = RequiredLogFields("uid1", "MyClass", "myMethod")
    val map = fields.toMap

    assert(map("modelUid") === "uid1")
    assert(map("className") === "MyClass")
    assert(map("method") === "myMethod")
    assert(map("libraryVersion") === BuildInfo.version)
    assert(map("libraryName") === "SynapseML")
    assert(map("protocolVersion") === "0.0.1")
  }

  test("RequiredLogFields.toMap size is 6") {
    val fields = RequiredLogFields("uid", "class", "method")
    assert(fields.toMap.size === 6)
  }

  test("RequiredErrorFields stores errorType and errorMessage") {
    val fields = RequiredErrorFields("java.lang.RuntimeException", "Test error message")
    assert(fields.errorType === "java.lang.RuntimeException")
    assert(fields.errorMessage === "Test error message")
  }

  test("RequiredErrorFields.toMap contains error fields") {
    val fields = RequiredErrorFields("ErrorType", "ErrorMessage")
    val map = fields.toMap

    assert(map("errorType") === "ErrorType")
    assert(map("errorMessage") === "ErrorMessage")
  }

  test("RequiredErrorFields can be created from Exception") {
    val exception = new RuntimeException("Test exception message")
    val fields = new RequiredErrorFields(exception)

    assert(fields.errorType === "java.lang.RuntimeException")
    assert(fields.errorMessage === "Test exception message")
  }

  test("RequiredErrorFields handles exception with no message") {
    // scalastyle:off null
    val exception = new RuntimeException(None.orNull: String)
    val fields = new RequiredErrorFields(exception)

    assert(fields.errorType === "java.lang.RuntimeException")
    assert(Option(fields.errorMessage).isEmpty)
    // scalastyle:on null
  }

  test("RequiredErrorFields.toMap is JSON-serializable when the exception has no message") {
    // Regression: Exception.getMessage is null when an exception is constructed
    // without a message. spray-json's JsString rejects null, so an unguarded
    // value made getPayload(...).toJson throw IllegalArgumentException and mask
    // whatever error was actually being logged.
    // scalastyle:off null
    val exception = new RuntimeException(None.orNull: String)
    // scalastyle:on null
    val map = new RequiredErrorFields(exception).toMap

    assert(map("errorMessage") === "")
    assert(map("errorType") === "java.lang.RuntimeException")

    import spray.json.DefaultJsonProtocol._
    import spray.json._
    val json = map.toJson.compactPrint
    assert(json.contains("\"errorMessage\":\"\""))
  }

  test("SynapseMLLogging.HadoopKeysToLog contains expected mappings") {
    val keys = SynapseMLLogging.HadoopKeysToLog

    assert(keys("trident.artifact.id") === "artifactId")
    assert(keys("trident.workspace.id") === "workspaceId")
    assert(keys("trident.capacity.id") === "capacityId")
    assert(keys("trident.artifact.workspace.id") === "artifactWorkspaceId")
    assert(keys("trident.lakehouse.id") === "lakehouseId")
    assert(keys("trident.activity.id") === "livyId")
    assert(keys("trident.artifact.type") === "artifactType")
    assert(keys("trident.tenant.id") === "tenantId")
  }

  test("SynapseMLLogging.HadoopKeysToLog size is 8") {
    assert(SynapseMLLogging.HadoopKeysToLog.size === 8)
  }

  test("SynapseMLLogging.LoggedClasses is a mutable set") {
    try {
      SynapseMLLogging.LoggedClasses.add("TestClass")
      assert(SynapseMLLogging.LoggedClasses.contains("TestClass"))
    } finally {
      // Clean up to avoid leaking state into other tests
      SynapseMLLogging.LoggedClasses.remove("TestClass")
    }
  }

  /** Runs `f` with `spark` as the active session, restoring a previously-active session if there
    * was one.
    *
    * `getHadoopConfEntries` resolves its configuration through `SparkSession.getActiveSession`, so
    * these tests need one. When another suite has left a session active on this thread it is
    * restored exactly, so nothing is overwritten.
    *
    * When there was none, the shared session is deliberately left active rather than cleared.
    * `TestBase` never establishes one — `getOrCreate` only calls `setActiveSession` on the branch
    * that actually constructs the session, not when it returns an existing one — so "restoring" an
    * empty previous value would clear the active session for every suite that later runs on this
    * thread. `EnsembleByKey.transformSchema` falls back to `getActiveSession` and silently defaults
    * `spark.sql.caseSensitive` to `false` when there is none, which turns three `EnsembleByKeySuite`
    * tests red when both suites share a JVM. Leaving the shared session active is the canonical
    * state, and is what every suite reading `getActiveSession` already assumes.
    */
  private def withActiveSharedSession[T](f: => T): T = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    try f finally previous.foreach(SparkSession.setActiveSession)
  }

  test("getHadoopConfEntries reads cluster-level Hadoop configuration") {
    // Fabric sets the trident.* keys on the cluster Hadoop conf. getHadoopConfEntries now derives
    // its conf from the session instead of spark.sparkContext, so this pins that existing
    // telemetry still resolves.
    withActiveSharedSession {
      val hc = spark.sparkContext.hadoopConfiguration
      try {
        hc.set("trident.workspace.id", "ws-from-cluster")
        assert(SynapseMLLogging.getHadoopConfEntries.get("workspaceId").contains("ws-from-cluster"))
      } finally {
        hc.unset("trident.workspace.id")
      }
    }
  }

  test("getHadoopConfEntries reads session-level overrides") {
    withActiveSharedSession {
      try {
        spark.conf.set("trident.artifact.id", "artifact-from-session")
        assert(SynapseMLLogging.getHadoopConfEntries.get("artifactId").contains("artifact-from-session"))
      } finally {
        spark.conf.unset("trident.artifact.id")
      }
    }
  }

  test("getHadoopConfEntries returns only known telemetry field names") {
    withActiveSharedSession {
      val known = SynapseMLLogging.HadoopKeysToLog.values.toSet
      assert(SynapseMLLogging.getHadoopConfEntries.keySet.subsetOf(known))
    }
  }

  test("getHadoopConfEntries is empty when no session is active") {
    // Runs on its own thread so that clearing the active session cannot strand suites that share
    // the main test thread; SparkSession's active-session slot is a thread local.
    var failure: Option[Throwable] = None
    val thread = new Thread(new Runnable {
      override def run(): Unit =
        try {
          SparkSession.clearActiveSession()
          assert(SynapseMLLogging.getHadoopConfEntries.isEmpty)
        } catch {
          case e: Throwable => failure = Some(e)
        }
    })
    thread.start()
    thread.join()
    failure.foreach(e => throw e)
  }
}
