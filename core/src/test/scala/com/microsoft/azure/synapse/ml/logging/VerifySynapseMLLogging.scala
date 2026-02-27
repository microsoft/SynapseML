// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

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
    val initialSize = SynapseMLLogging.LoggedClasses.size
    SynapseMLLogging.LoggedClasses.add("TestClass")
    assert(SynapseMLLogging.LoggedClasses.contains("TestClass"))
    // Clean up
    SynapseMLLogging.LoggedClasses.remove("TestClass")
  }
}
