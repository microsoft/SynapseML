// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

class VerifyGlobalParams extends TestBase {

  // Test keys
  case object TestStringKey extends GlobalKey[String]
  case object TestIntKey extends GlobalKey[Int]
  case object AnotherStringKey extends GlobalKey[String]

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Reset global params between tests
    GlobalParams.resetGlobalParam(TestStringKey)
    GlobalParams.resetGlobalParam(TestIntKey)
    GlobalParams.resetGlobalParam(AnotherStringKey)
  }

  test("setGlobalParam and getGlobalParam work for String") {
    GlobalParams.setGlobalParam(TestStringKey, "test-value")
    val result = GlobalParams.getGlobalParam(TestStringKey)
    assert(result === Some("test-value"))
  }

  test("setGlobalParam and getGlobalParam work for Int") {
    GlobalParams.setGlobalParam(TestIntKey, 42)
    val result = GlobalParams.getGlobalParam(TestIntKey)
    assert(result === Some(42))
  }

  test("getGlobalParam returns None for unset key") {
    val result = GlobalParams.getGlobalParam(TestStringKey)
    assert(result.isEmpty)
  }

  test("resetGlobalParam removes the parameter") {
    GlobalParams.setGlobalParam(TestStringKey, "value")
    assert(GlobalParams.getGlobalParam(TestStringKey).isDefined)
    GlobalParams.resetGlobalParam(TestStringKey)
    assert(GlobalParams.getGlobalParam(TestStringKey).isEmpty)
  }

  test("setGlobalParam overwrites existing value") {
    GlobalParams.setGlobalParam(TestStringKey, "first")
    GlobalParams.setGlobalParam(TestStringKey, "second")
    assert(GlobalParams.getGlobalParam(TestStringKey) === Some("second"))
  }

  test("multiple keys can be set independently") {
    GlobalParams.setGlobalParam(TestStringKey, "string-value")
    GlobalParams.setGlobalParam(TestIntKey, 100)
    GlobalParams.setGlobalParam(AnotherStringKey, "another-value")

    assert(GlobalParams.getGlobalParam(TestStringKey) === Some("string-value"))
    assert(GlobalParams.getGlobalParam(TestIntKey) === Some(100))
    assert(GlobalParams.getGlobalParam(AnotherStringKey) === Some("another-value"))
  }

  test("registerParam and getParam work together") {
    // Create a test Params implementation
    class TestParams(override val uid: String) extends Params {
      val testParam = new Param[String](this, "testParam", "test param")
      override def copy(extra: ParamMap): Params = this
    }

    val params = new TestParams(Identifiable.randomUID("test"))
    GlobalParams.registerParam(params.testParam, TestStringKey)
    GlobalParams.setGlobalParam(TestStringKey, "global-value")

    val result = GlobalParams.getParam(params.testParam)
    assert(result === Some("global-value"))
  }

  test("getParam returns None for unregistered param") {
    class TestParams(override val uid: String) extends Params {
      val unregisteredParam = new Param[String](this, "unregisteredParam", "not registered")
      override def copy(extra: ParamMap): Params = this
    }

    val params = new TestParams(Identifiable.randomUID("test"))
    val result = GlobalParams.getParam(params.unregisteredParam)
    assert(result.isEmpty)
  }
}
