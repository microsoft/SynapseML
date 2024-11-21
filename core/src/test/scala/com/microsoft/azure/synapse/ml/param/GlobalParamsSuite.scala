// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.Secrets.getAccessToken
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import spray.json.DefaultJsonProtocol.IntJsonFormat

trait TestGlobalParamsTrait extends HasGlobalParams {
  val testParam: Param[Double] = new Param[Double](
    this, "TestParam", "Test Param")

  GlobalParams.registerParam(testParam, TestParamKey)

  def getTestParam: Double = getGlobalParam(testParam)

  def setTestParam(v: Double): this.type = set(testParam, v)

  val testServiceParam = new ServiceParam[Int](
    this, "testServiceParam", "Test Service Param", isRequired = false)
}

class TestGlobalParams extends TestGlobalParamsTrait {

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override val uid: String = Identifiable.randomUID("TestGlobalParams")
}


case object TestParamKey extends GlobalKey[Double] {val name: String = "TestParam"; val isServiceParam = false}
case object TestServiceParamKey extends GlobalKey[Int]
{val name: String = "TestServiceParam"; val isServiceParam = true}

class GlobalParamSuite extends Flaky {

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }

  val testGlobalParams = new TestGlobalParams()

  test("Basic Usage") {
    GlobalParams.setGlobalParam(TestParamKey, 12.5)
    assert(testGlobalParams.getTestParam == 12.5)
  }

  test("Test Changing Value") {
    assert(testGlobalParams.getTestParam == 12.5)
    GlobalParams.setGlobalParam("TestParam", 19.853)
    assert(testGlobalParams.getTestParam == 19.853)
  }
}

