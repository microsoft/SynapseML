// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable

case object TestParamKey extends GlobalKey[Double]

class TestGlobalParams extends HasGlobalParams {
  override val uid: String = Identifiable.randomUID("TestGlobalParams")

  val testParam: Param[Double] = new Param[Double](
    this, "TestParam", "Test Param for testing")

  println(testParam.parent)
  println(hasParam(testParam.name))

  GlobalParams.registerParam(testParam, TestParamKey)

  def getTestParam: Double = $(testParam)

  def setTestParam(v: Double): this.type = set(testParam, v)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

class GlobalParamSuite extends Flaky {

  val testGlobalParams = new TestGlobalParams()

  test("Basic Usage") {
    GlobalParams.setGlobalParam(TestParamKey, 12.5)
    testGlobalParams.transferGlobalParamsToParamMap()
    assert(testGlobalParams.getTestParam == 12.5)
  }

  test("Test Setting Directly Value") {
    testGlobalParams.setTestParam(18.7334)
    GlobalParams.setGlobalParam(TestParamKey, 19.853)
    testGlobalParams.transferGlobalParamsToParamMap()
    assert(testGlobalParams.getTestParam == 18.7334)
  }
}

