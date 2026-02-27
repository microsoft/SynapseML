// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{IntParam, Params, DoubleParam, LongParam, FloatParam}

class VerifyHyperparamBuilder extends TestBase {

  // Helper trait for creating test params
  private trait TestParams extends Params {
    override val uid: String = "test"
    val intParam = new IntParam(this, "intParam", "test int param")
    val doubleParam = new DoubleParam(this, "doubleParam", "test double param")
    val longParam = new LongParam(this, "longParam", "test long param")
    val floatParam = new FloatParam(this, "floatParam", "test float param")
  }

  private object TestParamsInstance extends TestParams

  test("IntRangeHyperParam generates values within range") {
    val param = new IntRangeHyperParam(10, 20, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= 10 && value < 20)
    }
  }

  test("IntRangeHyperParam respects seed for reproducibility") {
    val param1 = new IntRangeHyperParam(0, 100, seed = 42)
    val param2 = new IntRangeHyperParam(0, 100, seed = 42)
    val values1 = (1 to 10).map(_ => param1.getNext())
    val values2 = (1 to 10).map(_ => param2.getNext())
    assert(values1 === values2)
  }

  test("DoubleRangeHyperParam generates values within range") {
    val param = new DoubleRangeHyperParam(0.0, 1.0, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= 0.0 && value < 1.0)
    }
  }

  test("DoubleRangeHyperParam respects seed for reproducibility") {
    val param1 = new DoubleRangeHyperParam(0.0, 10.0, seed = 42)
    val param2 = new DoubleRangeHyperParam(0.0, 10.0, seed = 42)
    val values1 = (1 to 10).map(_ => param1.getNext())
    val values2 = (1 to 10).map(_ => param2.getNext())
    assert(values1 === values2)
  }

  test("LongRangeHyperParam generates values") {
    val param = new LongRangeHyperParam(0L, 100L, seed = 42)
    val value = param.getNext()
    // Just verify it returns a Long
    assert(value.isInstanceOf[Long])
  }

  test("FloatRangeHyperParam generates values within range") {
    val param = new FloatRangeHyperParam(0.0f, 1.0f, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= 0.0f && value < 1.0f)
    }
  }

  test("DiscreteHyperParam selects from provided values") {
    val values = List("a", "b", "c")
    val param = new DiscreteHyperParam(values, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(values.contains(value))
    }
  }

  test("DiscreteHyperParam.getValues returns Java list") {
    val values = List(1, 2, 3)
    val param = new DiscreteHyperParam(values)
    val javaList = param.getValues
    assert(javaList.size() === 3)
    assert(javaList.get(0) === 1)
    assert(javaList.get(1) === 2)
    assert(javaList.get(2) === 3)
  }

  test("HyperparamBuilder builds empty array when no params added") {
    val builder = new HyperparamBuilder()
    val result = builder.build()
    assert(result.isEmpty)
  }

  test("HyperparamBuilder adds single hyperparam") {
    val builder = new HyperparamBuilder()
    builder.addHyperparam(TestParamsInstance.intParam, new IntRangeHyperParam(1, 10))
    val result = builder.build()
    assert(result.length === 1)
    assert(result.head._1 === TestParamsInstance.intParam)
  }

  test("HyperparamBuilder adds multiple hyperparams") {
    val builder = new HyperparamBuilder()
      .addHyperparam(TestParamsInstance.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(TestParamsInstance.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
    val result = builder.build()
    assert(result.length === 2)
  }

  test("HyperparamBuilder supports method chaining") {
    val builder = new HyperparamBuilder()
    val result = builder
      .addHyperparam(TestParamsInstance.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(TestParamsInstance.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
      .build()
    assert(result.length === 2)
  }

  test("HyperParamUtils.getRangeHyperParam returns IntRangeHyperParam for Int") {
    val result = HyperParamUtils.getRangeHyperParam(1, 10)
    assert(result.isInstanceOf[IntRangeHyperParam])
    assert(result.min === 1)
    assert(result.max === 10)
  }

  test("HyperParamUtils.getRangeHyperParam returns DoubleRangeHyperParam for Double") {
    val result = HyperParamUtils.getRangeHyperParam(0.0, 1.0)
    assert(result.isInstanceOf[DoubleRangeHyperParam])
    assert(result.min === 0.0)
    assert(result.max === 1.0)
  }

  test("HyperParamUtils.getRangeHyperParam returns LongRangeHyperParam for Long") {
    val result = HyperParamUtils.getRangeHyperParam(0L, 100L)
    assert(result.isInstanceOf[LongRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam returns FloatRangeHyperParam for Float") {
    val result = HyperParamUtils.getRangeHyperParam(0.0f, 1.0f)
    assert(result.isInstanceOf[FloatRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam throws for unsupported types") {
    assertThrows[Exception] {
      HyperParamUtils.getRangeHyperParam("a", "b")
    }
  }

  test("HyperParamUtils.getDiscreteHyperParam creates DiscreteHyperParam from Java ArrayList") {
    val javaList = new java.util.ArrayList[Int]()
    javaList.add(1)
    javaList.add(2)
    javaList.add(3)
    val result = HyperParamUtils.getDiscreteHyperParam(javaList)
    assert(result.isInstanceOf[DiscreteHyperParam[_]])
    val value = result.getNext()
    assert(Seq(1, 2, 3).contains(value))
  }

  test("RangeHyperParam stores min, max, and seed") {
    val param = new IntRangeHyperParam(5, 15, seed = 123)
    assert(param.min === 5)
    assert(param.max === 15)
    assert(param.seed === 123)
  }
}
