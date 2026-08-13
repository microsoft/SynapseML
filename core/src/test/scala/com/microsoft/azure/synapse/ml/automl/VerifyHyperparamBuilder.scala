// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{DoubleParam, FloatParam, IntParam, LongParam, ParamMap, Params}

import scala.collection.JavaConverters._

class VerifyHyperparamBuilder extends TestBase {

  // Helper class for creating test params
  private class TestParams extends Params {
    override val uid: String = "test"
    val intParam = new IntParam(this, "intParam", "test int param")
    val doubleParam = new DoubleParam(this, "doubleParam", "test double param")
    val longParam = new LongParam(this, "longParam", "test long param")
    val floatParam = new FloatParam(this, "floatParam", "test float param")
    override def copy(extra: ParamMap): Params = this
  }

  private val testParamsInstance = new TestParams

  test("IntRangeHyperParam generates values within range") {
    val hp = new IntRangeHyperParam(5, 15, seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(v => v >= 5 && v < 15))
    assert(values.toSet.size > 1) // not all the same
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

  test("LongRangeHyperParam generates values within range") {
    val param = new LongRangeHyperParam(0L, 100L, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= 0L && value < 100L, s"$value escaped [0, 100)")
    }
  }

  test("LongRangeHyperParam stays in range for a span wider than Long.MaxValue") {
    val param = new LongRangeHyperParam(Long.MinValue, Long.MaxValue, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= Long.MinValue && value < Long.MaxValue)
    }
  }

  test("LongRangeHyperParam respects seed for reproducibility") {
    val param1 = new LongRangeHyperParam(0L, 100L, seed = 42)
    val param2 = new LongRangeHyperParam(0L, 100L, seed = 42)
    val values1 = (1 to 10).map(_ => param1.getNext())
    val values2 = (1 to 10).map(_ => param2.getNext())
    assert(values1 === values2)
  }

  test("FloatRangeHyperParam generates values within range") {
    val param = new FloatRangeHyperParam(0.0f, 1.0f, seed = 42)
    for (_ <- 1 to 100) {
      val value = param.getNext()
      assert(value >= 0.0f && value < 1.0f)
    }
  }

  test("DiscreteHyperParam selects from provided values") {
    val hp = new DiscreteHyperParam(List("a", "b", "c"), seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(Set("a", "b", "c").contains))
    assert(values.toSet.size > 1)
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

  test("DiscreteHyperParam getValues returns Java list") {
    val hp = new DiscreteHyperParam(List(1, 2, 3))
    val javaList = hp.getValues
    assert(javaList.asScala.toList === List(1, 2, 3))
  }

  test("HyperparamBuilder builds empty array when no params added") {
    val builder = new HyperparamBuilder()
    val result = builder.build()
    assert(result.isEmpty)
  }

  test("HyperparamBuilder empty build returns empty array") {
    val hp = new HyperparamBuilder().build()
    assert(hp.isEmpty)
  }

  test("HyperparamBuilder adds single hyperparam") {
    val builder = new HyperparamBuilder()
    builder.addHyperparam(testParamsInstance.intParam, new IntRangeHyperParam(1, 10))
    val result = builder.build()
    assert(result.length === 1)
    assert(result.head._1 === testParamsInstance.intParam)
  }

  test("HyperparamBuilder adds multiple hyperparams") {
    val builder = new HyperparamBuilder()
      .addHyperparam(testParamsInstance.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(testParamsInstance.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
    val result = builder.build()
    assert(result.length === 2)
  }

  test("HyperparamBuilder supports method chaining") {
    val builder = new HyperparamBuilder()
    val result = builder
      .addHyperparam(testParamsInstance.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(testParamsInstance.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
      .build()
    assert(result.length === 2)
  }

  test("HyperparamBuilder builds array of param-dist pairs") {
    val hp = new HyperparamBuilder()
      .addHyperparam(testParamsInstance.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(testParamsInstance.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
      .build()
    assert(hp.length === 2)
    assert(hp.map(_._1.name).toSet === Set("intParam", "doubleParam"))
  }

  test("HyperParamUtils.getRangeHyperParam returns IntRangeHyperParam for Int") {
    val result = HyperParamUtils.getRangeHyperParam(1, 10)
    assert(result.isInstanceOf[IntRangeHyperParam])
    val intResult = result.asInstanceOf[IntRangeHyperParam]
    assert(intResult.min === 1)
    assert(intResult.max === 10)
  }

  test("HyperParamUtils.getRangeHyperParam matches Int type") {
    val hp = HyperParamUtils.getRangeHyperParam(1, 10)
    assert(hp.isInstanceOf[IntRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam returns DoubleRangeHyperParam for Double") {
    val result = HyperParamUtils.getRangeHyperParam(0.0, 1.0)
    assert(result.isInstanceOf[DoubleRangeHyperParam])
    val doubleResult = result.asInstanceOf[DoubleRangeHyperParam]
    assert(doubleResult.min === 0.0)
    assert(doubleResult.max === 1.0)
  }

  test("HyperParamUtils.getRangeHyperParam matches Double type") {
    val hp = HyperParamUtils.getRangeHyperParam(0.0, 1.0)
    assert(hp.isInstanceOf[DoubleRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam returns LongRangeHyperParam for Long") {
    val result = HyperParamUtils.getRangeHyperParam(0L, 100L)
    assert(result.isInstanceOf[LongRangeHyperParam])
    val longResult = result.asInstanceOf[LongRangeHyperParam]
    assert(longResult.min === 0L)
    assert(longResult.max === 100L)
  }

  test("HyperParamUtils.getRangeHyperParam matches Long type") {
    val hp = HyperParamUtils.getRangeHyperParam(0L, 100L)
    assert(hp.isInstanceOf[LongRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam returns FloatRangeHyperParam for Float") {
    val result = HyperParamUtils.getRangeHyperParam(0.0f, 1.0f)
    assert(result.isInstanceOf[FloatRangeHyperParam])
    val floatResult = result.asInstanceOf[FloatRangeHyperParam]
    assert(floatResult.min === 0.0f)
    assert(floatResult.max === 1.0f)
  }

  test("HyperParamUtils.getRangeHyperParam matches Float type") {
    val hp = HyperParamUtils.getRangeHyperParam(0.0f, 1.0f)
    assert(hp.isInstanceOf[FloatRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam throws for unsupported types") {
    assertThrows[Exception] {
      HyperParamUtils.getRangeHyperParam("a", "b")
    }
  }

  test("HyperParamUtils.getRangeHyperParam throws on unsupported type") {
    assertThrows[Exception] {
      HyperParamUtils.getRangeHyperParam("a", "z")
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

  test("HyperParamUtils.getDiscreteHyperParam creates from Java ArrayList") {
    val javaList = new java.util.ArrayList[String]()
    javaList.add("x")
    javaList.add("y")
    val hp = HyperParamUtils.getDiscreteHyperParam(javaList)
    val values = (1 to 50).map(_ => hp.getNext().toString)
    assert(values.forall(v => v == "x" || v == "y"))
  }

  test("RangeHyperParam stores min, max, and seed") {
    val param = new IntRangeHyperParam(5, 15, seed = 123)
    assert(param.min === 5)
    assert(param.max === 15)
    assert(param.seed === 123)
  }

  test("seeded RangeHyperParam produces deterministic sequences") {
    val hp1 = new IntRangeHyperParam(0, 100, seed = 123)
    val hp2 = new IntRangeHyperParam(0, 100, seed = 123)
    val seq1 = (1 to 10).map(_ => hp1.getNext())
    val seq2 = (1 to 10).map(_ => hp2.getNext())
    assert(seq1 === seq2)
  }
}
