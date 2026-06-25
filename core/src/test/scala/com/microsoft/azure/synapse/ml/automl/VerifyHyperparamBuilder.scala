// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{IntParam, DoubleParam, Param, Params, ParamMap}
import org.apache.spark.ml.util.Identifiable

import scala.collection.JavaConverters._

// scalastyle:off magic.number
class VerifyHyperparamBuilder extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
    val intParam = new IntParam(this, "intParam", "test int param") // scalastyle:ignore field.name
    val doubleParam = new DoubleParam(this, "doubleParam", "test double param") // scalastyle:ignore field.name
  }

  test("IntRangeHyperParam generates values within range") {
    val hp = new IntRangeHyperParam(5, 15, seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(v => v >= 5 && v < 15))
    assert(values.toSet.size > 1) // not all the same
  }

  test("DoubleRangeHyperParam generates values within range") {
    val hp = new DoubleRangeHyperParam(0.0, 1.0, seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(v => v >= 0.0 && v < 1.0))
  }

  test("FloatRangeHyperParam generates values within range") {
    val hp = new FloatRangeHyperParam(0.0f, 1.0f, seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(v => v >= 0.0f && v < 1.0f))
  }

  test("LongRangeHyperParam generates values") {
    val hp = new LongRangeHyperParam(0L, 100L, seed = 42)
    val value = hp.getNext()
    assert(value.isInstanceOf[Long])
  }

  test("DiscreteHyperParam selects from provided values") {
    val hp = new DiscreteHyperParam(List("a", "b", "c"), seed = 42)
    val values = (1 to 100).map(_ => hp.getNext())
    assert(values.forall(Set("a", "b", "c").contains))
    assert(values.toSet.size > 1)
  }

  test("DiscreteHyperParam getValues returns Java list") {
    val hp = new DiscreteHyperParam(List(1, 2, 3))
    val javaList = hp.getValues
    assert(javaList.asScala.toList === List(1, 2, 3))
  }

  test("HyperparamBuilder builds array of param-dist pairs") {
    val hp = new HyperparamBuilder()
      .addHyperparam(TestParams.intParam, new IntRangeHyperParam(1, 10))
      .addHyperparam(TestParams.doubleParam, new DoubleRangeHyperParam(0.0, 1.0))
      .build()
    assert(hp.length === 2)
    assert(hp.map(_._1.name).toSet === Set("intParam", "doubleParam"))
  }

  test("HyperparamBuilder empty build returns empty array") {
    val hp = new HyperparamBuilder().build()
    assert(hp.isEmpty)
  }

  test("HyperParamUtils.getRangeHyperParam matches Int type") {
    val hp = HyperParamUtils.getRangeHyperParam(1, 10)
    assert(hp.isInstanceOf[IntRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam matches Double type") {
    val hp = HyperParamUtils.getRangeHyperParam(0.0, 1.0)
    assert(hp.isInstanceOf[DoubleRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam matches Float type") {
    val hp = HyperParamUtils.getRangeHyperParam(0.0f, 1.0f)
    assert(hp.isInstanceOf[FloatRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam matches Long type") {
    val hp = HyperParamUtils.getRangeHyperParam(0L, 100L)
    assert(hp.isInstanceOf[LongRangeHyperParam])
  }

  test("HyperParamUtils.getRangeHyperParam throws on unsupported type") {
    assertThrows[Exception] {
      HyperParamUtils.getRangeHyperParam("a", "z")
    }
  }

  test("HyperParamUtils.getDiscreteHyperParam creates from Java ArrayList") {
    val javaList = new java.util.ArrayList[String]()
    javaList.add("x")
    javaList.add("y")
    val hp = HyperParamUtils.getDiscreteHyperParam(javaList)
    val values = (1 to 50).map(_ => hp.getNext().toString)
    assert(values.forall(v => v == "x" || v == "y"))
  }

  test("seeded RangeHyperParam produces deterministic sequences") {
    val hp1 = new IntRangeHyperParam(0, 100, seed = 123)
    val hp2 = new IntRangeHyperParam(0, 100, seed = 123)
    val seq1 = (1 to 10).map(_ => hp1.getNext())
    val seq2 = (1 to 10).map(_ => hp2.getNext())
    assert(seq1 === seq2)
  }
}
// scalastyle:on magic.number
