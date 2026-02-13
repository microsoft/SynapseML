// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{IntParam, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

// scalastyle:off magic.number
class VerifyParamSpace extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
    val intParam = new IntParam(this, "intParam", "test int param") // scalastyle:ignore field.name
  }

  test("GridSpace iterates over all ParamMaps") {
    val pm1 = ParamMap(TestParams.intParam -> 1)
    val pm2 = ParamMap(TestParams.intParam -> 2)
    val pm3 = ParamMap(TestParams.intParam -> 3)
    val grid = new GridSpace(Array(pm1, pm2, pm3))
    val result = grid.paramMaps.toList
    assert(result.length === 3)
  }

  test("GridSpace with empty array produces empty iterator") {
    val grid = new GridSpace(Array.empty[ParamMap])
    assert(!grid.paramMaps.hasNext)
  }

  test("RandomSpace produces infinite iterator") {
    val builder = new HyperparamBuilder()
      .addHyperparam(TestParams.intParam, new IntRangeHyperParam(1, 100))
    val space = new RandomSpace(builder.build())
    val values = space.paramMaps.take(50).toList
    assert(values.length === 50)
    values.foreach { pm =>
      val v = pm.get(TestParams.intParam)
      assert(v.isDefined)
      assert(v.get >= 1 && v.get < 100)
    }
  }

  test("RandomSpace iterator always hasNext") {
    val builder = new HyperparamBuilder()
      .addHyperparam(TestParams.intParam, new IntRangeHyperParam(0, 10))
    val space = new RandomSpace(builder.build())
    assert(space.paramMaps.hasNext)
    space.paramMaps.next()
    assert(space.paramMaps.hasNext)
  }

  test("Dist.getParamPair creates correct ParamPair") {
    val dist = new IntRangeHyperParam(5, 15, seed = 42)
    val pp = dist.getParamPair(TestParams.intParam)
    assert(pp.param.name === TestParams.intParam.name)
    val value = pp.value.asInstanceOf[Int]
    assert(value >= 5)
    assert(value < 15)
  }
}
// scalastyle:on magic.number
