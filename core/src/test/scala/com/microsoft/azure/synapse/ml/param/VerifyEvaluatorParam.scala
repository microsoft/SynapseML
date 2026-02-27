// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.evaluation.{
  BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
}
import org.apache.spark.ml.param.{ParamMap, Params}

class VerifyEvaluatorParam extends TestBase {

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val evaluatorParam = new EvaluatorParam(this, "evaluator", "An evaluator param")
    override def copy(extra: ParamMap): Params = this
  }

  test("EvaluatorParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.evaluatorParam.name === "evaluator")
    assert(holder.evaluatorParam.doc === "An evaluator param")
  }

  test("EvaluatorParam accepts BinaryClassificationEvaluator") {
    val holder = new TestParamsHolder
    val evaluator = new BinaryClassificationEvaluator()
    holder.set(holder.evaluatorParam, evaluator)
    assert(holder.isSet(holder.evaluatorParam))
  }

  test("EvaluatorParam accepts MulticlassClassificationEvaluator") {
    val holder = new TestParamsHolder
    val evaluator = new MulticlassClassificationEvaluator()
    holder.set(holder.evaluatorParam, evaluator)
    assert(holder.isSet(holder.evaluatorParam))
  }

  test("EvaluatorParam accepts RegressionEvaluator") {
    val holder = new TestParamsHolder
    val evaluator = new RegressionEvaluator()
    holder.set(holder.evaluatorParam, evaluator)
    assert(holder.isSet(holder.evaluatorParam))
  }

  test("EvaluatorParam assertEquality passes for same evaluator type") {
    val holder = new TestParamsHolder
    val eval1 = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setLabelCol("label")
    val eval2 = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setLabelCol("label")
    holder.evaluatorParam.assertEquality(eval1, eval2)
  }

  test("EvaluatorParam assertEquality throws for non-Evaluator types") {
    val holder = new TestParamsHolder
    assertThrows[AssertionError] {
      holder.evaluatorParam.assertEquality("not an evaluator", "also not")
    }
  }

  test("EvaluatorParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val binaryOnly = new EvaluatorParam(
        this, "binaryOnly", "Binary evaluator only",
        _.isInstanceOf[BinaryClassificationEvaluator]
      )
      override def copy(extra: ParamMap): Params = this
    }
    val evaluator = new BinaryClassificationEvaluator()
    holder.set(holder.binaryOnly, evaluator)
  }

  test("EvaluatorParam can be cleared") {
    val holder = new TestParamsHolder
    val evaluator = new RegressionEvaluator()
    holder.set(holder.evaluatorParam, evaluator)
    assert(holder.isSet(holder.evaluatorParam))
    holder.clear(holder.evaluatorParam)
    assert(!holder.isSet(holder.evaluatorParam))
  }

  test("EvaluatorParam returns None when not set") {
    val holder = new TestParamsHolder
    assert(holder.get(holder.evaluatorParam).isEmpty)
  }
}
