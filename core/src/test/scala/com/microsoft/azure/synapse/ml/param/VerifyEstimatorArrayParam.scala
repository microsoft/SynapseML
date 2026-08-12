// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{ParamMap, Params}

import java.util.{ArrayList => JArrayList}

class VerifyEstimatorArrayParam extends TestBase {

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val estimatorsParam = new EstimatorArrayParam(this, "estimators", "An array of estimators")
    override def copy(extra: ParamMap): Params = this
  }

  test("EstimatorArrayParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.estimatorsParam.name === "estimators")
    assert(holder.estimatorsParam.doc === "An array of estimators")
  }

  test("EstimatorArrayParam accepts empty array") {
    val holder = new TestParamsHolder
    holder.set(holder.estimatorsParam, Array.empty[Estimator[_]])
    assert(holder.get(holder.estimatorsParam).exists(_.isEmpty))
  }

  test("EstimatorArrayParam accepts array with single estimator") {
    val holder = new TestParamsHolder
    val estimators = Array[Estimator[_]](new LogisticRegression())
    holder.set(holder.estimatorsParam, estimators)
    assert(holder.get(holder.estimatorsParam).exists(_.length === 1))
  }

  test("EstimatorArrayParam accepts array with multiple estimators") {
    val holder = new TestParamsHolder
    val estimators = Array[Estimator[_]](
      new LogisticRegression(),
      new DecisionTreeClassifier(),
      new StringIndexer()
    )
    holder.set(holder.estimatorsParam, estimators)
    assert(holder.get(holder.estimatorsParam).exists(_.length === 3))
  }

  test("EstimatorArrayParam w() method accepts Java List") {
    val holder = new TestParamsHolder
    val javaList = new JArrayList[Estimator[_]]()
    javaList.add(new LogisticRegression())
    javaList.add(new DecisionTreeClassifier())

    val paramPair = holder.estimatorsParam.w(javaList)
    assert(paramPair.param === holder.estimatorsParam)
    assert(paramPair.value.length === 2)
  }

  test("EstimatorArrayParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val nonEmptyEstimators = new EstimatorArrayParam(
        this, "nonEmpty", "Non-empty estimator array",
        (arr: Array[Estimator[_]]) => arr.nonEmpty
      )
      override def copy(extra: ParamMap): Params = this
    }
    val estimators = Array[Estimator[_]](new LogisticRegression())
    holder.set(holder.nonEmptyEstimators, estimators)
  }

  test("EstimatorArrayParam can be cleared") {
    val holder = new TestParamsHolder
    val estimators = Array[Estimator[_]](new LogisticRegression())
    holder.set(holder.estimatorsParam, estimators)
    assert(holder.isSet(holder.estimatorsParam))
    holder.clear(holder.estimatorsParam)
    assert(!holder.isSet(holder.estimatorsParam))
  }

  test("EstimatorArrayParam returns None when not set") {
    val holder = new TestParamsHolder
    assert(holder.get(holder.estimatorsParam).isEmpty)
  }
}
