// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.{ParamMap, Params}

class VerifyModelParam extends TestBase {

  import spark.implicits._

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val modelParam = new ModelParam(this, "model", "A model param")
    override def copy(extra: ParamMap): Params = this
  }

  // Helper to create a trained model
  private def createTrainedModel(): LogisticRegressionModel = {
    val data = Seq(
      (0.0, 1.0, 0.0),
      (1.0, 0.0, 1.0),
      (0.0, 1.0, 0.0),
      (1.0, 0.0, 1.0)
    ).toDF("label", "f1", "f2")

    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2"))
      .setOutputCol("features")
    val assembled = assembler.transform(data)

    val lr = new LogisticRegression()
      .setMaxIter(5)
      .setLabelCol("label")
      .setFeaturesCol("features")
    lr.fit(assembled)
  }

  test("ModelParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.modelParam.name === "model")
    assert(holder.modelParam.doc === "A model param")
  }

  test("ModelParam accepts LogisticRegressionModel") {
    val holder = new TestParamsHolder
    val model = createTrainedModel()
    holder.set(holder.modelParam, model)
    assert(holder.isSet(holder.modelParam))
  }

  test("ModelParam pyValue returns model reference") {
    val holder = new TestParamsHolder
    val model = createTrainedModel()
    val pyVal = holder.modelParam.pyValue(model)
    assert(pyVal === "modelModel")
  }

  test("ModelParam pyLoadLine generates Python code") {
    val holder = new TestParamsHolder
    val pyCode = holder.modelParam.pyLoadLine(1)
    assert(pyCode.contains("Pipeline.load"))
    assert(pyCode.contains("model-1.model"))
    assert(pyCode.contains("complexParams"))
  }

  test("ModelParam rValue returns model reference") {
    val holder = new TestParamsHolder
    val model = createTrainedModel()
    val rVal = holder.modelParam.rValue(model)
    assert(rVal === "modelModel")
  }

  test("ModelParam rLoadLine generates R code") {
    val holder = new TestParamsHolder
    val rCode = holder.modelParam.rLoadLine(2)
    assert(rCode.contains("ml_load"))
    assert(rCode.contains("model-2.model"))
    assert(rCode.contains("ml_stages"))
  }

  test("ModelParam can be cleared") {
    val holder = new TestParamsHolder
    val model = createTrainedModel()
    holder.set(holder.modelParam, model)
    assert(holder.isSet(holder.modelParam))
    holder.clear(holder.modelParam)
    assert(!holder.isSet(holder.modelParam))
  }

  test("ModelParam returns None when not set") {
    val holder = new TestParamsHolder
    assert(holder.get(holder.modelParam).isEmpty)
  }
}
