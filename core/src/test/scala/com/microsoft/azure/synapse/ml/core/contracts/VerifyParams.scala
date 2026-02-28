// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.contracts

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable

// Test implementations of the param traits
class TestHasInputCol(override val uid: String)
  extends HasInputCol {
  def this() = this(Identifiable.randomUID("TestHasInputCol"))
  override def copy(extra: ParamMap): TestHasInputCol = defaultCopy(extra)
}

class TestHasOutputCol(override val uid: String)
  extends HasOutputCol {
  def this() = this(Identifiable.randomUID("TestHasOutputCol"))
  override def copy(extra: ParamMap): TestHasOutputCol = defaultCopy(extra)
}

class TestHasInputCols(override val uid: String)
  extends HasInputCols {
  def this() = this(Identifiable.randomUID("TestHasInputCols"))
  override def copy(extra: ParamMap): TestHasInputCols = defaultCopy(extra)
}

class TestHasOutputCols(override val uid: String)
  extends HasOutputCols {
  def this() = this(Identifiable.randomUID("TestHasOutputCols"))
  override def copy(extra: ParamMap): TestHasOutputCols = defaultCopy(extra)
}

class TestHasLabelCol(override val uid: String)
  extends HasLabelCol {
  def this() = this(Identifiable.randomUID("TestHasLabelCol"))
  override def copy(extra: ParamMap): TestHasLabelCol = defaultCopy(extra)
}

class TestHasFeaturesCol(override val uid: String)
  extends HasFeaturesCol {
  def this() = this(Identifiable.randomUID("TestHasFeaturesCol"))
  override def copy(extra: ParamMap): TestHasFeaturesCol = defaultCopy(extra)
}

class TestHasWeightCol(override val uid: String)
  extends HasWeightCol {
  def this() = this(Identifiable.randomUID("TestHasWeightCol"))
  override def copy(extra: ParamMap): TestHasWeightCol = defaultCopy(extra)
}

class TestHasScoredLabelsCol(override val uid: String)
  extends HasScoredLabelsCol {
  def this() = this(Identifiable.randomUID("TestHasScoredLabelsCol"))
  override def copy(extra: ParamMap): TestHasScoredLabelsCol = defaultCopy(extra)
}

class TestHasScoresCol(override val uid: String)
  extends HasScoresCol {
  def this() = this(Identifiable.randomUID("TestHasScoresCol"))
  override def copy(extra: ParamMap): TestHasScoresCol = defaultCopy(extra)
}

class TestHasScoredProbabilitiesCol(override val uid: String)
  extends HasScoredProbabilitiesCol {
  def this() = this(Identifiable.randomUID("TestHasScoredProbabilitiesCol"))
  override def copy(extra: ParamMap): TestHasScoredProbabilitiesCol = defaultCopy(extra)
}

class TestHasEvaluationMetric(override val uid: String)
  extends HasEvaluationMetric {
  def this() = this(Identifiable.randomUID("TestHasEvaluationMetric"))
  override def copy(extra: ParamMap): TestHasEvaluationMetric = defaultCopy(extra)
}

class TestHasValidationIndicatorCol(override val uid: String)
  extends HasValidationIndicatorCol {
  def this() = this(Identifiable.randomUID("TestHasValidationIndicatorCol"))
  override def copy(extra: ParamMap): TestHasValidationIndicatorCol = defaultCopy(extra)
}

class TestHasInitScoreCol(override val uid: String)
  extends HasInitScoreCol {
  def this() = this(Identifiable.randomUID("TestHasInitScoreCol"))
  override def copy(extra: ParamMap): TestHasInitScoreCol = defaultCopy(extra)
}

class TestHasGroupCol(override val uid: String)
  extends HasGroupCol {
  def this() = this(Identifiable.randomUID("TestHasGroupCol"))
  override def copy(extra: ParamMap): TestHasGroupCol = defaultCopy(extra)
}

class VerifyParams extends TestBase {

  test("HasInputCol set and get work correctly") {
    val obj = new TestHasInputCol()
    obj.setInputCol("myInput")
    assert(obj.getInputCol === "myInput")
  }

  test("HasOutputCol set and get work correctly") {
    val obj = new TestHasOutputCol()
    obj.setOutputCol("myOutput")
    assert(obj.getOutputCol === "myOutput")
  }

  test("HasInputCols set and get work correctly") {
    val obj = new TestHasInputCols()
    val cols = Array("col1", "col2", "col3")
    obj.setInputCols(cols)
    assert(obj.getInputCols.sameElements(cols))
  }

  test("HasOutputCols set and get work correctly") {
    val obj = new TestHasOutputCols()
    val cols = Array("out1", "out2")
    obj.setOutputCols(cols)
    assert(obj.getOutputCols.sameElements(cols))
  }

  test("HasLabelCol set and get work correctly") {
    val obj = new TestHasLabelCol()
    obj.setLabelCol("target")
    assert(obj.getLabelCol === "target")
  }

  test("HasFeaturesCol set and get work correctly") {
    val obj = new TestHasFeaturesCol()
    obj.setFeaturesCol("features")
    assert(obj.getFeaturesCol === "features")
  }

  test("HasWeightCol set and get work correctly") {
    val obj = new TestHasWeightCol()
    obj.setWeightCol("weight")
    assert(obj.getWeightCol === "weight")
  }

  test("HasScoredLabelsCol set and get work correctly") {
    val obj = new TestHasScoredLabelsCol()
    obj.setScoredLabelsCol("scoredLabels")
    assert(obj.getScoredLabelsCol === "scoredLabels")
  }

  test("HasScoresCol set and get work correctly") {
    val obj = new TestHasScoresCol()
    obj.setScoresCol("scores")
    assert(obj.getScoresCol === "scores")
  }

  test("HasScoredProbabilitiesCol set and get work correctly") {
    val obj = new TestHasScoredProbabilitiesCol()
    obj.setScoredProbabilitiesCol("probs")
    assert(obj.getScoredProbabilitiesCol === "probs")
  }

  test("HasEvaluationMetric set and get work correctly") {
    val obj = new TestHasEvaluationMetric()
    obj.setEvaluationMetric("accuracy")
    assert(obj.getEvaluationMetric === "accuracy")
  }

  test("HasValidationIndicatorCol set and get work correctly") {
    val obj = new TestHasValidationIndicatorCol()
    obj.setValidationIndicatorCol("isValidation")
    assert(obj.getValidationIndicatorCol === "isValidation")
  }

  test("HasInitScoreCol set and get work correctly") {
    val obj = new TestHasInitScoreCol()
    obj.setInitScoreCol("initScore")
    assert(obj.getInitScoreCol === "initScore")
  }

  test("HasGroupCol set and get work correctly") {
    val obj = new TestHasGroupCol()
    obj.setGroupCol("group")
    assert(obj.getGroupCol === "group")
  }

  // Test chaining
  test("param setters return this for chaining") {
    val obj = new TestHasInputCol()
    val result = obj.setInputCol("test")
    assert(result eq obj)
  }
}
