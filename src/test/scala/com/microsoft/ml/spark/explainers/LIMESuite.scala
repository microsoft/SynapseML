package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature._
import breeze.linalg.{norm, DenseVector => BDV}

class LIMESuite extends TestBase {
  test("TabularLIME can explain a simple logistic model locally with one variable") {
    import spark.implicits._

    val data = Seq(
      (-6.0, 0),
      (-5.0, 0),
      (5.0, 1),
      (6.0, 1)
    ) toDF("col1", "label")

    val vecAssembler = new VectorAssembler().setInputCols(Array("col1")).setOutputCol("features")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vecAssembler, classifier))
    val model = pipeline.fit(data)

    // coefficient should be around 3.61594667
    val coefficient = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray.head
    assert(math.abs(coefficient - 3.61594667) < 1e-5)

    val infer = Seq(
      Tuple1(0.0)
    ) toDF "col1"

    val predicted = model.transform(infer)

    val lime = new TabularLIME()
      .setInputCols(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setKernelWidth(0.001)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val weights = lime.explain(predicted)

    val weightsVec = BDV(weights.select("weights").as[Seq[Double]].head: _*)

    // The derivative of the logistic function with coefficient k at x = 0, simplifies to k/4.
    // We set the kernel width to a very small value so we only consider a very close neighborhood
    // for regression, and set L1 regularization to zero so it does not affect the fit coefficient.
    // Therefore, the coefficient of the lasso regression should approximately match the derivative.
    assert(norm(weightsVec - BDV(coefficient / 4)) < 1e-2)
  }

  test("TabularLIME can explain a simple logistic model locally with multiple variables") {
    import spark.implicits._

    val data = Seq(
      (-6.0, 1.0, 0),
      (-5.0, -3.0, 0),
      (5.0, -1.0, 1),
      (6.0, 3.0, 1)
    ) toDF("col1", "col2", "label")

    val vecAssembler = new VectorAssembler().setInputCols(Array("col1", "col2")).setOutputCol("features")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vecAssembler, classifier))
    val model = pipeline.fit(data)

    // coefficient should be around 3.61594667
    val coefficients = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray

    assert(math.abs(coefficients(0) - 3.5279868) < 1e-5)
    assert(math.abs(coefficients(1) - 0.5962254) < 1e-5)

    val infer = Seq(
      (0.0, 0.0)
    ) toDF ("col1", "col2")

    val predicted = model.transform(infer)

    val lime = new TabularLIME()
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setKernelWidth(0.05)
      .setNumSamples(1000)
      .setRegularization(0.01)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val weights = lime.explain(predicted)

    val weightsVec = BDV(weights.select("weights").as[Seq[Double]].head: _*)

    println(weightsVec)

    // With 0.01 L1 regularization, the second feature gets removed due to low importance.
    assert(weightsVec(0) > 0.0)
    assert(weightsVec(1) == 0.0)
  }
}
