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
    val result = weights.select("weights", "r2").as[(Seq[Double], Double)].head
    val weightsVec = BDV(result._1: _*)

    // The derivative of the logistic function with coefficient k at x = 0, simplifies to k/4.
    // We set the kernel width to a very small value so we only consider a very close neighborhood
    // for regression, and set L1 regularization to zero so it does not affect the fit coefficient.
    // Therefore, the coefficient of the lasso regression should approximately match the derivative.
    assert(norm(weightsVec - BDV(coefficient / 4)) < 1e-2)

    assert(math.abs(result._2 - 1d) < 1e-6, "R-squared of the fit should be close to 1.")
  }

  test("TabularLIME can explain a simple logistic model locally with multiple variables") {
    import spark.implicits._

    val data = Seq(
      (-6, 1.0, 0),
      (-5, -3.0, 0),
      (5, -1.0, 1),
      (6, 3.0, 1)
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

  test("TabularLIME can explain a simple logistic model locally with categorical variables") {
    import spark.implicits._

    val data = Seq(
      (1, 0),
      (2, 0),
      (3, 1),
      (4, 1)
    ) toDF("col1", "label")

    val encoder = new OneHotEncoder().setInputCol("col1").setOutputCol("col1_enc")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("col1_enc")

    val pipeline = new Pipeline().setStages(Array(encoder, classifier))
    val model = pipeline.fit(data)

    val infer = Seq(
      Tuple1(1),
      Tuple1(2),
      Tuple1(3),
      Tuple1(4)
    ) toDF ("col1")

    val predicted = model.transform(infer)

    val lime = new TabularLIME()
      .setInputCols(Array("col1"))
      .setCategoricalFeatures(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val weights = lime.explain(predicted)
    val results = weights.select("col1", "weights").as[(Int, Array[Double])].collect()
      .map(r => (r._1, r._2.head))
      .toMap

    // weights for data point 1 and 4 should be close, 2 and 3 should be close by symmetry.
    assert(math.abs(results(1) - 0.3780) < 1e-3)
    assert(math.abs(results(2) - 0.4323) < 1e-3)
    assert(math.abs(results(3) - 0.4311) < 1e-3)
    assert(math.abs(results(4) - 0.3808) < 1e-3)
  }
}
