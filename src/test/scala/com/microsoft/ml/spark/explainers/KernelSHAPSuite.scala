package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import breeze.linalg.{*, DenseMatrix => BDM}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.BreezeUtils._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.sql.functions._

class KernelSHAPSuite extends TestBase {
  import spark.implicits._

  test("TabularKernelSHAP can explain a model locally") {
    val data = (1 to 100).flatMap(_ => Seq(
      (-5d, "a", -5d, 0),
      (-5d, "b", -5d, 0),
      (5d, "a", 5d, 1),
      (5d, "b", 5d, 1)
    )).toDF("col1", "col2", "col3", "label")

    val pipeline = new Pipeline().setStages(Array(
      new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
      new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
      new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3")).setOutputCol("features"),
      new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
    ))

    val model = pipeline.fit(data)
    val coefficients = model.stages.last.asInstanceOf[LogisticRegressionModel].coefficients.toBreeze

    assert(math.abs(coefficients(0) - 1.9099146508533622) < 1e-5)
    assert(math.abs(coefficients(1)) < 1e-5)
    assert(math.abs(coefficients(2) - 1.9099146508533622) < 1e-5)

    val infer = Seq(
      (3d, "a", 3d)
    ) toDF("col1", "col2", "col3")

    val predicted = model.transform(infer)

    val kernelShap = LocalExplainer.KernelSHAP.tabular
      .setInputCols(Array("col1", "col2", "col3"))
      .setOutputCol("shapValues")
      .setBackgroundDataset(data)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val (probability, shapValues, r2) = kernelShap
      .explain(predicted)
      .select("probability", "shapValues", "r2").as[(SV, SV, Double)]
      .head

    val shapBz = shapValues.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(math.abs(shapBz(0) - avgLabel) < 1E-5)

    // Sum of shap values should match prediction
    assert(math.abs(probability(1) - breeze.linalg.sum(shapBz)) < 1E-5)

    // Null feature (col2) should have zero shap values
    assert(math.abs(shapBz(2)) < 1E-5)

    // col1 and col3 are symmetric so they should have same shap values.
    assert(math.abs(shapBz(1) - shapBz(3)) < 1E-5)

    // R-squared of the underlying regression should be close to 1.
    assert(math.abs(r2 - 1d) < 1E-5)
  }

  test("VectorKernelSHAP can explain a model locally") {
    val randBasis = RandBasis.withSeed(123)

    val m = BDM.rand[Double](100, 5, randBasis.gaussian)
    val l = m(*, ::).map {
      row =>
        if (row(2) + row(3) > 0.5) 1d else 0d
    }

    val data = m(*, ::).iterator.zip(l.valuesIterator).map {
      case (f, l) => (f.toSpark, l)
    }.toSeq.toDF("features", "label")

    val model = new LogisticRegression().setFeaturesCol("features").setLabelCol("label").fit(data)

    // println(model.coefficients)

    val infer = Seq(
      Tuple1(SVS.dense(1d, 1d, 1d, 1d, 1d))
    ) toDF "features"

    val predicted = model.transform(infer)

    val kernelShap = LocalExplainer.KernelSHAP.vector
      .setInputCol("features")
      .setOutputCol("shapValues")
      .setBackgroundDataset(data)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val (probability, shapValues, r2) = kernelShap
      .explain(predicted)
      .select("probability", "shapValues", "r2").as[(SV, SV, Double)]
      .head

    // println((probability, shapValues, r2))

    val shapBz = shapValues.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(math.abs(shapBz(0) - avgLabel) < 1E-5)

    // Sum of shap values should match prediction
    assert(math.abs(probability(1) - breeze.linalg.sum(shapBz)) < 1E-5)

    // Null feature (col2) should have zero shap values
    assert(math.abs(shapBz(1)) < 1E-2)
    assert(math.abs(shapBz(2)) < 1E-2)
    assert(math.abs(shapBz(5)) < 1E-2)

    // R-squared of the underlying regression should be close to 1.
    assert(math.abs(r2 - 1d) < 1E-5)
  }

  test("ImageKernelSHAP can explain a model locally") {

  }
}
