// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.KernelSHAP
import com.microsoft.azure.synapse.ml.explainers.{LocalExplainer, VectorSHAP}
import com.microsoft.azure.synapse.ml.stages.UDFTransformer
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.DoubleType
import org.scalactic.{Equality, TolerantNumerics}

class VectorSHAPExplainerSuite extends TestBase
  with TransformerFuzzing[VectorSHAP] {

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-3)

  override val sortInDataframeEquality = true

  import spark.implicits._

  private lazy val randBasis = RandBasis.withSeed(123)
  private lazy val m: BDM[Double] = BDM.rand[Double](1000, 5, randBasis.gaussian)
  private lazy val l: BDV[Double] = m(*, ::).map {
    row =>
      if (row(2) + row(3) > 0.5) 1d else 0d
  }

  lazy val data: DataFrame = m(*, ::).iterator.zip(l.valuesIterator).map {
    case (f, l) => (f.toSpark, l)
  }.toSeq.toDF("features", "label")

  lazy val model: LogisticRegressionModel = new LogisticRegression()
    .setFeaturesCol("features")
    .setLabelCol("label")
    .fit(data)

  // println(model.coefficients)

  lazy val infer: DataFrame = Seq(
    Tuple1(Vectors.dense(1d, 1d, 1d, 1d, 1d))
  ) toDF "features"

  lazy val kernelShap: VectorSHAP = KernelSHAP.vector
    .setInputCol("features")
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  test("VectorKernelSHAP can explain a model locally") {
    val predicted = model.transform(infer)
    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(Vector, Seq[Vector], Vector)]
      .head

    // println((probability, shapValues, r2))

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(shapBz(0) === avgLabel)

    // Sum of shap values should match prediction
    assert(probability(1) === breeze.linalg.sum(shapBz))

    // Null feature (col2) should have zero shap values
    assert(shapBz(1) === 0d)
    assert(shapBz(2) === 0d)
    assert(shapBz(5) === 0d)

    // R-squared of the underlying regression should be close to 1.
    assert(r2(0) === 1d)
  }

  test("VectorKernelSHAP explanation agrees with the shap library") {
    // The following function was tested with the shap library from https://github.com/slundberg/shap
    // with KernelExplainer, and the results should agree.
    val predictUdf = UDFUtils.oldUdf({
      vec: Vector =>
        val (x, y, z) = (vec(0), vec(1), vec(2))
        2 * x + 0.5 * y + 3 * z + 21 - 3 * x * y + 6 * x * z - 12 * x * y * z
    }, DoubleType)

    val model = new UDFTransformer()
      .setInputCol("inputs")
      .setOutputCol("output")
      .setUDF(predictUdf)

    val backgroundData = (0 until 100, 0 until 100, 0 until 100).zipped.toList
    val testData = (0 until 5, 0 until 5, 0 until 5).zipped.toList

    val assembler = new VectorAssembler().setInputCols(Array("x", "y", "z")).setOutputCol("inputs")
    val backgroundDf = assembler.transform(backgroundData.toDF("x", "y", "z"))
    val testDf = assembler.transform(testData.toDF("x", "y", "z"))

    val shap = LocalExplainer.KernelSHAP.vector.setInputCol("inputs").setOutputCol("shaps").setTargetCol("output")
      .setNumSamples(1000).setBackgroundData(backgroundDf).setModel(model)

    val actual: Seq[(Vector, Double)] = shap.transform(testDf).orderBy("x").collect()
      .map {
        row => (row.getAs[Seq[Vector]]("shaps").head, row.getAs[Vector]("r2")(0))
      }

    val expected: Seq[(Vector, Double)] = Seq(
      (Vectors.dense(-2930156.233697835, 975075.748907683, 985000.4989126588, 970100.9989126584), 1d),
      (Vectors.dense(-2930156.233961462, 975075.2489613278, 984995.4989522084, 970102.9989488379), 1d),
      (Vectors.dense(-2930156.234228749, 975053.7490012379, 984963.4989934831, 970086.9989931463), 1d),
      (Vectors.dense(-2930156.2344993753, 974987.249043968, 984880.4990304362, 970028.9990351569), 1d),
      (Vectors.dense(-2930156.234773022, 974851.749075277, 984722.4990798673, 969904.999072391), 1d)
    )

    (actual zip expected) foreach {
      case (left, right) =>
        assert(left._1.toDense === right._1.toDense)
        assert(left._2 === right._2)
    }
  }

  override def testObjects(): Seq[TestObject[VectorSHAP]] = Seq(new TestObject(kernelShap, infer))

  override def reader: MLReadable[_] = VectorSHAP
}
