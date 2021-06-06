// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import breeze.linalg.{*, DenseMatrix => BDM}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.image.{ImageFeaturizer, NetworkUtils}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.lime.SuperpixelData
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{HashingTF, OneHotEncoder, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class KernelSHAPSuite extends TestBase with NetworkUtils {
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
      .transform(predicted)
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
      .transform(predicted)
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
    val resNetTransformer: ImageFeaturizer = resNetModel().setCutOutputLayers(0).setInputCol("image")

    val cellSize = 30.0
    val modifier = 50.0
    val shap = LocalExplainer.KernelSHAP.image
      .setModel(resNetTransformer)
      .setTargetCol(resNetTransformer.getOutputCol)
      .setTargetClass(172)
      .setOutputCol("weights")
      .setSuperpixelCol("superpixels")
      .setMetricsCol("r2")
      .setInputCol("image")
      .setCellSize(cellSize)
      .setModifier(modifier)
      .setNumSamples(90)

    val imageResource = this.getClass.getResource("/greyhound.jpg")
    val imageDf = spark.read.image.load(imageResource.toString)

    val (image, superpixels, shapValues, r2) = shap
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, SV, Double)]
      .head

    // println(shapValues)
    // println(r2)

    // Base value should be almost zero.
    assert(math.abs(shapValues(0)) < 1e-4)

    // R2 should be almost 1.
    assert(math.abs(r2 - 1.0) < 1e-5)

    val spStates = shapValues.toBreeze.apply(1 to -1).map(_ >= 0.05).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)
    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.Superpixel
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  test("TextSHAP can explain a model locally") {
    val df: DataFrame = Seq(
      ("hi this is example 1", 1.0),
      ("hi this is cat 1", 0.0),
      ("hi this is example 1", 1.0),
      ("foo this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi this is cat 1", 0.0),
      ("hi this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi bar is cat 1", 0.0),
      ("hi this is example 1", 1.0)
    ) toDF("text", "label")

    val tok: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    val si: HashingTF = new HashingTF().setInputCol("tokens").setOutputCol("features")
    val lr: LogisticRegression = new LogisticRegression()
      .setFeaturesCol("features").setLabelCol("label").setProbabilityCol("prob")

    val textClassifier: Pipeline = new Pipeline().setStages(Array(tok, si, lr))

    val model: PipelineModel = textClassifier.fit(df)

    val shap = LocalExplainer.KernelSHAP.text
      .setModel(model)
      .setInputCol("text")
      .setTargetCol("prob")
      .setTargetClass(1)
      .setOutputCol("weights")
      .setTokensCol("tokens")
      .setNumSamples(100)

    val target: DataFrame = Seq(
      ("hi this is example 1", 1.0),
      ("hi bar is cat 1", 0.0)
    ) toDF("text", "label")

    val results = shap.transform(target).select("tokens", "weights", "r2")
      .as[(Seq[String], SV, Double)]
      .collect()
      .map {
        case (tokens, shapValues, r2) => (tokens(3), shapValues, r2)
      }

    results.foreach {
      case (_, _, r2) =>
        assert(math.abs(1 - r2) < 1e-5)
    }

    // Sum of shap values should match predicted value
    results.foreach {
      case (token, shapValues, _) if token == "example" =>
        assert(math.abs(1 - breeze.linalg.sum(shapValues.toBreeze)) < 1e-5)
        assert(shapValues(4) > 0.29)
      case (token, shapValues, r2) if token == "cat" =>
        assert(math.abs(breeze.linalg.sum(shapValues.toBreeze)) < 1e-5)
        assert(shapValues(4) < -0.29)
    }
  }
}
