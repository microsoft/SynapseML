// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, argsort, argtopk, norm, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Rand
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.image.{ImageFeaturizer, NetworkUtils}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.lime.SuperpixelData
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import com.microsoft.ml.spark.explainers.BreezeUtils._

class LIMESuite extends TestBase with NetworkUtils {
  import spark.implicits._

  test("TabularLIME can explain a simple logistic model locally with one variable") {
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

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setKernelWidth(0.001)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(0, 1))

    val (weights, r2) = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].head
    assert(weights.size == 2)
    assert(r2.size == 2)

    val weightsBz0 = weights.head.toBreeze
    val weightsBz1 = weights(1).toBreeze

    // The derivative of the logistic function with coefficient k at x = 0, simplifies to k/4.
    // We set the kernel width to a very small value so we only consider a very close neighborhood
    // for regression, and set L1 regularization to zero so it does not affect the fit coefficient.
    // Therefore, the coefficient of the lasso regression should approximately match the derivative.
    assert(norm(weightsBz0 + BDV(coefficient / 4)) < 1e-2)
    assert(math.abs(r2(0) - 1d) < 1e-6, "R-squared of the fit should be close to 1.")

    assert(norm(weightsBz1 - BDV(coefficient / 4)) < 1e-2)
    assert(math.abs(r2(1) - 1d) < 1e-6, "R-squared of the fit should be close to 1.")
  }

  test("TabularLIME can explain a simple logistic model locally with multiple variables") {
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

    val coefficients = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray

    assert(math.abs(coefficients(0) - 3.5279868) < 1e-5)
    assert(math.abs(coefficients(1) - 0.5962254) < 1e-5)

    val infer = Seq(
      (0.0, 0.0)
    ) toDF("col1", "col2")

    val predicted = model.transform(infer)

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setKernelWidth(0.05)
      .setNumSamples(1000)
      .setRegularization(0.01)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val (weights, _) = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].head
    assert(weights.size == 1)

    val weightsBz = weights.head

    // println(weightsBz)

    // With 0.01 L1 regularization, the second feature gets removed due to low importance.
    assert(weightsBz(0) > 0.0)
    assert(weightsBz(1) == 0.0)
  }

  test("TabularLIME can explain a model locally with categorical variables") {
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
    ) toDF "col1"

    val predicted = model.transform(infer)

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1"))
      .setCategoricalFeatures(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val weights = lime.transform(predicted)

    weights.show(false)

    val results = weights.select("col1", "weights").as[(Int, Seq[SV])].collect()
      .map(r => (r._1, r._2.head.toBreeze(0)))
      .toMap

    // Assuming local linear behavior:
    // col1 == 1 reduces the model output by more than 60% compared to model not knowing about col1
    assert(results(1) < -0.6)
    // col1 == 2 reduces the model output by more than 60% compared to model not knowing about col1
    assert(results(2) < -0.6)
    // col1 == 3 increases the model output by more than 60% compared to model not knowing about col1
    assert(results(3) > 0.6)
    // col1 == 4 increases the model output by more than 60% compared to model not knowing about col1
    assert(results(3) > 0.6)
  }

  test("VectorLIME can explain a model locally") {
    val nRows = 100
    val intercept = math.random()
    val d1 = 3
    val d2 = 1

    val coefficients: BDM[Double] = new BDM(d1, d2, Array(1.0, -1.0, 2.0))
    val x: BDM[Double] = BDM.rand(nRows, d1, Rand.gaussian)
    val y = x * coefficients + intercept

    val xRows = x(*, ::).iterator.toSeq.map(dv => SVS.dense(dv.toArray))
    val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
    val df = xRows.zip(yRows).toDF("features", "label")

    val model = new LinearRegression().fit(df)

    val predicted = model.transform(df)
    val lime = LocalExplainer.LIME.vector
      .setModel(model)
      .setInputCol("features")
      .setTargetCol(model.getPredictionCol)
      .setOutputCol("weights")
      .setNumSamples(1000)

    val weights = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].collect().map {
      case (m, _) => m.head.toBreeze
    }

    val weightsMatrix = BDM(weights: _*)
    weightsMatrix(*, ::).foreach {
      row =>
        assert(norm(row - coefficients(::, 0)) < 1e-6)
    }
  }

  private val resNetTransformer: ImageFeaturizer = resNetModel().setCutOutputLayers(0).setInputCol("image")

  private val cellSize = 30.0
  private val modifier = 50.0
  private val lime: ImageLIME = LocalExplainer.LIME.image
    .setModel(resNetTransformer)
    .setTargetCol(resNetTransformer.getOutputCol)
    .setSamplingFraction(0.7)
    .setTargetClasses(Array(172))
    .setOutputCol("weights")
    .setSuperpixelCol("superpixels")
    .setMetricsCol("r2")
    .setInputCol("image")
    .setCellSize(cellSize)
    .setModifier(modifier)
    .setNumSamples(50)

  private val imageResource = this.getClass.getResource("/greyhound.jpg")

  test("ImageLIME can explain a model locally for image type observation") {
    val imageDf = spark.read.image.load(imageResource.toString)

    val (image, superpixels, weights, r2) = lime
      .transform(imageDf)
      .select("image", "superpixels", "weights", "r2")
      .as[(ImageFormat, SuperpixelData, Seq[SV], SV)]
      .head

    // println(weights)
    // println(r2)
    assert(math.abs(r2(0) - 0.91754) < 1e-2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)

    // Uncomment the following lines lines to view the censoredImage image.
    // import com.microsoft.ml.spark.io.image.ImageUtils
    // import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
    // import java.awt.image.BufferedImage
    // val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    // val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superpixels, spStates)
    // Superpixel.displayImage(censoredImage)
    // Thread.sleep(100000)
  }

  test("ImageLIME can explain a model locally for binary type observation") {
    val binaryDf = spark.read.binary.load(imageResource.toString)
      .select(col("value.bytes").alias("image"))

    val (weights, r2) = lime
      .transform(binaryDf)
      .select("weights", "r2")
      .as[(Seq[SV], SV)]
      .head

    // println(weights)
    // println(r2)
    assert(math.abs(r2(0) - 0.91754) < 1e-2)

    val spStates = weights.head.toBreeze.map(_ >= 0.2).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)
  }

  test("TextLIME can explain a model locally") {
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

    val textLime = LocalExplainer.LIME.text
      .setModel(model)
      .setInputCol("text")
      .setTargetCol("prob")
      .setTargetClasses(Array(1))
      .setOutputCol("weights")
      .setTokensCol("tokens")
      .setSamplingFraction(0.7)
      .setNumSamples(1000)

    val target: DataFrame = Seq(
      ("hi this is example 1", 1.0),
      ("hi bar is cat 1", 0.0)
    ) toDF("text", "label")

    val results = textLime.transform(target).select("tokens", "weights", "r2")
      .as[(Seq[String], Seq[SV], SV)]
      .collect()
      .map {
        case (tokens, weights, r2) => (tokens(3), weights.head(3), r2(0))
      }

    results.foreach {
      case (token, weight, r2) if token == "example" =>
        assert(weight > 0.2)
        assert(r2 > 0.6)
      case (token, weight, r2) if token == "cat" =>
        assert(weight < 0)
        assert(r2 > 0.3)
    }
  }
}
