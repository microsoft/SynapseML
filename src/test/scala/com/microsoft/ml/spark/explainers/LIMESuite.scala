package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, norm, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Rand
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.image.{ImageFeaturizer, NetworkUtils}
import com.microsoft.ml.spark.io.IOImplicits._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vectors => SVS}
import org.apache.spark.ml.regression.LinearRegression

class LIMESuite extends TestBase with NetworkUtils {
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
    ) toDF("col1", "col2")

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

  test("TabularLIME can explain a model locally with categorical variables") {
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
    ) toDF "col1"

    val predicted = model.transform(infer)

    val lime = new TabularLIME()
      .setInputCols(Array("col1"))
      .setCategoricalFeatures(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setNumSamples(2000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClass(1)

    val weights = lime.explain(predicted)
    val results = weights.select("col1", "weights").as[(Int, Array[Double])].collect()
      .map(r => (r._1, r._2.head))
      .toMap

    // weights for data point 1 and 4 should be close, 2 and 3 should be close by symmetry.
    assert(math.abs(results(1) - 0.38) < 1e-2)
    assert(math.abs(results(2) - 0.43) < 1e-2)
    assert(math.abs(results(3) - 0.43) < 1e-2)
    assert(math.abs(results(4) - 0.38) < 1e-2)
  }

  test("VectorLIME can explain a model locally") {
    import spark.implicits._

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
    val lime = new VectorLIME()
      .setModel(model)
      .setInputCol("features")
      .setTargetCol(model.getPredictionCol)
      .setOutputCol("weights")
      .setNumSamples(1000)

    val weights = lime.explain(predicted)

    val weightsVectors = weights.select("weights").as[Seq[Double]].collect().map {
      vec => BDV(vec: _*)
    }

    val weightsMatrix = BDM(weightsVectors: _*)
    weightsMatrix(*, ::).foreach {
      row =>
        assert(norm(row - coefficients(::, 0)) < 1e-6)
    }
  }

  test("ImageLIME can explain a model locally") {
    import spark.implicits._

    val resNetTransformer: ImageFeaturizer = resNetModel().setCutOutputLayers(0).setInputCol("image")

    val cellSize = 30.0
    val modifier = 50.0
    val lime: ImageLIME = new ImageLIME()
      .setModel(resNetTransformer)
      .setTargetCol(resNetTransformer.getOutputCol)
      .setSamplingFraction(0.7)
      .setTargetClass(172)
      .setOutputCol("weights")
      .setMetricsCol("r2")
      .setInputCol("image")
      .setCellSize(cellSize)
      .setModifier(modifier)
      .setNumSamples(50)

    val imageResource = this.getClass.getResource("/greyhound.jpg")
    val imageDf = spark.read.image.load(imageResource.toString)
    val Tuple1(image) = imageDf.select("image").as[Tuple1[ImageFormat]].head

    val (weights, r2) = lime.explain(imageDf).select("weights", "r2").as[(Seq[Double], Double)].head
    // println(weights)
    // println(r2)
    assert(math.abs(r2 - 0.91754) < 1e-2)

    val spStates = weights.map(_ >= 0.2).toArray
    // println(spStates.count(identity))
    assert(spStates.count(identity) == 8)

    // Uncomment the following lines lines to view the censoredImage image.
    //    import com.microsoft.ml.spark.io.image.ImageUtils
    //    import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
    //    import java.awt.image.BufferedImage
    //    val originalImage = ImageUtils.toBufferedImage(image.data, image.width, image.height, image.nChannels)
    //    val superPixels = SuperpixelData.fromSuperpixel(new Superpixel(originalImage, cellSize, modifier))
    //    val censoredImage: BufferedImage = Superpixel.maskImage(originalImage, superPixels, spStates)
    //    Superpixel.displayImage(censoredImage)
    //    Thread.sleep(100000)
  }
}
