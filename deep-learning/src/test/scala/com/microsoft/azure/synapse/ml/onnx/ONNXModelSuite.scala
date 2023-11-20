// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import breeze.linalg.{argmax, argtopk}
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import com.microsoft.azure.synapse.ml.opencv.ImageTransformer
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.{Equality, TolerantNumerics}

import java.io.File
import java.net.URL
import scala.collection.mutable

class ONNXModelSuite extends TestBase
  with TransformerFuzzing[ONNXModel] {

  override def testObjects(): Seq[TestObject[ONNXModel]] = Seq(
    new TestObject(onnxIris, testDfIrisFloat),
    new TestObject(onnxIris, testDfIrisDouble),
    new TestObject(onnxIris, testDfIrisVector),
    new TestObject(onnxMNIST, testDfMNIST),
    new TestObject(onnxAdultsIncome, testDfAdultsIncome),
    new TestObject(onnxGH1902, testDfGH1902),
    new TestObject(onnxGH1996, testDfGH1996),
    new TestObject(onnxResNet50, testDfResNet50)
  )

  override def reader: MLReadable[_] = ONNXModel

  // Note that we could use OnnxHub to get models for tests, but this makes sure we test with some known binary models
  private val baseUrl = "https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/"
  private implicit val eqDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-4)
  private implicit val eqFloat: Equality[Float] = TolerantNumerics.tolerantFloatEquality(1E-5f)
  private implicit val eqMap: Equality[Map[Long, Float]] = mapEq[Long, Float]
  private implicit val eqSeqDouble: Equality[Seq[Double]] = (a: Seq[Double], b: Any) => {
    b match {
      case sd: Seq[Double] => a.zip(sd).forall(x => x._1 === x._2)
      case _ => false
    }
  }

  import spark.implicits._

  def downloadModel(modelName: String, baseUrl: String): File = {
    val f = FileUtilities.join(BuildInfo.datasetDir, "ONNXModel", modelName)
    if (!f.exists()) {
      FileUtils.copyURLToFile(new URL(new URL(baseUrl), modelName), f)
    }
    f
  }

  private lazy val onnxIris: ONNXModel = {
    // Making sure spark context is initialized
    spark
    val model = downloadModel("iris.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("float_input" -> "features"))
      .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability"))
  }

  private lazy val testDfIrisFloat: DataFrame = Seq(
    Array(6.7f, 3.1f, 4.7f, 1.5f),
    Array(4.9f, 3.0f, 1.4f, 0.2f),
    Array(5.8f, 2.7f, 5.1f, 1.9f)
  ) toDF "features"

  private lazy val testDfIrisDouble: DataFrame = Seq(
    Array(6.7d, 3.1d, 4.7d, 1.5d),
    Array(4.9d, 3.0d, 1.4d, 0.2d),
    Array(5.8d, 2.7d, 5.1d, 1.9d)
  ) toDF "features"

  private lazy val testDfIrisVector: DataFrame = Seq(
    Tuple1(Vectors.dense(6.7d, 3.1d, 4.7d, 1.5d)),
    Tuple1(Vectors.dense(4.9d, 3.0d, 1.4d, 0.2d)),
    Tuple1(Vectors.dense(5.8d, 2.7d, 5.1d, 1.9d))
  ) toDF "features"

  private lazy val testDfIrisWrongShape: DataFrame = Seq(
    Array(6.7f, 3.1f, 4.7f)  // Should be 4 values
  ) toDF "features"

  private lazy val testDfIrisWrongShape2: DataFrame = Seq(
    Array(6.7f, 3.1f, 4.7f, 1.5f),
    Array(6.7f, 3.1f, 4.7f)  // Should be 4 values
  ) toDF "features"

  private lazy val testDfIrisEmptyDimension: DataFrame = Seq(
    Array[Double]()  // No dimension should be empty
  ) toDF "features"

  test("ONNXModel throws for wrong shape") {
    val caught = intercept[SparkException] {
      onnxIris.transform(testDfIrisWrongShape).collect()
    }
    assert(caught.getMessage.contains("IllegalArgumentException"))

    // Test when bad shape is not first in batch
    val caught2 = intercept[SparkException] {
      onnxIris.transform(testDfIrisWrongShape2).collect()
    }
    assert(caught2.getMessage.contains("IllegalArgumentException"))
  }

  test("ONNXModel throws for empty dimension") {
    val caught = intercept[SparkException] {
      onnxIris.transform(testDfIrisEmptyDimension).collect()
    }
    assert(caught.getMessage.contains("IllegalArgumentException"))
  }

  test("ONNXModel can infer observations of matching input types") {
    val predicted = onnxIris.transform(testDfIrisFloat).as[(Seq[Float], Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
  }

  test("ONNXModel can infer observations of compatible input types") {
    val predicted = onnxIris.transform(testDfIrisDouble).as[(Seq[Double], Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
  }

  test("ONNXModel can infer observations of vector input types") {
    val predicted = onnxIris.transform(testDfIrisVector).as[(DenseVector, Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
  }

  private lazy val onnxMNIST: ONNXModel = {
    // Making sure spark context is initialized
    spark
    val model = downloadModel("mnist-8.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("Input3" -> "features"))
      .setFetchDict(Map("rawPrediction" -> "Plus214_Output_0"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setArgMaxDict(Map("rawPrediction" -> "prediction"))
      .setMiniBatchSize(1)
  }

  def getLibSVM2ImageUdf(origin: String, height: Int,
                         width: Int, nChannels: Int, mode: Int): UserDefinedFunction = {
    UDFUtils.oldUdf(
      {
        data: Vector =>
          val array = data.toArray.map(_.toByte)
          Row(origin, height, width, nChannels, mode, array)
      },

      ImageSchema.columnSchema
    )
  }

  private lazy val testDfMNIST: DataFrame = {
    val mnistDataLocation: String = {
      val loc = "/tmp/mnist.t"
      val f = new File(loc)
      if (f.exists()) {
        f.delete()
      }

      FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/mnist.t"), f)
      loc
    }

    val libSVM2ImageFunc = getLibSVM2ImageUdf(
      origin = "mnist.t",
      height = 28,
      width = 28,
      nChannels = 1,
      mode = 0
    )

    //noinspection ScalaCustomHdfsFormat
    val imageDf: DataFrame = spark.read
      .format("libsvm")
      .option("numFeatures", (28 * 28).toString)
      .load(mnistDataLocation)
      .withColumn("label", col("label").cast(IntegerType))
      .withColumn("image", libSVM2ImageFunc(col("features")))

    val imageTransformer = new ImageTransformer()
      .setInputCol("image")
      .setOutputCol("features")
      .resize(28, 28)
      .centerCrop(28, 28)
      .normalize(mean = Array(0d), std = Array(1d), colorScaleFactor = 1d / 255d)
      .setTensorElementType(FloatType)

    imageTransformer.transform(imageDf).cache()
  }

  test("ONNXModel can infer for MNIST model") {
    val prediction = onnxMNIST.transform(testDfMNIST)
      .select("label", "rawPrediction", "probability", "prediction")

    val rows = prediction.as[(Int, Array[Float], Vector, Double)].head(10)

    rows.foreach {
      case (label, rawPrediction, probability, prediction) =>
        assert(label == prediction.toInt)
        assert(argmax(rawPrediction) == label)
        assert(probability.argmax == label)
        assert(probability.toArray.sum === 1.0)
    }
  }

  private lazy val featuresAdultsIncome = Array("Age", "WorkClass", "fnlwgt", "Education", "EducationNum",
    "MaritalStatus", "Occupation", "Relationship", "Race", "Gender", "CapitalGain", "CapitalLoss", "HoursPerWeek",
    "NativeCountry")

  private lazy val onnxAdultsIncome = {
    spark
    val model = downloadModel("adults_income.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(featuresAdultsIncome.map(v => (v, v)).toMap)
      .setFetchDict(Map("probability" -> "output_probability"))
      .setArgMaxDict(Map("probability" -> "prediction"))
  }

  private lazy val testDfAdultsIncome = {
    val testDf = Seq(
      (39L, " State-gov", 77516L, " Bachelors", 13L, " Never-married", " Adm-clerical",
        " Not-in-family", " White", " Male", 2174L, 0L, 40L, " United-States"),
      (52L, " Self-emp-not-inc", 209642L, " Doctorate", 16L, " Married-civ-spouse", " Exec-managerial",
        " Husband", " White", " Male", 0L, 0L, 45L, " United-States")
    ).toDF(featuresAdultsIncome: _*)

    featuresAdultsIncome.foldLeft(testDf) {
      case (acc, feature) =>
        acc.withColumn(feature, array(col(feature)))
    }.repartition(1)
  }

  private lazy val onnxGH1902 = {
    spark
    val model = downloadModel("GH1902.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setDeviceType("CPU")
      .setFeedDict(featuresAdultsIncome.map(v => (v, v)).toMap)
      .setFetchDict(Map("probability" -> "probabilities"))
      .setArgMaxDict(Map("probability" -> "prediction"))
      .setMiniBatchSize(5000)
  }

  private lazy val onnxGH1996 = {
    spark
    val model = downloadModel("GH1996.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setDeviceType("CPU")
      .setFeedDict(Map("A" -> "i1", "B" -> "i2"))
      .setFetchDict(Map("Output" -> "Y"))
      .setMiniBatchSize(5)
  }

  private lazy val testDfGH1902 = {
    val testDf = Seq(
      (39L, " State-gov", 77516L, " Bachelors", 13L, " Never-married", " Adm-clerical",
        " Not-in-family", " White", " Male", 2174L, 0L, 40L, " United-States"),
      (52L, " Self-emp-not-inc", 209642L, " Doctorate", 16L, " Married-civ-spouse", " Exec-managerial",
        " Husband", " White", " Male", 0L, 0L, 45L, " United-States")
    ).toDF(featuresAdultsIncome: _*).repartition(1)

    testDf
  }

  private lazy val testDfGH1996 = Seq((true, true), (true, false), (false, false)).toDF("i1", "i2")

  test("ONNXModel can run transform for issue 1902") {
    val Array(row1, row2) = onnxGH1902.transform(testDfGH1902)
      .select("probability", "prediction")
      .orderBy(col("prediction"))
      .as[(Seq[Double], Double)]
      .collect()

    assert(row1._1 === Seq(0.9343283176422119, 0.0656716451048851))
    assert(row1._2 === 0.0)

    assert(row2._1 === Seq(0.16954122483730316, 0.8304587006568909))
    assert(row2._2 === 1.0)
  }

  test("ONNXModel can run transform on boolean type (GH1996)") {
    val Array(row1, row2, row3) = onnxGH1996.transform(testDfGH1996)
      .orderBy(col("i1"), col("i2"), col("Output"))
      .as[(Boolean, Boolean, Boolean)]
      .collect()

    assert(row1 === (false, false, false))
    assert(row2 === (true, false, false))
    assert(row3 === (true, true, true))
  }

  test("ONNXModel can translate zipmap output properly") {
    val Array(row1, row2) = onnxAdultsIncome.transform(testDfAdultsIncome)
      .select("probability", "prediction")
      .orderBy(col("prediction"))
      .as[(Map[Long, Float], Double)]
      .collect()

    assert(row1._1 == Map(0L -> 0.99f, 1L -> 0.01f))
    assert(row1._2 == 0.0)

    assert(row2._1 == Map(0L -> 0.19000047f, 1L -> 0.8099995f))
    assert(row2._2 == 1.0)
  }

  private lazy val onnxResNet50 = {
    spark
    val model = downloadModel("resnet50-v2-7.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("data" -> "features"))
      .setFetchDict(Map("rawPrediction" -> "resnetv24_dense0_fwd"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setArgMaxDict(Map("rawPrediction" -> "prediction"))
      .setMiniBatchSize(1)
  }

  // We must clear the softMax and argMax dictionaries, as they only apply to the full model transform
  private lazy val onnxResNet50ForSlicing = {
    onnxResNet50
      .setSoftMaxDict(Map.empty[String, String])
      .setArgMaxDict(Map.empty[String, String])
  }

  private lazy val testDfResNet50: DataFrame = {
    val greyhoundImageLocation: String = {
      val loc = "/tmp/greyhound.jpg"
      val f = new File(loc)
      if (f.exists()) {
        f.delete()
      }
      FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/datasets/LIME/greyhound.jpg"), f)
      loc
    }

    val imageDf = spark.read.image.load(greyhoundImageLocation)
    val imageTransformer = new ImageTransformer()
      .setInputCol("image")
      .setOutputCol("features")
      .resize(224, 224)
      .centerCrop(224, 224)
      .normalize(mean = Array(0.485, 0.456, 0.406), std = Array(0.229, 0.224, 0.225), colorScaleFactor = 1d / 255d)
      .setTensorElementType(FloatType)

    imageTransformer.transform(imageDf).cache()
  }

  override def getterSetterParamExamples(pipelineStage: ONNXModel): Map[Param[_], Any] = Map(
    (pipelineStage.deviceType, "cpu")
  )

  test("ONNXModel can infer for resnet50 model") {
    val (probability, prediction) = onnxResNet50.transform(testDfResNet50)
      .select("probability", "prediction")
      .as[(Vector, Double)]
      .head

    val top2 = argtopk(probability.toBreeze, 2).toArray
    assert(top2 sameElements Array(176, 172))
    assert(prediction.toInt == 176)
  }

  test("Load ONNX Hub Model") {
    spark
    val name = "resnet50"
    val hub = new ONNXHub()
    val bytes = hub.load(name)
    val model = new ONNXModel()
      .setModelPayload(bytes)
      .setFeedDict(Map("data" -> "features"))
      .setFetchDict(Map("rawPrediction" -> "resnetv17_dense0_fwd"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setArgMaxDict(Map("rawPrediction" -> "prediction"))
      .setMiniBatchSize(1)

    val (probability, _) = model.transform(testDfResNet50)
      .select("probability", "prediction")
      .as[(Vector, Double)]
      .head

    val top2 = argtopk(probability.toBreeze, 2).toArray
    assert(top2.contains(176))
  }

  test("ONNXModel automatic slicing based on fetch dictionary") {
    // Set the fetch dictionary to an intermediate output, and the model should automatically slice at that point.
    val intermediateOutputName = "resnetv24_pool1_fwd"
    val adjustedSlicedModel = onnxResNet50ForSlicing
      .setFetchDict(Map("rawFeatures" -> intermediateOutputName))

    assert(!adjustedSlicedModel.modelOutput.keys.toArray.contains(intermediateOutputName))

    val intermediateDf = adjustedSlicedModel.transform(testDfResNet50)

    val firstRowFeatures = extractFirstRowFromSlicedLayer(intermediateDf, "rawFeatures")
    assert(firstRowFeatures.length == 2048)
  }

  test("ONNXModel manual slicing") {
    // Note that we must also clear the softMax and argMax dictionaries, as they only apply to the full model transform
    val intermediateOutputName = "resnetv24_pool1_fwd"
    val slicedModel = onnxResNet50ForSlicing
      .sliceAtOutput(intermediateOutputName)
      .setFetchDict(Map("rawFeatures" -> intermediateOutputName))

    assert(slicedModel.modelOutput.keys.toArray.contains(intermediateOutputName))

    val intermediateDf = slicedModel.transform(testDfResNet50)

    val firstRowFeatures = extractFirstRowFromSlicedLayer(intermediateDf, "rawFeatures")
    assert(firstRowFeatures.length == 2048)
  }

  def extractFirstRowFromSlicedLayer(df: DataFrame, colName: String): Array[Float] = {
    df.select(colName).collect()(0)(0)
      .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Float]]]]
      .map(wrappedArr => wrappedArr.head.head)
      .toArray
  }

  private lazy val testTextModel: ONNXModel = {
    spark
    val model = downloadModel("TfIdfVectorizer.onnx", baseUrl)
    new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("text" -> "features"))
      .setFetchDict(Map("encoded" -> "result"))
  }

  private lazy val testDfText1: DataFrame = {
    Seq(
      Tuple1(Array("A", "B", "C"))
    ).toDF("features")
  }

  private lazy val testDfText2: DataFrame = {
    Seq(
      Tuple1(Array("A", "B", "C")),
      Tuple1(Array("A", "B", "B", "C", "A"))
    ).toDF("features")
  }

  test("ONNX model can accept variable size input if batch size is set to 1"){

    // If the array size in each row can vary, either pass in one row at a time,
    val Array(row) = testTextModel
      .setMiniBatchSize(10)
      .transform(testDfText1.orderBy("features"))
      .select("encoded")
      .as[Seq[Float]]
      .collect()

    assert(row == Seq(1.0, 1.0, 1.0))

    // Or set the mini batch size to 1.
    val Array(row1, row2) = testTextModel
      .setMiniBatchSize(1)
      .transform(testDfText2.orderBy("features"))
      .select("encoded")
      .as[Seq[Float]]
      .collect()

    assert(row1 == Seq(2.0, 2.0, 1.0))
    assert(row2 == Seq(1.0, 1.0, 1.0))


    assertThrows[Exception] {
      // When multiple rows are sent through the same batch, and the shape varies, we cannot
      // infer the proper tensor size.
      testTextModel
        .setMiniBatchSize(10)
        .transform(testDfText2.orderBy("features"))
        .select("encoded")
        .as[Seq[Float]]
        .collect()
    }
  }
}
