package com.microsoft.ml.spark.onnx

import breeze.linalg.{argmax, argtopk}
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.utils.BreezeUtils._
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.opencv.ImageTransformer
import org.apache.commons.io.FileUtils
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.functions._
import org.scalactic.{Equality, TolerantNumerics}

import java.io.File
import java.net.URL

class ONNXModelSuite extends TestBase {
  // with TransformerFuzzing[ONNXModel] {
  //  override def testObjects(): Seq[TestObject[ONNXModel]] = ???
  //
  //  override def reader: MLReadable[_] = ???

  private val baseUrl = "https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/"
  private implicit val eqFloat: Equality[Float] = TolerantNumerics.tolerantFloatEquality(1E-5f)
  private implicit val eqMap: Equality[Map[Long, Float]] = mapEq[Long, Float]

  import spark.implicits._

  def downloadModel(modelName: String, baseUrl: String): File = {
    val f = FileUtilities.join(BuildInfo.datasetDir, "ONNXModel", modelName)
    if (!f.exists()) {
      FileUtils.copyURLToFile(new URL(new URL(baseUrl), modelName), f)
    }
    f
  }

  test("ONNXModel can infer observations of matching input types") {
    // Making sure spark context is initialized
    spark

    val model = downloadModel("iris.onnx", baseUrl)
    val onnx = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("float_input" -> "features"))
      .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability"))

    val testDf = Seq(
      Array(6.7f, 3.1f, 4.7f, 1.5f),
      Array(4.9f, 3.0f, 1.4f, 0.2f),
      Array(5.8f, 2.7f, 5.1f, 1.9f)
    ) toDF "features"

    val predicted = onnx.transform(testDf).as[(Seq[Float], Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
  }

  test("ONNXModel can infer observations of compatible input types") {
    // Making sure spark context is initialized
    spark

    val model = downloadModel("iris.onnx", baseUrl)
    val onnx = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("float_input" -> "features"))
      .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability"))

    val testDf = Seq(
      Array(6.7d, 3.1d, 4.7d, 1.5d),
      Array(4.9d, 3.0d, 1.4d, 0.2d),
      Array(5.8d, 2.7d, 5.1d, 1.9d)
    ) toDF "features"

    val predicted = onnx.transform(testDf).as[(Seq[Double], Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
  }

  test("ONNXModel can infer observations of vector input types") {
    // Making sure spark context is initialized
    spark

    val model = downloadModel("iris.onnx", baseUrl)
    val onnx = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("float_input" -> "features"))
      .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability"))

    val testDf = Seq(
      Tuple1(Vectors.dense(6.7d, 3.1d, 4.7d, 1.5d)),
      Tuple1(Vectors.dense(4.9d, 3.0d, 1.4d, 0.2d)),
      Tuple1(Vectors.dense(5.8d, 2.7d, 5.1d, 1.9d))
    ) toDF "features"

    val predicted = onnx.transform(testDf).as[(DenseVector, Long, Map[Long, Float])].collect()

    assert(predicted(0)._2 == 1L)
    assert(predicted(0)._3 === Map(0L -> 0.0032624616f, 1L -> 0.78214455f, 2L -> 0.214593f))

    assert(predicted(1)._2 == 0L)
    assert(predicted(1)._3 === Map(0L -> 0.9666327f, 1L -> 0.033367135f, 2L -> 1.5725234E-7f))

    assert(predicted(2)._2 == 2L)
    assert(predicted(2)._3 === Map(0L -> 5.4029905E-4f, 1L -> 0.24569187f, 2L -> 0.75376785f))
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

  test("ONNXModel can infer for MNIST model") {
    val mnistDataLocation: String = {
      val loc = "/tmp/mnist.t"
      val f = new File(loc)
      if (f.exists()) {
        f.delete()
      }

      FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/mnist.t"), f)
      loc
    }

    val model = downloadModel("mnist-8.onnx", baseUrl)

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
      .normalize(Array(0d), Array(1d), 255)
      .setTensorElementType(FloatType)

    val testDf = imageTransformer.transform(imageDf).cache()

    val mnistModel = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("Input3" -> "features"))
      .setFetchDict(Map("rawPrediction"-> "Plus214_Output_0"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setArgMaxDict(Map("rawPrediction" -> "prediction"))
      .setMiniBatchSize(1)

    val prediction = mnistModel.transform(testDf)
      .select("label", "rawPrediction", "probability", "prediction")

    val rows = prediction.as[(Int, Array[Float], Vector, Double)].head(10)

    val epsilon = 1e-4
    implicit lazy val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)

    rows.foreach {
      case (label, rawPrediction, probability, prediction) =>
        assert(label == prediction.toInt)
        assert(argmax(rawPrediction) == label)
        assert(probability.argmax == label)
        assert(probability.toArray.sum === 1.0)
    }
  }

  test("ONNXModel can translate zipmap output properly") {
    val features = Array("Age", "WorkClass", "fnlwgt", "Education", "EducationNum", "MaritalStatus", "Occupation",
      "Relationship", "Race", "Gender", "CapitalGain", "CapitalLoss", "HoursPerWeek", "NativeCountry")

    val testDf = Seq(
      (39L, " State-gov", 77516L, " Bachelors", 13L, " Never-married", " Adm-clerical",
        " Not-in-family", " White", " Male", 2174L, 0L, 40L, " United-States"),
      (52L, " Self-emp-not-inc", 209642L, " Doctorate", 16L, " Married-civ-spouse", " Exec-managerial",
        " Husband", " White", " Male", 0L, 0L, 45L, " United-States")
    ).toDF(features: _*)

    val converted = features.foldLeft(testDf) {
      case (acc, feature) =>
        acc.withColumn(feature, array(col(feature)))
    }.repartition(1)

    val model = downloadModel("adults_income.onnx", baseUrl)
    val onnx = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(features.map(v => (v, v)).toMap)
      .setFetchDict(Map("probability"-> "output_probability"))
      .setArgMaxDict(Map("probability" -> "prediction"))

    val Array(row1, row2) = onnx.transform(converted)
      .select("probability", "prediction")
      .orderBy(col("prediction"))
      .as[(Map[Long, Float], Double)]
      .collect()

    assert(row1._1 == Map(0L -> 0.99f, 1L -> 0.01f))
    assert(row1._2 == 0.0)

    assert(row2._1 == Map(0L -> 0.19000047f, 1L -> 0.8099995f))
    assert(row2._2 == 1.0)
  }

  test("ONNXModel can infer for resnet50 model") {
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
      .normalize(Array(0.485, 0.456, 0.406), Array(0.229, 0.224, 0.225), 255)
      .setTensorElementType(FloatType)

    val testDf = imageTransformer.transform(imageDf).cache()

    val model = downloadModel("resnet50-v2-7.onnx", baseUrl)
    val onnx = new ONNXModel()
      .setModelLocation(model.getPath)
      .setFeedDict(Map("data" -> "features"))
      .setFetchDict(Map("rawPrediction" -> "resnetv24_dense0_fwd"))
      .setSoftMaxDict(Map("rawPrediction" -> "probability"))
      .setArgMaxDict(Map("rawPrediction" -> "prediction"))
      .setMiniBatchSize(1)

    val (probability, prediction) = onnx.transform(testDf)
      .select("probability", "prediction")
      .as[(Vector, Double)]
      .head

    val top2 = argtopk(probability.toBreeze, 2).toArray
    assert(top2 sameElements Array(176, 172))
    assert(prediction.toInt == 176)
  }
}
