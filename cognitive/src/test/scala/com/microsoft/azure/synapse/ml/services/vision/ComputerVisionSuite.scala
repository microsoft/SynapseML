// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.testutils.ImageDownloadUtils
import com.microsoft.azure.synapse.ml.core.spark.FluentAPI._
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{GetterSetterFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.NamespaceInjections.pipelineModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.{col, typedLit}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalactic.Equality

trait OCRUtils extends TestBase with ImageDownloadUtils {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

  lazy val pdfDf: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/paper.pdf"
  ).toDF("url")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .select("imageBytes")

}

class OCRSuite extends TransformerFuzzing[OCR] with CognitiveKey with Flaky with OCRUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val ocr: OCR = new OCR()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageUrlCol("url")
    .setDetectOrientation(true)
    .setOutputCol("ocr")

  lazy val bytesOCR: OCR = new OCR()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageBytesCol("imageBytes")
    .setDetectOrientation(true)
    .setOutputCol("bocr")

  test("Getters") {
    assert(ocr.getDetectOrientation)
    assert(ocr.getImageUrlCol === "url")
    assert(ocr.getSubscriptionKey == cognitiveKey)
    assert(bytesOCR.getImageBytesCol === "imageBytes")
  }

  test("Basic Usage with URL") {
    val model = pipelineModel(Array(
      ocr,
      OCR.flatten("ocr", "ocr")
    ))
    val results = model.transform(df).collect()
    assert(results(2).getString(2).startsWith("This is a lot of 12 point text"))
  }

  test("Basic Usage with Bytes") {
    val model = pipelineModel(Array(
      bytesOCR,
      OCR.flatten("bocr", "bocr")
    ))

    val results = model.transform(bytesDF).collect()
    assert(results(2).getString(2).startsWith("This is a lot of 12 point text"))
  }

  override def testObjects(): Seq[TestObject[OCR]] =
    Seq(new TestObject(ocr, df))

  override def reader: MLReadable[_] = OCR
}

class AnalyzeImageV4LiveSuite extends TestBase with CognitiveKey with Flaky with ImageDownloadUtils {

  import spark.implicits._

  private val objectsImage =
    "https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/images/windows-kitchen.jpg"

  private def analyzer: AnalyzeImageV4 = new AnalyzeImageV4()
    .setSubscriptionKey(cognitiveKey).setLocation(cognitiveLoc).setFeatures(Seq("tags", "objects"))
    .setOutputCol("analysis").setErrorCol("error").setConcurrency(1)

  private def assertObjects(row: Row): Unit = {
    assert(row.getAs[Row]("error") == null, "Image Analysis 4.0 returned an HTTP error")
    val result = ImageAnalysisV4Response.makeFromRowConverter(row.getAs[Row]("analysis"))
    assert(result.modelVersion.nonEmpty)
    assert(result.metadata.width > 0 && result.metadata.height > 0)
    assert(result.tagsResult.exists(_.values.nonEmpty))
    assert(result.objectsResult.exists(_.values.exists(_.tags.nonEmpty)))
  }

  test("GA Image Analysis 4.0 tags and objects with URL input") {
    assertObjects(analyzer.setImageUrlCol("url").transform(Seq(objectsImage).toDF("url")).head())
  }

  test("GA Image Analysis 4.0 tags and objects with byte input") {
    val data = Seq(downloadBytes(objectsImage)).toDF("image")
    assertObjects(analyzer.setImageBytesCol("image").transform(data).head())
  }

  test("GA Image Analysis 4.0 reads image text synchronously") {
    val image = "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg"
    val row = analyzer.setFeatures(Seq("read")).setImageUrlCol("url")
      .transform(Seq(image).toDF("url")).head()
    assert(row.getAs[Row]("error") == null, "Image Analysis 4.0 returned an HTTP error")
    val result = ImageAnalysisV4Response.makeFromRowConverter(row.getAs[Row]("analysis"))
    assert(result.readResult.exists(_.blocks.exists(_.lines.exists(_.text.nonEmpty))))
    assert(result.tagsResult.isEmpty && result.objectsResult.isEmpty)
  }
}

class AnalyzeImageSuite extends TransformerFuzzing[AnalyzeImage]
  with CognitiveKey with Flaky with GetterSetterFuzzing[AnalyzeImage] with ImageDownloadUtils {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", null), //scalastyle:ignore null
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", "en")
  ).toDF("url", "language")

  //scalastyle:off null
  lazy val nullDf: DataFrame = Seq(
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", null),
    (null, "en")
  ).toDF("url", "language")
  //scalastyle:on null

  def baseAI: AnalyzeImage = new AnalyzeImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setOutputCol("features")
    .setLanguageCol("language")
    .setVisualFeatures(
      Seq("Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Objects", "Brands")
    )
    .setDetails(Seq("Celebrities", "Landmarks"))

  def ai: AnalyzeImage = baseAI
    .setImageUrlCol("url")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .drop("url")

  def bytesAI: AnalyzeImage = baseAI
    .setImageBytesCol("imageBytes")

  test("Null handling"){
    assertThrows[IllegalArgumentException]{
      baseAI.transform(nullDf)
    }
    assert(ai.transform(nullDf).where(col("features").isNull).count() == 1)
  }

  test("full parametrization") {
    val row = (Seq("Categories"), "en", Seq("Celebrities"),
      "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg")
    val df = Seq(row).toDF()

    val staticAi = baseAI
      .setVisualFeatures(row._1)
      .setLanguage(row._2)
      .setDetails(row._3)
      .setImageUrl(row._4)

    val dynamicAi = baseAI
      .setVisualFeaturesCol("_1")
      .setLanguageCol("_2")
      .setDetailsCol("_3")
      .setImageUrlCol("_4")

    assert(dynamicAi.getVisualFeaturesCol == "_1")
    assert(dynamicAi.getLanguageCol == "_2")
    assert(dynamicAi.getDetailsCol == "_3")
    assert(dynamicAi.getImageUrlCol == "_4")
    assert(staticAi.getVisualFeatures == row._1)
    assert(staticAi.getLanguage == row._2)
    assert(staticAi.getDetails == row._3)
    assert(staticAi.getImageUrl == row._4)
    assert(staticAi.transform(df).collect().head.getAs[Row]("features") != null)
    assert(dynamicAi.transform(df).collect().head.getAs[Row]("features") != null)
  }

  test("Basic Usage with URL") {
    val fromRow = AIResponse.makeFromRowConverter
    val responses = ai.transform(df).select("features")
      .collect().toList.map(r =>
      fromRow(r.getStruct(0)))
    assert(responses.head.categories.get.head.name === "others_")
    assert(responses(1).categories.get.head.name === "text_sign")
  }

  test("Basic Usage with Bytes") {
    val fromRow = AIResponse.makeFromRowConverter
    val responses = bytesAI.transform(bytesDF).select("features")
      .collect().toList.map(r => fromRow(r.getStruct(0)))
    assert(responses.head.categories.get.head.name === "others_")
    assert(responses(1).categories.get.head.name === "text_sign")
  }

  test("Basic Usage with Bytes and null col") {
    val fromRow = AIResponse.makeFromRowConverter
    val responses = bytesAI.setImageUrlCol("url")
      .transform(bytesDF.withColumn("url", typedLit(null: String))) //scalastyle:ignore null
      .select("features")
      .collect().toList.map(r => fromRow(r.getStruct(0)))
    assert(responses.head.categories.get.head.name === "others_")
    assert(responses(1).categories.get.head.name === "text_sign")
  }

  override def testObjects(): Seq[TestObject[AnalyzeImage]] =
    Seq(new TestObject(ai, df))

  override def reader: MLReadable[_] = AnalyzeImage

  override implicit lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame] {
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case b: Dataset[_] =>
        baseDfEq.areEqual( //TODO remove flakiness fixing hack
          a.select("features.description.tags"),
          b.select("features.description.tags"))
    }
  }

}

class ReadImageSuite extends TransformerFuzzing[ReadImage]
  with CognitiveKey with Flaky with OCRUtils {
  override val compareDataInSerializationTest: Boolean = false

  lazy val readImage: ReadImage = new ReadImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageUrlCol("url")
    .setOutputCol("ocr")
    .setConcurrency(5)

  lazy val bytesReadImage: ReadImage = new ReadImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageBytesCol("imageBytes")
    .setOutputCol("ocr")
    .setConcurrency(5)

  private def assertQuote(text: String): Unit = {
    // Read models can recognize additional text after the complete quote.
    val quotes = Seq(
      "OPENS.ALL YOU HAVE TO DO IS WALK IN WHEN ONE DOOR CLOSES, ANOTHER CLOSED",
      "CLOSED WHEN ONE DOOR CLOSES, ANOTHER OPENS. ALL YOU HAVE TO DO IS WALK IN")
    assert(quotes.exists(text.startsWith), text)
  }

  test("Basic Usage with URL") {
    val results = df.mlTransform(readImage, ReadImage.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    val headStr = results.head.getString(0)
    assertQuote(headStr)
  }

  test("Basic Usage with pdf") {
    val results = pdfDf.mlTransform(readImage, ReadImage.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    val headStr = results.head.getString(0)
    val correctPrefix = "Full Tree Conditioned Tree Component Space " +
      "Efficiency Measured Data O(n × d) 380 MB Tree O((2n/l) × d)"

    assert(headStr.startsWith(correctPrefix))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF.mlTransform(bytesReadImage, ReadImage.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    val headStr = results.head.getString(0)
    assertQuote(headStr)
  }

  override def testObjects(): Seq[TestObject[ReadImage]] =
    Seq(new TestObject(readImage, df))

  override def reader: MLReadable[_] = ReadImage
}

class RecognizeDomainSpecificContentSuite extends TransformerFuzzing[RecognizeDomainSpecificContent]
  with CognitiveKey with Flaky with ImageDownloadUtils {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg"
  ).toDF("url")

  lazy val celeb: RecognizeDomainSpecificContent = new RecognizeDomainSpecificContent()
    .setSubscriptionKey(cognitiveKey)
    .setModel("celebrities")
    .setLocation(cognitiveLoc)
    .setImageUrlCol("url")
    .setOutputCol("celebs")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .select("imageBytes")

  lazy val bytesCeleb: RecognizeDomainSpecificContent = new RecognizeDomainSpecificContent()
    .setSubscriptionKey(cognitiveKey)
    .setModel("celebrities")
    .setLocation(cognitiveLoc)
    .setImageBytesCol("imageBytes")
    .setOutputCol("celebs")

  test("Basic Usage with URL") {
    val model = pipelineModel(Array(
      celeb, RecognizeDomainSpecificContent.getMostProbableCeleb("celebs", "celebs")))
    val results = model.transform(df)
    assert(results.head().getString(2) === "Leonardo DiCaprio")
  }

  test("Basic Usage with Bytes") {
    val model = pipelineModel(Array(
      bytesCeleb, RecognizeDomainSpecificContent.getMostProbableCeleb("celebs", "celebs")))
    val results = model.transform(bytesDF)
    assert(results.head().getString(2) === "Leonardo DiCaprio")
  }

  override implicit lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame] {
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case b: Dataset[_] =>
        val t = RecognizeDomainSpecificContent.getMostProbableCeleb("celebs", "celebs")
        baseDfEq.areEqual(t.transform(a), t.transform(b))
    }
  }

  override def testObjects(): Seq[TestObject[RecognizeDomainSpecificContent]] =
    Seq(new TestObject(celeb, df))

  override def reader: MLReadable[_] = RecognizeDomainSpecificContent
}

class GenerateThumbnailsSuite extends TransformerFuzzing[GenerateThumbnails]
  with CognitiveKey with Flaky with ImageDownloadUtils {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: GenerateThumbnails = new GenerateThumbnails()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setHeight(50).setWidth(50).setSmartCropping(true)
    .setImageUrlCol("url")
    .setOutputCol("thumbnails")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .select("imageBytes")

  lazy val bytesGT: GenerateThumbnails = new GenerateThumbnails()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setHeight(50).setWidth(50).setSmartCropping(true)
    .setImageBytesCol("imageBytes")
    .setOutputCol("thumbnails")

  test("Basic Usage with URL") {
    val results = t.transform(df)
    assert(results.head().getAs[Array[Byte]](2).length > 1000)
  }

  test("Basic Usage with Bytes") {
    val results = bytesGT.transform(bytesDF)
    assert(results.head().getAs[Array[Byte]](2).length > 1000)
  }

  override def testObjects(): Seq[TestObject[GenerateThumbnails]] =
    Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = GenerateThumbnails
}

class TagImageSuite extends TransformerFuzzing[TagImage] with CognitiveKey with Flaky with ImageDownloadUtils {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: TagImage = new TagImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageUrlCol("url")
    .setOutputCol("tags")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .select("imageBytes")

  lazy val bytesTI: TagImage = new TagImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setImageBytesCol("imageBytes")
    .setOutputCol("tags")

  private def assertPersonTag(tags: Seq[Row]): Unit = {
    // v3.2 can return the more specific "human face" tag before "person".
    assert(tags.exists(tag =>
      Set("person", "human face")(tag.getString(0)) && tag.getDouble(1) > .9), tags.toString)
  }

  test("Basic Usage with URL") {
    val results = t.transform(df)
    val tagResponse = results.head()
      .getAs[Row]("tags")
      .getSeq[Row](0)

    assertPersonTag(tagResponse)
  }

  test("Basic Usage with Bytes") {
    val results = bytesTI.transform(bytesDF)
    val tagResponse = results.head()
      .getAs[Row]("tags")
      .getSeq[Row](0)

    assertPersonTag(tagResponse)
  }

  override def testObjects(): Seq[TestObject[TagImage]] =
    Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = TagImage
}

class DescribeImageSuite extends TransformerFuzzing[DescribeImage]
  with CognitiveKey with Flaky with ImageDownloadUtils {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: DescribeImage = new DescribeImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setMaxCandidates(3)
    .setImageUrlCol("url")
    .setOutputCol("descriptions")

  lazy val bytesDF: DataFrame = df
    .withColumn("imageBytes", downloadBytesUdf(col("url")))
    .select("imageBytes")

  lazy val bytesDI: DescribeImage = new DescribeImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(cognitiveLoc)
    .setMaxCandidates(3)
    .setImageBytesCol("imageBytes")
    .setOutputCol("descriptions")

  test("Basic Usage with URL") {
    val results = t.transform(df)
    val tags = results.select("descriptions").take(1).head
      .getStruct(0).getStruct(0).getSeq[String](0).toSet
    assert(tags("person") && tags("glasses"))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDI.transform(bytesDF)
    val tags = results.select("descriptions").take(1).head
      .getStruct(0).getStruct(0).getSeq[String](0).toSet
    assert(tags("person") && tags("glasses"))
  }

  override def testObjects(): Seq[TestObject[DescribeImage]] =
    Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = DescribeImage

}
