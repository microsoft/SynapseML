// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive.AIResponse
import org.apache.spark.ml.NamespaceInjections.pipelineModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalactic.Equality
import org.apache.spark.sql.functions.typedLit
import org.scalatest.Assertion

trait VisionKey {
  lazy val visionKey = sys.env("VISION_API_KEY")
}

class OCRSuite extends TransformerFuzzing[OCR] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

  lazy val ocr = new OCR()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setDefaultLanguage("en")
    .setImageUrlCol("url")
    .setDetectOrientation(true)
    .setOutputCol("ocr")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesOCR = new OCR()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setDefaultLanguage("en")
    .setImageBytesCol("imageBytes")
    .setDetectOrientation(true)
    .setOutputCol("bocr")

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

class AnalyzeImageSuite extends TransformerFuzzing[AnalyzeImage] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", null),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", "en")
  ).toDF("url", "language")

  lazy val ai: AnalyzeImage = new AnalyzeImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setLanguageCol("language")
    .setDefaultLanguage("en")
    .setVisualFeatures(
      Seq("Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"))
    .setDetails(Seq("Celebrities", "Landmarks"))
    .setOutputCol("features")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .drop("url")

  lazy val bytesAI: AnalyzeImage = new AnalyzeImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setLanguageCol("language")
    .setDefaultLanguage("en")
    .setVisualFeatures(
      Seq("Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult")
    )
    .setDetails(Seq("Celebrities", "Landmarks"))
    .setOutputCol("features")

  test("Basic Usage with URL") {
    val fromRow = AIResponse.makeFromRowConverter
    val responses = ai.transform(df).select("features")
      .collect().toList.map(r => fromRow(r.getStruct(0)))
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
      .transform(bytesDF.withColumn("url", typedLit(null: String)))
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
        baseDfEq.areEqual(
          a.select("features.*").drop("requestId"),
          b.select("features.*").drop("requestId"))
    }
  }

}

class RecognizeTextSuite extends TransformerFuzzing[RecognizeText] with VisionKey {

  import com.microsoft.ml.spark.FluentAPI._
  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

  lazy val rt: RecognizeText = new RecognizeText()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setMode("Printed")
    .setOutputCol("ocr")
    .setConcurrency(5)

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesRT: RecognizeText = new RecognizeText()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setMode("Printed")
    .setOutputCol("ocr")
    .setConcurrency(5)

  test("Basic Usage with URL") {
    val results = df.mlTransform(rt, RecognizeText.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "OPENS.ALL YOU HAVE TO DO IS WALK IN WHEN ONE DOOR CLOSES, ANOTHER CLOSED" ||
      headStr === "CLOSED WHEN ONE DOOR CLOSES, ANOTHER OPENS. ALL YOU HAVE TO DO IS WALK IN")
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF.mlTransform(bytesRT, RecognizeText.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "OPENS.ALL YOU HAVE TO DO IS WALK IN WHEN ONE DOOR CLOSES, ANOTHER CLOSED" ||
      headStr === "CLOSED WHEN ONE DOOR CLOSES, ANOTHER OPENS. ALL YOU HAVE TO DO IS WALK IN")
  }

  override def testObjects(): Seq[TestObject[RecognizeText]] =
    Seq(new TestObject(rt, df))

  override def reader: MLReadable[_] = RecognizeText
}

class RecognizeDomainSpecificContentSuite extends TransformerFuzzing[RecognizeDomainSpecificContent] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg"
  ).toDF("url")

  lazy val celeb: RecognizeDomainSpecificContent = new RecognizeDomainSpecificContent()
    .setSubscriptionKey(visionKey)
    .setModel("celebrities")
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("celebs")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesCeleb: RecognizeDomainSpecificContent = new RecognizeDomainSpecificContent()
    .setSubscriptionKey(visionKey)
    .setModel("celebrities")
    .setLocation("eastus")
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

class GenerateThumbnailsSuite extends TransformerFuzzing[GenerateThumbnails] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: GenerateThumbnails = new GenerateThumbnails()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setHeight(50).setWidth(50).setSmartCropping(true)
    .setImageUrlCol("url")
    .setOutputCol("thumbnails")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesGT: GenerateThumbnails = new GenerateThumbnails()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
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

class TagImageSuite extends TransformerFuzzing[TagImage] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: TagImage = new TagImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("tags")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesTI: TagImage = new TagImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("tags")

  test("Basic Usage with URL") {
    val results = t.transform(df)
    val tagResponse = results.head()
      .getAs[Row]("tags")
      .getSeq[Row](0)

    assert(tagResponse.map(_.getString(0)).toList.head === "person")
    assert(tagResponse.map(_.getDouble(1)).toList.head > .9)
  }

  test("Basic Usage with Bytes") {
    val results = bytesTI.transform(bytesDF)
    val tagResponse = results.head()
      .getAs[Row]("tags")
      .getSeq[Row](0)

    assert(tagResponse.map(_.getString(0)).toList.head === "person")
    assert(tagResponse.map(_.getDouble(1)).toList.head > .9)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Assertion = {
    super.assertDFEq(df1.select("tags.tags"), df2.select("tags.tags"))(eq)
  }

  override def testObjects(): Seq[TestObject[TagImage]] =
    Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = TagImage
}

class DescribeImageSuite extends TransformerFuzzing[DescribeImage] with VisionKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

  lazy val t: DescribeImage = new DescribeImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setMaxCandidates(3)
    .setImageUrlCol("url")
    .setOutputCol("descriptions")

  lazy val bytesDF: DataFrame = BingImageSearch
    .downloadFromUrls("url", "imageBytes", 4, 10000)
    .transform(df)
    .select("imageBytes")

  lazy val bytesDI: DescribeImage = new DescribeImage()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
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
