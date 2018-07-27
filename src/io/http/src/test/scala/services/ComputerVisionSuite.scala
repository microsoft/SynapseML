// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.NamespaceInjections.pipelineModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalactic.Equality
import org.apache.spark.sql.functions.{struct, col}

trait VisionKey {
  lazy val visionKey = sys.env("VISION_API_KEY")
}

class OCRSuite extends TransformerFuzzing[OCR] with VisionKey {

  import session.implicits._
  import com.microsoft.ml.spark.FluentAPI._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

  lazy val ocr =  new OCR()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    //.setUrl("http://13.92.142.38:5000/vision/v1.0/ocr")
    .setImageUrlCol("url")
    .setDetectOrientation(true)
    .setOutputCol("ocr")

  test("Basic Usage") {
    val model = pipelineModel(Array(
      ocr,
      OCR.flatten("ocr", "ocr")
    ))
    val results = model.transform(df).collect()
    assert(results(2).getString(2).startsWith("This is a lot of 12 point text"))
  }

  test("grok repro"){
    val inputs = Seq(
      "https://azssparkdemo.blob.core.windows.net/images/1.png",
      "https://azssparkdemo.blob.core.windows.net/images/2.jpg",
      "https://azssparkdemo.blob.core.windows.net/images/3.jpg",
      "https://azssparkdemo.blob.core.windows.net/images/4.jpg",
      "https://azssparkdemo.blob.core.windows.net/images/5.jpg").toDF("url")

    inputs.mlTransform(
      new OCR().setSubscriptionKey("142b7e0b34c74935b2b6b4861e998e2f")
        .setLocation("westus")
        .setImageUrlCol("url")
        .setDetectOrientation(true)
        .setOutputCol("text"),
      OCR.flatten("text", "text")
    ).show()
  }

  override def testObjects(): Seq[TestObject[OCR]] =
    Seq(new TestObject(ocr, df))

  override def reader: MLReadable[_] = OCR
}

class RecognizeTextSuite extends TransformerFuzzing[RecognizeText] with VisionKey {

  import session.implicits._
  import com.microsoft.ml.spark.FluentAPI._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

  lazy val rt: RecognizeText =  new RecognizeText()
    .setSubscriptionKey(visionKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setMode("Printed")
    .setOutputCol("ocr")

  test("Basic Usage") {
    val results = df.mlTransform(rt, RecognizeText.flatten("ocr", "ocr"))
      .select("ocr")
      .collect()
    assert(results.head.getString(0) ===
      "CLOSED WHEN ONE DOOR CLOSES, ANOTHER OPENS.ALL YOU HAVE TO DO IS WALK IN")
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

  test("Basic Usage") {
    val model = pipelineModel(Array(
      celeb, RecognizeDomainSpecificContent.getProbableCeleb("celebs", "celebs")))
    val results = model.transform(df)
    assert(results.head().getString(2) === "Leonardo DiCaprio")
  }

  override implicit lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame]{
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case b:Dataset[_] =>
        val t = RecognizeDomainSpecificContent.getProbableCeleb("celebs", "celebs")
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

  test("Basic Usage") {
    val results = t.transform(df)
    assert(results.head().getAs[Array[Byte]](2).length > 1000)
  }

  override def testObjects(): Seq[TestObject[GenerateThumbnails]] =
    Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = GenerateThumbnails
}
