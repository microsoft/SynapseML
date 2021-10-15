// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split2

import com.microsoft.azure.synapse.ml.cognitive.split1.CognitiveKey
import com.microsoft.azure.synapse.ml.cognitive.{SpeechResponse, SpeechToText}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

import java.net.{URI, URL}

class SpeechToTextSuite extends TransformerFuzzing[SpeechToText]
  with CognitiveKey {

  import spark.implicits._

  val region = "eastus"
  val resourcesDir = System.getProperty("user.dir") + "/src/test/resources/"
  val uri = new URI(s"https://$region.api.cognitive.microsoft.com/sts/v1.0/issuetoken")
  val language = "en-us"
  val profanity = "masked"
  val format = "simple"

  lazy val stt = new SpeechToText()
    .setSubscriptionKey(cognitiveKey)
    .setLocation(region)
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")

  lazy val audioBytes: Array[Byte] = {
    IOUtils.toByteArray(new URL("https://mmlspark.blob.core.windows.net/datasets/Speech/test1.wav").openStream())
  }

  lazy val df: DataFrame = Seq(
    Tuple1(audioBytes)
  ).toDF("audio")

  override lazy val dfEq = new Equality[DataFrame] {
    override def areEqual(a: DataFrame, b: Any): Boolean =
      baseDfEq.areEqual(a.drop("audio"), b.asInstanceOf[DataFrame].drop("audio"))
  }

  override def testSerialization(): Unit = {
    tryWithRetries(Array(0, 100, 100, 100, 100))(super.testSerialization)
  }

  /** Simple similarity test using Jaccard index */
  def jaccardSimilarity(s1: String, s2: String): Double = {
    val a = Set(s1)
    val b = Set(s2)
    a.intersect(b).size.toDouble / (a | b).size.toDouble
  }

  test("Basic Usage") {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("simple")
      .transform(df).select("text")
      .collect().head.getStruct(0))
    result.DisplayText.get.contains("this is a test")
  }

  test("Detailed Usage") {
    val toObj = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("detailed")
      .transform(df).select("text")
      .collect().head.getStruct(0))
    result.NBest.get.head.Display.contains("this is a test")
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new SpeechToText()
        .setSubscriptionKey(cognitiveKey)
        .setLocation(region)
        .setOutputCol("text")
        .transform(df).collect()
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("audioData"))
    assert(caught.getMessage.contains("language"))
  }

  override def testObjects(): Seq[TestObject[SpeechToText]] =
    Seq(new TestObject(stt, df))

  override def reader: MLReadable[_] = SpeechToText
}
