// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URL

import com.microsoft.ml.spark.cognitive.SpeechResponse
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

trait SpeechKey {
  lazy val speechKey = sys.env("SPEECH_API_KEY")
}

class SpeechToTextSuite extends TransformerFuzzing[SpeechToText]
  with SpeechKey {
  import session.implicits._

  lazy val stt = new SpeechToText()
    .setSubscriptionKey(speechKey)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")

  lazy val audioBytes:Array[Byte] = {
    IOUtils.toByteArray(new URL("https://mmlspark.blob.core.windows.net/datasets/Speech/test1.wav").openStream())
  }

  lazy val df: DataFrame = Seq(
    Tuple1(audioBytes)
  ).toDF("audio")

  test("Basic Usage"){
    val toObj = SpeechResponse.makeFromRowConverter
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

  override def testObjects(): Seq[TestObject[SpeechToText]] =
    Seq(new TestObject(stt, df))

  override def reader: MLReadable[_] = SpeechToText
}
