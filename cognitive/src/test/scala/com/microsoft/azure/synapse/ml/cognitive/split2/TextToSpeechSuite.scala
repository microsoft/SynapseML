// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split2

import com.microsoft.azure.synapse.ml.cognitive.TextToSpeech
import com.microsoft.azure.synapse.ml.cognitive.split1.CognitiveKey
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.Files

class TextToSpeechSuite extends TransformerFuzzing[TextToSpeech] with CognitiveKey {

  import spark.implicits._

  lazy val saveDir: File = Files.createTempDirectory("AudioFiles-").toFile

  override def afterAll(): Unit = {
    if (saveDir.exists()) {
      FileUtils.forceDelete(saveDir)
    }
    super.afterAll()
  }

  def tts: TextToSpeech = new TextToSpeech()
    .setLocation("eastus")
    .setSubscriptionKey(cognitiveKey)
    .setTextCol("text")
    .setOutputFileCol("filename")
    .setErrorCol("error")

  lazy val df: DataFrame = Seq(
    ("Hello world this is some sample text", new File(saveDir, "test1.mp3").toString),
    ("Hola como estas", new File(saveDir, "test2.mp3").toString),
    ("Speech synthesis example", new File(saveDir, "test3.mp3").toString)).toDF("text", "filename")

  test("Basic Usage") {
    tts.transform(df).collect()
      .map(row => row.getString(1))
      .foreach(filename => assert(new File(filename).length() > 20))
  }

  test("Error Usage") {
    tts.setVoiceName("blahhh").transform(df).select("error").collect()
      .foreach(r => assert(r.getStruct(0).getString(0) === "AuthenticationFailure"))
  }

  override def testObjects(): Seq[TestObject[TextToSpeech]] = Seq(new TestObject(tts, df))

  override def reader: MLReadable[_] = TextToSpeech

}
