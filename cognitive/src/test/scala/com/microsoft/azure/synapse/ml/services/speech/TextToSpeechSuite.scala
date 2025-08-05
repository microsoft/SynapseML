// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.speech

import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import java.io.File

class TextToSpeechSuite extends TransformerFuzzing[TextToSpeech] with CognitiveKey {

  import spark.implicits._

  lazy val saveDir: File = tmpDir.toFile

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
    // Set an invalid voice name to trigger an error
    val errs = Set("AuthenticationFailure", "BadRequest")
    tts.setVoiceName("blahhh").transform(df).select("error").collect()
      .foreach(r => assert(errs.contains(r.getStruct(0).getString(0))))
  }

  lazy val ssmlDf: DataFrame = Seq((
    """<speak xmlns="http://www.w3.org/2001/10/synthesis" """ +
      """xmlns:mstts="http://www.w3.org/2001/mstts" """ +
      """xmlns:emo="http://www.w3.org/2009/10/emotionml" version="1.0" xml:lang="en-US">""" +
      """<voice name="en-US-JaneNeural"><mstts:express-as role='female' style='terrified'>""" +
      """This is how I sound right now.</mstts:express-as></voice></speak>""",
    new File(saveDir, "test1.mp3").toString)).toDF("text", "filename")

  def ttsSSML: TextToSpeech = new TextToSpeech()
    .setUseSSML(true)
    .setLocation("eastus")
    .setSubscriptionKey(cognitiveKey)
    .setTextCol("text")
    .setOutputFileCol("filename")
    .setErrorCol("error")

  test("ssml") {
    ttsSSML.transform(ssmlDf).collect().map(
      row => {
        val filename = row.getAs[String]("filename")
        assert(new File(filename).length() > 20)
      })
  }

  override def testObjects(): Seq[TestObject[TextToSpeech]] = Seq(new TestObject(tts, df))

  override def reader: MLReadable[_] = TextToSpeech
}
