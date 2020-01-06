// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.concurrent.Future

import com.microsoft.ml.spark.core.env.StreamUtilities
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalactic.Equality
import org.scalatest.Assertion
import java.util.concurrent.Future



trait SpeechKey {
  lazy val speechKey = sys.env.getOrElse("SPEECH_API_KEY", Secrets.SpeechApiKey)
}

class SpeechToTextSuite extends TransformerFuzzing[SpeechToText]
  with SpeechKey {

  import session.implicits._
  val region = "eastus"

  lazy val stt = new SpeechToText()
    .setSubscriptionKey(speechKey)
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

  test("Simple usage of new speech SDK") {
    import com.microsoft.cognitiveservices.speech.CancellationDetails
    import com.microsoft.cognitiveservices.speech.SpeechConfig
    import com.microsoft.cognitiveservices.speech._

    val speechSubscriptionKey: String = stt.getSubscriptionKey

    var exitCode: Int = 1
    val config: SpeechConfig = SpeechConfig.fromSubscription(speechSubscriptionKey, region)
    assert((config != null))

    val reco: SpeechRecognizer = new SpeechRecognizer(config)
    assert((reco != null))

    System.out.println("Say something...")

    val task: Future[SpeechRecognitionResult] = reco.recognizeOnceAsync
    assert((task != null))

    val result: SpeechRecognitionResult = task.get
    assert((result != null))

    if (result.getReason eq ResultReason.RecognizedSpeech) {
      System.out.println("We recognized: " + result.getText)
      exitCode = 0
    }
    else {
      if (result.getReason eq ResultReason.NoMatch) {
        System.out.println("NOMATCH: Speech could not be recognized.")
      }
      else {
        if (result.getReason eq ResultReason.Canceled) {
          val cancellation: CancellationDetails = CancellationDetails.fromResult(result)
          System.out.println("CANCELED: Reason=" + cancellation.getReason)
          if (cancellation.getReason eq CancellationReason.Error) {
            System.out.println("CANCELED: ErrorCode=" + cancellation.getErrorCode)
            System.out.println("CANCELED: ErrorDetails=" + cancellation.getErrorDetails)
            System.out.println("CANCELED: Did you update the subscription info?")
          }
        }
      }
    }
  }

  test("Basic Usage") {
    val toObj = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("simple")
      .transform(df).select("text")
      .collect().head.getStruct(0))
    result.DisplayText.get.contains("this is a test")
  }

  test("Speech SDK usage"){
    import com.microsoft.cognitiveservices.speech.{SpeechConfig, SpeechRecognizer}
    import com.microsoft.cognitiveservices.speech.audio.AudioConfig

    //load file into bytes
    lazy val soundFilePath: String = getClass.getResource("/hello_test.mp3").getPath
    val wavFilePath: String = getClass.getResource("/hello_test.wav").getPath
    val soundFile = new File(soundFilePath)
    val bytes = StreamUtilities.using(new FileInputStream(soundFile))(is => IOUtils.toByteArray(is)).get
    println(bytes.length)

    // set up speech to text
    val config: SpeechConfig = SpeechConfig.fromSubscription(speechKey, region)
    assert((config != null))

    val audioInput: AudioConfig = AudioConfig.fromWavFileInput(wavFilePath)
    val recognizer: SpeechRecognizer = new SpeechRecognizer(config, audioInput)
    var result = recognizer.recognizeOnceAsync.get.toString

    println(result)
    // load file into bytes
    // transcribe file into text
    // print text
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
