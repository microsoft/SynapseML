// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{ByteArrayInputStream, FileInputStream, FileNotFoundException, InputStream}
import java.net.URL
import java.util
import java.util.Collections
import java.util.concurrent.Future

import com.microsoft.cognitiveservices.speech.audio.{AudioConfig, AudioInputStream, PushAudioInputStream}
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.cognitiveservices.speech.{SpeechConfig, _}
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try


trait SpeechKey {
  lazy val speechKey = sys.env.getOrElse("SPEECH_API_KEY", Secrets.SpeechApiKey)
}

class SpeechToTextSuite extends TransformerFuzzing[SpeechToText]
  with SpeechKey {

  import session.implicits._

  val region = "eastus"
  val resourcesDir = System.getProperty("user.dir") + "/src/test/resources/"

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

  test("Basic Usage") {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val result = df.select("audio")

//    val result = toObj(stt.setFormat("simple")
//      .transform(df).select("text")
//      .collect().head.getStruct(0))
//    result.DisplayText.get.contains("this is a test")
  }

  test("Detailed Usage") {
    val toObj = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("detailed")
      .transform(df).select("text")
      .collect().head.getStruct(0))
    result.NBest.get.head.Display.contains("this is a test")
  }

  test("Simple usage of new speech SDK") {
    import com.microsoft.cognitiveservices.speech.{CancellationDetails, SpeechConfig, _}

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

  def makeEventHandler[T](f: (Any, T) => Unit): EventHandler[T] = {
    new EventHandler[T] {
      def onEvent(var1: Any, var2: T): Unit = f(var1, var2)
    }
  }

  def recognizingHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
  }

  def canceledHandler(s: Any, e: SpeechRecognitionCanceledEventArgs): Unit = {
  }

  def sessionStartedHandler(s: Any, e: SessionEventArgs): Unit = {
  }

  /**
    * @param filename a WAV file in the resources directory
    * @return text transcription of the WAV file
    */
  def wavToText(filename: String): String = {
    val wavFilePath = resourcesDir + "/" + filename
    val config: SpeechConfig = SpeechConfig.fromSubscription(speechKey, region)
    assert(config != null)

    val inputBytes = IOUtils.toByteArray(new FileInputStream(wavFilePath))
    val inputStream: InputStream = new ByteArrayInputStream(inputBytes)

    val pushStream: PushAudioInputStream = AudioInputStream.createPushStream
    val audioInput: AudioConfig = AudioConfig.fromStreamInput(pushStream)

    val recognizer = new SpeechRecognizer(config, audioInput)
    val resultPromise = Promise[String]()
    val stringBuffer = Collections.synchronizedList(new util.ArrayList[String])

    def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
      if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
        stringBuffer.add(e.getResult.getText)
      }
      else if (e.getResult.getReason eq ResultReason.NoMatch) {
      }
    }

    def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
      resultPromise.complete(Try(stringBuffer.toArray.mkString(" ")))
    }

    recognizer.recognizing.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizingHandler))
    recognizer.recognized.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizedHandler))
    recognizer.canceled.addEventListener(makeEventHandler[SpeechRecognitionCanceledEventArgs](canceledHandler))
    recognizer.sessionStarted.addEventListener(makeEventHandler[SessionEventArgs](sessionStartedHandler))
    recognizer.sessionStopped.addEventListener(makeEventHandler[SessionEventArgs](sessionStoppedHandler))

    recognizer.startContinuousRecognitionAsync.get
    pushStream.write(inputBytes)

    pushStream.close()
    inputStream.close()

    val result: String = Await.result(resultPromise.future, Duration.Inf)

    recognizer.stopContinuousRecognitionAsync.get()
    config.close()
    audioInput.close()
    result
  }

  /** Checks if 2 strings are close enough. Strips all non-alphanumerics and compares remaining chars */
  def stringsCloseEnough(string1: String, string2: String): Boolean = {
    val simpleString1 = string1.replaceAll("[^A-Za-z0-9]", "").toLowerCase()
    val simpleString2 = string2.replaceAll("[^A-Za-z0-9]", "").toLowerCase()
    simpleString1 == simpleString2
  }

  def speechTest(audioFilePath: String, textFilePath: String): Boolean = {
    val expectedPath = resourcesDir + "/" + textFilePath
    val expectedFile = scala.io.Source.fromFile(expectedPath)
    val expected = try expectedFile.mkString finally expectedFile.close()
    val result = wavToText(audioFilePath)
    stringsCloseEnough(expected, result)
  }

  test("Speech SDK Usage 1"){
    assert(speechTest("audio1.wav", "audio1.txt"))
  }

  test("Speech SDK Usage 2"){
    assert(speechTest("audio2.wav", "audio2.txt"))
  }

  test("Speech SDK File Doesn't Exist") {
    assertThrows[FileNotFoundException] {
      speechTest("audio3.wav", "audio3.txt")
    }
  }

  override def testObjects(): Seq[TestObject[SpeechToText]] =
    Seq(new TestObject(stt, df))

  override def reader: MLReadable[_] = SpeechToText
}
