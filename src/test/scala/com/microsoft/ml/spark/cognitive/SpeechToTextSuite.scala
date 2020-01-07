// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.net.URL
import java.util.concurrent.Future

import com.microsoft.cognitiveservices.speech._
import com.microsoft.cognitiveservices.speech.audio.AudioInputStream
import com.microsoft.cognitiveservices.speech.util.EventHandler
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalactic.Equality

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try



trait SpeechKey {
  lazy val speechKey = sys.env.getOrElse("SPEECH_API_KEY", Secrets.SpeechApiKey)
}

//class EventHandlerWrapper(f: )

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

  test("Basic Usage") {
    val toObj = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("simple")
      .transform(df).select("text")
      .collect().head.getStruct(0))
    result.DisplayText.get.contains("this is a test")
  }

  test("Speech SDK Usage 1"){
    import java.io.{FileInputStream, InputStream}
    import java.util
    import java.util.Collections

    import com.microsoft.cognitiveservices.speech.SpeechConfig
    import com.microsoft.cognitiveservices.speech.audio.{AudioConfig, AudioInputStream, PushAudioInputStream}

    val resourcesDir =  System.getProperty("user.dir") + "/src/test/resources/"
    val wavFilePath = resourcesDir + "/audio1.wav"
    val config: SpeechConfig = SpeechConfig.fromSubscription(speechKey, region)
    assert(config != null)

    val inputBytes = IOUtils.toByteArray(new FileInputStream(wavFilePath))
    val inputStream: InputStream = new ByteArrayInputStream(inputBytes)

    // Create the push stream to push audio to.
    val pushStream: PushAudioInputStream = AudioInputStream.createPushStream

    // Creates a speech recognizer using Push Stream as audio input.
    val audioInput: AudioConfig = AudioConfig.fromStreamInput(pushStream)

    val recognizer = new SpeechRecognizer(config, audioInput)

    def makeEventHandler[T](f: (Any, T) => Unit): EventHandler[T] = {
      new EventHandler[T] {
        def onEvent(var1: Any, var2: T): Unit = f(var1, var2)
      }
    }

    def recognizingHandler(s: Any,  e: SpeechRecognitionEventArgs): Unit = {
      println("RECOGNIZING: Text=" + e.getResult.getText)
    }

    val stringBuffer = Collections.synchronizedList(new util.ArrayList[String])

    def recognizedHandler(s: Any, e: SpeechRecognitionEventArgs): Unit = {
      if (e.getResult.getReason eq ResultReason.RecognizedSpeech) {
        println("RECOGNIZED: Text=" + e.getResult.getText)
        stringBuffer.add(e.getResult.getText)

      }
      else {
        if (e.getResult.getReason eq ResultReason.NoMatch) {
          println("NOMATCH: Speech could not be recognized.")
        }
      }
    }

    def canceledHandler(s: Any, e: SpeechRecognitionCanceledEventArgs): Unit = {
      println("CANCELED: Reason=" + e.getReason)
      if (e.getReason eq CancellationReason.Error) {
        println("CANCELED: ErrorCode=" + e.getErrorCode)
        println("CANCELED: ErrorDetails=" + e.getErrorDetails)
        println("CANCELED: Did you update the subscription info?")
      }
    }

    def sessionStartedHandler(s: Any, e: SessionEventArgs): Unit = {
      println("\n    Session started event.")
    }

    val resultPromise = Promise[String]()
    def sessionStoppedHandler(s: Any, e: SessionEventArgs): Unit = {
      println("\n    Session stopped event.")
      resultPromise.complete(Try(stringBuffer.toArray.mkString(" ")))
    }

    // Subscribes to events.
    recognizer.recognizing.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizingHandler))
    recognizer.recognized.addEventListener(makeEventHandler[SpeechRecognitionEventArgs](recognizedHandler))
    recognizer.canceled.addEventListener(makeEventHandler[SpeechRecognitionCanceledEventArgs](canceledHandler))
    recognizer.sessionStarted.addEventListener(makeEventHandler[SessionEventArgs](sessionStartedHandler))
    recognizer.sessionStopped.addEventListener(makeEventHandler[SessionEventArgs](sessionStoppedHandler))

    // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
    recognizer.startContinuousRecognitionAsync.get

    // Push audio read from the file into the PushStream.
    // The audio can be pushed into the stream before, after, or during recognition
    // and recognition will continue as data becomes available.
    pushStream.write(inputBytes)

    pushStream.close()
    inputStream.close()

    val result: String = Await.result(resultPromise.future, Duration.Inf)

    val expectedPath = resourcesDir + "/audio1.txt"
    val expectedFile = scala.io.Source.fromFile(expectedPath)
    val expected = try expectedFile.mkString finally expectedFile.close()
    assert(expected == result)

    println(s"Result: $result")

    recognizer.stopContinuousRecognitionAsync.get()
    config.close()
    audioInput.close()
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
