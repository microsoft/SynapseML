// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{FileInputStream, FileNotFoundException}
import java.net.URI

import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.math.{max, min}

trait SpeechKey {
  lazy val speechKey = sys.env.getOrElse("SPEECH_API_KEY", Secrets.SpeechApiKey)
}

class SpeechToTextSuite extends TransformerFuzzing[SpeechToText]
  with SpeechKey {

  import session.implicits._

  val region = "eastus"
  val resourcesDir = System.getProperty("user.dir") + "/src/test/resources/"
  val uri = new URI(s"https://$region.api.cognitive.microsoft.com/sts/v1.0/issuetoken")
  val language = "en-us"
  val profanity = "masked"
  val format = "simple"

  lazy val stt = new SpeechToText()
    .setSubscriptionKey(speechKey)
    .setLocation(region)
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")

  lazy val sdk = new SpeechToTextSDK()
    .setSubscriptionKey(speechKey)
    .setLocation(region)
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")
    .setProfanity("Masked")

  lazy val audioPaths = Array[String](resourcesDir + "audio1.wav", resourcesDir + "audio2.wav")

  lazy val audioBytes: Array[Array[Byte]] = audioPaths.map(
    path => IOUtils.toByteArray(new FileInputStream((path)))
  )

  lazy val audioDfs: Array[DataFrame] = audioBytes.map(bytes =>
    Seq(Tuple1(bytes)).toDF("audio")
  )

  override lazy val dfEq = new Equality[DataFrame] {
    override def areEqual(a: DataFrame, b: Any): Boolean =
      baseDfEq.areEqual(a.drop("audio"), b.asInstanceOf[DataFrame].drop("audio"))
  }

  override def testSerialization(): Unit = {
    tryWithRetries(Array(0, 100, 100, 100, 100))(super.testSerialization)
  }

  test("Basic Usage") {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("simple")
      .transform(audioDfs(1)).select("text")
      .collect().head.getStruct(0))
    result.DisplayText.get.contains("this is a test")
  }

  test("Detailed Usage") {
    val toObj = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("detailed")
      .transform(audioDfs(1)).select("text")
      .collect().head.getStruct(0))
    result.NBest.get.head.Display.contains("this is a test")
  }

  def editDistHelper(str1: String,  str2: String,  x: Int, y: Int): Int = {
    if (x == 0) {
      y
    } else if (y == 0) {
      x
    } else if (str1.charAt(x-1) == str2.charAt(y-1)) {
      editDistHelper(str1, str2, x-1, y-1)
    } else {
      1 + min(editDistHelper(str1, str2, x, y - 1), // Insert
        min(
          editDistHelper(str1, str2, x - 1, y), // Remove
          editDistHelper(str1, str2, x - 1, y - 1) // Replace
        )
      )
    }
  }

  /** Finds the edit distance between two strings, timing out if they are too different*/
  def editDist(str1: String,  str2: String, timeLimit: Int = 100000): Int = {
    val editDistance: Future[Int] = Future.apply {
      editDistHelper(str1.toLowerCase(), str2.toLowerCase(), str1.length, str2.length)
    }
    try{
      Await.result(editDistance, Duration(timeLimit, "millis"))
    } catch {
      case x: Exception => {
        max(str1.length, str2.length)
      }
    }
  }

  /** Checks if 2 strings are close enough. True if edit distance < 5% */
  def stringsCloseEnough(str1: String, str2: String, threshold: Double = 0.05): Boolean = {
    editDist(str1, str2) < threshold * (min(str1.length, str2.length))
  }

  def speechTest(audioFile: String, textFile: String): Boolean = {
    val audioFilePath = resourcesDir + audioFile
    val expectedPath = resourcesDir + textFile
    val expectedFile = scala.io.Source.fromFile(expectedPath)
    val expected = try expectedFile.mkString finally expectedFile.close()
    val bytes = IOUtils.toByteArray(new FileInputStream(audioFilePath))
    val result = sdk.audioBytesToText(bytes, speechKey, uri, language, profanity, format)
    stringsCloseEnough(expected, result)
  }

  def dfTest(format: String, audioFileNumber: Int, verbose: Boolean = false): Boolean = {
    val expectedFile = scala.io.Source.fromFile(resourcesDir + $"audio$audioFileNumber.txt")
    val expected = try expectedFile.mkString finally expectedFile.close()
    val result = sdk.setFormat(format)
      .transform(audioDfs(audioFileNumber-1)).select("text")
      .collect().head.getAs[String]("text")
    if (verbose) {
      println(s"Expected: $expected")
      println(s"Actual: $result")
    }
    stringsCloseEnough(result, expected)
  }

  test("Basic Edit Distance") {
    val str1 = "cat"
    val str2 = "cab"
    assert(editDist(str1, str2) == 1)
  }

  test("Basic Edit Distance 2") {
    val str1 = "beautiful"
    val str2 = "beauties"
    assert(editDist(str1, str2) == 3)
  }

  test("SDK audioBytesToText 1"){
    assert(speechTest("audio1.wav", "audio1.txt"))
  }

  test("SDK audioBytesToText 2"){
    assert(speechTest("audio2.wav", "audio2.txt"))
  }

  test("SDK audioBytesToText File Doesn't Exist") {
    assertThrows[FileNotFoundException] {
      speechTest("audio3.wav", "audio3.txt")
    }
  }

  test("Simple SDK Usage Audio 1") {
    assert(dfTest("simple", 1, true))
  }

  test("Detailed SDK Usage Audio 1") {
    assert(dfTest("detailed", 1, true))
  }

  test("Simple SDK Usage Audio 2") {
    assert(dfTest("simple", 2, true))
  }

  test("Detailed SDK Usage Audio 2") {
    assert(dfTest("detailed", 2, true))
  }

  override def testObjects(): Seq[TestObject[SpeechToText]] =
    Seq(new TestObject(stt, audioDfs(1)))

  override def reader: MLReadable[_] = SpeechToText
}
