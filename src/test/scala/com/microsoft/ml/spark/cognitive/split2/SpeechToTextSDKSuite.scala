// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split2

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.net.URI

import com.microsoft.ml.spark.cognitive.{SpeechResponse, SpeechToText, SpeechToTextSDK}
import com.microsoft.ml.spark.core.env.StreamUtilities
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality
import org.scalatest.Assertion

class SpeechToTextSDKSuite extends TransformerFuzzing[SpeechToTextSDK]
  with SpeechKey {

  import session.implicits._

  val region = "eastus"
  lazy val resourcesDir = new File(getClass.getResource("/").toURI)
  val uri = new URI(s"https://$region.api.cognitive.microsoft.com/sts/v1.0/issuetoken")
  val language = "en-us"
  val profanity = "masked"
  val format = "simple"

  val jaccardThreshold = 0.9

  def sdk: SpeechToTextSDK = new SpeechToTextSDK()
    .setSubscriptionKey(speechKey)
    .setLocation(region)
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")
    .setProfanity("Masked")

  lazy val audioPaths = Seq("audio1.wav", "audio2.wav", "audio3.mp3").map(new File(resourcesDir, _))

  lazy val audioBytes: Seq[Array[Byte]] = audioPaths.map(
    path => IOUtils.toByteArray(new FileInputStream(path))
  )

  lazy val Seq(bytes1, bytes2, bytes3) = audioBytes

  lazy val textPaths = Seq("audio1.txt", "audio2.txt", "audio3.txt", "audio4.txt").map(new File(resourcesDir, _))

  lazy val Seq(text1, text2, text3, text4) = textPaths.map(f =>
    StreamUtilities.usingSource(scala.io.Source.fromFile(f)) { source =>
      source.mkString
    }.get)

  lazy val Seq(audioDf1, audioDf2, audioDf3) = audioBytes.map(bytes =>
    Seq(Tuple1(bytes)).toDF("audio")
  )

  /** Simple similarity test using Jaccard index */
  def jaccardSimilarity(s1: String, s2: String): Double = {
    val a = s1.toLowerCase.sliding(2).toSet
    val b = s2.toLowerCase.sliding(2).toSet
    a.intersect(b).size.toDouble / (a | b).size.toDouble
  }

  override lazy val dfEq = new Equality[DataFrame] {
    override def areEqual(a: DataFrame, b: Any): Boolean = {
      jaccardSimilarity(
        speechArrayToText(extractResults(a, true)),
        speechArrayToText(extractResults(b.asInstanceOf[DataFrame], true))
      ) > jaccardThreshold
    }
  }

  def speechArrayToText(speechArray: Seq[SpeechResponse]): String = {
    speechArray.map(sr => sr.DisplayText.getOrElse("")).mkString(" ")
  }

  def speechTest(format: String, audioBytes: Array[Byte], expectedText: String): Assertion = {
    val resultArray = sdk.inputStreamToText(
      new ByteArrayInputStream(audioBytes),
      "wav",
      uri, speechKey, profanity, language, format, None)
    val result = speechArrayToText(resultArray.toSeq)
    if (format == "simple") {
      resultArray.foreach { rp =>
        assert(rp.NBest.isEmpty)
      }
    } else {
      resultArray.foreach { rp =>
        assert(rp.NBest.get.nonEmpty)
      }
    }
    assert(jaccardSimilarity(expectedText, result) > .9)
  }

  def extractResults(df: DataFrame, streaming: Boolean): Seq[SpeechResponse] = {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val collectedResults = df.select("text").collect()
    if (streaming) {
      collectedResults.map(row => toObj(row.getAs[Row](0)))
    } else {
      collectedResults.flatMap(row => row.getSeq[Row](0).map(toObj))
    }
  }

  def dfTest(format: String,
             input: DataFrame,
             expectedText: String,
             verbose: Boolean = false,
             sdk: SpeechToTextSDK = sdk,
             threshold: Double = jaccardThreshold): Assertion = {
    val resultSeq = extractResults(
      sdk.setFormat(format).transform(input),
      sdk.getStreamIntermediateResults)
    val result = speechArrayToText(resultSeq)

    if (verbose) {
      println(s"Expected: $expectedText")
      println(s"Actual: $result")
    }
    if (format == "simple") {
      resultSeq.foreach { rp =>
        assert(rp.NBest.isEmpty)
      }
    } else {
      resultSeq.foreach { rp =>
        assert(rp.NBest.get.nonEmpty)
      }
    }
    assert(jaccardSimilarity(expectedText, result) > threshold)
  }

  test("Simple audioBytesToText 1") {
    speechTest("simple", bytes1, text1)
  }

  test("Detailed audioBytesToText 1") {
    speechTest("detailed", bytes1, text1)
  }

  ignore("Detailed audioBytesToText 2") {
    speechTest("detailed", bytes2, text2)
  }

  test("Simple audioBytesToText 2") {
    speechTest("simple", bytes2, text2)
  }

  test("Simple SDK Usage Audio 1") {
    dfTest("simple", audioDf1, text1)
  }

  test("Detailed SDK Usage Audio 1") {
    dfTest("detailed", audioDf1, text1)
  }

  test("Simple SDK Usage Audio 2") {
    dfTest("simple", audioDf2, text2)
  }

  test("Simple SDK Usage without streaming") {
    dfTest("simple", audioDf1, text1, sdk = sdk.setStreamIntermediateResults(false))
  }

  test("Detailed SDK Usage Audio 2") {
    dfTest("detailed", audioDf2, text2)
  }

  test("URI based access") {
    val uriDf = Seq(Tuple1(audioPaths(1).toURI.toString))
      .toDF("audio")
    dfTest("detailed", uriDf, text2)
  }

  test("URL based access") {
    tryWithRetries(Array(100, 500)) { () => //For handling flaky build machines
      val uriDf = Seq(Tuple1("https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav"))
        .toDF("audio")
      dfTest("detailed", uriDf, text2)
    }
  }

  test("SAS URL based access") {
    val sasURL = "https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav" +
      "?st=2020-03-17T16%3A17%3A41Z&se=2026-03-18T16%3A17%3A00Z&sp=rl" +
      "&sv=2018-03-28&sr=b&sig=RbTzSQkKrIs3q9qmWFSNhwVpFED9COR4uqEIJyG2u7o%3D"

    tryWithRetries(Array(100, 500)) { () => //For handling flaky build machines
      val uriDf = Seq(Tuple1(sasURL))
        .toDF("audio")
      dfTest("detailed", uriDf, text2, sdk = sdk)
    }
  }

  test("Detailed SDK with mp3 (Linux only)") {
    dfTest("detailed", audioDf3, text3, sdk = sdk.setFileType("mp3"), verbose = true, threshold = .7)
  }

  test("m3u8 based access") {
    val streamUrl = "https://mnmedias.api.telequebec.tv/m3u8/29880.m3u8"
    val sdk2 = sdk.setExtraFfmpegArgs(Array("-acodec", "mp3", "-ab", "257k", "-f", "mp3", "-t", "60"))
      .setLanguage("fr-FR")
    // 20 seconds of streaming
    tryWithRetries(Array(100, 500)) { () => //For handling flaky build machines
      val uriDf = Seq(Tuple1(streamUrl))
        .toDF("audio")
      dfTest(
        "detailed",
        uriDf, text4, sdk = sdk2, threshold = .6)
    }
  }

  test("API vs. SDK") {
    val stt = new SpeechToText()
      .setSubscriptionKey(speechKey)
      .setLocation(region)
      .setOutputCol("text")
      .setAudioDataCol("audio")
      .setLanguage("en-US")
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val apiResult = toObj(stt.setFormat("simple")
      .transform(audioDf2).select("text")
      .collect().head.getStruct(0)).DisplayText.getOrElse("")

    val sdkResult = speechArrayToText(sdk.setFormat(format)
      .transform(audioDf2)
      .select("text").collect()
      .map(row => toObj(row.getAs[Row](0)))
    )
    assert(jaccardSimilarity(apiResult, sdkResult) > jaccardThreshold)
  }

  override def testObjects(): Seq[TestObject[SpeechToTextSDK]] =
    Seq(new TestObject(sdk, audioDf2))

  override def reader: MLReadable[_] = SpeechToTextSDK
}
