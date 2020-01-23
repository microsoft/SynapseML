// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.io.{File, FileInputStream}
import java.net.URI

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

  lazy val sdk = new SpeechToTextSDK()
    .setSubscriptionKey(speechKey)
    .setLocation(region)
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")
    .setProfanity("Masked")

  lazy val audioPaths = Seq("audio1.wav", "audio2.wav").map(new File(resourcesDir, _))

  lazy val audioBytes: Seq[Array[Byte]] = audioPaths.map(
    path => IOUtils.toByteArray(new FileInputStream(path))
  )

  lazy val Seq(bytes1, bytes2) = audioBytes

  lazy val textPaths = Seq("audio1.txt", "audio2.txt").map(new File(resourcesDir, _))

  lazy val Seq(text1, text2) = textPaths.map(f =>
    StreamUtilities.usingSource(scala.io.Source.fromFile(f)) { source =>
      source.mkString
    }.get)

  lazy val Seq(audioDf1, audioDf2) = audioBytes.map(bytes =>
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
      jaccardSimilarity(extractFirstResult(a), extractFirstResult(b.asInstanceOf[DataFrame])) > jaccardThreshold
    }
  }

  def speechArrayToText(speechArray: Seq[SpeechResponse]): String = {
    speechArray.map(sr => sr.DisplayText.getOrElse("")).mkString(" ")
  }

  def speechTest(format: String, audioBytes: Array[Byte], expectedText: String): Assertion = {
    val resultArray = sdk.audioBytesToText(audioBytes, speechKey, uri, language, profanity, format)
    val result = speechArrayToText(resultArray)
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

  def extractFirstResult(df: DataFrame): String = {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val resultSeq = df
      .select("text").collect()
      .map(row => row.getSeq[Row](0).map(toObj))
      .head
    speechArrayToText(resultSeq)
  }

  def dfTest(format: String, input: DataFrame, expectedText: String, verbose: Boolean = false): Assertion = {
    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val resultSeq = sdk.setFormat(format)
      .transform(input)
      .select("text").collect()
      .map(row => row.getSeq[Row](0).map(toObj))
      .head
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
    assert(jaccardSimilarity(expectedText, result) > jaccardThreshold)
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

  test("Detailed SDK Usage Audio 2") {
    dfTest("detailed", audioDf2, text2)
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
      .map(row => row.getSeq[Row](0).map(toObj))
      .head)
    assert(jaccardSimilarity(apiResult, sdkResult) > jaccardThreshold)
  }

  override def testObjects(): Seq[TestObject[SpeechToTextSDK]] =
    Seq(new TestObject(sdk, audioDf2))

  override def reader: MLReadable[_] = SpeechToTextSDK
}
