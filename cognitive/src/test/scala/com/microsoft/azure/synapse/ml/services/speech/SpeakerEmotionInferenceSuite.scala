// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.speech

import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import java.io.File

class SpeakerEmotionInferenceSuite extends TransformerFuzzing[SpeakerEmotionInference] with CognitiveKey {

  import spark.implicits._

  lazy val saveDir: File = tmpDir.toFile

  def ssmlGenerator: SpeakerEmotionInference = new SpeakerEmotionInference()
    .setLocation("eastus")
    .setSubscriptionKey(cognitiveKey)
    .setLocale("en-US")
    .setVoiceName("en-US-JaneNeural")
    .setTextCol("text")
    .setOutputCol("ssml")

  val testData: Map[String, String] = Map[String, String](
    ("\"A\" \"B\" \"C\"",
      "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xmlns:mstts='https://www.w3.org/2001/mstts' " +
        "xml:lang='en-US'><voice name='en-US-JaneNeural'>" +
        "<mstts:express-as role='male' style='calm'>\"A\"</mstts:express-as> " +
        "<mstts:express-as role='male' style='calm'>\"B\"</mstts:express-as> " +
        "<mstts:express-as role='male' style='calm'>\"C\"</mstts:express-as></voice></speak>\n"),
    ("\"I'm shouting excitedly!\" she shouted excitedly.",
      "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' " +
        "xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>" +
        "<mstts:express-as role='female' style='excited'>\"I'm shouting excitedly!\"</mstts:express-as> she shouted " +
        "excitedly.</voice></speak>\n"),
    ("This text has no quotes in it, so isValid should be false",
      "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' " +
        "xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>" +
        "This text has no quotes in it, so isValid should be false</voice></speak>\n"),
    ("\"This is an example of a sentence with unmatched quotes,\" she said.\"",
      "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' " +
        "xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>" +
        "<mstts:express-as role='female' style='calm'>\"This is an example of a sentence with unmatched quotes,\"" +
        "</mstts:express-as> she said.\"</voice></speak>\n"))

  lazy val df: DataFrame = testData.keys.toSeq.toDF("text")

  def normalizeSSML(ssml: String): String = {
    val ignoredAttributes: List[String] = List("name", "style", "role")
    ignoredAttributes.foldLeft(ssml)((acc, attr) =>
      acc.replaceAll(s"""\\s+$attr='[^']*'""", s"$attr="))
  }

  /*
    We're testing the structure of the returned call not the quality of the api, so ignore specifics like role and style
   */
  def assertFuzzyEquals(actualSSML: String, expectedSSML: String): Unit = {
    assert(normalizeSSML(expectedSSML).equals(normalizeSSML(actualSSML)))
  }

  test("basic") {
    val transformed = ssmlGenerator.transform(df)
    transformed.show(truncate = false)
    transformed.collect().foreach { row =>
      val actual = testData.getOrElse(row.getString(0), "")
      val expected = row.getString(2)
      assertFuzzyEquals(actual, expected)
    }
  }

  test("arbitrary df size") {
    val bigDF = Seq(("A", "B", "C", "Hello")).toDF("A", "B", "C", "text")
    ssmlGenerator.transform(bigDF).collect().map(
      row => {
        val actual = row.getString(5)
        val expected =
          """<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' """ +
            """xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>""" +
            s"""Hello</voice></speak>\n"""
        assertFuzzyEquals(actual, expected)
      })
  }

  val ssmlFormatTestData = Map[(String, SpeakerEmotionInferenceResponse), String](
    ((""""A", "B", "C"""", SpeakerEmotionInferenceResponse(true, Seq(
      SSMLConversation(0, 3, """"A"""", "male", "calm"),
      SSMLConversation(5, 8, """"B"""", "male", "calm"),
      SSMLConversation(10, 13, """"C"""", "male", "calm")))) ->
      ("""<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' """ +
        """xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>""" +
        """<mstts:express-as role='male' style='calm'>"A"</mstts:express-as>, """ +
        """<mstts:express-as role='male' style='calm'>"B"</mstts:express-as>, """ +
        """<mstts:express-as role='male' style='calm'>"C"</mstts:express-as></voice></speak>""" + "\n")),
    (("""Z"A"Z"B"Z"C"Z""", SpeakerEmotionInferenceResponse(true, Seq(
      SSMLConversation(1, 4, """"A"""", "male", "calm"),
      SSMLConversation(5, 8, """"B"""", "male", "calm"),
      SSMLConversation(9, 12, """"C"""", "male", "calm")))) ->
      ("""<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' """ +
        """xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'><voice name='en-US-JaneNeural'>Z""" +
        """<mstts:express-as role='male' style='calm'>"A"</mstts:express-as>Z<mstts:express-as role='male' """ +
        """style='calm'>"B"</mstts:express-as>Z<mstts:express-as role='male' style='calm'>"C"""" +
        """</mstts:express-as>Z</voice></speak>""" + "\n")),
    ((""""A""B""C"""", SpeakerEmotionInferenceResponse(true, Seq(
      SSMLConversation(0, 3, """"A"""", "male", "calm"),
      SSMLConversation(3, 6, """"B"""", "male", "calm"),
      SSMLConversation(6, 9, """"C"""", "male", "calm")))) ->
      ("""<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' """ +
        """xmlns:mstts='https://www.w3.org/2001/mstts' xml:lang='en-US'>""" +
        """<voice name='en-US-JaneNeural'><mstts:express-as role='male' style='calm'>"A"""" +
        """</mstts:express-as><mstts:express-as role='male' style='calm'>"B"</mstts:express-as>""" +
        """<mstts:express-as role='male' style='calm'>"C"</mstts:express-as></voice></speak>""" + "\n")))

  test("formatSSML") {
    ssmlFormatTestData.map(test => {
      val result = ssmlGenerator.formatSSML(
        test._1._1,
        "en-US",
        "en-US-JaneNeural",
        test._1._2)
      assertResult(test._2)(result)
    })
  }

  def tts: TextToSpeech = new TextToSpeech()
    .setUseSSML(true)
    .setLocation("eastus")
    .setSubscriptionKey(cognitiveKey)
    .setTextCol("ssml")
    .setOutputFileCol("filename")

  lazy val synthesisDF: DataFrame = Seq(
    ("Hello world this is some sample text",
      new File(saveDir, "test1.mp3").getAbsolutePath),
    (""""Hi, how are you?" she said.""",
      new File(saveDir, "test2.mp3").getAbsolutePath),
    ("""She was terrified and said. "This is how I sound right now."""",
      new File(saveDir, "test3.mp3").getAbsolutePath),
    (""""I'm really excited!" she said, excitedly.""",
      new File(saveDir, "test4.mp3").getAbsolutePath),
    ("""She screamed "Hi," and followed up with "how are you?" before reaching for the baseball.""",
      new File(saveDir, "test5.mp3").getAbsolutePath)).toDF("text", "filename")

  test("integration with TTS") {
    val ssmlDF = ssmlGenerator.transform(synthesisDF)
    ssmlDF.collect()
    val resultDF = tts.transform(ssmlDF)
    resultDF.collect()
      .map(row => row.getAs[String]("filename"))
      .foreach(filename => assert(new File(filename).length() > 20))
  }

  override def testObjects(): Seq[TestObject[SpeakerEmotionInference]] =
    Seq(new TestObject(ssmlGenerator, df))

  override def reader: MLReadable[_] = SpeakerEmotionInference
}
