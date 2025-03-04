// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{ TestObject, TransformerFuzzing }
import com.microsoft.azure.synapse.ml.services.text.{ SentimentAssessment, TextEndpoint }
import com.microsoft.azure.synapse.ml.Secrets
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.functions.{ col, flatten, map }
import org.scalactic.{ Equality, TolerantNumerics }
import org.scalatest.funsuite.AnyFunSuiteLike

trait LanguageServiceEndpoint {
  lazy val languageApiKey: String = sys.env.getOrElse("CUSTOM_LANGUAGE_KEY", Secrets.LanguageApiKey)
  lazy val languageApiLocation: String = sys.env.getOrElse("LANGUAGE_API_LOCATION", "eastus")
}

class AnalyzeTextLROSuite extends AnyFunSuiteLike {
  test("Validate that response map and creator handle same kinds") {
    val transformer = new AnalyzeTextLongRunningOperations()
    val responseKinds = transformer.responseDataTypeSchemaMap.keySet
    val creatorKinds = transformer.requestCreatorMap.keySet
    assert(responseKinds == creatorKinds)
  }
}

class ExtractiveSummarizationSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)


  private val df = Seq(
    Seq(
      """At Microsoft, we have been on a quest to advance AI beyond existing techniques, by taking a more holistic,
        |human-centric approach to learning and understanding. As Chief Technology Officer of Azure AI services,
        |I have been working with a team of amazing scientists and engineers to turn this quest into a reality.
        |In my role, I enjoy a unique perspective in viewing the relationship among three attributes of human
        |cognition: monolingual text (X), audio or visual sensory signals, (Y) and multilingual (Z). At the
        |intersection of all three, there’s magic—what we call XYZ-code as illustrated in Figure 1—a joint
        |representation to create more powerful AI that can speak, hear, see, and understand humans better. We
        |believe XYZ-code enables us to fulfill our long-term vision: cross-domain transfer learning, spanning
        |modalities and languages. The goal is to have pretrained models that can jointly learn representations to
        |support a broad range of downstream AI tasks, much in the way humans do today. Over the past five years, we
        |have achieved human performance on benchmarks in conversational speech recognition, machine translation,
        |conversational question answering, machine reading comprehension, and image captioning. These five
        |breakthroughs provided us with strong signals toward our more ambitious aspiration to produce a leap in AI
        |capabilities, achieving multi-sensory and multilingual learning that is closer in line with how humans learn
        | and understand. I believe the joint XYZ-code is a foundational component of this aspiration, if grounded
        | with external knowledge sources in the downstream AI tasks""".stripMargin,
      "",
      """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam ultricies interdum eros, vehicula dignissim
        |odio dignissim id. Nam sagittis lacinia enim at fringilla. Nunc imperdiet porta ex. Vestibulum quis nisl
        |feugiat, dapibus nulla nec, dictum lorem. Vivamus ut urna a ante cursus egestas. In vulputate facilisis
        |nunc, vitae aliquam neque faucibus a. Fusce et venenatis nisi. Duis eleifend condimentum neque. Mauris eu
        |pulvinar sapien. Nam at nibh sem. Integer sapien ex, viverra vel interdum non, volutpat sed tellus. Aenean
        | nec maximus nibh. Maecenas sagittis turpis vel nibh condimentum vulputate. Pellentesque viverra
        | ullamcorper urna vitae rutrum. Nunc fermentum sem vitae commodo efficitur.""".stripMargin
      )
    ).toDF("text")


  test("Basic usage") {
    val model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setKind(AnalysisTaskKind.ExtractiveSummarization)
      .setOutputCol("response")
      .setErrorCol("error")
    val responses = model.transform(df)
                         .withColumn("documents", col("response.documents"))
                         .withColumn("modelVersion", col("response.modelVersion"))
                         .withColumn("errors", col("response.errors"))
                         .withColumn("statistics", col("response.statistics"))
                         .collect()
    assert(responses.length == 1)
    val response = responses.head
    val documents = response.getAs[Seq[Row]]("documents")
    val errors = response.getAs[Seq[Row]]("errors")
    assert(documents.length == errors.length)
    assert(documents.length == 3)
    val sentences = documents.head.getAs[Seq[Row]]("sentences")
    assert(sentences.nonEmpty)
    sentences.foreach { sentence =>
      assert(sentence.getAs[String]("text").nonEmpty)
      assert(sentence.getAs[Double]("rankScore") > 0.0)
      assert(sentence.getAs[Int]("offset") >= 0)
      assert(sentence.getAs[Int]("length") > 0)
    }
  }


  test("show-stats and sentence-count") {
    val sentenceCount = 10
    val model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setKind(AnalysisTaskKind.ExtractiveSummarization)
      .setOutputCol("response")
      .setErrorCol("error")
      .setShowStats(true)
      .setSentenceCount(sentenceCount)
    val responses = model.transform(df)
                         .withColumn("documents", col("response.documents"))
                         .withColumn("modelVersion", col("response.modelVersion"))
                         .withColumn("errors", col("response.errors"))
                         .withColumn("statistics", col("response.statistics"))
                         .collect()
    assert(responses.length == 1)
    val response = responses.head
    val stats = response.getAs[Seq[Row]]("statistics")
    assert(stats.length == 3)
    stats.foreach { stat =>
      assert(stat.getAs[Int]("documentsCount") == 3)
      assert(stat.getAs[Int]("validDocumentsCount") == 2)
      assert(stat.getAs[Int]("erroneousDocumentsCount") == 1)
      assert(stat.getAs[Int]("transactionsCount") == 3)
    }

    val documents = response.getAs[Seq[Row]]("documents")
    for (document <- documents) {
      if (document != null) {
        val sentences = document.getAs[Seq[Row]]("sentences")
        assert(sentences.length == sentenceCount)
        sentences.foreach { sentence =>
          assert(sentence.getAs[String]("text").nonEmpty)
          assert(sentence.getAs[Double]("rankScore") > 0.0)
          assert(sentence.getAs[Int]("offset") >= 0)
          assert(sentence.getAs[Int]("length") > 0)
        }
      }
    }
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](new AnalyzeTextLongRunningOperations()
                                             .setSubscriptionKey(textKey)
                                             .setLocation(textApiLocation)
                                             .setTextCol("text")
                                             .setLanguage("en")
                                             .setKind("ExtractiveSummarization")
                                             .setOutputCol("response"),
                                                         df))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}


class AbstractiveSummarizationSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)


  private val df = Seq(
    Seq(
      """At Microsoft, we have been on a quest to advance AI beyond existing techniques, by taking a more holistic,
        |human-centric approach to learning and understanding. As Chief Technology Officer of Azure AI services,
        |I have been working with a team of amazing scientists and engineers to turn this quest into a reality.
        |In my role, I enjoy a unique perspective in viewing the relationship among three attributes of human
        |cognition: monolingual text (X), audio or visual sensory signals, (Y) and multilingual (Z). At the
        |intersection of all three, there’s magic—what we call XYZ-code as illustrated in Figure 1—a joint
        |representation to create more powerful AI that can speak, hear, see, and understand humans better. We
        |believe XYZ-code enables us to fulfill our long-term vision: cross-domain transfer learning, spanning
        |modalities and languages. The goal is to have pretrained models that can jointly learn representations to
        |support a broad range of downstream AI tasks, much in the way humans do today. Over the past five years, we
        |have achieved human performance on benchmarks in conversational speech recognition, machine translation,
        |conversational question answering, machine reading comprehension, and image captioning. These five
        |breakthroughs provided us with strong signals toward our more ambitious aspiration to produce a leap in AI
        |capabilities, achieving multi-sensory and multilingual learning that is closer in line with how humans learn
        | and understand. I believe the joint XYZ-code is a foundational component of this aspiration, if grounded
        | with external knowledge sources in the downstream AI tasks""".stripMargin
      )
    ).toDF("text")


  test("Basic usage") {
    val model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setKind("AbstractiveSummarization")
      .setOutputCol("response")
      .setErrorCol("error")
      .setPollingDelay(5 * 1000)
      .setMaxPollingRetries(30)
    val responses = model.transform(df)
                         .withColumn("documents", col("response.documents"))
                         .withColumn("modelVersion", col("response.modelVersion"))
                         .withColumn("errors", col("response.errors"))
                         .withColumn("statistics", col("response.statistics"))
                         .collect()
    assert(responses.length == 1)
    val response = responses.head
    val documents = response.getAs[Seq[Row]]("documents")
    val errors = response.getAs[Seq[Row]]("errors")
    assert(documents.length == errors.length)
    assert(documents.length == 1)
    val summaries = documents.head.getAs[Seq[Row]]("summaries")
    assert(summaries.nonEmpty)
  }


  test("show-stats and summary-length") {
    val model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setKind(AnalysisTaskKind.AbstractiveSummarization)
      .setOutputCol("response")
      .setErrorCol("error")
      .setShowStats(true)
      .setSummaryLength(SummaryLength.Short)
      .setPollingDelay(5 * 1000)
      .setMaxPollingRetries(30)
    val responses = model.transform(df)
                         .withColumn("documents", col("response.documents"))
                         .withColumn("modelVersion", col("response.modelVersion"))
                         .withColumn("errors", col("response.errors"))
                         .withColumn("statistics", col("response.statistics"))
                         .collect()
    assert(responses.length == 1)
    val response = responses.head
    val stat = response.getAs[Seq[Row]]("statistics").head
    assert(stat.getAs[Int]("documentsCount") == 1)
    assert(stat.getAs[Int]("validDocumentsCount") == 1)
    assert(stat.getAs[Int]("erroneousDocumentsCount") == 0)


    val document = response.getAs[Seq[Row]]("documents").head
    val summaries = document.getAs[Seq[Row]]("summaries")
    assert(summaries.length == 1)
    val summary = summaries.head.getAs[String]("text")
    assert(summary.length <= 750)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](new AnalyzeTextLongRunningOperations()
                                             .setSubscriptionKey(textKey)
                                             .setLocation(textApiLocation)
                                             .setTextCol("text")
                                             .setLanguage("en")
                                             .setKind("AbstractiveSummarization")
                                             .setPollingDelay(5 * 1000)
                                             .setMaxPollingRetries(30)
                                             .setSummaryLength(SummaryLength.Short)
                                             .setOutputCol("response"),
                                                         Seq("Microsoft Azure AI Data Fabric").toDF("text")))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}

class HealthcareSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {


  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  private val df = Seq(
    "The doctor prescried 200mg Ibuprofen."
    ).toDF("text")

  test("Basic usage") {
    val model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setKind("Healthcare")
      .setOutputCol("response")
      .setShowStats(true)
      .setErrorCol("error")
    val responses = model.transform(df)
                         .withColumn("documents", col("response.documents"))
                         .withColumn("modelVersion", col("response.modelVersion"))
                         .withColumn("errors", col("response.errors"))
                         .withColumn("statistics", col("response.statistics"))
                         .collect()
    assert(responses.length == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](new AnalyzeTextLongRunningOperations()
                                             .setSubscriptionKey(textKey)
                                             .setLocation(textApiLocation)
                                             .setTextCol("text")
                                             .setLanguage("en")
                                             .setKind("Healthcare")
                                             .setOutputCol("response"),
                                                         df))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}

class SentimentAnalysisLROSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    "Great atmosphere. Close to plenty of restaurants, hotels, and transit! Staff are friendly and helpful.",
    "What a sad story!"
    ).toDF("text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setTextCol("text")
    .setKind(AnalysisTaskKind.SentimentAnalysis)
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("sentiment", col("documents.sentiment"))
                      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("sentiment", col("documents.sentiment"))
                      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("sentiment", col("documents.sentiment"))
                      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
                      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  test("Opinion Mining") {
    val result = model.setOpinionMining(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("sentiment", col("documents.sentiment"))
                      .withColumn("assessments", flatten(col("documents.sentences.assessments")))
                      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
    val fromRow = SentimentAssessment.makeFromRowConverter
    assert(result.head.getAs[Seq[Row]]("assessments").map(fromRow).head.sentiment == "positive")
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}


class KeyPhraseLROSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Text Analytics is one of the Azure Cognitive Services."),
    ("en", "My cat might need to see a veterinarian.")
    ).toDF("language", "text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("KeyPhraseExtraction")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("keyPhrases", col("documents.keyPhrases"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("keyPhrases", col("documents.keyPhrases"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("keyPhrases", col("documents.keyPhrases"))
                      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}


class AnalyzeTextPIILORSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    "My SSN is 859-98-0987",
    "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of your personal check.",
    "Is 998.214.865-68 your Brazilian CPF number?"
    ).toDF("text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setTextCol("text")
    .setKind("PiiEntityRecognition")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("redactedText", col("documents.redactedText"))
                      .withColumn("entities", col("documents.entities.text"))
                      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("redactedText", col("documents.redactedText"))
                      .withColumn("entities", col("documents.entities.text"))
                      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("redactedText", col("documents.redactedText"))
                      .withColumn("entities", col("documents.entities.text"))
                      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
                      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}


class EntityLinkingLROSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Pike place market is my favorite Seattle attraction.")
    ).toDF("language", "text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("EntityLinking")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
                      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeTextLongRunningOperations
}


class EntityRecognitionLROSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Pike place market is my favorite Seattle attraction.")
    ).toDF("language", "text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("EntityRecognition")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
                      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class CustomEntityRecognitionSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations]
                                           with LanguageServiceEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame =
    Seq("Maria Sullivan with a mailing address of 334 Shinn Avenue, City of Wampum, State of Pennsylvania")
      .toDF("text")

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(languageApiKey)
    .setLocation(languageApiLocation)
    .setLanguage("en")
    .setTextCol("text")
    .setKind(AnalysisTaskKind.CustomEntityRecognition)
    .setOutputCol("response")
    .setErrorCol("error")
    .setDeploymentName("custom-ner-unitest-deployment")
    .setProjectName("for-unit-test")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("entities", col("documents.entities"))
                      .collect()
    val entities = result.head.getAs[Seq[Row]]("entities")
    assert(entities.length == 4)
    val resultMap: Map[String, String] = entities.map { entity =>
      entity.getAs[String]("text") -> entity.getAs[String]("category")
    }.toMap
    assert(resultMap("Maria Sullivan") == "BorrowerName")
    assert(resultMap("334 Shinn Avenue") == "BorrowerAddress")
    assert(resultMap("Wampum") == "BorrowerCity")
    assert(resultMap("Pennsylvania") == "BorrowerState")
  }


  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}


class MultiLableClassificationSuite extends TransformerFuzzing[AnalyzeTextLongRunningOperations]
                                           with LanguageServiceEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = {
    // description of movie Finding Nemo
    Seq("In the depths of the ocean, a father's worst nightmare comes to life. A grieving and determined father, " +
          "must overcome his fears and navigate, the treacherous waters to find his missing son. The journey is " +
          "fraught with relentless predators, dark secrets, and the haunting realization that the ocean is a vast, " +
          "unforgiving abyss. Will a Father's unwavering resolve be enough to reunite him with his son, or will " +
          "the shadows of the deep consume them both? Dive into the darkness and discover the lengths a parent will " +
          "go to for their child.")
      .toDF("text")
  }

  def model: AnalyzeTextLongRunningOperations = new AnalyzeTextLongRunningOperations()
    .setSubscriptionKey(languageApiKey)
    .setLocation(languageApiLocation)
    .setLanguage("en")
    .setTextCol("text")
    .setKind(AnalysisTaskKind.CustomMultiLabelClassification)
    .setOutputCol("response")
    .setErrorCol("error")
    .setDeploymentName("multi-class-movie-dep")
    .setProjectName("for-unit-test-muti-class")

  test("Basic usage") {
    val result = model.transform(df)
                      .withColumn("documents", col("response.documents"))
                      .withColumn("classifications", col("documents.classifications"))
                      .collect()
    val classifications = result.head.getAs[Seq[Row]]("classifications")
    assert(classifications.nonEmpty)
    assert(classifications.head.getAs[String]("category").nonEmpty)
    assert(classifications.head.getAs[Double]("confidenceScore") > 0.0)
  }


  override def testObjects(): Seq[TestObject[AnalyzeTextLongRunningOperations]] =
    Seq(new TestObject[AnalyzeTextLongRunningOperations](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}



