// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode}


class PIIExtractionSuiteV4 extends TransformerFuzzing[PIIV4] with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("en", "en", "en"), Seq("This person is named John Doe", "He lives on 123 main street",
      "His phone number was 12345677")),
    (Seq("en"), Seq("I live in Vancouver."))
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val unbatcheddf: DataFrame = Seq(
    ("en", "This person is named John Doe"),
    ("en", "He lives on 123 main street."),
    ("en", "His phone number was 12345677")
  ).toDF("lang", "text")

  df.printSchema()
  df.show(10, false)

  def extractor: PIIV4 = new PIIV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("PII - Basic Usage") {
    val replies = extractor.transform(df)
      .select("output.result.redactedText")
      .collect()
    assert(replies(0).schema(0).name == "redactedText")
    df.printSchema()
    df.show(10, false)
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }

  test("PII - Invalid Document Input") {
    val replies = extractor.transform(invalidDocDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(1).get(0).toString == "Document text is empty.")
    assert(codes(1).get(0).toString == "InvalidDocument")
  }

  test("PII - batch usage") {
    extractor.setLanguageCol("lang")
      .setBatchSize(2)
    val results = extractor.transform(unbatcheddf.coalesce(1)).cache()
    results.show()
    val tdf = results
      .select("lang", "output.result.redactedText")
      .collect()
    assert(tdf.length == 3)
  }

  override def testObjects(): Seq[TestObject[PIIV4]] = Seq(new TestObject(
    extractor, df
  ))

  override def reader: MLReadable[_] = PIIV4
}

class HealthcareSuiteV4 extends TransformerFuzzing[HealthcareV4] with TextKey {

  import spark.implicits._

  lazy val df3: DataFrame = Seq(
    ("en", "20mg of ibuprofen twice a day")
  ).toDF("lang", "text")

  lazy val df4: DataFrame = Seq(
    ("en", "1tsp of Tylenol every 4 hours")
  ).toDF("lang", "text")

  lazy val df5: DataFrame = Seq(
    ("en", "6-drops of Vitamin B-12 every evening")
  ).toDF("lang", "text")

  lazy val extractor: HealthcareV4 = new HealthcareV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setUrl("https://eastus.api.cognitive.microsoft.com/")
    .setOutputCol("output")

  lazy val invalidDocumentType: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val invalidLanguageInput: DataFrame = Seq(
    (Seq("abc", "/."), Seq("I feel sick.", "I have severe neck and shoulder pain."))
  ).toDF("lang", "text")

  lazy val unbatchedDF: DataFrame = Seq(
    ("en", "Woman in NAD with a h/o CAD, DM2, asthma and HTN on ramipril for 8 years awoke from sleep around"),
    ("es", "Estoy enfermo"),
    ("en", "Patient's brother died at the age of 64 from lung cancer. She was admitted for likely gastroparesis")
  ).toDF("lang", "text")

  test("Healthcare - Output Assertion") {
    val replies = extractor.transform(df3.coalesce(1))
      .select("output")
      .collect()

    val firstRow = replies(0)
    assert(replies(0).schema(0).name == "output")
    val fromRow = HealthcareResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val healthcareEntities = resFirstRow.result.head.get.entities
    assert(healthcareEntities.nonEmpty)

    println("Entities:")
    println("=========")
    healthcareEntities.foreach(entity => println(s"entity: ${entity.text} | category: ${entity.category}"))

    println("\nRelations:")
    println("=========")
    val relations = resFirstRow.result.head.get.entityRelation
    relations.foreach(relation =>
      println(s"${
        relation.roles.map(role =>
          s"${role.entity.text}(${role.name})").mkString(s"<--${relation.relationType}-->")
      }"))
  }

  test("Healthcare - Invalid Document Input Type") {
    val replies = extractor.transform(invalidDocumentType.coalesce(1))
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()

    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")
  }

  test("Healthcare - Batch Usage") {
    extractor.setLanguageCol("lang")
    val results = extractor.transform(unbatchedDF.coalesce(1)).cache()
    results.show()
    val tdf = results
      .select("lang", "output.result.entities")
      .collect()
    assert(tdf.length == 3)
  }

  test("Healthcare - Print Complete Entities") {
    val replies = extractor.transform(df5.coalesce(1))
      .select("output")
      .collect()

    val firstRow = replies(0)
    assert(replies(0).schema(0).name == "output")
    val fromRow = HealthcareResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val healthcareEntities = resFirstRow.result.head.get.entities
    assert(healthcareEntities.nonEmpty)

    println("Entities:")
    println("=========")
    healthcareEntities.foreach(entity => println(s"entity: ${entity.text} | category: ${entity.category} " +
      s"| subCategory: ${entity.subCategory} | length: ${entity.length} | dataSources: ${entity.dataSources}" +
      s"normalizedText: ${entity.normalizedText} | confidenceScore: ${entity.confidenceScore}"))
  }

  override def testObjects(): Seq[TestObject[HealthcareV4]] = Seq(new TestObject(
    extractor, df3
  ))

  override def reader: MLReadable[_] = HealthcareV4
}

class EntityLinkingSuiteV4 extends TransformerFuzzing[EntityLinkingV4] with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("en", "en"), Seq("Our tour guide took us up the Space Needle during our trip to Seattle last week.",
      "Pike place market is my favorite Seattle attraction.")),
    (Seq("en"), Seq("It's incredibly sunny outside! I'm so happy"))
  ).toDF("lang", "text")

  lazy val unbatchedDF: DataFrame = Seq(
    ("en", "Our tour guide took us up the Space Needle during our trip to Seattle last week."),
    ("en", "Pike place market is my favorite Seattle attraction"),
    ("en", "It's incredibly sunny outside! I'm so happy")
  ).toDF("lang", "text")

  lazy val invalidDocumentType: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  def extractor: EntityLinkingV4 = new EntityLinkingV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("Entity Linking -  Output Assertion") {
    val replies = extractor.transform(df)
      .select("output")
      .collect()

    val firstRow = replies(0)
    assert(replies(0).schema(0).name == "output")

    val fromRow = LinkedEntityResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val linkedEntities = resFirstRow.result.head.get.entities
    assert(linkedEntities.nonEmpty)
    println("Entities:")
    println("=========")
    linkedEntities.foreach(entity => println(s"entity: ${entity.name} | url: ${entity.url}  " +
      s"| data source: ${entity.dataSource}"))

    println("\nMatches:")
    println("=========")
    linkedEntities.foreach(entity =>
      println(s"${
        entity.matches.map(matches =>
          s"${matches.text} (confidence: ${matches.confidenceScore}) " +
            s"(length: ${matches.length})").mkString(s"<--${linkedEntities.toString()}-->")
      }"))

    assert(linkedEntities.toList.head.name == "Space Needle")
  }

  test("Entity Linking - Batch Usage") {
    extractor.setLanguageCol("lang")
    val results = extractor.transform(unbatchedDF.coalesce(1)).cache()
    results.show()
    val tdf = results
      .select("lang", "output.result.entities")
      .collect()
    assert(tdf.length == 3)
  }

  test("Entity Linking - Invalid Document Input Type") {
    val replies = extractor.transform(invalidDocumentType.coalesce(1))
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()

    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")
  }

  override def testObjects(): Seq[TestObject[EntityLinkingV4]] = Seq(new TestObject(
    extractor, df
  ))

  override def reader: MLReadable[_] = EntityLinkingV4
}

class NamedEntityRecognitionSuiteV4 extends TransformerFuzzing[NERV4] with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("en", "en"), Seq("Our tour guide took us up the Space Needle during our trip to Seattle last week.",
      "Pike place market is my favorite Seattle attraction.")),
    (Seq("en"), Seq("It's incredibly sunny outside! I'm so happy"))
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val unbatchedDf: DataFrame = Seq(
    ("en", "Jeff bought three dozen eggs because there was a 50% discount."),
    ("en", "The Great Depression began in 1929. By 1933, the GDP in America fell by 25%.")
  ).toDF("lang", "text")

  lazy val batchedDf: DataFrame = Seq(
    (Seq("en"), Seq("Jeff bought three dozen eggs because there was a 50% discount."))
  ).toDF("lang", "text")

  def extractor: NERV4 = new NERV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("NER - Output Assertion") {
    val replies = extractor.transform(batchedDf)
      .select("output")
      .collect()

    val firstRow = replies(0)
    assert(replies(0).schema(0).name == "output")
    val fromRow = NERResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val firstRowOutput = resFirstRow.result.head.get.entities
    assert(firstRowOutput.nonEmpty)

    println("Output: ")
    println("=========")
    firstRowOutput.foreach(entity => println(s"entity: ${entity.text} | category: ${entity.category} |" +
      s" confidence score: ${entity.confidenceScore}"))
  }

  test("NER - Basic Batch Usage") {
    val results = extractor.transform(unbatchedDf.coalesce(1)).cache()
    val matches = results.withColumn("match",
      col("output.result")
        .getItem(0)
        .getItem("entities")
        .getItem(0))
      .select("match")

    val firstRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]
    assert(firstRow.getAs[String]("text") === "Jeff")
    assert(firstRow.getAs[Int]("offset") === 0)
    assert(firstRow.getAs[Double]("confidenceScore") > 0.7)
    assert(firstRow.getAs[String]("category") === "Person")

    val secondRow = matches.collect()(1)(0).asInstanceOf[GenericRowWithSchema]
    assert(secondRow.getAs[String]("text") === "Great Depression")
    assert(secondRow.getAs[Int]("offset") === 4)
    assert(secondRow.getAs[Double]("confidenceScore") > 0.7)
    assert(secondRow.getAs[String]("category") === "Event")
  }

  test("NER - Invalid Document Input") {
    val replies = extractor.transform(invalidDocDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(1).get(0).toString == "Document text is empty.")
    assert(codes(1).get(0).toString == "InvalidDocument")
  }

  override def testObjects(): Seq[TestObject[NERV4]] = Seq(new TestObject(
    extractor, df
  ))

  override def reader: MLReadable[_] = NERV4
}
