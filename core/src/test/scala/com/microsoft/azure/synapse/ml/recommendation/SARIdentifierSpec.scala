// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, StringType, StructType}

import java.nio.file.Files

class SARIdentifierSpec extends TestBase {

  private val userCol = "user"
  private val itemCol = "item"
  private val ratingCol = "rating"

  private def stringRatings: DataFrame = {
    import spark.implicits._
    Seq(
      ("user-c", "item-30", 4.0),
      ("user-a", "item-10", 5.0),
      ("user-a", "item-20", 2.0),
      ("user-b", "item-10", 3.0),
      ("user-b", "item-30", 1.0),
      ("user-c", "item-20", 4.0)
    ).toDF(userCol, itemCol, ratingCol)
  }

  private def newSar: SAR = new SAR()
    .setUserCol(userCol)
    .setItemCol(itemCol)
    .setRatingCol(ratingCol)
    .setSupportThreshold(1)
    .setSimilarityFunction("jaccard")

  test("SAR preserves string identifiers in transforms and recommendations") {
    import spark.implicits._
    val model = newSar.fit(stringRatings)

    val scored = model.transform(stringRatings)
    assert(scored.count() == stringRatings.count())
    assert(scored.schema(userCol).dataType == StringType)
    assert(scored.schema(itemCol).dataType == StringType)
    assert(scored.schema(model.getPredictionCol).dataType == FloatType)
    assert(scored.filter(col(model.getPredictionCol).isNull).count() == 0)

    val userRecommendations = model.recommendForAllUsers(2)
    assert(userRecommendations.schema(userCol).dataType == StringType)
    val userRecType = userRecommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(userRecType(itemCol).dataType == StringType)
    assert(userRecommendations.select(userCol).as[String].collect().toSet ==
      Set("user-a", "user-b", "user-c"))

    val itemRecommendations = model.recommendForAllItems(2)
    assert(itemRecommendations.schema(itemCol).dataType == StringType)
    val itemRecType = itemRecommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(itemRecType(userCol).dataType == StringType)

    val userSubset = Seq(Some("user-a"), Some("user-a"), Some("missing-user"), None).toDF(userCol)
    assert(model.recommendForUserSubset(userSubset, 2).select(userCol).as[String].collect().toSeq ==
      Seq("user-a"))

    val itemSubset = Seq(Some("item-20"), Some("item-20"), Some("missing-item"), None).toDF(itemCol)
    assert(model.recommendForItemSubset(itemSubset, 2).select(itemCol).as[String].collect().toSeq ==
      Seq("item-20"))
  }

  test("SAR mappings are deterministic and model owned") {
    val forward = newSar.fit(stringRatings)
    val reversed = newSar.fit(stringRatings.orderBy(desc(userCol), desc(itemCol)))

    val forwardUsers = forward.getUserIdMapping.orderBy("index").collect().map(_.get(0)).toSeq
    val reversedUsers = reversed.getUserIdMapping.orderBy("index").collect().map(_.get(0)).toSeq
    val forwardItems = forward.getItemIdMapping.orderBy("index").collect().map(_.get(0)).toSeq
    val reversedItems = reversed.getItemIdMapping.orderBy("index").collect().map(_.get(0)).toSeq

    assert(forwardUsers == Seq("user-a", "user-b", "user-c"))
    assert(forwardUsers == reversedUsers)
    assert(forwardItems == Seq("item-10", "item-20", "item-30"))
    assert(forwardItems == reversedItems)
  }

  test("SAR preserves wide numeric identifiers without casting them") {
    import spark.implicits._
    val numericRatings = Seq(
      (3000000000L, 9000000000L, 4.0),
      (3000000000L, 9000000001L, 2.0),
      (4000000000L, 9000000000L, 5.0),
      (4000000000L, 9000000002L, 3.0)
    ).toDF(userCol, itemCol, ratingCol)

    val model = newSar.fit(numericRatings)
    assert(!model.getUserIdsFitInt)
    assert(!model.getItemIdsFitInt)
    val recommendations = model.recommendForAllUsers(3)
    assert(recommendations.schema(userCol).dataType == LongType)
    val recommendationType = recommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(recommendationType(itemCol).dataType == LongType)
    assert(recommendations.select(userCol).as[Long].collect().toSet == Set(3000000000L, 4000000000L))
    assert(recommendations.select("recommendations.item").collect()
      .flatMap(_.getSeq[Long](0)).forall(_ >= 9000000000L))
    assert(model.transform(numericRatings).count() == numericRatings.count())
  }

  test("SAR accepts only round-trip-safe numeric scoring casts") {
    import spark.implicits._
    val numericRatings = Seq(
      (1L, 10L, 1.0),
      (1L, 20L, 2.0),
      (2L, 10L, 3.0),
      (2L, 20L, 4.0)
    ).toDF(userCol, itemCol, ratingCol)
    val model = newSar.fit(numericRatings)

    val integerScoring = Seq((1, 10), (2, 20)).toDF(userCol, itemCol)
    val integerSchema = model.transformSchema(integerScoring.schema)
    assert(integerSchema(userCol).dataType == IntegerType)
    assert(integerSchema(itemCol).dataType == IntegerType)
    assert(model.transform(integerScoring).count() == 2)

    val mixedDoubleScoring = Seq(
      (1.0, 10.0),
      (1.5, 10.0),
      (2.0, 20.25)
    ).toDF(userCol, itemCol)
    val safelyScored = model.transform(mixedDoubleScoring).select(userCol, itemCol).collect()
    assert(safelyScored.length == 1)
    assert(safelyScored.head.getDouble(0) == 1.0)
    assert(safelyScored.head.getDouble(1) == 10.0)

    val integerModel = newSar.fit(Seq(
      (1, 10, 1.0),
      (1, 20, 2.0),
      (2, 10, 3.0),
      (2, 20, 4.0)
    ).toDF(userCol, itemCol, ratingCol))
    val rangeChecked = Seq(
      (1L, 10L),
      (Int.MaxValue.toLong + 1L, 10L)
    ).toDF(userCol, itemCol)
    assert(integerModel.transform(rangeChecked).count() == 1)
  }

  test("SAR numeric compatibility is ANSI-safe for wide Long identifiers") {
    import spark.implicits._
    val previousAnsi = spark.conf.getOption("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try {
      val integerRatings = Seq(
        (1, 10, 1.0),
        (1, 20, 2.0),
        (2, 10, 3.0),
        (2, 20, 4.0)
      ).toDF(userCol, itemCol, ratingCol)
      val integerModel = newSar.fit(integerRatings)
      val rangeChecked = Seq(
        (1L, 10L),
        (Int.MaxValue.toLong + 1L, 10L)
      ).toDF(userCol, itemCol)
      assert(integerModel.transform(rangeChecked).count() == 1)

      val wideRatings = Seq(
        (3000000000L, 9000000000L, 1.0),
        (3000000000L, 9000000001L, 2.0),
        (4000000000L, 9000000000L, 3.0)
      ).toDF(userCol, itemCol, ratingCol)
      val wideModel = newSar.fit(wideRatings)
      val recommendations = wideModel.recommendForAllUsers(2)
      assert(recommendations.schema(userCol).dataType == LongType)
      assert(recommendations.collect().nonEmpty)
    } finally {
      previousAnsi match {
        case Some(value) => spark.conf.set("spark.sql.ansi.enabled", value)
        case None => spark.conf.unset("spark.sql.ansi.enabled")
      }
    }
  }

  test("SAR mapped recommendation planning avoids repeated mapping and source scans") {
    val model = newSar.fit(stringRatings)
    val context = spark.sparkContext
    val userMappingReads = context.longAccumulator("sar-user-mapping-reads")
    val itemMappingReads = context.longAccumulator("sar-item-mapping-reads")
    val countedUserMapping = model.getUserIdMapping
    val countedItemMapping = model.getItemIdMapping
    model
      .setUserIdMapping(spark.createDataFrame(
        countedUserMapping.rdd.map(row => {
          userMappingReads.add(1)
          row
        }),
        countedUserMapping.schema
      ))
      .setItemIdMapping(spark.createDataFrame(
        countedItemMapping.rdd.map(row => {
          itemMappingReads.add(1)
          row
        }),
        countedItemMapping.schema
      ))

    val jobGroup = s"sar-recommendation-planning-${System.nanoTime()}"
    context.setJobGroup(jobGroup, "detect eager recommendation actions")
    try {
      model.recommendForAllUsers(2)
      model.recommendForAllItems(2)
      val jobCount = context.statusTracker.getJobIdsForGroup(jobGroup).length
      assert(userMappingReads.value == 0L)
      assert(itemMappingReads.value == 0L)
      assert(jobCount <= 45, s"Recommendation planning launched $jobCount jobs")
    } finally {
      context.clearJobGroup()
    }
  }

  test("SAR keeps established integer recommendation schemas for round-trip numeric IDs") {
    import spark.implicits._
    val numericRatings = Seq(
      (0.0, 0.0, 1.0),
      (0.0, 1.0, 2.0),
      (1.0, 0.0, 3.0),
      (1.0, 1.0, 4.0)
    ).toDF(userCol, itemCol, ratingCol)
    val model = newSar.fit(numericRatings)
    assert(model.getUserIdsFitInt)
    assert(model.getItemIdsFitInt)

    val userRecommendations = model.recommendForAllUsers(2)
    assert(userRecommendations.schema(userCol).dataType == IntegerType)
    val userRecommendationType = userRecommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(userRecommendationType(itemCol).dataType == IntegerType)

    val itemRecommendations = model.recommendForAllItems(2)
    assert(itemRecommendations.schema(itemCol).dataType == IntegerType)
    val itemRecommendationType = itemRecommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(itemRecommendationType(userCol).dataType == IntegerType)
  }

  test("SAR supports legacy numeric models without persisted mappings") {
    import spark.implicits._
    val numericRatings = Seq(
      (0.0, 0.0, 1.0),
      (0.0, 1.0, 2.0),
      (1.0, 0.0, 3.0),
      (1.0, 1.0, 4.0)
    ).toDF(userCol, itemCol, ratingCol)
    val fitted = newSar.fit(numericRatings)
    val legacyModel = new SARModel()
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setUserDataFrame(fitted.getUserDataFrame)
      .setItemDataFrame(fitted.getItemDataFrame)

    assert(!legacyModel.isDefined(legacyModel.userIdMapping))
    assert(!legacyModel.isDefined(legacyModel.itemIdMapping))
    assert(!legacyModel.getUserIdsFitInt)
    assert(!legacyModel.getItemIdsFitInt)
    assert(legacyModel.transform(numericRatings).count() == numericRatings.count())
    val integerScoring = Seq((0, 0), (1, 1)).toDF(userCol, itemCol)
    assert(legacyModel.transform(integerScoring).count() == integerScoring.count())

    val recommendations = legacyModel.recommendForAllUsers(2)
    assert(recommendations.schema(userCol).dataType == IntegerType)
    val recommendationType = recommendations.schema("recommendations").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(recommendationType(itemCol).dataType == IntegerType)
  }

  test("SAR legacy top-K considers only mapped destination identifiers") {
    import spark.implicits._
    val userFactors = Seq((0.0, Seq(0.0f, 0.0f, 0.0f))).toDF(userCol, "flatList")
    val itemFactors = Seq(
      (0.0, Seq(0.0f, 0.0f, 0.0f)),
      (2.0, Seq(0.0f, 0.0f, 0.0f))
    ).toDF(itemCol, "itemAffinities")
    val legacyModel = new SARModel()
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setUserDataFrame(userFactors)
      .setItemDataFrame(itemFactors)

    val itemIds = legacyModel.recommendForAllUsers(2)
      .select("recommendations.item")
      .head()
      .getSeq[Int](0)
    assert(itemIds == Seq(0, 2))
  }

  test("SAR drops unknown and null identifiers during scoring") {
    import spark.implicits._
    val model = newSar.fit(stringRatings)
    val scoringData = Seq(
      (Some("user-a"), Some("item-10")),
      (Some("unknown-user"), Some("item-10")),
      (Some("user-a"), Some("unknown-item")),
      (None, Some("item-10")),
      (Some("user-a"), None)
    ).toDF(userCol, itemCol)

    val rows = model.transform(scoringData).collect()
    assert(rows.length == 1)
    assert(rows.head.getAs[String](userCol) == "user-a")
    assert(rows.head.getAs[String](itemCol) == "item-10")
  }

  test("SAR rejects null training identifiers and unsupported identifier types") {
    import spark.implicits._
    val nullUserData = Seq(
      (Some("user-a"), "item-10", 1.0),
      (None, "item-20", 1.0)
    ).toDF(userCol, itemCol, ratingCol)
    val nullError = intercept[IllegalArgumentException](newSar.fit(nullUserData))
    assert(nullError.getMessage.contains("null"))
    assert(nullError.getMessage.contains(userCol))

    val nullItemData = Seq(
      ("user-a", Some("item-10"), 1.0),
      ("user-b", None, 1.0)
    ).toDF(userCol, itemCol, ratingCol)
    val nullItemError = intercept[IllegalArgumentException](newSar.fit(nullItemData))
    assert(nullItemError.getMessage.contains("null"))
    assert(nullItemError.getMessage.contains(itemCol))

    val unsupported = Seq((true, "item-10", 1.0)).toDF(userCol, itemCol, ratingCol)
    val typeError = intercept[IllegalArgumentException](newSar.transformSchema(unsupported.schema))
    assert(typeError.getMessage.contains("string or numeric"))
  }

  test("SAR transformSchema preserves identifier types and declares prediction") {
    val estimatorSchema = newSar.transformSchema(stringRatings.schema)
    assert(estimatorSchema(userCol).dataType == StringType)
    assert(estimatorSchema(itemCol).dataType == StringType)
    assert(estimatorSchema(newSar.getPredictionCol).dataType == FloatType)

    val model = newSar.fit(stringRatings)
    val modelSchema = model.transformSchema(stringRatings.select(userCol, itemCol).schema)
    assert(modelSchema(userCol).dataType == StringType)
    assert(modelSchema(itemCol).dataType == StringType)
    assert(modelSchema(model.getPredictionCol).dataType == FloatType)

    import spark.implicits._
    val wrongType = Seq((1L, "item-10")).toDF(userCol, itemCol)
    val error = intercept[IllegalArgumentException](model.transformSchema(wrongType.schema))
    assert(error.getMessage.contains("was trained with"))
  }

  test("SAR string mappings and outputs survive save and load") {
    val model = newSar.fit(stringRatings)
    val root = Files.createTempDirectory("sar-string-identifiers")
    val path = root.resolve("model").toString

    try {
      model.write.overwrite().save(path)
      val loaded = SARModel.load(path)

      assert(loaded.isSet(loaded.userIdsFitInt))
      assert(loaded.isSet(loaded.itemIdsFitInt))
      assert(loaded.getUserIdsFitInt == model.getUserIdsFitInt)
      assert(loaded.getItemIdsFitInt == model.getItemIdsFitInt)
      assert(loaded.getUserIdMapping.orderBy("index").collect().toSeq ==
        model.getUserIdMapping.orderBy("index").collect().toSeq)
      assert(loaded.getItemIdMapping.orderBy("index").collect().toSeq ==
        model.getItemIdMapping.orderBy("index").collect().toSeq)
      assert(loaded.transform(stringRatings).orderBy(userCol, itemCol).collect().toSeq ==
        model.transform(stringRatings).orderBy(userCol, itemCol).collect().toSeq)
      assert(loaded.recommendForAllUsers(2).orderBy(userCol).collect().toSeq ==
        model.recommendForAllUsers(2).orderBy(userCol).collect().toSeq)
    } finally {
      FileUtils.forceDelete(root.toFile)
    }
  }
}
