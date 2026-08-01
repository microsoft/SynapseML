// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, LongType, StringType, StructType}

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
    assert(legacyModel.transform(numericRatings).count() == numericRatings.count())
    assert(legacyModel.recommendForAllUsers(2).schema(userCol).dataType == DoubleType)
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
