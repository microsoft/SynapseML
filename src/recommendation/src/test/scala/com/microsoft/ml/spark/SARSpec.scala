// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vectors => mlVectors}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.language.existentials

class SARSpec extends RecommendationTestBase with EstimatorFuzzing[SAR] {
  ignore("SAR streaming") {
    df.write.mode(SaveMode.Overwrite).parquet("/mnt/test.parquet")

    val userSchema = new StructType()
      .add("customerIDOrg", "string")
      .add("itemIDOrg", "string")
      .add("rating", "double")
      .add("timeOld", "double")

    val dataFrameStream: DataFrame = session
      .readStream
      .schema(userSchema)
      .parquet("/mnt/test.parquet")

    test_eval_metrics(dataFrameStream, "customerIDOrg", "itemIDOrg")
  }

  ignore("SAR cold items") {
    val (df, itemFeaturesDF) = getColdTestData()
    test_eval_metrics_cold_items(df, itemFeaturesDF, "customerIDOrg", "itemIDOrg")
  }

  test("SAR Metrics Eval Jaccard W/O R - W/O T - W/ Seed")(
    test_eval_metrics_no_ratings(df.withColumnRenamed("rating", "notrating")))

  test("SAR Metrics Eval Jaccard W/O R - W/O T - W/O Seed")(
    test_eval_metrics_no_ratings(df.withColumnRenamed("rating", "notrating"), seed = false))

  test("SAR Metrics Eval Jaccard")(test_eval_metrics(df))

  test("SAR Metrics Eval Lift")(test_eval_metrics(df, similarityFunction = "lift"))

  test("SAR Metrics Eval Cooccurrence")(test_eval_metrics(df, similarityFunction = "Cooccurrence"))

  test("SAR test recommendations without indexer")(test_recommendations_without_indexer(df))

  ignore("SAR hyper parameter sweep")(test_hyper_param_sweep(df))

  override def testObjects(): List[TestObject[SAR]] = {
    val customerIndex = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("movieId")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(df).transform(df)

    List(
      new TestObject(new SAR()
        .setUserCol(customerIndex.getOutputCol)
        .setItemCol(ratingsIndex.getOutputCol)
        .setRatingCol("rating")
        .setAutoIndex(false), transformedDf)
    )
  }

  override def reader: SAR.type = SAR

  override def modelReader: SARModel.type = SARModel
}
