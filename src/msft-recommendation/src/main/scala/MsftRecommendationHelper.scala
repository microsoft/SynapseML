// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame

import scala.util.Random

class MsftRecommendationHelper() {
  def split(dfRaw: DataFrame): (DataFrame, DataFrame) = {
    val ratingsTemp = dfRaw.dropDuplicates()

    val customerIndexer = new StringIndexer()
      .setInputCol("customerID")
      .setOutputCol("customerIDindex")

    val ratingsIndexed1 = customerIndexer.fit(ratingsTemp).transform(ratingsTemp)

    val itemIndexer = new StringIndexer()
      .setInputCol("itemID")
      .setOutputCol("itemIDindex")

    val ratings = itemIndexer.fit(ratingsIndexed1).transform(ratingsIndexed1)
      .drop("customerID").withColumnRenamed("customerIDindex", "customerID")
      .drop("itemID").withColumnRenamed("itemIDindex", "itemID")

    ratings.cache()

    val minRatingsU = 1
    val minRatingsI = 1
    val RATIO = 0.75

    import dfRaw.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val tmpDF = ratings
      .groupBy("customerID")
      .agg('customerID, count('itemID))
      .withColumnRenamed("count(itemID)", "nitems")
      .where(col("nitems") >= minRatingsU)

    val inputDF = ratings.groupBy("itemID")
      .agg('itemID, count('customerID))
      .withColumnRenamed("count(customerID)", "ncustomers")
      .where(col("ncustomers") >= minRatingsI)
      .join(ratings, "itemID")
      .drop("ncustomers")
      .join(tmpDF, "customerID")
      .drop("nitems")

    inputDF.cache()

    val nusers_by_item = inputDF.groupBy("itemID")
      .agg('itemID, count("customerID"))
      .withColumnRenamed("count(customerID)", "nusers")
      .rdd

    val perm_indices = nusers_by_item.map(r => (r(0), Random.shuffle(List(r(1))), List(r(1))))
    perm_indices.cache()

    val tr_idx = perm_indices.map(r => (r._1, r._2.slice(0, math.round(r._3.size.toDouble * RATIO).toInt)))

    val train = inputDF.rdd
      .groupBy(r => r(1))
      .join(tr_idx)
      .flatMap(r => r._2._1.slice(0, r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2)))
      .toDF("customerID", "itemID", "rating")

    train.cache()

    val testIndex = perm_indices.map(r => (r._1, r._2.drop(math.round(r._3.size.toDouble * RATIO).toInt)))

    val test = inputDF.rdd
      .groupBy(r => r(1))
      .join(testIndex)
      .flatMap(r => r._2._1.drop(r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2))).toDF("customerID", "itemID", "rating")

    test.cache()

    (train, test)
  }

}

