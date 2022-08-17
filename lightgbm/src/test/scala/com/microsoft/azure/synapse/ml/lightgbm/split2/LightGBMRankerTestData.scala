// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split2

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.lightgbm.split1.LightGBMTestUtils
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRanker, LightGBMRankerModel, LightGBMUtils}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

//scalastyle:off magic.number

/** Base class for tests for LightGBM Ranker. */
abstract class LightGBMRankerTestData extends Benchmarks with EstimatorFuzzing[LightGBMRanker]
  with LightGBMTestUtils {

  val queryCol = "query"

  lazy val rankingDF: DataFrame = {
    val df1 = spark.read.format("libsvm")
      .load(DatasetUtils.rankingTrainFile("rank.train").toString)
      .withColumn("iid", monotonically_increasing_id())

    def createRows = udf((colValue: Int, index: Int) => List.fill(colValue)(index).toArray)

    val df2 = spark.read.format("csv")
      .option("inferSchema", value = true)
      .load(DatasetUtils.rankingTrainFile("rank.train.query").toString)
      .withColumn("index", monotonically_increasing_id())
      .withColumn(queryCol, explode(createRows(col("_c0"), col("index"))))
      .withColumn("iid", monotonically_increasing_id())
      .drop("_c0", "index")
      .join(df1, "iid").drop("iid")
      .withColumnRenamed("label", labelCol)
      .repartition(numPartitions)

    LightGBMUtils.getFeaturizer(df2, labelCol, "_features", groupColumn = Some(queryCol))
      .transform(df2)
      .drop("features")
      .withColumnRenamed("_features", "features")
      .cache()
  }

  def baseModel: LightGBMRanker = {
    new LightGBMRanker()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setGroupCol(queryCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setRepartitionByGroupingColumn(false)
      .setNumLeaves(5)
      .setNumIterations(10)
  }

  override def testObjects(): Seq[TestObject[LightGBMRanker]] = {
    Seq(new TestObject(baseModel, rankingDF))
  }

  override def reader: MLReadable[_] = LightGBMRanker

  override def modelReader: MLReadable[_] = LightGBMRankerModel
}
