// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{monotonically_increasing_id, col}
import org.apache.spark.sql.functions._

/** Tests to validate the functionality of LightGBM Ranker module. */
class VerifyLightGBMRanker extends Benchmarks with EstimatorFuzzing[LightGBMRanker] {
  lazy val moduleName = "lightgbm"
  var portIndex = 30
  val numPartitions = 2
  val queryCol = "query"

  def rankingDataset(): DataFrame = {
    val trainDF = session.read.format("libsvm")
      .load(DatasetUtils.rankingTrainFile("rank.train").toString)
      .withColumn("iid", monotonically_increasing_id())
    def createRows = udf((colValue: Int, index: Int) => List.fill(colValue)(index).toArray)
    var query = session.read.format("csv")
      .option("inferSchema", true)
      .load(DatasetUtils.rankingTrainFile("rank.train.query").toString)
      .withColumn("index", monotonically_increasing_id())
    query = query
      .withColumn(queryCol, explode(createRows(col("_c0"), col("index"))))
      .withColumn("iid", monotonically_increasing_id())
      .drop("_c0", "index")
    query.join(trainDF, "iid").drop("iid").cache()
  }

  test("Verify LightGBM Ranker on ranking dataset") {
    // Increment port index
    portIndex += numPartitions
    val labelColumnName ="label"
    val dataset = rankingDataset.repartition(numPartitions)
    // val dataset = rankingDataset
    dataset.printSchema()
    dataset.show()
    val featuresColumn = "_features"
    val lgbm = new LightGBMRanker()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setGroupCol(queryCol)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn,
      groupColumn = Some(queryCol))
    val model = lgbm.fit(featurizer.transform(dataset))
    model.transform(featurizer.transform(dataset))
    assert(model != null)
  }

  override def testObjects(): Seq[TestObject[LightGBMRanker]] = {
    val labelCol = "label"
    val featuresCol = "_features"
    val dataset = rankingDataset.repartition(numPartitions)
    val featurizer = LightGBMUtils.featurizeData(dataset, labelCol, featuresCol, groupColumn = Some(queryCol))
    val train = featurizer.transform(dataset)

    Seq(new TestObject(
      new LightGBMRanker()
        .setLabelCol(labelCol)
        .setFeaturesCol(featuresCol)
        .setGroupCol(queryCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10),
      train))
  }

  override def reader: MLReadable[_] = LightGBMRanker

  override def modelReader: MLReadable[_] = LightGBMRankerModel
}
