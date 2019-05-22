// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.functions._

/** Tests to validate the functionality of LightGBM Ranker module. */
class VerifyLightGBMRanker extends Benchmarks with EstimatorFuzzing[LightGBMRanker] with OsUtils {
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

  override def testExperiments(): Unit = {
    assume(!isWindows)
    super.testExperiments()
  }

  override def testSerialization(): Unit = {
    assume(!isWindows)
    super.testSerialization()
  }

  test("Verify LightGBM Ranker on ranking dataset") {
    assume(!isWindows)
    // Increment port index
    portIndex += numPartitions
    val labelColumnName ="label"
    val dataset = rankingDataset.repartition(numPartitions)
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

  test("Verify LightGBM Ranker with int and long query column") {
    val labelColumnName = "label"
    val featuresColumn = "_features"
    val dataset = session.createDataFrame(Seq(
      (0L, 1, 1.2, 2.3),
      (0L, 0, 3.2, 2.35),
      (1L, 0, 1.72, 1.39),
      (1L, 1, 1.82, 3.8)
    )).toDF(queryCol, labelColumnName, "f1", "f2")
    val assembler = new VectorAssembler().setInputCols(Array("f1", "f2")).setOutputCol(featuresColumn)
    val output = assembler.transform(dataset).select(queryCol, labelColumnName, featuresColumn)
    val lgbm = new LightGBMRanker()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setGroupCol(queryCol)
      .setNumIterations(2)
    val model = lgbm.fit(output)
    model.transform(output)
    assert(model != null)
    val modelInt = lgbm.fit(output.withColumn(queryCol, col(queryCol).cast("Int")))
    modelInt.transform(output)
    assert(modelInt != null)
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
