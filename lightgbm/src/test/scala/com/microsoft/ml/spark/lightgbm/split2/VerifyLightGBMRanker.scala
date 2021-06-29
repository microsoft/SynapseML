// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.split2

import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.CardinalityTypes._
import com.microsoft.ml.spark.lightgbm.dataset.{DatasetUtils => CardinalityUtils}
import com.microsoft.ml.spark.lightgbm.split1.LightGBMTestUtils
import com.microsoft.ml.spark.lightgbm.{LightGBMRanker, LightGBMRankerModel, LightGBMUtils, TrainUtils}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, _}
import org.apache.spark.sql.types.StructType
import org.scalatest.Matchers._

//scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM Ranker module. */
class VerifyLightGBMRanker extends Benchmarks with EstimatorFuzzing[LightGBMRanker]
  with LightGBMTestUtils {

  import spark.implicits._

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

  override def testExperiments(): Unit = {
    super.testExperiments()
  }

  override def testSerialization(): Unit = {
    super.testSerialization()
  }

  test("Verify LightGBM Ranker on ranking dataset") {
    assertFitWithoutErrors(baseModel, rankingDF)
  }

  test("Throws error when group column is not long, int or string") {
    val df = rankingDF.withColumn(queryCol, from_json(lit("{}"), StructType(Seq())))

    // Throws SparkException instead of IllegalArgumentException because the type
    // inspection is part of the spark job instead of before it
    assertThrows[SparkException] {
      baseModel.fit(df).transform(df).collect()
    }
  }

  test("Verify LightGBM Ranker with int, long and string query column") {
    val baseDF = Seq(
      (0L, 1, 1.2, 2.3),
      (0L, 0, 3.2, 2.35),
      (1L, 0, 1.72, 1.39),
      (1L, 1, 1.82, 3.8)
    ).toDF(queryCol, labelCol, "f1", "f2")

    val df = new VectorAssembler()
      .setInputCols(Array("f1", "f2"))
      .setOutputCol(featuresCol)
      .transform(baseDF)
      .select(queryCol, labelCol, featuresCol)

    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray), df)
    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray),
      df.withColumn(queryCol, col(queryCol).cast("Int")))
    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray),
      df.withColumn(queryCol, concat(lit("str_"), col(queryCol))))
  }

  test("Verify LightGBM Ranker feature shaps") {
    val baseDF = Seq(
      (0L, 1, 1.2, 2.3),
      (0L, 0, 3.2, 2.35),
      (1L, 0, 1.72, 1.39),
      (1L, 1, 1.82, 3.8)
    ).toDF(queryCol, labelCol, "f1", "f2")

    val df = new VectorAssembler()
      .setInputCols(Array("f1", "f2"))
      .setOutputCol(featuresCol)
      .transform(baseDF)
      .select(queryCol, labelCol, featuresCol)

    val fitModel = baseModel.setEvalAt(1 to 3 toArray).fit(df)

    val featuresInput = Vectors.dense(Array[Double](0.0, 0.0))
    assert(fitModel.numFeatures == 2)
    assertFeatureShapLengths(fitModel, featuresInput, df)
    assert(fitModel.predict(featuresInput) == fitModel.getFeatureShaps(featuresInput).sum)
  }

  test("verify cardinality counts: int") {
    val counts = CardinalityUtils.countCardinality(Seq(1, 1, 2, 2, 2, 3))

    counts shouldBe Seq(2, 3, 1)
  }

  test("verify cardinality counts: string") {
    val counts = CardinalityUtils.countCardinality(Seq("a", "a", "b", "b", "b", "c"))

    counts shouldBe Seq(2, 3, 1)
  }

  override def testObjects(): Seq[TestObject[LightGBMRanker]] = {
    Seq(new TestObject(baseModel, rankingDF))
  }

  override def reader: MLReadable[_] = LightGBMRanker

  override def modelReader: MLReadable[_] = LightGBMRankerModel
}
