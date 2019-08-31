// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.split2

import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.ml.spark.lightgbm.split1.LightGBMTestUtils
import com.microsoft.ml.spark.lightgbm.{LightGBMRanker, LightGBMRankerModel, LightGBMUtils}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, _}
import org.apache.spark.sql.types.StringType

//scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM Ranker module. */
class VerifyLightGBMRanker extends Benchmarks with EstimatorFuzzing[LightGBMRanker]
  with LightGBMTestUtils {

  import session.implicits._

  val queryCol = "query"

  lazy val rankingDF: DataFrame = {
    val df1 = session.read.format("libsvm")
      .load(DatasetUtils.rankingTrainFile("rank.train").toString)
      .withColumn("iid", monotonically_increasing_id())

    def createRows = udf((colValue: Int, index: Int) => List.fill(colValue)(index).toArray)

    val df2 = session.read.format("csv")
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

  test("Throws error when group column is not long or int") {
    val df = rankingDF.withColumn(queryCol, col(queryCol).cast(StringType))

    // Throws SparkException instead of IllegalArgumentException because the type
    // inspection is part of the spark job instead of before it
    assertThrows[SparkException] {
      baseModel.fit(df).transform(df).collect()
    }
  }

  test("Verify LightGBM Ranker with int and long query column") {
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
  }

  override def testObjects(): Seq[TestObject[LightGBMRanker]] = {
    Seq(new TestObject(baseModel, rankingDF))
  }

  override def reader: MLReadable[_] = LightGBMRanker

  override def modelReader: MLReadable[_] = LightGBMRankerModel
}
