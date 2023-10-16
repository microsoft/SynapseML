// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split2

import com.microsoft.azure.synapse.ml.lightgbm.LightGBMConstants
import com.microsoft.azure.synapse.ml.lightgbm.dataset.DatasetUtils.countCardinality
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.language.postfixOps

//scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM Ranker module in streaming mode. */
class VerifyLightGBMRankerStream extends LightGBMRankerTestData {
  override val dataTransferMode: String = LightGBMConstants.StreamingDataTransferMode

  import spark.implicits._

  test("Verify LightGBM Ranker on ranking dataset" + executionModeSuffix) {
    assertFitWithoutErrors(baseModel, rankingDF)
  }

  test("Throws error when group column is not long, int or string" + executionModeSuffix) {
    val df = rankingDF.withColumn(queryCol, from_json(lit("{}"), StructType(Seq())))
    assertThrows[IllegalArgumentException] {
      baseModel.fit(df).transform(df).collect()
    }
  }

  test("Verify LightGBM Ranker with int, long and string query column" + executionModeSuffix) {
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

  test("Verify LightGBM Ranker feature shaps" + executionModeSuffix) {
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

  test("verify cardinality counts: int" + executionModeSuffix) {
    val counts = countCardinality(Seq(1, 1, 2, 2, 2, 3))
   assert(counts === Seq(2, 3, 1))
  }

  test("verify cardinality counts: long" + executionModeSuffix) {
    val counts = countCardinality(Seq(1L, 1L, 2L, 2L, 2L, 3L))
    assert(counts === Seq(2, 3, 1))
  }

  test("verify cardinality counts: string" + executionModeSuffix) {
    val counts = countCardinality(Seq("a", "a", "b", "b", "b", "c"))
    assert(counts === Seq(2, 3, 1))
  }
}
