// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalactic.Equality

class RankingAdapterSpec extends RankingTestBase with EstimatorFuzzing[RankingAdapter] {
  override def testObjects(): Seq[TestObject[RankingAdapter]] = {
    List(new TestObject(adapter, transformedDf))
  }

  override def reader: MLReadable[_] = RankingAdapter

  override def modelReader: MLReadable[_] = RankingAdapterModel

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      // sort rows and round decimals before compare two dataframes
      import org.apache.spark.sql.functions._
      val roundListDecimals: Seq[Float] => Seq[Float] = _.map { value =>
        BigDecimal(value.toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toFloat
      }
      val castListToIntUDF = udf(roundListDecimals)
      val sortedDF = df.orderBy(col("prediction"))
      val updatedDF: DataFrame = sortedDF.withColumn("label", castListToIntUDF(col("label")))
      updatedDF
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)}
}

class RankingAdapterModelSpec extends RankingTestBase with TransformerFuzzing[RankingAdapterModel] {
  override def testObjects(): Seq[TestObject[RankingAdapterModel]] = {
    val df = transformedDf
    List(new TestObject(adapter.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingAdapterModel

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      // sort rows and round decimals before comparing dataframes
      import org.apache.spark.sql.functions._
      val roundListDecimals: Seq[Float] => Seq[Float] = _.map { value =>
        BigDecimal(value.toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toFloat
      }
      val castListToIntUDF = udf(roundListDecimals)
      val sortedDF = df.orderBy(col("prediction"))
      val updatedDF: DataFrame = sortedDF.withColumn("label", castListToIntUDF(col("label")))
      updatedDF
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)}
}
