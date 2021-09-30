package com.microsoft.ml.spark.exploratory

import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.DoubleType

import scala.math.abs

class AggregateMeasuresSuite extends DataImbalanceTestBase {

  import spark.implicits._

  private lazy val aggregateMeasures: AggregateMeasures = {
    spark
    new AggregateMeasures()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setVerbose(true)
  }

  test("AggregateMeasures can calculate Aggregate Measures end-to-end") {
    val aggregateMeasuresDf = aggregateMeasures.transform(sensitiveFeaturesDf)
    aggregateMeasuresDf.show(truncate = false)
    aggregateMeasuresDf.printSchema()
  }

  private lazy val oneSensitiveFeatureDf = sensitiveFeaturesDf
    .groupBy("Gender")
    .agg(count("*").cast(DoubleType).alias("countSensitive"))
    .withColumn("countAll", lit(sensitiveFeaturesDf.count.toDouble))
    .withColumn("countSensitiveProb", col("countSensitive") / col("countAll"))
    .select("countSensitiveProb").as[Double].collect()

  private lazy val expectedOneFeature = AggregateMeasureCalculator(oneSensitiveFeatureDf, 1d, 1e-12)

  private lazy val expectedOneFeatureDiffEpsilon = AggregateMeasureCalculator(oneSensitiveFeatureDf, 0.9, 1e-12)

  private lazy val actualOneFeature: Map[String, Double] = {
    spark
    new AggregateMeasures()
      .setSensitiveCols(Array("Gender"))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)
  }

  private lazy val actualOneFeatureDiffEpsilon: Map[String, Double] = {
    spark
    new AggregateMeasures()
      .setSensitiveCols(Array("Gender"))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)
  }

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 1 sensitive feature") {
    assert(abs(actualOneFeature("atkinson_index") - expectedOneFeature.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 1 sensitive feature") {
    assert(abs(
      actualOneFeatureDiffEpsilon("atkinson_index") - expectedOneFeatureDiffEpsilon.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 1 sensitive feature") {
    assert(abs(actualOneFeature("theil_l_index") - expectedOneFeature.theilLIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 1 sensitive feature") {
    assert(abs(actualOneFeature("theil_t_index") - expectedOneFeature.theilTIndex) < errorTolerance)
  }

  private lazy val bothSensitiveFeaturesDf = sensitiveFeaturesDf
    .groupBy("Gender", "Ethnicity")
    .agg(count("*").cast(DoubleType).alias("countSensitive"))
    .withColumn("countAll", lit(sensitiveFeaturesDf.count.toDouble))
    .withColumn("countSensitiveProb", col("countSensitive") / col("countAll"))
    .select("countSensitiveProb").as[Double].collect()

  private lazy val expectedBothFeatures = AggregateMeasureCalculator(bothSensitiveFeaturesDf, 1d, 1e-12)

  private lazy val expectedBothFeaturesDiffEpsilon = AggregateMeasureCalculator(bothSensitiveFeaturesDf, 0.9, 1e-12)

  private lazy val actualBothFeatures: Map[String, Double] = {
    spark
    new AggregateMeasures()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)
  }

  private lazy val actualBothFeaturesDiffEpsilon: Map[String, Double] = {
    spark
    new AggregateMeasures()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)
  }

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 2 sensitive features") {
    assert(abs(actualBothFeatures("atkinson_index") - expectedBothFeatures.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 2 sensitive features") {
    assert(abs(
      actualBothFeaturesDiffEpsilon("atkinson_index") - expectedBothFeaturesDiffEpsilon.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 2 sensitive features") {
    assert(abs(actualBothFeatures("theil_l_index") - expectedBothFeatures.theilLIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 2 sensitive features") {
    assert(abs(actualBothFeatures("theil_t_index") - expectedBothFeatures.theilTIndex) < errorTolerance)
  }
}
