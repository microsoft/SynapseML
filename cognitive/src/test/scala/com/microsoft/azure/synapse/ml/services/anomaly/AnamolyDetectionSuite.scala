// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.anomaly

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities.getAccessToken
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

trait AnomalyKey {

  lazy val anomalyKey: String = sys.env.getOrElse("ANOMALY_API_KEY", Secrets.AnomalyApiKey)
  lazy val anomalyLocation = "westus2"

}

trait AnomalyDetectorSuiteBase extends TestBase with AnomalyKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
  ).toDF("timestamp", "value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(sort_array(collect_list(col("inputs"))).alias("inputs"))

  lazy val df2: DataFrame = Seq(
    ("2000-01-24T08:46:00Z", 826.0),
    ("2000-01-24T08:47:00Z", 799.0),
    ("2000-01-24T08:48:00Z", 890.0),
    ("2000-01-24T08:49:00Z", 900.0),
    ("2000-01-24T08:50:00Z", 766.0),
    ("2000-01-24T08:51:00Z", 805.0),
    ("2000-01-24T08:52:00Z", 821.0),
    ("2000-01-24T08:53:00Z", 20000.0),
    ("2000-01-24T08:54:00Z", 883.0),
    ("2000-01-24T08:55:00Z", 898.0),
    ("2000-01-24T08:56:00Z", 957.0),
    ("2000-01-24T08:57:00Z", 924.0),
    ("2000-01-24T08:58:00Z", 881.0),
    ("2000-01-24T08:59:00Z", 837.0),
    ("2000-01-24T09:00:00Z", 90000.0)
  ).toDF("timestamp", "value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(sort_array(collect_list(col("inputs"))).alias("inputs"))

}

class DetectLastAnomalySuite extends TransformerFuzzing[DetectLastAnomaly] with AnomalyDetectorSuiteBase {

  lazy val ad: DetectLastAnomaly = new DetectLastAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation(anomalyLocation)
    .setOutputCol("anomalies")
    .setSeriesCol("inputs")
    .setGranularity("monthly")
    .setErrorCol("errors")

  test("Basic Usage") {
    val fromRow = ADLastResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly)
  }

  test("Basic usage with AAD auth") {
    val aadToken = getAccessToken(Secrets.ServicePrincipalClientId,
      Secrets.ServiceConnectionSecret,
      "https://cognitiveservices.azure.com/")
    val ad = new DetectLastAnomaly()
      .setAADToken(aadToken)
      .setCustomServiceName("synapseml-ad-custom")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly")
      .setErrorCol("errors")
    val fromRow = ADLastResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly)
  }

  test("minutely Usage") {
    val fromRow = ADLastResponse.makeFromRowConverter
    val result = fromRow(ad.setGranularity("minutely").transform(df2)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly)
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new DetectLastAnomaly()
        .setSubscriptionKey(anomalyKey)
        .setLocation(anomalyLocation)
        .setOutputCol("anomalies")
        .setErrorCol("errors")
        .transform(df).collect()
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("granularity"))
    assert(caught.getMessage.contains("series"))
  }

  override def testObjects(): Seq[TestObject[DetectLastAnomaly]] =
    Seq(new TestObject(ad, df))

  override def reader: MLReadable[_] = DetectLastAnomaly
}

class DetectAnomaliesSuite extends TransformerFuzzing[DetectAnomalies] with AnomalyDetectorSuiteBase {

  lazy val ad: DetectAnomalies = new DetectAnomalies()
    .setSubscriptionKey(anomalyKey)
    .setLocation(anomalyLocation)
    .setOutputCol("anomalies")
    .setSeriesCol("inputs")
    .setGranularity("monthly")

  test("Basic Usage") {
    val fromRow = ADEntireResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly.count({ b => b }) == 2)
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new DetectAnomalies()
        .setSubscriptionKey(anomalyKey)
        .setLocation(anomalyLocation)
        .setOutputCol("anomalies")
        .transform(df).collect()
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("granularity"))
    assert(caught.getMessage.contains("series"))
  }

  override def testObjects(): Seq[TestObject[DetectAnomalies]] =
    Seq(new TestObject(ad, df))

  override def reader: MLReadable[_] = DetectAnomalies
}

class SimpleDetectAnomaliesSuite extends TransformerFuzzing[SimpleDetectAnomalies]
  with AnomalyDetectorSuiteBase {

  lazy val baseSeq = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 9000.0)
  )

  import spark.implicits._

  lazy val sdf: DataFrame = baseSeq.map(p => (p._1, p._2, 1.0))
    .++(baseSeq.map(p => (p._1, p._2, 2.0)))
    .toDF("timestamp", "value", "group")

  lazy val sdf2: DataFrame = baseSeq.map(p => (p._1, p._2, 1.0))
    .++(baseSeq.reverse.map(p => (p._1, p._2, 2.0)))
    .toDF("timestamp", "value", "group")

  lazy val sdf3: DataFrame = baseSeq.map(p => (p._1, p._2, 1.0))
    .++(baseSeq.reverse.take(2).map(p => (p._1, p._2, 2.0)))
    .toDF("timestamp", "value", "group")

  lazy val sad: SimpleDetectAnomalies = new SimpleDetectAnomalies()
    .setSubscriptionKey(anomalyKey)
    .setLocation(anomalyLocation)
    .setOutputCol("anomalies")
    .setGroupbyCol("group")
    .setGranularity("monthly")

  test("Basic Usage") {
    val result = sad.transform(sdf)
      .collect().head.getAs[Row]("anomalies")
    assert(!result.getBoolean(0))
  }

  test("Reverse Reverse!") {
    sad.transform(sdf2)
      .show(truncate = false)
  }

  test("Error handling") {
    sad.transform(sdf3)
      .show(truncate = false)
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new SimpleDetectAnomalies()
        .setSubscriptionKey(anomalyKey)
        .setLocation(anomalyLocation)
        .setOutputCol("anomalies")
        .setGroupbyCol("group")
        .transform(sdf).collect()
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("granularity"))
  }

  //TODO Nulls, different cardinalities

  override def testObjects(): Seq[TestObject[SimpleDetectAnomalies]] =
    Seq(new TestObject(sad, sdf))

  override def reader: MLReadable[_] = SimpleDetectAnomalies
}
