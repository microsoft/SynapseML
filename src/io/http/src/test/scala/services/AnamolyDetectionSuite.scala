// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive.{ADEntireResponse, ADLastResponse}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, lit, struct}

trait AnomalyKey {
  lazy val anomalyKey = sys.env("ANOMALY_API_KEY")
}

trait AnomalyDetectorSuiteBase extends TestBase with AnomalyKey{

  import session.implicits._

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
    ("1973-03-01T00:00:00Z", 9000.0)
  ).toDF("timestamp","value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(collect_list(col("inputs")).alias("inputs"))

}

class DetectLastAnomalySuite extends TransformerFuzzing[DetectLastAnomaly] with AnomalyDetectorSuiteBase  {

  lazy val ad = new DetectLastAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("anomalies")
    .setSeriesCol("inputs")
    .setGranularity("monthly")

  test("Basic Usage"){
    val fromRow = ADLastResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly)
  }

  override def testObjects(): Seq[TestObject[DetectLastAnomaly]] =
    Seq(new TestObject(ad, df))

  override def reader: MLReadable[_] = DetectLastAnomaly
}

class DetectAnomaliesSuite extends TransformerFuzzing[DetectAnomalies] with AnomalyDetectorSuiteBase  {

  lazy val ad = new DetectAnomalies()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("anomalies")
    .setSeriesCol("inputs")
    .setGranularity("monthly")

  test("Basic Usage"){
    val fromRow = ADEntireResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
    assert(result.isAnomaly.count({b => b}) == 2)
  }

  override def testObjects(): Seq[TestObject[DetectAnomalies]] =
    Seq(new TestObject(ad, df))

  override def reader: MLReadable[_] = DetectAnomalies
}
