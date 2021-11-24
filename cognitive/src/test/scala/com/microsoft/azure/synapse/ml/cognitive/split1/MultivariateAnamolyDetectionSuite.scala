// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality


trait MADUtils extends TestBase {
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/sample_data_5_3000.zip"
  ).toDF("source")

  lazy val startTime: String = "2021-01-01T00:00:00Z"

  lazy val endTime: String = "2021-01-02T12:00:00Z"
}

class MultivariateAnomalyModelSuite extends TransformerFuzzing[MultivariateAnomalyModel] with AnomalyKey with MADUtils {

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "result.modelInfo.status")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  def mad: MultivariateAnomalyModel = new MultivariateAnomalyModel()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("result")
    .setSourceCol("source")
    .setStartTime(startTime)
    .setEndTime(endTime)

  test("Basic Usage") {
    val result = mad.setSlidingWindow(200).transform(df)
      .withColumn("status", col("result.modelInfo.status"))
      .withColumn("source", col("result.modelInfo.source"))
      .withColumn("variableStates", col("result.modelInfo.diagnosticsInfo.variableStates"))
      .select("status", "source", "variableStates")
      .collect()

    assert(result.head.getString(0).equals("READY"))
    assert(result.head.getString(1).equals("https://mmlspark.blob.core.windows.net/datasets/sample_data_5_3000.zip"))
    assert(result.head.getSeq(2).length.equals(5))
  }

  test("Throw errors if slidingWindow is not between 28 and 2880") {
    val caught = intercept[IllegalArgumentException] {
      mad.setSlidingWindow(20).transform(df).collect()
    }
    assert(caught.getMessage.contains("parameter slidingWindow given invalid value"))
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new MultivariateAnomalyModel()
        .setSubscriptionKey(anomalyKey)
        .setLocation("westus2")
        .setOutputCol("result")
        .transform(df).collect()
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("slidingWindow"))
    assert(caught.getMessage.contains("source"))
    assert(caught.getMessage.contains("startTime"))
    assert(caught.getMessage.contains("endTime"))
  }

  override def testObjects(): Seq[TestObject[MultivariateAnomalyModel]] =
    Seq(new TestObject(mad.setSlidingWindow(200), df))

  override def reader: MLReadable[_] = MultivariateAnomalyModel
}

