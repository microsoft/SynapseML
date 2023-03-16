// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.powerbi.PowerBIWriter
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{current_timestamp, lit}

import java.io.File
import scala.collection.JavaConverters._

class PowerBiSuite extends TestBase with FileReaderUtils {

  lazy val url: String = sys.env.getOrElse("MML_POWERBI_URL", Secrets.PowerbiURL)
  lazy val df: DataFrame = spark
    .createDataFrame(Seq(
      (Some(0), "a"),
      (Some(1), "b"),
      (Some(2), "c"),
      (Some(3), ""),
      (None, "bad_row")))
    .toDF("bar", "foo")
    .withColumn("baz", current_timestamp())
  lazy val bigdf: DataFrame = (1 to 5).foldRight(df) { case (_, ldf) => ldf.union(df) }.repartition(2)
  lazy val delayDF: DataFrame = {
    val rows = Array.fill(100){df.collect()}.flatten.toList.asJava
    val df2 = spark
      .createDataFrame(rows, df.schema)
      .coalesce(1).cache()
    df2.count()
    df2.map({x => Thread.sleep(10); x})(RowEncoder(df2.schema))
  }

  test("write to powerBi") {
    PowerBIWriter.write(df, url)
  }

  test("write to powerBi with delays"){
    PowerBIWriter.write(delayDF, url)
  }

  test("using dynamic minibatching"){
    PowerBIWriter.write(delayDF, url, Map("minibatcher"->"dynamic", "maxBatchSize"->"50"))
  }

  test("using timed minibatching"){
    PowerBIWriter.write(delayDF, url, Map("minibatcher"->"timed"))
  }

  test("using consolidated timed minibatching"){
    PowerBIWriter.write(delayDF, url, Map(
      "minibatcher"->"timed",
      "consolidate"->"true"))
  }

  test("using buffered batching"){
    PowerBIWriter.write(delayDF, url, Map("buffered"->"true"))
  }

  ignore("throw useful error message when given an improper dataset") {
    //TODO figure out why this does not throw errors on the build machine
    assertThrows[SparkException] {
      PowerBIWriter.write(df.withColumn("bad", lit("foo")), url)
    }
  }

  test("stream to powerBi") {
    bigdf.write.parquet(tmpDir + File.separator + "powerBI.parquet")
    val sdf = spark.readStream.schema(df.schema).parquet(tmpDir + File.separator + "powerBI.parquet")
    val q1 = PowerBIWriter.stream(sdf, url).start()
    q1.processAllAvailable()
  }

}
