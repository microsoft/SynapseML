// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.functions.current_timestamp

class PowerBiSuite extends TestBase with FileReaderUtils {

  val url = sys.env("POWERBI_URL")
  val df = session
    .createDataFrame(Seq(
      (Some(0), "a"),
      (Some(1), "b"),
      (Some(2), "c"),
      (Some(3), ""),
      (None, "bad_row")))
    .toDF("bar", "foo")
    .withColumn("baz", current_timestamp())

  test("write to powerBi") {
    PowerBIWriter.write(df, url)
  }

  test("stream to powerBi") {
    df.write.parquet(tmpDir + "powerBI.parquet")
    val sdf = session.readStream.schema(df.schema).parquet(tmpDir + "powerBI.parquet")
    val q1 = PowerBIWriter.stream(sdf, url).start()
    q1.processAllAvailable()
  }
}
