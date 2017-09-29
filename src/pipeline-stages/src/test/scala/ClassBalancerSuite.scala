// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.DataFrame

class ClassBalancerSuite extends TestBase with RoundTripTestBase {
  val df = session
    .createDataFrame(Seq((0, 1.0, "Hi I"),
      (1, 1.0, "I wish for snow today"),
      (2, 2.0, "I wish for snow today"),
      (3, 2.0, "I wish for snow today"),
      (4, 2.0, "I wish for snow today"),
      (5, 2.0, "I wish for snow today"),
      (6, 0.0, "I wish for snow today"),
      (7, 1.0, "I wish for snow today"),
      (8, 0.0, "we Cant go to the park, because of the snow!"),
      (9, 2.0, "")))
    .toDF("index", "label", "sentence")

  val dfRoundTrip: DataFrame = df
  val reader: MLReadable[_] = ClassBalancer
  val modelReader: MLReadable[_] = ClassBalancerModel
  val stageRoundTrip: PipelineStage with MLWritable = new ClassBalancer()
    .setInputCol("label")

  test("should roundtrip serialize") {
    testRoundTrip()
  }

  test("yield proper weights") {
    val model = new ClassBalancer()
      .setInputCol("label").fit(df)
    val df2 = model.transform(df)
    df2.show()

    assert(df2.collect()(8).getDouble(3) == 2.5)
    assert(df2.schema.fields.toSet == model.transformSchema(df.schema).fields.toSet)
  }
}
