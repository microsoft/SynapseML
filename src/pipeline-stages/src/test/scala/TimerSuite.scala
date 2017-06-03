// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.DataFrame

class TimerSuite extends RoundTripTestBase {

  import session.implicits._

  val df = session
    .createDataFrame(Seq((0, "Hi I"),
      (1, "I wish for snow today"),
      (2, "we Cant go to the park, because of the snow!"),
      (3, "")))
    .toDF("label", "sentence")

  test("Work with transformers and estimators") {

    val tok = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")

    val df2 = new Timer().setStage(tok).fit(df).transform(df)

    val df3 = new HashingTF().setInputCol("tokens").setOutputCol("hash").transform(df2)

    val idf = new IDF().setInputCol("hash").setOutputCol("idf")

    val df4 = new Timer().setStage(idf).fit(df3).transform(df3)

  }

  test("should work within pipelines") {
    val tok = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")

    val ttok = new Timer().setStage(tok)

    val hash = new HashingTF().setInputCol("tokens").setOutputCol("hash")

    val idf = new IDF().setInputCol("hash").setOutputCol("idf")

    val tidf = new Timer().setStage(idf)

    val pipe = new Pipeline().setStages(Array(ttok, hash, tidf))

    pipe.fit(df).transform(df)

  }
  val dfRoundTrip: DataFrame = df
  val reader: MLReadable[_] = Timer
  val modelReader: MLReadable[_] = TimerModel
  val stageRoundTrip: PipelineStage with MLWritable = {
    val tok = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")

    new Timer().setStage(tok)
  }

  test("should roundtrip serialize") {
    testRoundTrip(ignoreEstimators = true)
  }

}
