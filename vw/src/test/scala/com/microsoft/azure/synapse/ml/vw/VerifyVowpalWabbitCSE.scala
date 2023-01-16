// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row, functions => F, types => T}

class VerifyVowpalWabbitCSETransformer extends Benchmarks with TransformerFuzzing[VowpalWabbitCSETransformer] {
  test ("Verify VowpaWabbitCSETransformer") {
    val df = new VowpalWabbitCSETransformer().transform(dataset)

    val row = df.collect().head

    // example count
    assert(row(df.schema.names.indexOf("exampleCount")) == 3)
    assert(row(df.schema.names.indexOf("probPredNonZeroCount")) == 2)
  }

  private def dataset: DataFrame = {
    val predictions = Seq(
      Row("e1", Row(0.0f), 0.8f, 1, Seq(Row(1, 8f), Row(0, 5f), Row(2, 5f))),
      Row("e2", Row(1.0f), 0.2f, 0, Seq(Row(0, 5f), Row(0, 3f))),
      Row("e3", Row(0.0f), 0.5f, 0, Seq(Row(1, 3f), Row(0, 1f)))
    )

    val predictionSchema = new T.StructType()
      .add("EventId", T.StringType)
      .add("rewards", new T.StructType()
        .add("reward", T.FloatType))
      .add("probLog", T.FloatType)
      .add("chosenActionIndex", T.IntegerType)
      .add("predictions", new T.ArrayType(new T.StructType()
        .add("action", T.IntegerType)
        .add("score", T.FloatType), false))

    spark.createDataFrame(spark.sparkContext
      .parallelize(predictions), predictionSchema)
  }

  override def testObjects(): Seq[TestObject[VowpalWabbitCSETransformer]] =
    Seq(new TestObject(
      new VowpalWabbitCSETransformer(),
      dataset))

  override def reader: MLReadable[_] = VowpalWabbitCSETransformer
}
