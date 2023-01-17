// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row, functions => F, types => T}

class VerifyVowpalWabbitDSJson extends Benchmarks with TransformerFuzzing[VowpalWabbitDSJsonTransformer] {
  test ("Verify VowpalWabbitDSJson") {
    val df = new VowpalWabbitDSJsonTransformer().transform(dataset)

    val row = df.collect().head

    assert(row(df.schema.names.indexOf("EventId")) == "13118d9b4c114f8485d9dec417e3aefe")
    assert(row(df.schema.names.indexOf("chosenActionIndex")) == 3)
  }

  private def dataset: DataFrame = {
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,
      "VowpalWabbit", "Train", "dsjson_cb_part1.json").toString
    spark.read.text(fileLocation)
  }

  override def testObjects(): Seq[TestObject[VowpalWabbitDSJsonTransformer]] =
    Seq(new TestObject(
      new VowpalWabbitDSJsonTransformer(),
      dataset))

  override def reader: MLReadable[_] = VowpalWabbitDSJsonTransformer
}
