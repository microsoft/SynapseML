// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable

class VerifyVowpalWabbitGeneric extends Benchmarks with EstimatorFuzzing[VowpalWabbitGeneric] {
  lazy val moduleName = "vw"
  val numPartitions = 2

  test ("Verify VowpalWabbit Classifier from string") {
    import spark.implicits._

    val vw = new VowpalWabbitGeneric()
      .setNumPasses(2)

    val dataset = Seq("1 |a b c", "0 |d e f").map(StringFeatures).toDF

    val classifier = vw.fit(dataset)

    val predictionDF = classifier.transform(dataset)

    predictionDF.show()

    val labelOneCnt = predictionDF.where($"prediction" > 0.5).count()
    assert(labelOneCnt == 1)
  }

  override def reader: MLReadable[_] = VowpalWabbitGeneric
  override def modelReader: MLReadable[_] = VowpalWabbitGenericModel

  override def testObjects(): Seq[TestObject[VowpalWabbitGeneric]] = {
    import spark.implicits._

    val dataset = Seq("1 |a b c", "0 |d e f").map(StringFeatures).toDF
    Seq(new TestObject(
      new VowpalWabbitGeneric(),
      dataset))
  }
}

case class StringFeatures(input: String)
