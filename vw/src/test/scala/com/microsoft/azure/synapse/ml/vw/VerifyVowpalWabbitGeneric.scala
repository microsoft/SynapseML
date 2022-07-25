// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.policyeval.{
  CressieRead, CressieReadInput, CressieReadInterval,
  CressieReadIntervalInput, EmpiricalBernsteinCS, EmpiricalBernsteinCSInput, Ips, IpsInput, Snips, SnipsInput
}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => F, types => T}

import java.io.File

class VerifyVowpalWabbitGeneric extends Benchmarks with EstimatorFuzzing[VowpalWabbitGeneric] {
  val numPartitions = 2

  test("Verify VowpalWabbitGeneric from string") {
    import spark.implicits._

    val vw = new VowpalWabbitGeneric()
      .setNumPasses(2)

    val dataset = Seq("1 |a b c", "0 |d e f").toDF("input")

    val classifier = vw.fit(dataset)

    val predictionDF = classifier.transform(dataset)

    val labelOneCnt = predictionDF.where($"prediction" > 0.5).count()
    assert(labelOneCnt == 1)
  }

  test("Verify VowpalWabbitGeneric using csoaa") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Cost-Sensitive-One-Against-All-(csoaa)-multi-class-example
    import spark.implicits._

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--csoaa 4")

    val dataset = Seq(
      "1:0 2:1 3:1 4:1 | a b c",
      "1:1 2:1 3:0 4:1 | b c d"
    ).toDF("input")

    val classifier = vw.fit(dataset)

    val actual = classifier.transform(dataset).select("prediction")

    val expected = Seq(1, 3).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    import spark.implicits._

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--oaa 4")
      .setNumPasses(4)

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d",
      "1 | a c e",
      "4 | b d f",
      "2 | d e f"
    ).toDF("input")

    val classifier = vw.fit(dataset)

    val actual = classifier.transform(dataset).select("prediction")

    val expected = Seq(1, 3, 1, 4, 2).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa w/ probs") {
    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    import spark.implicits._

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--oaa 4")
      .setNumPasses(4)

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d",
      "1 | a c e",
      "4 | b d f",
      "2 | d e f"
    ).toDF("input")

    val classifier = vw.fit(dataset)

    val pred = classifier
      .setTestArgs("--probabilities")
      .transform(dataset)
      .select($"input", F.posexplode($"predictions"))
      // .withColumn("prob", F.round($"col", 1))
      .withColumn("maxProb", F.max("col").over(Window.partitionBy("input")))

    assert(pred.where(F.expr("col < 0 and col > 1")).count() == 0, "predictions must be between 0 and 1")

    val predMatched = pred
      .where($"col" === $"maxProb")
      .withColumn("label", F.substring($"input", 0, 1).cast(T.IntegerType))
      .where($"label" === $"pos" + 1)

    assert(predMatched.count() == 5, "Highest prob prediction must match label")
  }

  test("Verify VowpalWabbitGeneric using CATS") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/CATS,-CATS-pdf-for-Continuous-Actions
    import spark.implicits._

    val vw = new VowpalWabbitGenericProgressive()
      .setPassThroughArgs("--cats_pdf 3 --bandwidth 5000 --min_value 0 --max_value 20000")
      .setUseBarrierExecutionMode(false)

    val dataset = Seq(
      "ca 185.121:0.657567:6.20426e-05 | a b",
      "ca 772.592:0.458316:6.20426e-05 | b c",
      "ca 15140.6:0.31791:6.20426e-05 | d"
    ).toDF("input").coalesce(2)

    val predictionDF = vw
      .transform(dataset)
    // .select($"input", F.posexplode($"segments"))

    predictionDF.show(truncate = false)
  }

  override def reader: MLReadable[_] = VowpalWabbitGeneric

  override def modelReader: MLReadable[_] = VowpalWabbitGenericModel

  override def testObjects(): Seq[TestObject[VowpalWabbitGeneric]] = {
    import spark.implicits._

    val dataset = Seq("1 |a b c", "0 |d e f").toDF("input")
    Seq(new TestObject(
      new VowpalWabbitGeneric(),
      dataset))
  }
}
