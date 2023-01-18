// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => F, types => T}

class VerifyVowpalWabbitGenericProgressive
  extends TestBase
    with TransformerFuzzing[VowpalWabbitGenericProgressive] {
  import spark.implicits._

  lazy val simpleDf = Seq("1 |a b c", "0 |d e f", "1 |a b c")
    .toDF("input")
    // TODO: remove coalese and fix it
    .coalesce(1)

  test("Verify VerifyVowpalWabbitGenericProgressive from string") {

    val vw = new VowpalWabbitGenericProgressive()

    val actual = vw.transform(simpleDf)
      .select(F.round($"prediction", 1).alias("prediction"))

    val expected = Seq(0, 0.2, 0.6).toDF("prediction")

    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using csoaa") {
    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Cost-Sensitive-One-Against-All-(csoaa)-multi-class-example
    val vw = new VowpalWabbitGenericProgressive()
      .setPassThroughArgs("--csoaa 4")

    val dataset = Seq(
      "1:0 2:1 3:1 4:1 | a b c",
      "1:1 2:1 3:0 4:1 | b c d",
      "1:0 2:1 3:1 4:1 | a b c",
      "1:1 2:1 3:0 4:1 | b c d")
      .toDF("input")
      // TODO: remove coalese and fix it
      .coalesce(1)

    val actual = vw.transform(dataset)
      .select("prediction")

    val expected = Seq(1, 1, 3, 3).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    val vw = new VowpalWabbitGenericProgressive()
      .setPassThroughArgs("--oaa 4")

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d",
      "1 | a c e",
      "4 | b d f",
      "2 | d e f")
      .toDF("input")
      // TODO: remove coalese and fix it
      .coalesce(1)

    val actual = vw.transform(dataset)
      .select("prediction")

    val expected = Seq(1, 1, 1, 3, 4).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa w/ probs") {
    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    val vw = new VowpalWabbitGenericProgressive()
      .setPassThroughArgs("--oaa 4 --probabilities")

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d")
      .toDF("input")
      .coalesce(1)

    val actual = vw.transform(dataset)
      .select($"input", F.posexplode($"predictions"))
      .select(F.round($"col", 2).alias("pred"))

    val expected = Seq(
      0.25, 0.25, 0.25, 0.25,
      0.35, 0.22, 0.22, 0.22)
      .toDF("pred")

    verifyResult(expected, actual)
  }

  // TODO: wait for VW to fix CATS producing output through API
  // https://github.com/VowpalWabbit/vowpal_wabbit/pull/4071/files
  // nasty workaround using -p /tmp/foo to activate
//  test("Verify VowpalWabbitGeneric using CATS") {
//
//    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/CATS,-CATS-pdf-for-Continuous-Actions
//    val vw = new VowpalWabbitGenericProgressive()
//      .setPassThroughArgs("--cats_pdf 3 --bandwidth 5000 --min_value 0 --max_value 20000 -p /tmp/foo")
//      .setUseBarrierExecutionMode(false)
//
//    val dataset = Seq(
//      "ca 185.121:0.657567:6.20426e-05 | a b",
//      "ca 772.592:0.458316:6.20426e-05 | b c",
//      "ca 15140.6:0.31791:6.20426e-05 | d")
//      .toDF("input")
//      .coalesce(1)
//
//    val predictionDF = vw
//      .transform(dataset)
//    // .select($"input", F.posexplode($"segments"))
//
//    predictionDF.show(truncate = false)
//  }

  lazy val vwTest = new VowpalWabbitGenericProgressive()

  override def testObjects(): Seq[TestObject[VowpalWabbitGenericProgressive]] =
    Seq(new TestObject(vwTest, simpleDf))

  override def reader: MLReadable[_] = VowpalWabbitGenericProgressive
}
