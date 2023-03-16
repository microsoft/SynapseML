// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions._

import scala.math.abs

class FeatureBalanceMeasureSuite extends DataBalanceTestBase with TransformerFuzzing[FeatureBalanceMeasure] {

  override val sortInDataframeEquality = true

  override def testObjects(): Seq[TestObject[FeatureBalanceMeasure]] = Seq(
    new TestObject(featureBalanceMeasure, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = FeatureBalanceMeasure

  import AssociationMetrics._
  import spark.implicits._

  private def featureBalanceMeasure: FeatureBalanceMeasure =
    new FeatureBalanceMeasure()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)

  test("FeatureBalanceMeasure can calculate Feature Balance Measures end-to-end") {
    val df = featureBalanceMeasure.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private lazy val feature: String = "Gender"
  private lazy val val1: String = "Male"
  private lazy val val2: String = "Female"

  private def actualGenderMaleFemale: Map[String, Double] =
    METRICS zip new FeatureBalanceMeasure()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(false) // Verbose adds additional cols to the measure struct, so disable it for unit test
      .transform(sensitiveFeaturesDf)
      .filter((col("ClassA") === val1 && col("ClassB") === val2) || (col("ClassB") === val2 && col("ClassB") === val1))
      .select(array(col("FeatureBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private object ExpectedGenderMaleFemale {
    // Values were computed using:
    // val CALC = AssociationMetricsCalculator(sensitiveFeaturesDf.count, 3d / 9d, 4d / 9d, 2d / 9d, 3d / 9d, 1d / 9d)
    val DPGAP = 0.16666666666666669
    val SDCGAP = 0.1190476190476191
    val JIGAP = 0.20000000000000004
    val LLRGAP = 0.6931471805599454
    val PMIGAP = 0.4054651081081645
    val NPMIYGAP = -0.3690702464285426
    val NPMIXYGAP = -0.03915457938162986
    val SPMIGAP = 1.0986122886681098
    val KRCGAP = 0.18801108758923135
    val TTESTGAP = 0.19245008972987523
  }

  test(s"FeatureBalanceMeasure can calculate Feature Balance Measures for $feature=$val1 vs. $feature=$val2") {
    val actual = actualGenderMaleFemale
    val expected = ExpectedGenderMaleFemale
    assert(abs(actual(DP)) === abs(expected.DPGAP))
    assert(abs(actual(SDC)) === abs(expected.SDCGAP))
    assert(abs(actual(JI)) === abs(expected.JIGAP))
    assert(abs(actual(LLR)) === abs(expected.LLRGAP))
    assert(abs(actual(PMI)) === abs(expected.PMIGAP))
    assert(abs(actual(NPMIY)) === abs(expected.NPMIYGAP))
    assert(abs(actual(NPMIXY)) === abs(expected.NPMIXYGAP))
    assert(abs(actual(SPMI)) === abs(expected.SPMIGAP))
    assert(abs(actual(KRC)) === abs(expected.KRCGAP))
    assert(abs(actual(TTEST)) === abs(expected.TTESTGAP))
  }
}
