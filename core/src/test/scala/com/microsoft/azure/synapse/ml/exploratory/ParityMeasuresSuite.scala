// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions._

import scala.math.abs

class ParityMeasuresSuite extends DataBalanceTestBase with TransformerFuzzing[ParityMeasures] {

  override def testObjects(): Seq[TestObject[ParityMeasures]] = Seq(
    new TestObject(parityMeasures, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = ParityMeasures

  import spark.implicits._

  private lazy val parityMeasures: ParityMeasures =
    new ParityMeasures()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)

  test("ParityMeasures can calculate Parity Measures end-to-end") {
    val df = parityMeasures.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private lazy val expectedGenderMaleFemale: GapCalculator =
    GapCalculator(sensitiveFeaturesDf.count, 3d / 9d, 4d / 9d, 2d / 4d, 3d / 9d, 1d / 3d)

  private lazy val feature: String = "Gender"
  private lazy val val1: String = "Male"
  private lazy val val2: String = "Female"

  private lazy val actual: Map[String, Double] =
    new ParityMeasures()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .filter(
        (col("ClassA") === val1 && col("ClassB") === val2) ||
          (col("ClassB") === val2 && col("ClassB") === val1)
      )
      .as[(String, String, String, Map[String, Double])]
      .collect()(0)._4

  test(s"ParityMeasures can calculate Demographic Parity for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("dp")) == abs(expectedGenderMaleFemale.dpGap))
  }

  test(s"ParityMeasures can calculate Sorensen-Dice Coefficient for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("sdc")) == abs(expectedGenderMaleFemale.sdcGap))
  }

  test(s"ParityMeasures can calculate Jaccard Index for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("ji")) == abs(expectedGenderMaleFemale.jiGap))
  }

  test(s"ParityMeasures can calculate Log-Likelihood Ratio for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("llr")) == abs(expectedGenderMaleFemale.llrGap))
  }

  test(s"ParityMeasures can calculate PMI (Pointwise Mutual Information) for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("pmi")) == abs(expectedGenderMaleFemale.pmiGap))
  }

  test(s"ParityMeasures can calculate Normalized PMI, p(y) normalization for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("n_pmi_y")) == abs(expectedGenderMaleFemale.nPmiYGap))
  }

  test(s"ParityMeasures can calculate Normalized PMI, p(x,y) normalization for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("n_pmi_xy")) == abs(expectedGenderMaleFemale.nPmiXYGap))
  }

  test(s"ParityMeasures can calculate Squared PMI for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("s_pmi")) == abs(expectedGenderMaleFemale.sPmiGap))
  }

  test(s"ParityMeasures can calculate Kendall Rank Correlation for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("krc")) == abs(expectedGenderMaleFemale.krcGap))
  }

  test(s"ParityMeasures can calculate t-test for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("t_test")) == abs(expectedGenderMaleFemale.tTestGap))
  }
}
