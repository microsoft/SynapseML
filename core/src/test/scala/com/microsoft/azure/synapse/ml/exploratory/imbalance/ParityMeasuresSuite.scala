// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory.imbalance

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

  private def parityMeasures: ParityMeasures =
    new ParityMeasures()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)

  test("ParityMeasures can calculate Parity Measures end-to-end") {
    val df = parityMeasures.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private lazy val feature: String = "Gender"
  private lazy val val1: String = "Male"
  private lazy val val2: String = "Female"

  private def actual: Map[String, Double] =
    new ParityMeasures()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .filter((col("ClassA") === val1 && col("ClassB") === val2) || (col("ClassB") === val2 && col("ClassB") === val1))
      .as[(String, String, String, Map[String, Double])]
      .collect()(0)._4

  private object ExpectedGenderMaleFemale {
    // Values were computed using:
    // val CALCULATOR = GapCalculator(sensitiveFeaturesDf.count, 3d / 9d, 4d / 9d, 2d / 4d, 3d / 9d, 1d / 3d)
    val DPGAP = 0.125
    val SDCGAP = 0.1428571428571429
    val JIGAP = 0.8000000000000007
    val LLRGAP = 0.4054651081081644
    val PMIGAP = 0.11778303565638346
    val NPMIYGAP = -0.10721073928562769
    val NPMIXYGAP = -0.16992500144231237
    val SPMIGAP = 0.5232481437645479
    val KRCGAP = 0.4771175004652308
    val TTESTGAP = 0.24747125955024085
  }

  test(s"ParityMeasures can calculate Parity Measures for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("dp")) == abs(ExpectedGenderMaleFemale.DPGAP))
    assert(abs(actual("sdc")) == abs(ExpectedGenderMaleFemale.SDCGAP))
    assert(abs(actual("ji")) == abs(ExpectedGenderMaleFemale.JIGAP))
    assert(abs(actual("llr")) == abs(ExpectedGenderMaleFemale.LLRGAP))
    assert(abs(actual("pmi")) == abs(ExpectedGenderMaleFemale.PMIGAP))
    assert(abs(actual("n_pmi_y")) == abs(ExpectedGenderMaleFemale.NPMIYGAP))
    assert(abs(actual("n_pmi_xy")) == abs(ExpectedGenderMaleFemale.NPMIXYGAP))
    assert(abs(actual("s_pmi")) == abs(ExpectedGenderMaleFemale.SPMIGAP))
    assert(abs(actual("krc")) == abs(ExpectedGenderMaleFemale.KRCGAP))
    assert(abs(actual("t_test")) == abs(ExpectedGenderMaleFemale.TTESTGAP))
  }
}
