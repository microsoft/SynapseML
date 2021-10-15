package com.microsoft.ml.spark.exploratory

import org.apache.spark.sql.functions._

import scala.math.abs

class AssociationGapsSuite extends DataImbalanceTestBase {

  import spark.implicits._

  private lazy val associationGaps: AssociationGaps =
    new AssociationGaps()
      .setSensitiveCols(features)
      .setLabelCol(label)
      .setVerbose(true)

  test("AssociationGaps can calculate Association Gaps end-to-end") {
    val associationGapsDf = associationGaps.transform(sensitiveFeaturesDf)
    associationGapsDf.show(truncate = false)
    associationGapsDf.printSchema()
  }

  private lazy val expectedGenderMaleFemale: GapCalculator =
    GapCalculator(sensitiveFeaturesDf.count, 3d / 9d, 4d / 9d, 2d / 4d, 3d / 9d, 1d / 3d)

  private lazy val feature: String = "Gender"
  private lazy val val1: String = "Male"
  private lazy val val2: String = "Female"

  private lazy val actual: Map[String, Double] =
    new AssociationGaps()
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

  test(s"AssociationGaps can calculate Demographic Parity for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("dp")) == abs(expectedGenderMaleFemale.dpGap))
  }

  test(s"AssociationGaps can calculate Sorensen-Dice Coefficient for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("sdc")) == abs(expectedGenderMaleFemale.sdcGap))
  }

  test(s"AssociationGaps can calculate Jaccard Index for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("ji")) == abs(expectedGenderMaleFemale.jiGap))
  }

  test(s"AssociationGaps can calculate Log-Likelihood Ratio for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("llr")) == abs(expectedGenderMaleFemale.llrGap))
  }

  test(s"AssociationGaps can calculate PMI (Pointwise Mutual Information) for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("pmi")) == abs(expectedGenderMaleFemale.pmiGap))
  }

  test(s"AssociationGaps can calculate Normalized PMI, p(y) normalization for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("n_pmi_y")) == abs(expectedGenderMaleFemale.nPmiYGap))
  }

  test(s"AssociationGaps can calculate Normalized PMI, p(x,y) normalization for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("n_pmi_xy")) == abs(expectedGenderMaleFemale.nPmiXYGap))
  }

  test(s"AssociationGaps can calculate Squared PMI for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("s_pmi")) == abs(expectedGenderMaleFemale.sPmiGap))
  }

  test(s"AssociationGaps can calculate Kendall Rank Correlation for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("krc")) == abs(expectedGenderMaleFemale.krcGap))
  }

  test(s"AssociationGaps can calculate t-test for $feature=$val1 vs. $feature=$val2") {
    assert(abs(actual("t_test")) == abs(expectedGenderMaleFemale.tTestGap))
  }
}
