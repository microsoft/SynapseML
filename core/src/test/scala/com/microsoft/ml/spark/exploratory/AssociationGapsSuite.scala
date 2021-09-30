package com.microsoft.ml.spark.exploratory

import org.apache.spark.sql.functions._

import scala.math.abs

class AssociationGapsSuite extends DataImbalanceTestBase {

  import spark.implicits._

  private lazy val associationGapSimulated: AssociationGaps = {
    spark
    new AssociationGaps()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setLabelCol("Label")
      .setVerbose(true)
  }

  test("AssociationGaps can calculate Association Gaps end-to-end") {
    val associationGaps = associationGapSimulated.transform(sensitiveFeaturesDf)
    associationGaps.show(truncate = false)
    associationGaps.printSchema()
  }

  private lazy val expectedGenderMaleFemale: GapCalculator =
    GapCalculator(sensitiveFeaturesDf.count, 3d / 9d, 4d / 9d, 2d / 4d, 3d / 9d, 1d / 3d)

  private lazy val actualGenderMaleFemale: Map[String, Double] = {
    spark
    new AssociationGaps()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setLabelCol("Label")
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .filter(
        (col("ClassA") === "Male" && col("ClassB") === "Female") ||
          (col("ClassB") === "Female" && col("ClassB") === "Male")
      )
      .as[(String, String, String, Map[String, Double])]
      .collect()(0)._4
  }

  test("AssociationGaps can calculate Demographic Parity for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("dp")) == abs(expectedGenderMaleFemale.dpGap))
  }

  test("AssociationGaps can calculate Sorensen-Dice Coefficient for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("sdc")) == abs(expectedGenderMaleFemale.sdcGap))
  }

  test("AssociationGaps can calculate Jaccard Index for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("ji")) == abs(expectedGenderMaleFemale.jiGap))
  }

  test("AssociationGaps can calculate Log-Likelihood Ratio for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("llr")) == abs(expectedGenderMaleFemale.llrGap))
  }

  test("AssociationGaps can calculate PMI (Pointwise Mutual Information) for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("pmi")) == abs(expectedGenderMaleFemale.pmiGap))
  }

  test("AssociationGaps can calculate Normalized PMI, p(y) normalization for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("n_pmi_y")) == abs(expectedGenderMaleFemale.nPmiYGap))
  }

  test("AssociationGaps can calculate Normalized PMI, p(x,y) normalization for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("n_pmi_xy")) == abs(expectedGenderMaleFemale.nPmiXYGap))
  }

  test("AssociationGaps can calculate Squared PMI for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("s_pmi")) == abs(expectedGenderMaleFemale.sPmiGap))
  }

  test("AssociationGaps can calculate Kendall Rank Correlation for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("krc")) == abs(expectedGenderMaleFemale.krcGap))
  }

  test("AssociationGaps can calculate t-test for 1 sensitive feature") {
    assert(abs(actualGenderMaleFemale("t_test")) == abs(expectedGenderMaleFemale.tTestGap))
  }
}
