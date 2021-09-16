package com.microsoft.ml.spark.exploratory

import scala.math.{abs, log, pow, sqrt}
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AssociationGapTransformerSuite extends TestBase {

  import spark.implicits._

  private lazy val testDfGenderEthnicity: DataFrame = Seq(
    (0, "FEMALE", "Asian", 3035, 20657),
    (1, "OTHER", "Other", 1065, 30909),
    (0, "FEMALE", "White", 1056, 10901),
    (0, "MALE", "White", 6003, 60390),
    (0, "MALE", "Hispanic", 5092, 10612),
    (0, "FEMALE", "Asian", 2038, 20521),
    (0, "OTHER", "Black", 2037, 10817),
    (0, "MALE", "Asian", 1084, 20056),
    (0, "FEMALE", "Black", 5048, 10626),
    (1, "FEMALE", "White", 2045, 30429),
    (0, "OTHER", "White", 2063, 30860)
  ) toDF("Label", "Gender", "Ethnicity", "Usage", "Income")

  private lazy val associationGapGenderEthnicity: AssociationGapTransformer = {
    spark
    new AssociationGapTransformer()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setLabelCol("Label")
      .setVerbose(true)
  }

  test("AssociationGapTransformer can calculate Association Gaps for 2 sensitive columns: Gender and Ethnicity") {
    val associationMeasures = associationGapGenderEthnicity.transform(testDfGenderEthnicity)

    associationMeasures.show(truncate = false)

    associationMeasures.printSchema()
  }

  case class GapCalculator(numRows: Double, pY: Double, pX1: Double, pX1andY: Double, pX2: Double, pX2andY: Double) {
    val pYgivenX1: Double = pX1andY / pX1
    val pX1givenY: Double = pX1andY / pY
    val pYgivenX2: Double = pX2andY / pX2
    val pX2givenY: Double = pX2andY / pY

    val dpGap: Double = pYgivenX1 - pYgivenX2
    val sdcGap: Double = pX1andY / (pX1 + pY) - pX2andY / (pX2 + pY)
    val jiGap: Double = pX1andY / (pX1 + pY - pX1andY) - pX2andY / (pX2 + pY - pX2andY)
    val llrGap: Double = log(pX1givenY) - log(pX2givenY)
    val pmiGap: Double = log(pYgivenX1) - log(pYgivenX2)
    val nPmiYGap: Double = log(pYgivenX1) / log(pY) - log(pYgivenX2) / log(pY)
    val nPmiXYGap: Double = log(pYgivenX1) / log(pX1andY) - log(pYgivenX2) / log(pX2andY)
    val sPmiGap: Double = log(pow(pX1andY, 2) / (pX1 * pY)) - log(pow(pX2andY, 2) / (pX2 * pY))
    val krcGap: Double = {
      val aX1 = pow(numRows, 2) * (1 - 2 * pX1 - 2 * pY + 2 * pX1andY + 2 * pX1 * pY)
      val bX1 = numRows * (2 * pX1 + 2 * pY - 4 * pX1andY - 1)
      val cX1 = pow(numRows, 2) * sqrt((pX1 - pow(pX1, 2)) * (pY - pow(pY, 2)))

      val aX2 = pow(numRows, 2) * (1 - 2 * pX2 - 2 * pY + 2 * pX2andY + 2 * pX2 * pY)
      val bX2 = numRows * (2 * pX2 + 2 * pY - 4 * pX2andY - 1)
      val cX2 = pow(numRows, 2) * sqrt((pX2 - pow(pX2, 2)) * (pY - pow(pY, 2)))

      (aX1 + bX1) / cX1 - (aX2 + bX2) / cX2
    }
    val tTestGap: Double = (pX1andY - pX1 * pY) / sqrt(pX1 * pY) - (pX2andY - pX2 * pY) / sqrt(pX2 * pY)
  }

  private lazy val testDfGender: DataFrame = Seq(
    (0, "Male"),
    (0, "Male"),
    (1, "Male"),
    (1, "Male"),
    (0, "Female"),
    (0, "Female"),
    (1, "Female"),
    (0, "Other"),
    (0, "Other")
  ) toDF("Label", "Gender")

  private lazy val expectedGenderMaleFemale: GapCalculator =
    GapCalculator(testDfGender.count, 3d/9d, 4d/9d, 2d/4d, 3d/9d, 1d/3d)

  private lazy val actualGenderMaleFemale: Map[String, Double] = {
    spark
    new AssociationGapTransformer()
      .setSensitiveCols(Array("Gender"))
      .setLabelCol("Label")
      .setVerbose(true)
      .transform(testDfGender)
      .filter(
        (col("ClassA") === "Male" && col("ClassB") === "Female") ||
          (col("ClassB") === "Female" && col("ClassB") === "Male")
      )
      .as[(String, String, String, Map[String, Double])]
      .collect()(0)._4
  }

  test("AssociationGapTransformer can calculate Demographic Parity") {
    assert(abs(actualGenderMaleFemale("dp")) == abs(expectedGenderMaleFemale.dpGap))
  }

  test("AssociationGapTransformer can calculate Sorensen-Dice Coefficient") {
    assert(abs(actualGenderMaleFemale("sdc")) == abs(expectedGenderMaleFemale.sdcGap))
  }

  test("AssociationGapTransformer can calculate Jaccard Index") {
    assert(abs(actualGenderMaleFemale("ji")) == abs(expectedGenderMaleFemale.jiGap))
  }

  test("AssociationGapTransformer can calculate Log-Likelihood Ratio") {
    assert(abs(actualGenderMaleFemale("llr")) == abs(expectedGenderMaleFemale.llrGap))
  }

  test("AssociationGapTransformer can calculate Pointwise Mutual Information") {
    assert(abs(actualGenderMaleFemale("pmi")) == abs(expectedGenderMaleFemale.pmiGap))
  }

  test("AssociationGapTransformer can calculate Normalized Pointwise Mutual Information, p(y) normalization") {
    assert(abs(actualGenderMaleFemale("n_pmi_y")) == abs(expectedGenderMaleFemale.nPmiYGap))
  }

  test("AssociationGapTransformer can calculate Normalized Pointwise Mutual Information, p(x,y) normalization") {
    assert(abs(actualGenderMaleFemale("n_pmi_xy")) == abs(expectedGenderMaleFemale.nPmiXYGap))
  }

  test("AssociationGapTransformer can calculate Squared Pointwise Mutual Information") {
    assert(abs(actualGenderMaleFemale("s_pmi")) == abs(expectedGenderMaleFemale.sPmiGap))
  }

  test("AssociationGapTransformer can calculate Kendall Rank Correlation") {
    assert(abs(actualGenderMaleFemale("krc")) == abs(expectedGenderMaleFemale.krcGap))
  }

  test("AssociationGapTransformer can calculate t-test") {
    assert(abs(actualGenderMaleFemale("t_test")) == abs(expectedGenderMaleFemale.tTestGap))
  }
}
