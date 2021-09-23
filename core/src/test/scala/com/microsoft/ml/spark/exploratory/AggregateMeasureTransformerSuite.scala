package com.microsoft.ml.spark.exploratory

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.DoubleType

import scala.math.{abs, log, pow}

class AggregateMeasureTransformerSuite extends TestBase {

  import spark.implicits._

  private lazy val testDfSimulated: DataFrame = Seq(
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

  private lazy val aggregateMeasureSimulated: AggregateMeasureTransformer = {
    spark
    new AggregateMeasureTransformer()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setVerbose(true)
  }

  test("AggregateMeasureTransformer can calculate Aggregate Measures for 2 sensitive columns") {
    val aggregateMeasures = aggregateMeasureSimulated.transform(testDfSimulated)
    aggregateMeasures.show(truncate = false)
    aggregateMeasures.printSchema()
  }

  case class AggregateMeasureCalculator(benefits: Array[Double], epsilon: Double, errorTolerance: Double) {
    val numBenefits: Double = benefits.length
    val meanBenefits: Double = benefits.sum / numBenefits
    val normBenefits: Array[Double] = benefits.map(_ / meanBenefits)

    val atkinsonIndex: Double = {
      val alpha = 1d - epsilon
      if (abs(alpha) < errorTolerance) {
        1d - pow(normBenefits.product, 1d / numBenefits)
      } else {
        val powerMean = normBenefits.map(pow(_, alpha)).sum / numBenefits
        1d - pow(powerMean, 1d / alpha)
      }
    }
    val theilLIndex: Double = generalizedEntropyIndex(0d)
    val theilTIndex: Double = generalizedEntropyIndex(1d)

    def generalizedEntropyIndex(alpha: Double): Double = {
      if (abs(alpha - 1d) < errorTolerance) {
        normBenefits.map(x => x * log(x)).sum / numBenefits
      } else if (abs(alpha) < errorTolerance) {
        normBenefits.map(-1 * log(_)).sum / numBenefits
      } else {
        normBenefits.map(pow(_, alpha) - 1d).sum / (numBenefits * alpha * (alpha - 1d))
      }
    }
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

  private lazy val genderBenefits = testDfGender
    .groupBy("Gender")
    .agg(count("*").cast(DoubleType).alias("countSensitive"))
    .withColumn("countAll", lit(testDfGender.count.toDouble))
    .withColumn("countSensitiveProb", col("countSensitive") / col("countAll"))
    .select("countSensitiveProb").as[Double].collect()

  private lazy val expectedGenderDefaults = AggregateMeasureCalculator(genderBenefits, 1d, 1e-12)

  private lazy val expectedGenderNondefaultEpsilon = AggregateMeasureCalculator(genderBenefits, 0.9, 1e-12)

  private lazy val actualGenderDefaults: Map[String, Double] = {
    spark
    new AggregateMeasureTransformer()
      .setSensitiveCols(Array("Gender"))
      .setVerbose(true)
      .transform(testDfGender)
      .as[Map[String, Double]]
      .collect()(0)
  }

  private lazy val actualGenderNondefaultEpsilon: Map[String, Double] = {
    spark
    new AggregateMeasureTransformer()
      .setSensitiveCols(Array("Gender"))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(testDfGender)
      .as[Map[String, Double]]
      .collect()(0)
  }

  test("AggregateMeasureTransformer can calculate Atkinson Index for Default Epsilon (1.0)") {
    assert(actualGenderDefaults("atkinson_index") == expectedGenderDefaults.atkinsonIndex)
  }

  test("AggregateMeasureTransformer can calculate Atkinson Index for Nondefault Epsilon (0.9)") {
    assert(actualGenderNondefaultEpsilon("atkinson_index") == expectedGenderNondefaultEpsilon.atkinsonIndex)
  }

  test("AggregateMeasureTransformer can calculate Theil L Index") {
    assert(actualGenderDefaults("theil_l_index") == expectedGenderDefaults.theilLIndex)
  }

  test("AggregateMeasureTransformer can calculate Theil T Index") {
    assert(actualGenderDefaults("theil_t_index") == expectedGenderDefaults.theilTIndex)
  }
}
