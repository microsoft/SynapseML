package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector => SV}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class VectorLIME(override val uid: String)
  extends LIMEBase(uid) with HasInputCol {

  def this() = {
    this(Identifiable.randomUID("VectorLIME"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       featureCol: String,
                                       distanceCol: String): DataFrame = {
    val numSamples = this.getNumSamples

    val featureStats = this.createFeatureStats(this.backgroundData.getOrElse(df))

    val sampler = new LIMEVectorSampler(featureStats)

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", SQLDataTypes.VectorType),
        StructField("feature", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        vector: SV =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          (1 to numSamples).map(_ => sampler.sample(vector))
      },
      returnDataType
    )

    df.withColumn("samples", explode(samplesUdf(col(getInputCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.feature").alias(featureCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  private def createFeatureStats(df: DataFrame): Seq[FeatureStats[Double, Double]] = {
    val Row(std: SV) = df
      .select(Summarizer.metrics("std").summary(col($(inputCol))).as("summary"))
      .select("summary.std")
      .first()

    std.toArray.zipWithIndex.map {
      case (v, i) =>
        ContinuousFeatureStats(i, v)
    }
  }

  override protected def validateInputSchema(schema: StructType): Unit = {
    super.validateInputSchema(schema)
    require(
      schema(getInputCol).dataType == SQLDataTypes.VectorType,
      s"Field $getInputCol is expected to be vector type, but got ${schema(getInputCol).dataType} instead."
    )
  }
}
