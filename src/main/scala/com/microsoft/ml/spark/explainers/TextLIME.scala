package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

trait TextLIMEParams extends LIMEParams with HasSamplingFraction with HasInputCol {
  self: TextLIME =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  val tokensCol = new Param[String](this,
    "tokensCol", "The column holding the tokens")

  def getTokensCol: String = $(tokensCol)

  def setTokensCol(v: String): this.type = this.set(tokensCol, v)

  setDefault(numSamples -> 1000, regularization -> 0.0, samplingFraction -> 0.7, tokensCol -> "tokens")
}

class TextLIME(override val uid: String)
  extends LIMEBase(uid) with TextLIMEParams {
  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       featureCol: String,
                                       distanceCol: String
                                      ): DataFrame = {
    val numSamples = this.getNumSamples

    val tokenDF = new Tokenizer().setInputCol(getInputCol).setOutputCol(getTokensCol).transform(df)

    val samplingFraction = this.getSamplingFraction

    val samplesUdf = UDFUtils.oldUdf(
      {
        (tokens: Seq[String]) =>
          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = TextFeature(samplingFraction)
          (1 to numSamples).map {
            _ =>
              val (sampleTokens, features, distance) = sampler.sample(tokens)
              val sampleText = sampleTokens.mkString(" ")
              (sampleText, features, distance)
          }
      },
      getSampleSchema
    )

    tokenDF.withColumn("samples", explode(samplesUdf(col(getTokensCol))))
      .select(
        col(idCol),
        col(getTokensCol),
        col("samples.distance").alias(distanceCol),
        col("samples.feature").alias(featureCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  private def getSampleSchema: DataType = {
    ArrayType(
      StructType(Seq(
        StructField("sample", StringType),
        StructField("feature", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == StringType,
      s"Field $getInputCol is expected to be string type, but got ${schema(getInputCol).dataType} instead."
    )
  }
}
