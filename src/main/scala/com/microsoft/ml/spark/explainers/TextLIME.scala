package com.microsoft.ml.spark.explainers


import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}

trait TextLIMEParams extends LIMEParams with HasSamplingFraction with HasInputCol {
  self: TextLIME =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  val tokenCol = new Param[String](this,
    "tokenCol", "The column holding the token")

  def getTokenCol: String = $(tokenCol)

  def setTokenCol(v: String): this.type = set(tokenCol, v)

  setDefault(numSamples -> 1000, regularization -> 0.0, samplingFraction -> 0.7)
}

class TextLIME(override val uid: String)
  extends LIMEBase(uid) with TextLIMEParams {
  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       featureCol: String,
                                       distanceCol: String
                                      ): DataFrame = {
    val numSamples = this.getNumSamples
    val tokenizer = new Tokenizer().setInputCol(getInputCol).setOutputCol(getTokenCol)
    val tokenDF = tokenizer.transform(df)

    val sampleType = ArrayType(
      StructType(Seq(
        StructField("sample", StringType),
        StructField("feature", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )

???
  }
}
