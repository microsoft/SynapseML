package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TabularSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCols
    with HasBackgroundData {

  def this() = {
    this(Identifiable.randomUID("TabularSHAP"))
  }

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  private def getEffectiveNumSamples(numFeature: Int): Int = {
    val maxSamplesNeeded = math.pow(2, numFeature)
    math.min(this.getNumSamplesOpt.getOrElse(2 * numFeature + 2048), maxSamplesNeeded).toInt
  }

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    val instances = df.select(col(idCol), struct(getInputCols.map(col): _*).alias("instance"))
    val background = this.backgroundData.getOrElse(instances)
      .select(struct(getInputCols.map(col): _*).alias("background"))

    val featureSize = this.getInputCols.length

    val effectiveNumSamples = this.getEffectiveNumSamples(featureSize)

    // println(s"effectiveNumSamples: $effectiveNumSamples")

    val sampleType = StructType(this.getInputCols.map {
      feature =>
        df.schema.fields.find(_.name == feature).getOrElse {
          throw new Exception(s"Column $feature not found in schema ${df.schema.simpleString}")
        }
    })

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", sampleType),
        StructField("coalition", SQLDataTypes.VectorType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: Row, background: Row) =>
          val sampler = new KernelSHAPTabularSampler(background, effectiveNumSamples)
          (1 to effectiveNumSamples) map {
            _ =>
              implicit val randBasis: RandBasis = RandBasis.mt0
              sampler.sample(instance)
          } map {
            case (sample, state, _) => (sample, state)
          }
      },
      returnDataType
    )

    instances.crossJoin(background)
      .withColumn("samples", explode(samplesUdf(col("instance"), col("background"))))
      .select(
        col(idCol),
        col("samples.coalition").alias(coalitionCol),
        col("samples.sample.*")
      )
  }
}
