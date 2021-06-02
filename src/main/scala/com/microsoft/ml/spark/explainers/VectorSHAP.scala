package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

class VectorSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCol
    with HasBackgroundData {

  logClass()

  def this() = {
    this(Identifiable.randomUID("VectorSHAP"))
  }

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    val instances = df.select(col(idCol), col(getInputCol).alias("instance"))
    val background = this.backgroundData.getOrElse(instances)
      .select(col(getInputCol).alias("background"))

    val numSampleOpt = this.getNumSamplesOpt

    val returnDataType = ArrayType(
      StructType(Seq(
        StructField("sample", VectorType),
        StructField("coalition", VectorType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: SV, background: SV) =>
          val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(numSampleOpt, instance.size)
          val sampler = new KernelSHAPVectorSampler(instance, background, effectiveNumSamples)
          (1 to effectiveNumSamples) map {
            _ =>
              implicit val randBasis: RandBasis = RandBasis.mt0
              sampler.sample
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
        col("samples.sample").alias(getInputCol)
      )
  }

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == VectorType,
      s"Field $getInputCol from input must be Vector type, but got ${schema(getInputCol).dataType} instead."
    )

    if (backgroundData.isDefined) {
      val dataType = backgroundData.get.schema(getInputCol).dataType

      require(
        dataType == VectorType,
        s"Field $getInputCol from background dataset must be Vector type, but got $dataType instead."
      )
    }
  }
}
