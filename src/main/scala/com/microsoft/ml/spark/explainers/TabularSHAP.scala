package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TabularSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCols
    with HasBackgroundData {

  logClass()
  
  def this() = {
    this(Identifiable.randomUID("TabularSHAP"))
  }

  def setInputCols(values: Array[String]): this.type = this.set(inputCols, values)

  override protected def createSamples(df: DataFrame, idCol: String, coalitionCol: String): DataFrame = {
    val instances = df.select(col(idCol), struct(getInputCols.map(col): _*).alias("instance"))
    val background = this.backgroundData.getOrElse(df)
      .select(struct(getInputCols.map(col): _*).alias("background"))

    val featureSize = this.getInputCols.length

    val effectiveNumSamples = KernelSHAPBase.getEffectiveNumSamples(this.getNumSamplesOpt, featureSize)

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
        StructField("coalition", VectorType)
      ))
    )

    val samplesUdf = UDFUtils.oldUdf(
      {
        (instance: Row, background: Row) =>
          val sampler = new KernelSHAPTabularSampler(instance, background, effectiveNumSamples)
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
        col("samples.sample.*")
      )
  }

  override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    if (backgroundData.isDefined) {
      val bgDf = backgroundData.get

      this.getInputCols.foreach {
        col =>
          val inputField: StructField = schema(col)
          val backgroundField = bgDf.schema(col)

          require(
            DataType.equalsStructurally(inputField.dataType, backgroundField.dataType, ignoreNullability = true),
            s"Field $col has type ${inputField.dataType} from input instance, but type ${backgroundField.dataType} " +
              s"from background dataset."
          )
      }
    }
  }
}
