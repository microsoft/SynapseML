package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

class VectorSHAP(override val uid: String)
  extends KernelSHAPBase(uid)
    with HasInputCol
    with HasBackgroundData {

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
        StructField("sample", SQLDataTypes.VectorType),
        StructField("coalition", SQLDataTypes.VectorType)
      ))
    )

    ???
  }

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    require(
      schema(getInputCol).dataType == SQLDataTypes.VectorType,
      s"Field $getInputCol from input must be Vector type, but got ${schema(getInputCol).dataType} instead."
    )

    if (backgroundData.isDefined) {
      val dataType = backgroundData.get.schema(getInputCol).dataType

      require(
        dataType == SQLDataTypes.VectorType,
        s"Field $getInputCol from background dataset must be Vector type, but got ${dataType} instead."
      )
    }
  }
}
