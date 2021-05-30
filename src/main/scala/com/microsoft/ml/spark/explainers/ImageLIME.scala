package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime.{HasCellSize, HasModifier, SuperpixelData, SuperpixelTransformer}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

trait ImageLIMEParams extends LIMEParams with HasCellSize with HasModifier with HasSamplingFraction with HasInputCol {
  self: ImageLIME =>

  val superpixelCol = new Param[String](
    this,
    "superpixelCol",
    "The column holding the superpixel decompositions"
  )

  def getSuperpixelCol: String = $(superpixelCol)

  def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(numSamples -> 900, cellSize -> 16, modifier -> 130, regularization -> 0.0, samplingFraction -> 0.7,
    superpixelCol -> "superpixels")
}

class ImageLIME(override val uid: String)
  extends LIMEBase(uid) with ImageLIMEParams {

  def this() = {
    this(Identifiable.randomUID("ImageLIME"))
  }

  override protected def preprocess(df: DataFrame): DataFrame = {
    // Dataframe with new column containing superpixels (Array[Cluster]) for each row (image to explain)
    new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(getSuperpixelCol)
      .transform(df)
  }

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       featureCol: String,
                                       distanceCol: String
                                      ): DataFrame = {

    val (numSamples, samplingFraction) = (this.getNumSamples, this.getSamplingFraction)

    val samplesUdf = UDFUtils.oldUdf(
      {
        (image: Row, sp: Row) =>
          val bi = ImageUtils.toBufferedImage(image)
          val spd = SuperpixelData.fromRow(sp)

          implicit val randBasis: RandBasis = RandBasis.mt0
          val sampler = ImageFeature(samplingFraction, spd)
          (1 to numSamples).map {
            _ =>
              val (outputImage, feature, distance) = sampler.sample(bi)
              val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(outputImage)
              val imageFormat = ImageFormat(path, height, width, nChannels, mode, decoded)
              (imageFormat, feature, distance)
          }
      },
      getSampleSchema
    )

    df.withColumn("samples", explode(samplesUdf(col(getInputCol), col(getSuperpixelCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.feature").alias(featureCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  private def getSampleSchema: DataType = {
    ArrayType(
      StructType(Seq(
        StructField("sample", ImageSchema.columnSchema),
        StructField("feature", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )
  }

  override protected def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    // TODO: support binary input schema

    require(
      ImageSchemaUtils.isImage(schema(getInputCol).dataType),
      s"Field $getInputCol is expected to be image type, but got ${schema(getInputCol).dataType} instead."
    )
  }
}
