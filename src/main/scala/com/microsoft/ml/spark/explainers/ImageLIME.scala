package com.microsoft.ml.spark.explainers

import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.schema.{DatasetExtensions, ImageSchemaUtils}
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime.{HasCellSize, HasModifier, SuperpixelData, SuperpixelTransformer}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

trait ImageLIMEParams extends LIMEParams with HasCellSize with HasModifier with HasSamplingFraction with HasInputCol {
  self: ImageLIME =>

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(numSamples -> 900, cellSize -> 16, modifier -> 130, regularization -> 0.0, samplingFraction -> 0.7)
}

class ImageLIME(override val uid: String)
  extends LIMEBase(uid) with ImageLIMEParams {

  def this() = {
    this(Identifiable.randomUID("ImageLIME"))
  }

  override protected def createSamples(df: DataFrame,
                                       idCol: String,
                                       featureCol: String,
                                       distanceCol: String
                                      ): DataFrame = {

    val numSamples = this.getNumSamples
    val superpixelsCol = DatasetExtensions.findUnusedColumnName("superpixels", df)

    // Dataframe with new column containing superpixels (Array[Cluster]) for each row (image to explain)
    val spDF = new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(superpixelsCol)
      .transform(df)

    val samplingFraction = this.getSamplingFraction

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

    spDF.withColumn("samples", explode(samplesUdf(col(getInputCol), col(superpixelsCol))))
      .select(
        col(idCol),
        col("samples.distance").alias(distanceCol),
        col("samples.feature").alias(featureCol),
        col("samples.sample").alias(getInputCol)
      )
  }

  private def getSampleSchema: DataType = {
    val sampleType = ArrayType(
      StructType(Seq(
        StructField("sample", ImageSchema.columnSchema),
        StructField("feature", SQLDataTypes.VectorType),
        StructField("distance", DoubleType)
      ))
    )
    sampleType
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
