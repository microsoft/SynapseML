
package com.microsoft.ml.spark

import java.awt.Color
import java.awt.color.ColorSpace
import java.awt.image.BufferedImage

import com.microsoft.ml.spark.schema.{BinaryFileSchema, ImageSchema}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.image.{ImageSchema => SparkImageSchema}
import org.apache.spark.ml.param.{ArrayMapParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ListBuffer

abstract class ImageNativeTransformerStage(params: Map[String, Any]) extends Serializable {
  def apply(image: BufferedImage): BufferedImage
  val stageName: String
}

private class ResizeImageNative(params: Map[String, Any]) extends ImageNativeTransformerStage(params) {
  val height: Int = params(ResizeImageNative.height).asInstanceOf[Int]
  val width: Int = params(ResizeImageNative.width).asInstanceOf[Int]
  val numberOfChannels: Int = params(ResizeImageNative.numberOfChannels).asInstanceOf[Int]
  override val stageName: String = ResizeImageNative.stageName

  override def apply(image: BufferedImage): BufferedImage = {
    val resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    val bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    bufferedImage.getGraphics.drawImage(resizedImage, 0, 0, null)
    resizedImage
  }
}

/** Resize object contains the information for resizing;
  * "height"
  * "width"
  * "stageName" = "resize"
  * "numberOfChannels"
  */
private object ResizeImageNative {
  val stageName = "resize"
  val height = "height"
  val width = "width"
  val numberOfChannels = "numberOfChannels"
}


/** Pipelined image processing. */
object ImageNativeTransformer extends DefaultParamsReadable[ImageNativeTransformer] {

  override def load(path: String): ImageNativeTransformer = super.load(path)

  /** Returns the OCV type (int) of the passed-in image */
  private def getOCVType(img: BufferedImage): Int = {
    val isGray = img.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = img.getColorModel.hasAlpha
    if (isGray) {
      SparkImageSchema.ocvTypes("CV_8UC1")
    } else if (hasAlpha) {
      SparkImageSchema.ocvTypes("CV_8UC4")
    } else {
      SparkImageSchema.ocvTypes("CV_8UC3")
    }
  }

  /**
    * Takes a Java BufferedImage and returns a Row Image (spImage).
    *
    * @param image Java BufferedImage.
    * @return Row image in spark.ml.image format with channels in BGR(A) order.
    */
  private def toSparkImage(image: BufferedImage, origin: String = null): Row = {
    val nChannels = image.getColorModel.getNumComponents
    val height = image.getHeight
    val width = image.getWidth
    val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val hasAlpha = image.getColorModel.hasAlpha
    val decoded = new Array[Byte](height * width * nChannels)

    // The logic below is copied from ImageSchema's decode method, see
    // https://github.com/apache/spark/blob/7bd46d987156/mllib/src/main/scala/org/apache/spark/ml/image/ImageSchema.scala#L134
    if (isGray) {
      var offset = 0
      val raster = image.getRaster
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          decoded(offset) = raster.getSample(w, h, 0).toByte
          offset += 1
        }
      }
    } else {
      var offset = 0
      for (h <- 0 until height) {
        for (w <- 0 until width) {
          val color = new Color(image.getRGB(w, h), hasAlpha)
          decoded(offset) = color.getBlue.toByte
          decoded(offset + 1) = color.getGreen.toByte
          decoded(offset + 2) = color.getRed.toByte
          if (hasAlpha) {
            decoded(offset + 3) = color.getAlpha.toByte
          }
          offset += nChannels
        }
      }
    }
    Row(origin, height, width, nChannels, getOCVType(image), decoded)
  }

  def process(stages: Seq[ImageNativeTransformerStage], decode: Boolean)(row: Row): Option[Row] = {

    if (row == null) return None

    var bufferedImage  = ImageSchema.toBufferedImage(row)
    for (stage <- stages) {
      bufferedImage = stage.apply(bufferedImage)
    }
    Some(toSparkImage(bufferedImage))
  }
}


@InternalWrapper
class ImageNativeTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with MMLParams {

  import com.microsoft.ml.spark.ImageNativeTransformer._

  def this() = this(Identifiable.randomUID("ImageNativeTransformer"))

  val stages: ArrayMapParam = new ArrayMapParam(this, "stages", "Image transformation stages")

  def setStages(value: Array[Map[String, Any]]): this.type = set(stages, value)

  val emptyStages: Array[Map[String, Any]] = Array[Map[String, Any]]()

  def getStages: Array[Map[String, Any]] = if (isDefined(stages)) $(stages) else emptyStages

  private def addStage(stage: Map[String, Any]): this.type = set(stages, getStages :+ stage)

  setDefault(inputCol -> "image", outputCol -> (uid + "_output"))

  // every stage has a name like "resize", "normalize", "unroll"
  val stageName = "action"

  def resize(height: Int, width: Int): this.type = {
    require(width >= 0 && height >= 0, "width and height should be nonnegative")

    addStage(Map(stageName -> ResizeImageNative.stageName,
      ResizeImageNative.width -> width,
      ResizeImageNative.height -> height,
      ResizeImageNative.numberOfChannels -> 3))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    //  load native OpenCV library on each partition
    // TODO: figure out more elegant way
    val spark = dataset.sqlContext

    val schema = dataset.toDF.schema

    val df = dataset.toDF

    val isBinary = BinaryFileSchema.isBinaryFile(df, $(inputCol))
    assert(ImageSchema.isImage(df, $(inputCol)) || isBinary,
      "input column should have Image or BinaryFile type")

    val transforms = ListBuffer[ImageNativeTransformerStage]()
    for (stage <- getStages) {
      stage(stageName) match  {
        case ResizeImageNative.stageName    => transforms += new ResizeImageNative(stage)
        case unsupported: String => throw new IllegalArgumentException(s"unsupported transformation $unsupported")
      }
    }

    val func = process(transforms, decode = isBinary)(_)
    val convert = udf(func, ImageSchema.columnSchema)

    df.withColumn($(outputCol), convert(df($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), ImageSchema.columnSchema)
  }
}