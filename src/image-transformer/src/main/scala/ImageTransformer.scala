// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.param._
import com.microsoft.ml.spark.schema.ImageSchema
import scala.collection.mutable.ListBuffer
import com.microsoft.ml.spark.schema.BinaryFileSchema
import scala.collection.mutable.{ListBuffer, WrappedArray}
import org.opencv.core.Core
import org.opencv.core.Mat
import org.opencv.core.{Rect, Size}
import org.opencv.imgproc.Imgproc
import org.apache.spark.ml.util.Identifiable

abstract class ImageTransformerStage(params: Map[String, Any]) extends Serializable {
  def apply(image: Mat): Mat
  val stageName: String
}

class ResizeImage(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val height = params(ResizeImage.height).asInstanceOf[Int].toDouble
  val width = params(ResizeImage.width).asInstanceOf[Int].toDouble
  override val stageName = ResizeImage.stageName

  override def apply(image: Mat): Mat = {
    var resized = new Mat()
    val sz = new Size(width, height)
    Imgproc.resize(image, resized, sz)
    resized
  }
}

object ResizeImage {
  val stageName = "resize"
  val height = "height"
  val width = "width"
}

class CropImage(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val x = params(CropImage.x).asInstanceOf[Int]
  val y = params(CropImage.y).asInstanceOf[Int]
  val height = params(CropImage.height).asInstanceOf[Int]
  val width = params(CropImage.width).asInstanceOf[Int]
  override val stageName = CropImage.stageName

  override def apply(image: Mat): Mat = {
    val rect = new Rect(x, y, width, height)
    new Mat(image, rect)
  }
}

object CropImage {
  val stageName = "crop"
  val x = "x"
  val y = "y"
  val height = "height"
  val width = "width"
}

/**
  * Applies a color format to the image, eg COLOR_BGR2GRAY.
  */
class ColorFormat(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val format = params(ColorFormat.format).asInstanceOf[Int]
  override val stageName = ColorFormat.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Imgproc.cvtColor(image, dst, format)
    dst
  }
}

object ColorFormat {
  val stageName = "colorformat"
  val format = "format"
}

/**
  * Blurs the image
  * @param params
  */
class Blur(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val height = params(Blur.height).asInstanceOf[Double]
  val width = params(Blur.width).asInstanceOf[Double]
  override val stageName = Blur.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Imgproc.blur(image, dst, new Size(height, width))
    dst
  }
}

object Blur {
  val stageName = "blur"
  val height = "height"
  val width = "width"
}

/**
  * Applies a threshold to the image
  * @param params
  */
class Threshold(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val threshold = params(Threshold.threshold).asInstanceOf[Double]
  val maxVal = params(Threshold.maxVal).asInstanceOf[Double]
  // EG Imgproc.THRESH_BINARY
  val thresholdType = params(Threshold.thresholdType).asInstanceOf[Int]
  override val stageName = Threshold.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Imgproc.threshold(image, dst, threshold, maxVal, thresholdType)
    dst
  }
}

object Threshold {
  val stageName = "threshold"
  val threshold = "threshold"
  val maxVal = "maxVal"
  val thresholdType = "type"
}

/**
  * Applies gaussian kernel to the image
  */
class GaussianKernel(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val appertureSize = params(GaussianKernel.appertureSize).asInstanceOf[Int]
  val sigma = params(GaussianKernel.sigma).asInstanceOf[Double]
  override val stageName = GaussianKernel.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    val kernel = Imgproc.getGaussianKernel(appertureSize, sigma)
    Imgproc.filter2D(image, dst, -1, kernel)
    dst
  }
}

object GaussianKernel {
  val stageName = "gaussiankernel"
  val appertureSize = "appertureSize"
  val sigma = "sigma"
}

/**
  * Pipelined image processing
  */
object ImageTransformer extends DefaultParamsReadable[ImageTransformer] {

  override def load(path: String): ImageTransformer = super.load(path)

  /**
    * Convert Spark image representation to OpenCV format
    */
  private def row2mat(row: Row): (String, Mat) = {
    val path    = ImageSchema.getPath(row)
    val height  = ImageSchema.getHeight(row)
    val width   = ImageSchema.getWidth(row)
    val ocvType = ImageSchema.getType(row)
    val bytes   = ImageSchema.getBytes(row)

    val img = new Mat(height, width, ocvType)
    img.put(0,0,bytes)
    (path, img)
  }

  /**
    *  Convert from OpenCV format to Dataframe Row; unroll if needed
    */
  private def mat2row(img: Mat, path: String = ""): Row = {
    var ocvBytes = new Array[Byte](img.total.toInt*img.elemSize.toInt)
    img.get(0,0,ocvBytes)         //extract OpenCV bytes
    Row(path, img.height, img.width, img.`type`, ocvBytes)
  }

  /**
    * Apply all OpenCV transformation stages to a single image; unroll the result if needed
    * For null inputs or binary files that could not be parsed, return None.
    * Break on OpenCV errors.
    */
  def process(stages: Seq[ImageTransformerStage], decode: Boolean)(row: Row): Option[Row] = {

    if (row == null) return None

    val decoded = if (decode) {
      val path = BinaryFileSchema.getPath(row)
      val bytes = BinaryFileSchema.getBytes(row)

      //early return if the image can't be decompressed
      ImageReader.decode(path, bytes).getOrElse(return None)
    } else row

    var (path, img) = row2mat(decoded)
    for (stage <- stages) {
      img = stage.apply(img)
    }
    Some(mat2row(img, path))
  }
}

@InternalWrapper
class ImageTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with MMLParams {

  import com.microsoft.ml.spark.ImageTransformer._

  def this() = this(Identifiable.randomUID("ImageTransformer"))

  val stages: ArrayMapParam = new ArrayMapParam(this, "stages", "image transformation stages")
  def setStages(value: Array[Map[String, Any]]): this.type = set(stages, value)
  def getStages: Array[Map[String, Any]] = $(stages)
  private def addStage(stage: Map[String, Any]): this.type = set(stages, $(stages) :+ stage)

  setDefault(inputCol -> "image",
    outputCol -> (uid + "_output"),
    stages -> Array[Map[String, Any]]()
  )

  // every stage has a name like "resize", "normalize", "unroll"
  val stageName = "action"

  def resize(height: Int, width: Int): this.type = {
    require(width >= 0 && height >= 0, "width and height should be nonnegative")

    addStage(Map(stageName -> ResizeImage.stageName,
      ResizeImage.width -> width,
      ResizeImage.height -> height))
  }

  def crop(x: Int, y: Int, height: Int, width: Int): this.type = {
    require(x >= 0 && y >= 0 && width >= 0 && height >= 0, "crop values should be nonnegative")

    addStage(Map(stageName -> CropImage.stageName,
      CropImage.width -> width,
      CropImage.height -> height,
      CropImage.x -> x,
      CropImage.y -> y))
  }

  def colorFormat(format: Int): this.type = {
    addStage(Map(stageName -> ColorFormat.stageName, ColorFormat.format -> format))
  }

  def blur(height: Double, width: Double): this.type = {
    addStage(Map(stageName -> Blur.stageName, Blur.height -> height, Blur.width -> width))
  }

  def threshold(threshold: Double, maxVal: Double, thresholdType: Int): this.type = {
    addStage(Map(stageName -> Threshold.stageName,
      Threshold.maxVal -> maxVal,
      Threshold.threshold -> threshold,
      Threshold.thresholdType -> thresholdType))
  }

  def gaussianKernel(appertureSize: Int, sigma: Double): this.type = {
    addStage(Map(stageName -> GaussianKernel.stageName,
      GaussianKernel.appertureSize -> appertureSize,
      GaussianKernel.sigma -> sigma))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    //  load native OpenCV library on each partition
    // TODO: figure out more elegant way
    val spark = dataset.sqlContext

    val schema = dataset.toDF.schema

    val loaded = ImageSchema.loadLibraryForAllPartitions(dataset.toDF.rdd, Core.NATIVE_LIBRARY_NAME)

    val df = spark.createDataFrame(loaded, schema)

    val isBinary = BinaryFileSchema.isBinaryFile(df, $(inputCol))
    assert(ImageSchema.isImage(df, $(inputCol)) || isBinary, "input column should have Image or BinaryFile type")

    var transforms = ListBuffer[ImageTransformerStage]()
    for(stage <- $(stages)) {
      stage(stageName) match  {
        case ResizeImage.stageName => transforms += new ResizeImage(stage)
        case CropImage.stageName => transforms += new CropImage(stage)
        case ColorFormat.stageName => transforms += new ColorFormat(stage)
        case Blur.stageName => transforms += new Blur(stage)
        case Threshold.stageName => transforms += new Threshold(stage)
        case GaussianKernel.stageName => transforms += new GaussianKernel(stage)
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


