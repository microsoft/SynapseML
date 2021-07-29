// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.opencv

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.ml.spark.core.schema.{BinaryFileSchema, ImageSchemaUtils}
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{ImageInjections, Transformer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.opencv.core.{Core, CvType, Mat, Rect, Scalar, Size}
import org.opencv.imgproc.Imgproc

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

//scalastyle:off field.name
/** Image processing stage.
  *
  * @param params Map of parameters
  */
abstract class ImageTransformerStage(params: Map[String, Any]) extends Serializable {
  def apply(image: Mat): Mat

  val stageName: String
}

object ImageTransformerStage {
  // every stage has a name like "resize", "normalize", "unroll"
  val stageNameKey = "action"

  def apply(stage: Map[String, Any]): ImageTransformerStage = {
    stage(stageNameKey) match {
      case ResizeImage.stageName => new ResizeImage(stage)
      case CropImage.stageName => new CropImage(stage)
      case ColorFormat.stageName => new ColorFormat(stage)
      case Blur.stageName => new Blur(stage)
      case Threshold.stageName => new Threshold(stage)
      case GaussianKernel.stageName => new GaussianKernel(stage)
      case Flip.stageName => new Flip(stage)
      case CenterCropImage.stageName => new CenterCropImage(stage)
      case unsupported: String => throw new IllegalArgumentException(s"unsupported transformation $unsupported")
    }
  }
}

/** Resizes the image. The parameters of the ParameterMap are:
  * "height" - the height of the resized image
  * "width" - the width of the resized image
  * "stageName"
  * "size" - the shorter side of the resized image if keep aspect ratio is true, otherwise,
  *          the side length of both height and width.
  * "keepAspectRatio" - if true, then the shorter side will be resized to "size" parameter
  * Please refer to [[http://docs.opencv.org/2.4/modules/imgproc/doc/geometric_transformations.html#resize OpenCV]]
  * for more information
  *
  * @param params ParameterMap of the parameters
  */
class ResizeImage(params: Map[String, Any]) extends ImageTransformerStage(params) {
  override val stageName: String = ResizeImage.stageName

  override def apply(image: Mat): Mat = {
    val resized = new Mat()

    val sz = if (params.isDefinedAt(ResizeImage.size)) {
      val specifiedSize = params(ResizeImage.size).asInstanceOf[Int]
      if (params(ResizeImage.keepAspectRatio).asInstanceOf[Boolean]) {
        val (originalWidth, originalHeight) = (image.width, image.height)
        val shorterSize = math.min(originalWidth, originalHeight)
        val ratio = 1.0 * specifiedSize / shorterSize
        val (targetWidth, targetHeight) = (math.round(ratio * originalWidth), math.round(ratio * originalHeight))
        new Size(targetWidth, targetHeight)
      } else {
        new Size(specifiedSize, specifiedSize)
      }
    } else {
      val height: Double = params(ResizeImage.height).asInstanceOf[Int].toDouble
      val width: Double = params(ResizeImage.width).asInstanceOf[Int].toDouble
      new Size(width, height)
    }

    Imgproc.resize(image, resized, sz)
    resized
  }
}

/** Resize object contains the information for resizing;
  * "height"
  * "width"
  * "stageName" = "resize"
  */
object ResizeImage {
  val stageName = "resize"
  val height = "height"
  val width = "width"
  val size = "size"
  val keepAspectRatio = "keepAspectRatio"
}

/** Crops the image for processing. The parameters are:
  * "x" - First dimension; start of crop
  * "y" - second dimension - start of crop
  * "height" -height of cropped image
  * "width" - width of cropped image
  * "stageName" - "crop"
  *
  * @param params ParameterMap of the dimensions for cropping
  */
class CropImage(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val x: Int = params(CropImage.x).asInstanceOf[Int]
  val y: Int = params(CropImage.y).asInstanceOf[Int]
  val height: Int = params(CropImage.height).asInstanceOf[Int]
  val width: Int = params(CropImage.width).asInstanceOf[Int]
  override val stageName: String = CropImage.stageName

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

class CenterCropImage(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val height: Int = params(CropImage.height).asInstanceOf[Int]
  val width: Int = params(CropImage.width).asInstanceOf[Int]

  override val stageName: String = CenterCropImage.stageName

  override def apply(image: Mat): Mat = {
    val (cropWidth, cropHeight) = (math.min(width, image.width), math.min(height, image.height))
    val (midX, midY) = (image.width / 2, image.height / 2)
    val rect = new Rect(midX - cropWidth / 2, midY - cropHeight / 2, cropWidth, cropHeight)
    new Mat(image, rect)
  }
}

object CenterCropImage {
  val stageName = "centercrop"
  val height = "height"
  val width = "width"
}

/** Converts an image from one color space to another, eg COLOR_BGR2GRAY. Refer to
  * [[http://docs.opencv.org/2.4/modules/imgproc/doc/miscellaneous_transformations.html#cvtcolor OpenCV]]
  * for more information.
  *
  * @param params Map of parameters and values
  */
class ColorFormat(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val format: Int = params(ColorFormat.format).asInstanceOf[Int]
  override val stageName: String = ColorFormat.stageName

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

/** Flips the image
  *
  * @param params Map of parameters and values
  */
class Flip(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val flipCode: Int = params(Flip.flipCode).asInstanceOf[Int]
  override val stageName: String = Flip.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Core.flip(image, dst, flipCode)
    dst
  }
}

object Flip {
  val stageName: String = "flip"
  val flipCode: String = "flipCode"

  val flipUpDown: Int = 0
  val flipLeftRight: Int = 1
  val flipBoth: Int = -1
}

/** Blurs the image using a box filter.
  * The com.microsoft.ml.spark.core.serialize.params are a map of the dimensions of the blurring box. Please refer to
  * [[http://docs.opencv.org/2.4/modules/imgproc/doc/filtering.html#blur OpenCV]] for more information.
  *
  * @param params Map of parameters and values
  */
class Blur(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val height: Double = params(Blur.height).asInstanceOf[Double]
  val width: Double = params(Blur.width).asInstanceOf[Double]
  override val stageName: String = Blur.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Imgproc.blur(image, dst, new Size(height, width))
    dst
  }
}

object Blur {
  val stageName: String = "blur"
  val height: String = "height"
  val width: String = "width"
}

/** Applies a threshold to each element of the image. Please refer to
  * [[http://docs.opencv.org/2.4/modules/imgproc/doc/miscellaneous_transformations.html#threshold threshold]] for
  * more information
  *
  * @param params Map of parameters and values
  */
class Threshold(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val threshold: Double = params(Threshold.threshold).asInstanceOf[Double]
  val maxVal: Double = params(Threshold.maxVal).asInstanceOf[Double]
  // EG Imgproc.THRESH_BINARY
  val thresholdType: Int = params(Threshold.thresholdType).asInstanceOf[Int]
  override val stageName: String = Threshold.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    Imgproc.threshold(image, dst, threshold, maxVal, thresholdType)
    dst
  }
}

object Threshold {
  val stageName: String = "threshold"
  val threshold: String = "threshold"
  val maxVal: String = "maxVal"
  val thresholdType: String = "type"
}

/** Applies gaussian kernel to blur the image. Please refer to
  * [[http://docs.opencv.org/2.4/modules/imgproc/doc/filtering.html#gaussianblur OpenCV]] for detailed information
  * about the parameters and their allowable values.
  *
  * @param params Map of parameter values containg the aperture and sigma for the kernel.
  */
class GaussianKernel(params: Map[String, Any]) extends ImageTransformerStage(params) {
  val appertureSize: Int = params(GaussianKernel.apertureSize).asInstanceOf[Int]
  val sigma: Double = params(GaussianKernel.sigma) match {
    case d: Double => d
    case i: Int => i.toDouble
  }
  override val stageName: String = GaussianKernel.stageName

  override def apply(image: Mat): Mat = {
    val dst = new Mat()
    val kernel = Imgproc.getGaussianKernel(appertureSize, sigma)
    Imgproc.filter2D(image, dst, -1, kernel)
    dst
  }
}

object GaussianKernel {
  val stageName: String = "gaussiankernel"
  val apertureSize: String = "apertureSize"
  val sigma: String = "sigma"
}

/** Pipelined image processing. */
object ImageTransformer extends DefaultParamsReadable[ImageTransformer] {

  override def load(path: String): ImageTransformer = super.load(path)

  /** Convert Spark image representation to OpenCV format. */
  private def row2mat(row: Row): (String, Mat) = {
    val path = ImageSchema.getOrigin(row)
    val height = ImageSchema.getHeight(row)
    val width = ImageSchema.getWidth(row)
    val ocvType = ImageSchema.getMode(row)
    val bytes = ImageSchema.getData(row)

    val img = new Mat(height, width, ocvType)
    img.put(0, 0, bytes)
    (path, img)
  }

  /** Convert from OpenCV format to Dataframe Row; unroll if needed. */
  private def mat2row(img: Mat, path: String = ""): Row = {
    val ocvBytes = new Array[Byte](img.total.toInt * img.elemSize.toInt)
    img.get(0, 0, ocvBytes) //extract OpenCV bytes
    Row(path, img.height, img.width, img.channels(), img.`type`, ocvBytes)
  }

  /**
   * Convert Spark image representation to OpenCV format.
   */
  def decodeImage(decodeMode: String)(r: Any): Option[(String, Mat)] = {
    Option(r).flatMap {
      row =>
        (row, decodeMode) match {
          case (row: Row, "binaryfile") =>
            val path = BinaryFileSchema.getPath(row)
            val bytes = BinaryFileSchema.getBytes(row)
            //early return if the image can't be decompressed
            ImageInjections.decode(path, bytes).map(_.getStruct(0))
          case (bytes: Array[Byte], "binary") =>
            //noinspection ScalaStyle
            ImageInjections.decode(null, bytes).map(_.getStruct(0))
          case (row: Row, "image") =>
            Some(row)
          case (_, mode) =>
            throw new MatchError(s"Unknown decoder mode $mode")
        }
    } map row2mat
  }

  /**
   * Apply all OpenCV transformation stages to a single image. Break on OpenCV errors.
   */
  def processImage(stages: Seq[ImageTransformerStage])(image: Mat): Mat = {
    stages.foldLeft(image) {
      case (imgInternal, stage) => stage.apply(imgInternal)
    }
  }

  /**
   * Extract channels from image.
   */
  def extractChannels(channelOrder: String)(image: Mat): Array[Mat] = {
    // OpenCV channel order is BGR - reverse the order if the intended order is RGB.
    // Also remove alpha channel if nChannels is 4.
    val converted = if (image.channels == 4) {
      // remove alpha channel and order color channels if necessary
      val dest = new Mat(image.rows, image.cols, CvType.CV_8UC3)
      val colorConversion = if (channelOrder.toLowerCase == "rgb") Imgproc.COLOR_BGRA2RGB else Imgproc.COLOR_BGRA2BGR
      Imgproc.cvtColor(image, dest, colorConversion)
      dest
    } else if (image.channels == 3 && channelOrder.toLowerCase == "rgb") {
      // Reorder channel if nChannel is 3 and intended tensor channel order is RGB.
      val dest = new Mat(image.rows, image.cols, CvType.CV_8UC3)
      Imgproc.cvtColor(image, dest, Imgproc.COLOR_BGR2RGB)
      dest
    } else {
      image
    }

    val channelLength = converted.channels
    val channelMats = ListBuffer.fill(channelLength)(Mat.zeros(converted.rows, converted.cols, CvType.CV_8U))
    Core.split(converted, channelMats.asJava)

    channelMats.toArray
  }

  /**
   * Normalize each channel.
   */
  def normalizeChannels(means: Option[Array[Double]], stds: Option[Array[Double]], scale: Option[Int])
                       (channels: Array[Mat]): Array[Mat] = {
    val channelLength = channels.length
    require(means.forall(channelLength == _.length))
    require(stds.forall(channelLength == _.length))

    channels
      .zip(means.getOrElse(Array.fill(channelLength)(0d)))
      .zip(stds.getOrElse(Array.fill(channelLength)(1d)))
      .map {
        case ((matrix: Mat, m: Double), sd: Double) =>
          val t = new Mat(matrix.rows, matrix.cols, CvType.CV_64F)
          matrix.convertTo(t, CvType.CV_64F)
          Core.divide(t, new Scalar(scale.getOrElse(1)), t) // Standardized
          Core.subtract(t, new Scalar(m), t) // Centered
          Core.divide(t, new Scalar(sd), t) // Normalized
          t
      }
  }

  private def to2DArray(m: Mat): Array[Array[Double]] = {
    val array = Array.ofDim[Double](m.rows, m.cols)
    array.indices foreach {
      i => m.get(i, 0, array(i))
    }

    array
  }

  /**
   * Convert channel matrices to tensor in the shape of (C * H * W)
   */
  def convertToTensor(matrices: Array[Mat]): Array[Array[Array[Double]]] = {
    matrices.map(to2DArray)
  }

  /**
   *  Convert from OpenCV format to Dataframe Row.
   */
  def encodeImage(path: String, image: Mat): Row = {
    mat2row(image, path)
  }
}

/** Image processing stage. Please refer to OpenCV for additional information
  *
  * @param uid The id of the module
  */
class ImageTransformer(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with Wrappable with DefaultParamsWritable with BasicLogging {
  logClass()

  import ImageTransformer._
  import ImageTransformerStage._

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("ImageTransformer"))

  val stages: ArrayMapParam = new ArrayMapParam(this, "stages", "Image transformation stages")

  def setStages(value: Array[Map[String, Any]]): this.type = set(stages, value)

  val emptyStages: Array[Map[String, Any]] = Array[Map[String, Any]]()

  def getStages: Array[Map[String, Any]] = if (isDefined(stages)) $(stages) else emptyStages

  private def addStage(stage: Map[String, Any]): this.type = set(stages, getStages :+ stage)

  val toTensor: BooleanParam = new BooleanParam(
    this,
    "toTensor",
    "Convert output image to tensor in the shape of (C * H * W)"
  )

  def getToTensor: Boolean = $(toTensor)
  def setToTensor(value: Boolean): this.type = this.set(toTensor, value)

  @transient
  private lazy val validElementTypes: Array[DataType] = Array(FloatType, DoubleType)
  val tensorElementType: DataTypeParam = new DataTypeParam(
    parent = this,
    name = "tensorElementType",
    doc = "The element data type for the output tensor. Only used when toTensor is set to true. " +
      "Valid values are DoubleType or FloatType. Default value: FloatType.",
    isValid = ParamValidators.inArray(validElementTypes)
  )

  def getTensorElementType: DataType = $(tensorElementType)
  def setTensorElementType(value: DataType): this.type = this.set(tensorElementType, value)

  val tensorChannelOrder: Param[String] = new Param[String](
    parent = this,
    name = "tensorChannelOrder",
    doc = "The color channel order of the output channels. Valid values are RGB and GBR. Default: RGB.",
    isValid = ParamValidators.inArray(Array("rgb", "RGB", "bgr", "BGR"))
  )

  def getTensorChannelOrder: String = $(tensorChannelOrder)
  def setTensorChannelOrder(value: String): this.type = this.set(tensorChannelOrder, value)

  val normalizeMean: DoubleArrayParam = new DoubleArrayParam(
    this,
    "normalizeMean",
    "The mean value to use for normalization for each channel. " +
      "The length of the array must match the number of channels of the input image."
  )

  def getNormalizeMean: Array[Double] = $(normalizeMean)
  def setNormalizeMean(value: Array[Double]): this.type = this.set(normalizeMean, value)

  val normalizeStd: DoubleArrayParam = new DoubleArrayParam(
    this,
    "normalizeStd",
    "The standard deviation to use for normalization for each channel. " +
      "The length of the array must match the number of channels of the input image."
  )

  def getNormalizeStd: Array[Double] = $(normalizeStd)
  def setNormalizeStd(value: Array[Double]): this.type = this.set(normalizeStd, value)

  val colorScale: IntParam = new IntParam(
    this,
    "colorScale",
    "The scale of color. Used for normalization.",
    ParamValidators.gt(0d)
  )

  def getColorScale: Int = $(colorScale)
  def setColorScale(value: Int): this.type = this.set(colorScale, value)

  setDefault(
    inputCol -> "image",
    outputCol -> (uid + "_output"),
    toTensor -> false,
    tensorChannelOrder -> "RGB",
    tensorElementType -> FloatType
  )

  def normalize(mean: Array[Double], std: Array[Double], colorScale: Int): this.type = {
    this
      .setToTensor(true)
      .setNormalizeMean(mean)
      .setNormalizeStd(std)
      .setColorScale(colorScale)
  }

  def resize(height: Int, width: Int): this.type = {
    require(width >= 0 && height >= 0, "width and height should be non-negative")

    addStage(Map(stageNameKey -> ResizeImage.stageName,
      ResizeImage.width -> width,
      ResizeImage.height -> height))
  }

  /**
   * If keep aspect ratio is set to true, the shorter side of the image will be resized to the specified size.
   */
  def resize(size: Int, keepAspectRatio: Boolean): this.type = {
    require(size >= 0, "size should be non-negative")
    addStage(Map(stageNameKey -> ResizeImage.stageName,
      ResizeImage.size -> size,
      ResizeImage.keepAspectRatio -> keepAspectRatio
    ))
  }

  def crop(x: Int, y: Int, height: Int, width: Int): this.type = {
    require(x >= 0 && y >= 0 && width >= 0 && height >= 0, "crop values should be non-negative")

    addStage(Map(stageNameKey -> CropImage.stageName,
      CropImage.width -> width,
      CropImage.height -> height,
      CropImage.x -> x,
      CropImage.y -> y))
  }

  def centerCrop(height: Int, width: Int): this.type = {
    require(width >= 0 && height >= 0, "crop values should be non-negative")

    addStage(
      Map(
        stageNameKey -> CenterCropImage.stageName,
        CenterCropImage.width -> width,
        CenterCropImage.height -> height
      )
    )
  }

  def colorFormat(format: Int): this.type = {
    addStage(Map(stageNameKey -> ColorFormat.stageName, ColorFormat.format -> format))
  }

  def blur(height: Double, width: Double): this.type = {
    addStage(Map(stageNameKey -> Blur.stageName, Blur.height -> height, Blur.width -> width))
  }

  def threshold(threshold: Double, maxVal: Double, thresholdType: Int): this.type = {
    addStage(Map(stageNameKey -> Threshold.stageName,
      Threshold.maxVal -> maxVal,
      Threshold.threshold -> threshold,
      Threshold.thresholdType -> thresholdType))
  }

  /** Flips the image
    *
    * @param flipCode is a flag to specify how to flip the image:
    * - 0 means flipping around the x-axis (i.e. up-down)
    * - positive value (for example, 1) means flipping around y-axis (left-right)
    * - negative value (for example, -1) means flipping around both axes (diagonally)
    *                 See OpenCV documentation for details.
    * @return
    */
  def flip(flipCode: Int): this.type = {
    addStage(Map(stageNameKey -> Flip.stageName, Flip.flipCode -> flipCode))
  }

  def gaussianKernel(apertureSize: Int, sigma: Double): this.type = {
    addStage(Map(stageNameKey -> GaussianKernel.stageName,
      GaussianKernel.apertureSize -> apertureSize,
      GaussianKernel.sigma -> sigma))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({

      //  load native OpenCV library on each partition
      // TODO: figure out more elegant way
      val df = OpenCVUtils.loadOpenCV(dataset.toDF)
      val inputDataType = df.schema(getInputCol).dataType
      val decodeMode = getDecodeType(inputDataType)

      val transforms = getStages.map(ImageTransformerStage.apply)

      val outputColumnSchema = if ($(toTensor)) tensorUdfSchema else imageColumnSchema
      val processStep = processImage(transforms) _
      val extractStep = extractChannels(getTensorChannelOrder) _
      val normalizeStep = normalizeChannels(get(normalizeMean), get(normalizeStd), get(colorScale)) _
      val toTensorStep = convertToTensor _

      val convertFunc = if ($(toTensor)) {
        inputRow: Any =>
          decodeImage(decodeMode)(inputRow) map {
            case (_, image) =>
              processStep
                .andThen(extractStep)
                .andThen(normalizeStep)
                .andThen(toTensorStep)
                .apply(image)
          }
      } else {
        inputRow: Any =>
          decodeImage(decodeMode)(inputRow) map {
            case (path, image) =>
              val encodeStep = encodeImage(path, _)
              processStep.andThen(encodeStep).apply(image)
          }
      }

      val convert = UDFUtils.oldUdf(convertFunc, outputColumnSchema)
      if ($(toTensor)) {
        df.withColumn(getOutputCol, convert(df(getInputCol)).cast(tensorColumnSchema))
      } else {
        df.withColumn(getOutputCol, convert(df(getInputCol)))
      }
    })
  }

  private def getDecodeType(inputDataType: DataType): String = {
    inputDataType match {
      case s if ImageSchemaUtils.isImage(s) => "image"
      case s if BinaryFileSchema.isBinaryFile(s) => "binaryfile"
      case s if s == BinaryType => "binary"
      case s =>
        throw new IllegalArgumentException(s"input column should have Image or BinaryFile type, got $s")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  private lazy val tensorUdfSchema = ArrayType(ArrayType(ArrayType(DoubleType)))
  private lazy val tensorColumnSchema =  ArrayType(ArrayType(ArrayType($(tensorElementType))))
  private lazy val imageColumnSchema = ImageSchema.columnSchema
  override def transformSchema(schema: StructType): StructType = {
    assert(!schema.fieldNames.contains(getOutputCol), s"Input schema already contains output field $getOutputCol")

    val outputColumnSchema = if ($(toTensor)) tensorColumnSchema else imageColumnSchema
    schema.add(getOutputCol, outputColumnSchema)
  }
}

//scalastyle:on field.name
