// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions.findUnusedColumnName
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.opencv.ImageTransformer
import com.microsoft.azure.synapse.ml.param.TransformerParam
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{FloatType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object ImageFeaturizer extends ComplexParamsReadable[ImageFeaturizer]

/** The <code>ImageFeaturizer</code> relies on a ONNX model to do the featurization. One can set
  * this model using the <code>setOnnxModel</code> parameter with a model you create yourself, or
  * <code>setModel</code> to get a predefined named model from the ONNXHub.
  *
  * The <code>ImageFeaturizer</code> takes an input column of images (the type returned by the
  * <code>ImageReader</code>), and automatically resizes them to fit the ONNXModel's inputs.  It
  * then feeds them through a pre-trained ONNX model.
  *
  * @param uid the uid of the image transformer
  */
class ImageFeaturizer(val uid: String) extends Transformer with HasInputCol with HasOutputCol
  with Wrappable with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.DeepLearning)

  def this() = this(Identifiable.randomUID("ImageFeaturizer"))

  override protected lazy val pyInternalWrapper = true

  // Parameters related to the inner model

  val onnxModel: TransformerParam = new TransformerParam(
    this,
    "onnxModel",
    "The internal ONNX model used in the featurizer",
    { t => t.isInstanceOf[ONNXModel] })
  setDefault(onnxModel, new ONNXModel())
  /** @group setParam */
  def setOnnxModel(value: ONNXModel): this.type = set(onnxModel, value)
  /** @group getParam */
  def getOnnxModel: ONNXModel = $(onnxModel).asInstanceOf[ONNXModel]

  /** @group setParam */
  def setMiniBatchSize(value: Int): this.type = set(onnxModel, getOnnxModel.setMiniBatchSize(value))
  /** @group getParam */
  def getMiniBatchSize: Int = getOnnxModel.getMiniBatchSize

  def setModelLocation(path: String): this.type = {
    set(onnxModel, getOnnxModel.setModelLocation(path))
  }

  def setModel(name: String): this.type = {
    val hub = new ONNXHub()
    val info = hub.getModelInfo(name)
    val bytes = hub.load(name)
    setModel(bytes).setModelInfo(info)
  }

  def setModelInfo(info: ONNXModelInfo): this.type = {
    val ioPorts = info.metadata.ioPorts.getOrElse(throw new Exception("IO ports not defined."))
    val input = ioPorts.inputs.head
    val output = ioPorts.outputs.head
    val shape = input.shape
    if (shape.length < 4) {
      throw new Exception("Image shape must have 4 dimensions.")
    }
    this
      .setImageHeight(shape.apply(2).getOrElse(throw new Exception("Image height not defined in shape.")))
      .setImageWidth(shape.apply(3).getOrElse(throw new Exception("Image width not defined in shape.")))
      .setImageTensorName(input.name)
      .setOutputTensorName(output.name)
    info.metadata.extraPorts.foreach(port => this.setFeatureTensorName(port.features.head.name))
    this
  }

  /** @group setParam */
  def setModel(bytes: Array[Byte]): this.type = set(onnxModel, getOnnxModel.setModelPayload(bytes))
  /** @group getParam */
  def getModel: Array[Byte] = getOnnxModel.getModelPayload

  val imageHeight: IntParam = new IntParam(
    this, "imageHeight", "Size required by model", ParamValidators.gtEq(0))
  /** @group setParam */
  def setImageHeight(value: Int): this.type = set(imageHeight, value)
  /** @group getParam */
  def getImageHeight: Int = $(imageHeight)

  val imageWidth: IntParam = new IntParam(
    this, "imageWidth", "Size required by model", ParamValidators.gtEq(0))
  /** @group setParam */
  def setImageWidth(value: Int): this.type = set(imageWidth, value)
  /** @group getParam */
  def getImageWidth: Int = $(imageWidth)

  val channelNormalizationMeans: DoubleArrayParam = new DoubleArrayParam(
    this, "channelNormalizationMeans", "Normalization means for color channels")
  setDefault(channelNormalizationMeans -> Array(0.485, 0.456, 0.406))
  /** @group setParam */
  def setChannelNormalizationMeans(value: Array[Double]): this.type = set(channelNormalizationMeans, value)
  /** @group getParam */
  def getChannelNormalizationMeans: Array[Double] = $(channelNormalizationMeans)

  val channelNormalizationStds: DoubleArrayParam = new DoubleArrayParam(
    this, "channelNormalizationStds", "Normalization std's for color channels")
  setDefault(channelNormalizationStds -> Array(0.229, 0.224, 0.225))
  /** @group setParam */
  def setChannelNormalizationStds(value: Array[Double]): this.type = set(channelNormalizationStds, value)
  /** @group getParam */
  def getChannelNormalizationStds: Array[Double] = $(channelNormalizationStds)

  val colorScaleFactor: DoubleParam = new DoubleParam(
    this, "colorScaleFactor", "Color scale factor")
  setDefault(colorScaleFactor -> 1d / 255d)
  /** @group setParam */
  def setColorScaleFactor(value: Double): this.type = set(colorScaleFactor, value)
  /** @group getParam */
  def getColorScaleFactor: Double = $(colorScaleFactor)

  // TODO make nulls pass through
  val dropNa: BooleanParam =
    new BooleanParam(this, "dropNa", "Whether to drop na values before mapping")
  setDefault(dropNa -> true)
  /** @group setParam */
  def setDropNa(value: Boolean): this.type = set(dropNa, value)
  /** @group getParam */
  def getDropNa: Boolean = $(dropNa)

  /** Name of the output node which represents features
    *
    * @group param
    */
  val featureTensorName: Param[String] = new Param[String](this, "featureTensorName",
    "the name of the tensor to include in the fetch dict")
  /** @group setParam */
  def setFeatureTensorName(value: String): this.type = set(featureTensorName, value)
  /** @group getParam */
  def getFeatureTensorName: String = $(featureTensorName)

  /** Name of the output node which represents probabilities
    *
    * @group param
    */
  val outputTensorName: Param[String] = new Param[String](this, "outputTensorName",
    "the name of the tensor to include in the fetch dict")
  setDefault(outputTensorName -> "")
  /** @group setParam */
  def setOutputTensorName(value: String): this.type = set(outputTensorName, value)
  /** @group getParam */
  def getOutputTensorName: String = $(outputTensorName)

  val headless: BooleanParam = new BooleanParam(this, "headless",
    "whether to use the feature tensor or the output tensor")
  setDefault(headless -> true)
  /** @group setParam */
  def setHeadless(value: Boolean): this.type = set(headless, value)
  /** @group getParam */
  def getHeadless: Boolean = $(headless)

  /** Name of the input node for images
    *
    * @group param
    */
  val imageTensorName: Param[String] = new Param[String](this, "imageTensorName",
    "the name of the tensor to include in the fetch dict")
  /** @group setParam */
  def setImageTensorName(value: String): this.type = set(imageTensorName, value)
  /** @group getParam */
  def getImageTensorName: String = $(imageTensorName)

  val ignoreDecodingErrors: BooleanParam = new BooleanParam(
    this,
    "ignoreDecodingErrors",
    "Whether to throw on decoding errors or just return None"
  )
  setDefault(ignoreDecodingErrors -> false)
  /** @group setParam */
  def getIgnoreDecodingErrors: Boolean = $(ignoreDecodingErrors)
  /** @group getParam */
  def setIgnoreDecodingErrors(value: Boolean): this.type = this.set(ignoreDecodingErrors, value)


  val autoConvertToColor: BooleanParam = new BooleanParam(
    this,
    "autoConvertToColor",
    "Whether to automatically convert black and white images to color. default = true"
  )
  setDefault(autoConvertToColor -> true)
  def getAutoConvertToColor: Boolean = $(autoConvertToColor)
  def setAutoConvertToColor(value: Boolean): this.type = this.set(autoConvertToColor, value)

  setDefault(outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val imgCol = DatasetExtensions.findUnusedColumnName("images")(dataset.columns.toSet)

      val transformed = new ImageTransformer()
        .setInputCol(getInputCol)
        .setOutputCol(imgCol)
        .setIgnoreDecodingErrors(getIgnoreDecodingErrors)
        .setAutoConvertToColor(getAutoConvertToColor)
        .resize(getImageHeight, getImageWidth)
        .centerCrop(getImageHeight, getImageWidth)
        .normalize(
          getChannelNormalizationMeans,
          getChannelNormalizationStds,
          getColorScaleFactor)
        .setTensorElementType(FloatType)
        .transform(dataset)

      val dropped = if (getDropNa) {
        transformed.na.drop(List(imgCol))
      } else {
        transformed
      }

      val outputTensor =
        if (getHeadless) getFeatureTensorName
        else getOutputTensorName
      val tempCol = findUnusedColumnName("onnx", transformed)

      val rawResult = getOnnxModel
        .setFeedDict(Map(getImageTensorName->imgCol))
        .setFetchDict(Map(tempCol->outputTensor))
        .transform(dropped).drop(imgCol)

      val result = if (getHeadless) {
        val outputUDF = udf(convertFeaturesToVector)
        rawResult.withColumn(getOutputCol, outputUDF(col(tempCol)))
      } else {
        val outputUDF = udf(convertOutputToVector)
        rawResult.withColumn(getOutputCol, outputUDF(col(tempCol)))
      }

      result.drop(tempCol)
    }, dataset.columns.length)
  }

  val convertOutputToVector: Seq[Float] => DenseVector = (raw: Seq[Float]) => {
    new DenseVector(raw.map(_.toDouble).toArray)
  }

  val convertFeaturesToVector: Seq[Seq[Seq[Float]]] => DenseVector = (raw: Seq[Seq[Seq[Float]]]) => {
    new DenseVector(raw.map(_.head.head.toDouble).toArray)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /** Add the features column to the schema
    *
    * @param schema Schema to transform
    * @return schema with features column
    */
  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, VectorType)
  }

}
