// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cntk

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.onnx.{ONNXHub, ONNXModel, ONNXModelInfo}
import com.microsoft.azure.synapse.ml.opencv.ImageTransformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.types.{FloatType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object ImageFeaturizer extends ComplexParamsReadable[ImageFeaturizer]

/** The <code>ImageFeaturizer</code> relies on a CNTK model to do the featurization, one can set
  * this model using the <code>modelLocation</code> parameter. To map the nodes of the CNTK model
  * onto the standard "layers" structure of a feed forward neural net, one needs to supply a list of
  * node names that range from the output node, back towards the input node of the CNTK Function.
  * This list does not need to be exhaustive, and is provided to you if you use a model downloaded
  * from the <code>ModelDownloader</code>, one can find this layer list in the schema of the
  * downloaded model.
  *
  * The <code>ImageFeaturizer</code> takes an input column of images (the type returned by the
  * <code>ImageReader</code>), and automatically resizes them to fit the CMTKModel's inputs.  It
  * then feeds them through a pre-trained CNTK model.  One can truncate the model using the <code>
  * cutOutputLayers </code> parameter that determines how many layers to truncate from the output of
  * the network.  For example, layer=0 means that no layers are removed, layer=2 means that the
  * image featurizer returns the activations of the layer that is two layers from the output layer.
  *
  * @param uid the uid of the image transformer
  */
class ImageFeaturizer(val uid: String) extends Transformer with HasInputCol with HasOutputCol
  with Wrappable with ComplexParamsWritable with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("ImageFeaturizer"))

  override protected lazy val pyInternalWrapper = true

  val onnxModel: TransformerParam =
    new TransformerParam(
      this,
      "onnxModel", "The internal ONNX model used in the featurizer",
      { t => t.isInstanceOf[ONNXModel] })

  /** @group setParam */
  def setOnnxModel(value: ONNXModel): this.type = set(onnxModel, value)

  val emptyOnnxModel = new ONNXModel()

  /** @group getParam */
  def getOnnxModel: ONNXModel =
    if (isDefined(onnxModel)) $(onnxModel).asInstanceOf[ONNXModel] else emptyOnnxModel

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
    val input = info.metadata.ioPorts.get.inputs.head
    setImageHeight(input.shape.head.right.get)
      .setImageWidth(input.shape.apply(1).right.get)
      .setImageTensorName(input.name)
      .setFeatureTensorName(info.metadata.extraPorts.get.features.head.name)
  }

  def setModel(bytes: Array[Byte]): this.type = {
    set(onnxModel, getOnnxModel.setModelPayload(bytes))
  }

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

  //TODO make nulls pass through
  val dropNa: BooleanParam =
    new BooleanParam(this, "dropNa", "Whether to drop na values before mapping")

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

  /** @group setParam */
  def setOutputTensorName(value: String): this.type = set(outputTensorName, value)

  /** @group getParam */
  def getOutputTensorName: String = $(outputTensorName)

  val headless: BooleanParam = new BooleanParam(this, "headless",
    "whether to use the feature tensor or the output tensor")

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

  setDefault(outputCol -> (uid + "_output"), dropNa -> true, headless->true)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val imgCol = DatasetExtensions.findUnusedColumnName("images")(dataset.columns.toSet)

      val transformed = new ImageTransformer()
        .setInputCol(getInputCol)
        .setOutputCol(imgCol)
        .resize(getImageHeight, getImageWidth)
        .centerCrop(getImageHeight, getImageWidth)
        .normalize(
          mean = Array(0.485, 0.456, 0.406),
          std = Array(0.229, 0.224, 0.225),
          colorScaleFactor = 1d / 255d)
        .setTensorElementType(FloatType)
        .transform(dataset)

      val dropped = if (getDropNa) {
        transformed.na.drop(List(imgCol))
      } else {
        transformed
      }

      val outputTensor = if (getHeadless){
        getFeatureTensorName
      }else{
        getOutputTensorName
      }

      getOnnxModel
        .setFeedDict(Map(getImageTensorName->imgCol))
        .setFetchDict(Map(getOutputCol->outputTensor))
        .transform(dropped).drop(imgCol)
    })

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /** Add the features column to the schema
    *
    * @param schema
    * @return schema with features column
    */
  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, VectorType)
  }

}
