// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI
import javax.xml.bind.DatatypeConverter.{parseBase64Binary, printBase64Binary}

import com.microsoft.ml.spark.FileUtilities.File
import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, FloatType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ImageFeaturizer extends DefaultParamsReadable[ImageFeaturizer]

/**
  * The <code>ImageFeaturizer</code> relies on a CNTK model to do the featurization, one can set this model using
  * the <code>modelLocation</code> parameter. To map the nodes of the CNTK model onto the standard "layers" structure
  * of a feed forward neural net, one needs to supply a list of node names that range from the output node,
  * back towards the input node of the CNTK Function.
  * This list does not need to be exhaustive, and is provided to you if you
  * use a model downloaded from the <code>ModelDownloader</code>, one can find this layer list in the schema of the
  * downloaded model.
  *
  * The <code>ImageFeaturizer</code> takes an input column of images
  * (the type returned by the <code>ImageReader</code>), and
  * automatically resizes them to fit the CMTKModel's inputs. It then feeds them through a pre-trained
  * CNTK model. One can truncate the model using the <code> cutOutputLayers </code> parameter that
  * determines how many layers to truncate from the output of the network.
  * For example, layer=0 means tha no layers are removed,
  * layer=2 means that the image featurizer returns the activations of the layer that is two layers
  * from the output layer.
  *
  * @param uid the uid of the image transformer
  */
@InternalWrapper
class ImageFeaturizer(val uid: String) extends Transformer with HasInputCol with HasOutputCol
  with MMLParams with CNTKModelParam {
  def this() = this(Identifiable.randomUID("ImageFeaturizer"))

  /** Select which node of the CNTKFunction's inputs to us as the input (default: 0)
    * @group param
    */
  val inputNode: IntParam = IntParam(this, "inputNode", "which node of the CNTKFunction's inputs" +
    "to use as the input (default 0)")

  /** @group setParam */
  def setInputNode(value: Int): this.type = set(inputNode, value)

  /** @group getParam */
  def getInputNode: Int = $(inputNode)

  /** The number of layer to cut off the endof the network; 0 leaves the network intact, 1 rmoves
    * the output layer, etc.
    * @group param
    */
  val cutOutputLayers: IntParam = IntParam(this, "cutOutputLayers", "the number of layers to cut " +
    "off the end of the network, 0 leaves the network intact," +
    " 1 removes the output layer, etc", ParamValidators.gtEq(0))

  /** @group setParam */
  def setCutOutputLayers(value: Int): this.type = set(cutOutputLayers, value)

  /** @group getParam */
  def getCutOutputLayers: Int = $(cutOutputLayers)

  /** Array with valid CNTK nodes to choose from; the first entries of this array should be closer
    * to the output node.
    * @group param
    */
  val layerNames: StringArrayParam = new StringArrayParam(this, "layerNames",
    "Array with valid CNTK nodes to choose from, this first entries of this array should be closer to the " +
      "output node")

  /** @group setParam */
  def setLayerNames(value: Array[String]): this.type = set(layerNames, value)

  /** @group getParam */
  def getLayerNames: Array[String] = $(layerNames)

  def setModel(spark: SparkSession, modelSchema: ModelSchema): this.type = {
    setLayerNames(modelSchema.layerNames)
      .setInputNode(modelSchema.inputNode)
      .setModelLocation(spark, modelSchema.uri.toString)
  }

  setDefault(cutOutputLayers -> 1, inputNode -> 0, outputCol -> (uid + "_output"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val spark = dataset.sparkSession

    val resizedCol = DatasetExtensions.findUnusedColumnName("resized")(dataset.columns.toSet)

    val cntkModel = new CNTKModel()
      .setModel(getModel)
      .setInputNode(getInputNode)
      .setOutputNodeName(getLayerNames.apply(getCutOutputLayers))
      .setInputCol(resizedCol)
      .setOutputCol(getOutputCol)

    val requiredSize = CNTKModel.loadModelFromBytes(cntkModel.getModel)
      .getArguments.get(0).getShape().getDimensions

    val prepare = new ImageTransformer()
      .setInputCol($(inputCol))
      .resize(requiredSize(0).toInt, requiredSize(1).toInt)

    val unroll = new UnrollImage()
      .setInputCol(prepare.getOutputCol)
      .setOutputCol(resizedCol)

    val resizedDF = prepare.transform(dataset)
    val unrolledDF = unroll.transform(resizedDF).drop(prepare.getOutputCol)
    val featurizedDF = cntkModel.transform(unrolledDF).drop(resizedCol)
    featurizedDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /** Add the features column to the schema
    * @param schema
    * @return schema with features column
    */
  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, VectorType)
  }

}
