// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.nio.file.Paths

import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object TFModel extends ComplexParamsReadable[TFModel]

@InternalWrapper
class TFModel(override val uid: String) extends Model[TFModel] with ComplexParamsWritable
  with HasInputCol with HasOutputCol with Wrappable{

  def this() = this(Identifiable.randomUID("TFModel"))

  /** Path to the tensorflow model
    * @group param
    */
  val modelPath: Param[String] = new Param(this, "modelPath", "Path to the tensorflow model")

  /** @group setParam */
  def setModelPath(value: String): this.type = set(modelPath, value)

  /** @group getParam */
  def getModelPath: String                   = $(modelPath)

  /** Name of protobuf file
    * @group param
    */
  val graphFile: Param[String] = new Param(this, "graphFile", "Name of protobuf file")

  /** @group setParam */
  def setGraphFile(value: String): this.type = set(graphFile, value)

  /** @group getParam */
  def getGraphFile: String                   = $(graphFile)

  /** Name of text file containing labels
    * @group param
    */
  val labelFile: Param[String] = new Param(this, "labelFile", "Name of text file containing labels")

  /** @group setParam */
  def setLabelFile(value: String): this.type =
    set(labelFile, value)

  /** @group getParam */
  def getLabelFile: String                   = $(labelFile)


  /** preprocessing info: width, height, mean and scale
    * @group param
    */
  val expectedDims: Param[Array[Float]] = new Param(this, "expectedDims", "Preprocessing info in array")

  /** @group setParam */
  def setExpectedDims(value: Array[Float]): this.type = set(expectedDims, value)

  /** @group getParam */
  def getExpectedDims: Array[Float]                   = $(expectedDims)

  /** Name of input node in TF graph - set to 'input' by default
    * @group param
    */
  val inputTensorName: Param[String] = new Param(this, "inputTensorName", "Name of input node in TF graph")
  //  set(inputTensorName, "input")


  /** @group setParam */
  def setInputTensorName(value: String): this.type = set(inputTensorName, value)

  /** @group getParam */
  def getInputTensorName: String                   = $(inputTensorName)

  /** Name of output node in TF graph - set to 'output' by default
    * @group param
    */
  val outputTensorName: Param[String] = new Param(this, "outputTensorName", "Name of output node in TF graph")
  //  set(outputTensorName, "output")


  /** @group setParam */
  def setOutputTensorName(value: String): this.type = set(outputTensorName, value)

  /** @group getParam */
  def getOutputTensorName: String                   = $(outputTensorName)

  setDefault(expectedDims -> Array[Float](224f,224f,128f,255f))
  setDefault(inputTensorName -> "input")
  setDefault(outputTensorName -> "output")

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, StringType)

  /** Evaluate the model (on images)
    * @param dataset the dataset to featurize
    * @return featurized dataset - StringType
    */
  def transform(dataset: Dataset[_]): DataFrame = {

    //Start of Set-up for evaluation code
    val executer = new TFModelExecutor()
    val labels = executer.readAllLinesOrExit(Paths.get(getModelPath,getLabelFile))
    val graph = executer.readAllBytesOrExit(Paths.get(getModelPath,getGraphFile))
    //End of set-up for evaluation code

    val df = dataset.toDF()

    val encoder = RowEncoder(new StructType().add(StructField("Output labels", StringType)))
    val output = df.mapPartitions{ it =>
        it.map { r =>
          val rawData = r.toSeq.toArray
          val rawDataDouble = rawData(0).asInstanceOf[Array[Byte]]
          val height = rawData(1).asInstanceOf[Int]
          val width = rawData(2).asInstanceOf[Int]
          val typeForEncode = rawData(3).asInstanceOf[Int]
          val prediction: String = executer.evaluateForSpark(graph, labels, rawDataDouble, height, width,
            typeForEncode, getExpectedDims, getInputTensorName, getOutputTensorName)
          Row.fromSeq(Array(prediction).toSeq)
        }
    }(encoder)
    output
  }

}
