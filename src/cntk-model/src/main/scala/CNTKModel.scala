// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File
import javax.xml.bind.DatatypeConverter._

import com.microsoft.CNTK.{Function => CNTKFunction, DataType => CNTKDataType, _}
import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.commons.io.FileUtils._
import org.apache.spark.broadcast._
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private object CNTKModelUtils extends java.io.Serializable {

  private def applyModel(inputIndex: Int,
                         broadcastModelBytes: Broadcast[Array[Byte]],
                         minibatchSize: Int,
                         inputNode: Int,
                         outputNode: Option[String])(inputRows: Iterator[Row]): Iterator[Row] = {
    val device = DeviceDescriptor.useDefaultDevice
    val m = CNTKModel.loadModelFromBytes(broadcastModelBytes.value, device)
    val model = outputNode
      .map { name => CNTKLib.AsComposite(Option(m.findByName(name)).getOrElse(
              throw new IllegalArgumentException(s"Node $name does not exist"))) }
      .getOrElse(m)

    val inputVar = model.getArguments.get(inputNode)
    require(inputVar.getDataType() == CNTKDataType.Float, "input variable type is not Float input type")
    val inputShape = inputVar.getShape

    // This defines and instantiates an iterator, hasNext and next are the abstract methods that
    // define the interface and inputBuffer and outputBuffer hold the input and output rows so that
    // they can be joined and returned.
    // The logic inside next checks to see if the buffer is empty, and if so sends a new batch off
    // to be evaluated
    new Iterator[Row] {
      val inputBuffer    = new ListBuffer[Row]()
      val outputBuffer   = new ListBuffer[Row]()
      val inputSize: Int = inputShape.getTotalSize().toInt
      val inputFVV       = new FloatVectorVector(minibatchSize.toLong)
      val fvs: Array[FloatVector] =
        (0 until minibatchSize).map(_ => new FloatVector(inputSize.toLong)).toArray

      def hasNext: Boolean = inputRows.hasNext || outputBuffer.nonEmpty

      def next(): Row = {
        if (outputBuffer.isEmpty) {
          var paddedRows = 0
          for (i <- 0 until minibatchSize) {
            if (inputRows.hasNext) {
              val row = inputRows.next()
              inputBuffer += row
              for ((x, j) <- row.getSeq[Float](inputIndex).view.zipWithIndex) {
                fvs(i).set(j, x)
              }
            } else {
              //TODO remove padding after CNTK bug is fixed
              paddedRows += 1
              for (j <- 0 until inputSize) {
                fvs(i).set(j, 0.0.toFloat)
              }
            }
            inputFVV.set(i, fvs(i))
          }

          val inputVal =
            Value.createDenseFloat(inputShape, inputFVV, device)
          val inputDataMap = new UnorderedMapVariableValuePtr()
          inputDataMap.add(inputVar, inputVal)

          val outputDataMap = new UnorderedMapVariableValuePtr()
          val outputVar     = model.getOutputs.get(0)
          outputDataMap.add(outputVar, null)

          model.evaluate(inputDataMap, outputDataMap, device)

          val outputFVV = new FloatVectorVector()
          outputDataMap.getitem(outputVar).copyVariableValueToFloat(outputVar, outputFVV)
          assert(outputBuffer.isEmpty,
                 "The output row buffer should be empty before new elements are added.")
          outputBuffer ++= toSeqSeq(outputFVV)
            .dropRight(paddedRows)
            .map(fs => Row(Vectors.dense(fs.map(_.toDouble).toArray)))
        }
        val ret = Row.merge(inputBuffer.head, outputBuffer.head)
        inputBuffer.remove(0)
        outputBuffer.remove(0)
        ret
      }
    }
  }

  // here just for serialization
  val applyModelFunc = (inputIndex: Int, broadcastModelBytes: Broadcast[Array[Byte]],
                        minibatchSize: Int, inputNode: Int,
                        outputNode: Option[String]) => {
    (inputRows: Iterator[Row]) => {
      applyModel(inputIndex, broadcastModelBytes, minibatchSize, inputNode, outputNode)(inputRows)
    }
  }

  private def toSeqSeq(fvv: FloatVectorVector): Seq[Seq[Float]] = {
    (0 until fvv.size.toInt).map(i => (0 until fvv.get(i).size.toInt).map(j => fvv.get(i).get(j)))
  }
}

object CNTKModel extends DefaultParamsReadable[CNTKModel] {
  def loadModelFromBytes(bytes: Array[Byte],
                         device: DeviceDescriptor =
                           DeviceDescriptor.useDefaultDevice): CNTKFunction = {
    import java.util.UUID._
    val modelFile = new File(s"$getTempDirectoryPath/$randomUUID.model")
    writeByteArrayToFile(modelFile, bytes)
    val model = try {
      CNTKFunction.load(modelFile.getPath, device)
    } finally forceDelete(modelFile)
    model
  }

  override def load(path: String): CNTKModel = super.load(path)
}

@InternalWrapper
class CNTKModel(override val uid: String) extends Model[CNTKModel] with DefaultParamsWritable
  with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("CNTKModel"))

  /** Array of bytes containing the seialized <code>CNTKModel</code>
    * @group param
    */
  val model: Param[String] =
    new Param(this, "model", "Array of bytes containing the serialized CNTKModel")

  /** @group setParam */
  def setModel(spark: SparkSession, path: String): CNTKModel = {
    val modelBytes = spark.sparkContext.binaryFiles(path).first()._2.toArray
    set(model, printBase64Binary(modelBytes))
  }

  /** @group getParam */
  def getModel: Array[Byte] = parseBase64Binary($(model))

  /** Index of the input node
    * @group param
    */
  val inputNode: IntParam                 = new IntParam(this, "inputNode", "index of the input node")

  /** @group setParam */
  def setInputNode(value: Int): this.type = set(inputNode, value)

  /** @group getParam */
  def getInputNode: Int                   = $(inputNode)
  setDefault(inputNode -> 0)

  /** Index of the output node
    * @group param
    */
  val outputNodeIndex: IntParam = new IntParam(this, "outputNodeIndex", "index of the output node")

  /** @group setParam */
  def setOutputNodeIndex(value: Int): this.type = set(outputNodeIndex, value)

  /** @group getParam */
  def getOutputNodeIndex: Int                   = $(outputNodeIndex)

  /** Name of the output node
    * @group param
    */
  val outputNodeName: Param[String] = new Param(this, "outputNodeName", "name of the output node")

  /** @group setParam */
  def setOutputNodeName(value: String): this.type = set(outputNodeName, value)

  /** @group getParam */
  def getOutputNodeName: String                   = $(outputNodeName)

  /** Size of minibatches. Must be greater than 0; default is 10
    * @group param
    */
  val miniBatchSize: IntParam =
    new IntParam(this, "miniBatchSize", "size of minibatches", ParamValidators.gt(0))

  /** @group setParam */
  def setMiniBatchSize(value: Int): this.type = set(miniBatchSize, value)

  /** @group getParam */
  def getMiniBatchSize: Int                   = $(miniBatchSize)
  setDefault(miniBatchSize -> 10)

  def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, VectorType)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  /** Train the model
    * @param dataset
    * @return trained dataset
    */
  def transform(dataset: Dataset[_]): DataFrame = {
    val spark      = dataset.sparkSession
    val sc         = spark.sparkContext
    val inputIndex = dataset.columns.indexOf(getInputCol)
    val device     = DeviceDescriptor.useDefaultDevice

    if (inputIndex == -1)
      throw new IllegalArgumentException(s"Input column $getInputCol does not exist")

    val model = CNTKModel.loadModelFromBytes(getModel, device)

    val setByName  = get(outputNodeName)
    val setByIndex = get(outputNodeIndex)
    if ((setByName.isDefined && setByIndex.isDefined) ||
          (!setByName.isDefined && !setByIndex.isDefined))
      throw new Exception("Must specify one and only one of outputNodeName or outputNodeIndex")

    val outputNode: Option[String] =
      if (setByName.isDefined) setByName
      else                     setByIndex.map(i => model.getOutputs.get(i).getName)

    val coersionOptionUDF = dataset.schema.fields(inputIndex).dataType match {
      case ArrayType(tp, _) =>
        tp match {
          case DoubleType => Some(udf((x: mutable.WrappedArray[Double]) => x.map(_.toFloat)))
          case FloatType  => None
          case _ =>
            throw new IllegalArgumentException(s"improper column type: $tp, need Array[Float]")
        }
      case VectorType => Some(udf((x: DenseVector) => x.toArray.map(_.toFloat)))
    }

    val coercedCol = DatasetExtensions.findUnusedColumnName("coerced")(dataset.columns.toSet)
    val (df, selectedIndex) = coersionOptionUDF match {
      case Some(coersionUDF) =>
        val coercedDF = dataset.toDF().withColumn(coercedCol, coersionUDF(col(getInputCol)))
        (coercedDF, coercedDF.columns.indexOf(coercedCol))
      case None => (dataset.toDF(), inputIndex)
    }

    val inputType = df.schema($(inputCol)).dataType
    val broadcastModelBytes = sc.broadcast(getModel)
    val rdd = df.rdd.mapPartitions(
      CNTKModelUtils.applyModelFunc(selectedIndex,
                                    broadcastModelBytes,
                                    getMiniBatchSize,
                                    getInputNode,
                                    outputNode))
    val output = spark.createDataFrame(rdd, df.schema.add(StructField(getOutputCol, VectorType)))

    coersionOptionUDF match {
      case Some(_) => output.drop(coercedCol)
      case None    => output
    }
  }

}
