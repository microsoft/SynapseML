// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.CNTK.CNTKExtensions._
import com.microsoft.CNTK.{DataType => CNTKDataType, SerializableFunction => CNTKFunction, _}
import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.spark.broadcast._
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable

private object CNTKModelUtils extends java.io.Serializable {

  def applyCNTKFunction(model: CNTKFunction,
                        inputFVV: FloatVectorVector,
                        inputVar: Variable,
                        device: DeviceDescriptor): FloatVectorVector = {
    val inputShape    = inputVar.getShape
    val inputVal      = Value.createDenseFloat(inputShape, inputFVV, device)
    val inputDataMap  = new UnorderedMapVariableValuePtr()
    inputDataMap.add(inputVar, inputVal)

    val outputDataMap = new UnorderedMapVariableValuePtr()
    val outputVar     = model.getOutputs.get(0)
    outputDataMap.add(outputVar, null)

    model.evaluate(inputDataMap, outputDataMap, device)

    val outputFVV = new FloatVectorVector()
    outputDataMap.getitem(outputVar).copyVariableValueToFloat(outputVar, outputFVV)
    outputFVV
  }

  private def deleteFVV(fvv: FloatVectorVector): Unit = {
    val size = fvv.size().toInt
    (0 until size).foreach{i =>
      fvv.get(i).delete()
    }
    fvv.delete()
  }

  def applyModel(inputIndex: Int,
                 broadcastedModel: Broadcast[CNTKFunction],
                 inputNode: Int,
                 outputNodeName: Option[String],
                 outputNodeIndex: Option[Int])(inputRows: Iterator[Row]): Iterator[Row] = {
    if (!inputRows.hasNext) {
      Iterator() // Quickly skip empty partitions
    } else {
      val device = DeviceDescriptor.useDefaultDevice
      //CNTKLib.SetMaxNumCPUThreads(1)
      val m = fromSerializable(broadcastedModel.value).clone(ParameterCloningMethod.Share)
      val outputNode: Option[CNTKFunction] = (outputNodeName, outputNodeIndex) match {
        case (Some(name), None) =>
          Some(Option(m.findByName(name)).getOrElse(
            throw new IllegalArgumentException(s"Node $name does not exist")))
        case (None, Some(index)) => Some(m.getOutputs.get(index).getOwner)
        case (Some(_), Some(_)) =>
          throw new Exception("Must specify one and only one of outputNodeName or outputNodeIndex")
        case _ => None
      }

      val model = outputNode.map(CNTKLib.AsComposite(_)).getOrElse(m)

      val inputVar = model.getArguments.get(inputNode)
      require(inputVar.getDataType == CNTKDataType.Float,
        "input variable type is not Float input type")

      inputRows.map { row =>
        val arr = row.getSeq[mutable.WrappedArray[Float]](inputIndex)
        val inFVV = toFVV(arr)
        val outFVV = applyCNTKFunction(model, inFVV, inputVar, device)
        val outArr = toArrVect(outFVV)
        deleteFVV(inFVV)
        deleteFVV(outFVV)
        Row.merge(row, Row(outArr))
      }
    }
  }

  private def toArrVect(fvv: FloatVectorVector): Seq[DenseVector] = {
    (0 until fvv.size.toInt).map { i =>
      val fv = fvv.get(i)
      new DenseVector((0 until fvv.get(i).size.toInt).map { j =>
        fv.get(j).toDouble
      }.toArray)
    }
  }

  private def toFVV(arr: Seq[mutable.WrappedArray[Float]]): FloatVectorVector = {
    val inputFVV = new FloatVectorVector(arr.length.toLong)
    arr.zipWithIndex.foreach { case (vect, i) =>
      val fv = new FloatVector(vect.length.toLong)
      vect.zipWithIndex.foreach { case (x, j) =>
        fv.set(j, x)
      }
      inputFVV.set(i, fv)
    }
    inputFVV
  }
}

object CNTKModel extends ComplexParamsReadable[CNTKModel]

@InternalWrapper
class CNTKModel(override val uid: String) extends Model[CNTKModel] with ComplexParamsWritable
  with HasInputCol with HasOutputCol with HasMiniBatcher with Wrappable {

  def this() = this(Identifiable.randomUID("CNTKModel"))

  /** Array of bytes containing the serialized CNTK <code>Function</code>
    * @group param
    */
  val model: CNTKFunctionParam =
    new CNTKFunctionParam(this, "model", "Array of bytes containing the serialized CNTKModel")

  private var broadcastedModelOption: Option[Broadcast[CNTKFunction]] = None

  /** @group setParam */
  def setModel(value: CNTKFunction): this.type = {
    // Free up memory used by the previous model
    // TODO: investigate using destroy()
    broadcastedModelOption.foreach(_.unpersist())
    broadcastedModelOption = None
    set(model, value)
  }

  /** @group getParam */
  def getModel: CNTKFunction = $(model)

  /** @group setParam */
  def setModelLocation(spark: SparkSession, path: String): this.type = {
    val modelBytes = spark.sparkContext.binaryFiles(path).first()._2.toArray
    setModel(CNTKFunction.loadModelFromBytes(modelBytes))
  }

  /** Index of the input node
    * @group param
    */
  val inputNode: IntParam                 = new IntParam(this, "inputNode", "Index of the input node")

  /** @group setParam */
  def setInputNode(value: Int): this.type = set(inputNode, value)

  /** @group getParam */
  def getInputNode: Int                   = $(inputNode)
  setDefault(inputNode -> 0)

  /** Returns the dimensions of the required input */
  def getInputShape: Array[Int] =
    getModel.getInputs.get(getInputNode).getShape.getDimensions.map(_.toInt)

  /** Index of the output node
    * @group param
    */
  val outputNodeIndex: IntParam = new IntParam(this, "outputNodeIndex", "Index of the output node")

  /** @group setParam */
  def setOutputNodeIndex(value: Int): this.type = set(outputNodeIndex, value)

  /** @group getParam */
  def getOutputNodeIndex: Int                   = $(outputNodeIndex)

  /** Name of the output node
    * @group param
    */
  val outputNodeName: Param[String] = new Param(this, "outputNodeName", "Name of the output node")

  /** @group setParam */
  def setOutputNodeName(value: String): this.type = set(outputNodeName, value)

  /** @group getParam */
  def getOutputNodeName: String                   = $(outputNodeName)

  setDefault(miniBatcher-> new FixedMiniBatchTransformer().setBatchSize(10))

  def transformSchema(schema: StructType): StructType = {
    schema(getInputCol).dataType match {
      case t if t == VectorType =>
      case ArrayType(DoubleType, _) =>
      case ArrayType(FloatType, _) =>
      case _ => throw new IllegalArgumentException("Need to pass a vector or Array[Float | Double] column")
    }
    schema.add(getOutputCol, VectorType)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def rebroadcastCNTKModel(spark: SparkSession): Unit = {
    broadcastedModelOption = Some(spark.sparkContext.broadcast(getModel))
  }

  private def toArrayFloatUDF(df: DataFrame): Option[UserDefinedFunction] = {
    val funcOpt = df.schema(getInputCol).dataType match {
      case ArrayType(tp, _) =>
        tp match {
          case DoubleType => Some((x: mutable.WrappedArray[Double]) => x.map(_.toFloat))
          case FloatType  => None
        }
      case VectorType => Some((x: DenseVector) =>
        x.toArray.map(_.toFloat))

    }
    funcOpt.map(f => udf(f, ArrayType(FloatType)))
  }

  /** Evaluate the model
    * @param dataset the dataset to featurize
    * @return featurized dataset
    */
  def transform(dataset: Dataset[_]): DataFrame = {
    val spark      = dataset.sparkSession
    val inputIndex = dataset.columns.indexOf(getInputCol)
    val df = dataset.toDF()

    transformSchema(df.schema) // Ccheck if the schema is correct

    if (inputIndex == -1)
      throw new IllegalArgumentException(s"Input column $getInputCol does not exist")

    val setByName  = get(outputNodeName)
    val setByIndex = get(outputNodeIndex)
    if ((setByName.isDefined && setByIndex.isDefined) ||
          (setByName.isEmpty && setByIndex.isEmpty))
      throw new Exception("Must specify one and only one of outputNodeName or outputNodeIndex")

    val coercedCol = DatasetExtensions.findUnusedColumnName("coerced")(dataset.columns.toSet)
    val (coercedDf, selectedIndex) = toArrayFloatUDF(df).map {f =>
        val d = df.withColumn(coercedCol, f(col(getInputCol)))
        (d, d.columns.indexOf(coercedCol))
    }.getOrElse((df, inputIndex))

    if (broadcastedModelOption.isEmpty) rebroadcastCNTKModel(spark)
    val broadcastedModel = broadcastedModelOption.get
    val batchedDF = getMiniBatcher.transform(coercedDf)

    val encoder = RowEncoder(batchedDF.schema
      .add(StructField(getOutputCol, ArrayType(VectorType))))

    val output = batchedDF.mapPartitions(
      CNTKModelUtils.applyModel(selectedIndex,
                                broadcastedModel,
                                getInputNode,
                                get(outputNodeName),
                                get(outputNodeIndex)))(encoder)

    val unbatchedOutput = new FlattenBatch().transform(output)

    toArrayFloatUDF(df) match {
      case Some(_) => unbatchedOutput.drop(coercedCol)
      case None    => unbatchedOutput
    }
  }

}
