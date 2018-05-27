// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.CNTK.CNTKExtensions._
import com.microsoft.CNTK.CNTKExtensions
import com.microsoft.CNTK.CNTKUtils._
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
import spray.json.DefaultJsonProtocol._
import DatasetExtensions.findUnusedColumnName
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

private object CNTKModelUtils extends java.io.Serializable {

  def applyCNTKFunction(model: CNTKFunction,
                        feedDict: Map[Variable, GenericVectorVector],
                        outputVars: List[Variable],
                        device: DeviceDescriptor): List[GenericVectorVector] = {

    val valueMap = feedDict.map { case (v, gvv) =>
      gvv match {
        case Left(fvv) => v -> Value.createDenseFloat(v.getShape, fvv, device)
        case Right(dvv) => v -> Value.createDenseDouble(v.getShape, dvv, device)
      }
    }

    val inputDataMap = new UnorderedMapVariableValuePtr()
    valueMap.foreach { case (vr, vl) => inputDataMap.add(vr, vl) }

    val outputDataMap = new UnorderedMapVariableValuePtr()

    outputVars.foreach(ov => outputDataMap.add(ov, null))
    model.evaluate(inputDataMap, outputDataMap, device)

    outputVars.map { ov: Variable =>
      ov.getDataType match {
        case CNTKDataType.Float =>
          val fvv = new FloatVectorVector()
          outputDataMap.getitem(ov).copyVariableValueToFloat(ov, fvv)
          Left(fvv)
        case CNTKDataType.Double =>
          val dvv = new DoubleVectorVector()
          outputDataMap.getitem(ov).copyVariableValueToDouble(ov, dvv)
          Right(dvv)
      }
    }
  }

  def applyModel(inputMap: Map[String, Int],
                 broadcastedModel: Broadcast[CNTKFunction],
                 outputMap: Map[String, String],
                 convertToDenseVector: Boolean
                )
                (inputRows: Iterator[Row]): Iterator[Row] = {

    if (!inputRows.hasNext) {
      Iterator() // Quickly skip empty partitions
    } else {
      val device = DeviceDescriptor.useDefaultDevice
      //CNTKLib.SetMaxNumCPUThreads(1)
      val m = CNTKExtensions.fromSerializable(broadcastedModel.value).clone(ParameterCloningMethod.Share)

      val inputMapVar = inputMap.map { case (k, v) => v -> m.getInputVar(k) }
      val outputMapVar = outputMap.map { case (k, v) => m.getOutputVar(v) -> k }

      val preprocessFunction: Row => Map[Variable, GenericVectorVector] = {
        val inputExtractors = inputMapVar.map {
          case (colnum, variable) =>
            variable -> {
              variable.getDataType match {
                case CNTKDataType.Float =>
                  r: Row => SSFToGVV(r.getAs[Seq[Seq[Float]]](colnum))
                case CNTKDataType.Double =>
                  r: Row => SSDToGVV(r.getAs[Seq[Seq[Double]]](colnum))
              }
            }
        }

        { row: Row => inputExtractors.mapValues(f => f(row)) }
      }

      val outputVars = outputMapVar.keys.toList
      val floatConverter = if (convertToDenseVector) {
        { fvv: FloatVectorVector => toSeqDV(fvv) }
      } else {
        { fvv: FloatVectorVector => toSeqSeq(fvv) }
      }
      val doubleConverter = if (convertToDenseVector) {
        { dvv: DoubleVectorVector => toSeqDV(dvv) }
      } else {
        { dvv: DoubleVectorVector => toSeqSeq(dvv) }
      }

      val outputVarVector = new VariableVector()
      outputVars.foreach(outputVarVector.add)
      val of = CNTKLib.Combine(outputVarVector)

      inputRows.map { row =>
        val feedDict = preprocessFunction(row)
        val outputGVVs = applyCNTKFunction(of, feedDict, outputVars, device)
        val resultRow = Row(outputGVVs.map {
          case Left(vv) => floatConverter(vv)
          case Right(vv) => doubleConverter(vv)
        }:_*)
        val outputRow = Row.merge(row, resultRow)
        feedDict.values.foreach(deleteGVV)
        outputGVVs.foreach(deleteGVV)
        outputRow
      }
    }
  }

}

object CNTKModel extends ComplexParamsReadable[CNTKModel]

@InternalWrapper
class CNTKModel(override val uid: String) extends Model[CNTKModel] with ComplexParamsWritable
  with HasMiniBatcher with Wrappable {

  def this() = this(Identifiable.randomUID("CNTKModel"))

  /** Array of bytes containing the serialized CNTK <code>Function</code>
    *
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
  def setModelLocation(path: String): this.type = {
    val modelBytes = SparkContext.getOrCreate().binaryFiles(path).first()._2.toArray
    setModel(CNTKFunction.loadModelFromBytes(modelBytes))
  }

  val batchInput = new BooleanParam(this, "batchInput",
    "whether to use a batcher")

  setDefault(batchInput -> true)

  def setBatchInput(v: Boolean): this.type = set(batchInput, v)

  def getBatchInput: Boolean = $(batchInput)

  val shapeOutput = new BooleanParam(this, "shapeOutput",
    "whether to shape the output")

  setDefault(shapeOutput -> false)

  def setShapeOutput(v: Boolean): this.type = set(shapeOutput, v)

  def getShapeOutput: Boolean = $(shapeOutput)

  val convertOutputToDenseVector = new BooleanParam(this, "convertOutputToDenseVector",
    "whether to convert the output to dense vectors")

  setDefault(convertOutputToDenseVector -> true)

  def setConvertOutputToDenseVector(v: Boolean): this.type = set(convertOutputToDenseVector, v)

  def getConvertOutputToDenseVector: Boolean = $(convertOutputToDenseVector)

  val feedDict: MapParam[String, String] = new MapParam[String, String](this, "feedDict",
    " Map of CNTK Variable names (keys) and Column Names (values)")

  setDefault(feedDict -> Map((argumentPrefix + 0) -> (argumentPrefix + 0)))

  def setFeedDict(value: Map[String, String]): this.type = set(feedDict, value)

  def setFeedDict(k: String, v: String): this.type = set(feedDict, Map(k -> v))

  def getFeedDict: Map[String, String] = $(feedDict)

  val fetchDict: MapParam[String, String] = new MapParam[String, String](this, "fetchDict",
    " Map of Column Names (keys) and CNTK Variable names (values)")
  setDefault(fetchDict -> Map((outputPrefix + 0) -> (outputPrefix + 0)))

  def setFetchDict(value: Map[String, String]): this.type = set("fetchDict", value)

  def setFetchDict(k: String, v: String): this.type = set(fetchDict, Map(k -> v))

  def getFetchDict: Map[String, String] = $(fetchDict)

  // Alternative Input APIs

  def setInputNodeIndex(value: Int): this.type = {
    val fd = getFeedDict
    if (fd.isEmpty) {
      setFeedDict(argumentPrefix + value, argumentPrefix + value)
    } else if (fd.size == 1) {
      setFeedDict(argumentPrefix + value, fd.values.head)
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  def getInputNodeIndex: Int = {
    val fd = getFeedDict
    if (fd.size == 1) {
      fd.keys.head match {
        case node if node.startsWith(argumentPrefix) =>
          node.stripPrefix(argumentPrefix).toInt
        case _ => throw new RuntimeException("Feed dict did not have the proper structure")
      }
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  def setInputNode(value: String): this.type = {
    val fd = getFeedDict
    if (fd.isEmpty) {
      setFeedDict(value, value)
    } else if (fd.size == 1) {
      setFeedDict(value, fd.values.head)
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  def getInputNode: String = {
    val fd = getFeedDict
    if (fd.size == 1) {
      fd.keys.head
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  def setInputCol(value: String): this.type = {
    val fd = getFeedDict
    if (fd.isEmpty) {
      setFeedDict(value, value)
    } else if (fd.size == 1) {
      setFeedDict(fd.keys.head, value)
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  def getInputCol: String = {
    val fd = getFeedDict
    if (fd.size == 1) {
      fd.values.head
    } else {
      throw new IllegalArgumentException("existing feed dict has too many elements," +
        " consider using the more expressive feedDict param directly")
    }
  }

  // Alternative Output APIs

  def setOutputNodeIndex(value: Int): this.type = {
    val fd = getFetchDict
    if (fd.isEmpty) {
      setFetchDict(outputPrefix + value, outputPrefix + value)
    } else if (fd.size == 1) {
      setFetchDict(fd.keys.head, outputPrefix + value)
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  def getOutputNodeIndex: Int = {
    val fd = getFetchDict
    if (fd.size == 1) {
      fd.values.head match {
        case node if node.startsWith(outputPrefix) =>
          node.stripPrefix(outputPrefix).toInt
        case _ => throw new RuntimeException("Fetch dict did not have the proper structure")
      }
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  def setOutputNode(value: String): this.type = {
    val fd = getFetchDict
    if (fd.isEmpty) {
      setFetchDict(value, value)
    } else if (fd.size == 1) {
      setFetchDict(fd.keys.head, value)
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  def getOutputNode: String = {
    val fd = getFetchDict
    if (fd.size == 1) {
      fd.values.head
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  def setOutputCol(value: String): this.type = {
    val fd = getFetchDict
    if (fd.isEmpty) {
      setFetchDict(value, value)
    } else if (fd.size == 1) {
      setFetchDict(value, fd.values.head)
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  def getOutputCol: String = {
    val fd = getFetchDict
    if (fd.size == 1) {
      fd.keys.head
    } else {
      throw new IllegalArgumentException("existing fetch dict has too many elements," +
        " consider using the more expressive fetchDict param directly")
    }
  }

  /** Returns the dimensions of the required input */
  def getInputShapes: List[Array[Int]] = {
    getModel.getArguments.toList.map(_.getShape.getDimensions.map(_.toInt))
  }

  setDefault(miniBatcher -> new FixedMiniBatchTransformer().setBatchSize(10))

  private def getElementType(t: DataType): DataType = {
    t match {
      case ArrayType(et, _) => getElementType(et)
      case et => et
    }
  }

  def transformSchema(schema: StructType): StructType = {
    getFeedDict.foreach { case (varName, colName) =>
      val colType = schema(colName).dataType
      val innerTypes = Set(VectorType,
        ArrayType(DoubleType, true),
        ArrayType(FloatType, true),
        ArrayType(DoubleType, false),
        ArrayType(FloatType, false))
      val allowedTypes = if (getBatchInput) {
        innerTypes
      } else {
        innerTypes.map(ArrayType(_))
      }
      assert(allowedTypes.contains(colType))
    }

    if (getConvertOutputToDenseVector) {
      getFetchDict.toList.sorted
        .foldLeft(schema) { case (st, (varname, colname)) => st.add(colname, VectorType) }
    } else {
      getModel.getOutputSchema(getFetchDict)
        .foldLeft(schema) { case (st, sf) => st.add(sf) }
    }

  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def rebroadcastCNTKModel(spark: SparkSession): Unit = {
    broadcastedModelOption = Some(spark.sparkContext.broadcast(getModel))
  }

  private val coercionPrefix = s"coerced_$uid"

  private def coerceType(schema: StructType, colname: String, targetElementType: DataType):
  (Option[(UserDefinedFunction, String)]) = {
    val colType = schema(colname).dataType match {
      case ArrayType(dt, _) => dt
    }

    val funcOpt = (colType, targetElementType) match {
      case (VectorType, DoubleType) =>
        Some({ av: Seq[DenseVector] => av.map(_.toArray) })
      case (VectorType, FloatType) =>
        Some({ av: Seq[DenseVector] => av.map(_.toArray.map(_.toFloat)) })
      case (ArrayType(FloatType, _), DoubleType) =>
        Some({ av: Seq[Seq[Float]] => av.map(_.map(_.toDouble)) })
      case (ArrayType(DoubleType, _), FloatType) =>
        Some({ av: Seq[Seq[Double]] => av.map(_.map(_.toFloat)) })
      case (ArrayType(DoubleType, _), DoubleType) =>
        None
      case (ArrayType(FloatType, _), FloatType) =>
        None
    }

    funcOpt.map { f =>
      (udf(f, ArrayType(ArrayType(targetElementType))),
        findUnusedColumnName(coercionPrefix, schema))
    }
  }

  private def coerceDFAndFeedDict(df: DataFrame,
                                  feedDict: Map[String, String]
                                 ): (DataFrame, Map[String, String]) = {
    feedDict.foldLeft((df, feedDict)) {
      case ((dfInternal, fdInternal), (varname, colname)) =>
        val elementType = variableToElementType(getModel.getInputVar(varname))
        coerceType(dfInternal.schema, colname, elementType) match {
          case Some((udfVal, newColName)) =>
            (
              dfInternal.withColumn(newColName, udfVal(col(colname))),
              fdInternal.updated(varname, newColName)
            )
          case None =>
            (dfInternal, feedDict)
        }
    }
  }

  /** Evaluate the model
    *
    * @param dataset the dataset to featurize
    * @return featurized dataset
    */
  def transform(dataset: Dataset[_]): DataFrame = {
    val spark = dataset.sparkSession
    val df = dataset.toDF()

    transformSchema(df.schema) // Check if the schema is correct

    val batchedDF = if (getBatchInput) {
      getMiniBatcher.transform(df)
    } else {
      df
    }

    val (preprocessedDF, coercedFeedDict) =
      coerceDFAndFeedDict(batchedDF, getFeedDict)

    val columnIndexToVar = coercedFeedDict.map { case (k, v) =>
      k -> preprocessedDF.schema.fieldIndex(v) }

    if (broadcastedModelOption.isEmpty) rebroadcastCNTKModel(spark)

    val encoder = RowEncoder(getModel.getOutputSchema(getFetchDict)
      .foldLeft(preprocessedDF.schema) { case (st, sf) =>
        if (getConvertOutputToDenseVector)
          st.add(sf.name, ArrayType(VectorType))
        else
          st.add(sf.name, ArrayType(sf.dataType))
      })

    val outputDF = preprocessedDF.mapPartitions { it =>
      CNTKModelUtils.applyModel(
        columnIndexToVar,
        broadcastedModelOption.get,
        getFetchDict,
        getConvertOutputToDenseVector)(it)
    }(encoder)

    val droppedDF = outputDF.drop(outputDF.columns.filter(_.startsWith(coercionPrefix)): _*)

    val unbatchedDF = if (getBatchInput) {
      new FlattenBatch().transform(droppedDF)
    } else {
      droppedDF
    }

    unbatchedDF
  }

}
