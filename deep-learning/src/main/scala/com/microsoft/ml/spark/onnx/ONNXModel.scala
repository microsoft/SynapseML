// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.onnx

import ai.onnxruntime.OrtSession.SessionOptions
import ai.onnxruntime.OrtSession.SessionOptions.OptLevel
import ai.onnxruntime.{OrtSession, _}
import breeze.linalg.{argmax, softmax, DenseVector => BDV}
import com.microsoft.ml.spark.HasFeedFetchDicts
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.core.utils.BreezeUtils._
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages._
import com.microsoft.ml.spark.core.env.StreamUtilities.using
import org.apache.spark.SparkContext
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import spray.json.DefaultJsonProtocol._

import java.nio._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag

trait ONNXModelParams extends Params with HasMiniBatcher with HasFeedFetchDicts {
  val modelPayload: ByteArrayParam = new ByteArrayParam(
    this,
    "modelPayload",
    "Array of bytes containing the serialized ONNX model."
  )

  def getModelPayload: Array[Byte] = $(modelPayload)

  def setModelPayload(value: Array[Byte]): this.type = this.set(modelPayload, value)

  def setModelLocation(path: String): this.type = {
    val modelBytes = SparkContext.getOrCreate().binaryFiles(path).first()._2.toArray
    this.setModelPayload(modelBytes)
  }

  val softMaxDict: MapParam[String, String] = new MapParam[String, String](
    this,
    "softMaxDict",
    "A map between output dataframe columns, where the value column will be computed from taking " +
      "the softmax of the key column. If the 'rawPrediction' column contains logits outputs, then one can " +
      "set softMaxDict to `Map(\"rawPrediction\" -> \"probability\")` to obtain the probability outputs."
  )

  def setSoftMaxDict(value: Map[String, String]): this.type = set(softMaxDict, value)

  def setSoftMaxDict(k: String, v: String): this.type = set(softMaxDict, Map(k -> v))

  def getSoftMaxDict: Map[String, String] = get(softMaxDict).getOrElse(Map.empty)

  val argMaxDict: MapParam[String, String] = new MapParam[String, String](
    this,
    "argMaxDict",
    "A map between output dataframe columns, where the value column will be computed from taking " +
      "the argmax of the key column. This can be used to convert probability output to predicted label."
  )

  def setArgMaxDict(value: Map[String, String]): this.type = set(argMaxDict, value)

  def setArgMaxDict(k: String, v: String): this.type = set(argMaxDict, Map(k -> v))

  def getArgMaxDict: Map[String, String] = get(argMaxDict).getOrElse(Map.empty)

  setDefault(
    miniBatcher -> new FixedMiniBatchTransformer().setBatchSize(10) //scalastyle:ignore magic.number
  )
}

object ONNXModel extends ComplexParamsReadable[ONNXModel] {
  private[onnx] def initializeOrt(modelContent: Array[Byte]): OrtSession = {
    val env: OrtEnvironment = OrtEnvironment.getEnvironment
    val options = new SessionOptions()
    options.setOptimizationLevel(OptLevel.BASIC_OPT)
    env.createSession(modelContent, options)
  }

  private[onnx] def mapOnnxJavaTypeToDataType(javaType: OnnxJavaType): DataType = {
    javaType match {
      case OnnxJavaType.INT8 => ByteType
      case OnnxJavaType.INT16 => ShortType
      case OnnxJavaType.INT32 => IntegerType
      case OnnxJavaType.INT64 => LongType
      case OnnxJavaType.FLOAT => FloatType
      case OnnxJavaType.DOUBLE => DoubleType
      case OnnxJavaType.BOOL => BooleanType
      case OnnxJavaType.STRING => StringType
      case OnnxJavaType.UNKNOWN => BinaryType
    }
  }

  private[onnx] def mapTensorInfoToDataType(tensorInfo: TensorInfo): DataType = {
    val dataType = mapOnnxJavaTypeToDataType(tensorInfo.`type`)

    def nestedArrayType(depth: Int, dataType: DataType): ArrayType = {
      if (depth == 1)
        ArrayType(dataType)
      else
        ArrayType(nestedArrayType(depth - 1, dataType))
    }

    if (tensorInfo.isScalar) {
      dataType
    } else if (tensorInfo.getShape.length == 1) {
      // first dimension is assumed to be batch size.
      dataType
    } else {
      nestedArrayType(tensorInfo.getShape.length - 1, dataType)
    }
  }

  @tailrec
  private[onnx] def mapValueInfoToDataType(valueInfo: ValueInfo): DataType = {
    valueInfo match {
      case mapInfo: MapInfo =>
        val keyDataType = mapOnnxJavaTypeToDataType(mapInfo.keyType)
        val valueDataType = mapOnnxJavaTypeToDataType(mapInfo.valueType)
        MapType(keyDataType, valueDataType)
      case seqInfo: SequenceInfo =>
        if (seqInfo.sequenceOfMaps) {
          mapValueInfoToDataType(seqInfo.mapInfo)
        } else {
          mapOnnxJavaTypeToDataType(seqInfo.sequenceType)
        }
      case tensorInfo: TensorInfo =>
        mapTensorInfoToDataType(tensorInfo)
    }
  }

  private[onnx] def mapOnnxValueToArray(value: OnnxValue): Seq[Any] = {
    value.getInfo match {
      case tensorInfo: TensorInfo =>
        if (tensorInfo.isScalar)
          Seq(value.getValue)
        else {
          value.getValue.asInstanceOf[Array[_]].toSeq
        }
      case sequenceInfo: SequenceInfo =>
        if (sequenceInfo.sequenceOfMaps) {
          value.getValue.asInstanceOf[java.util.List[java.util.Map[_, _]]]
            .asScala.toArray.map(_.asScala.toMap)
        } else {
          value.getValue.asInstanceOf[java.util.List[_]].asScala
        }
      case _: MapInfo =>
        Array(value.getValue.asInstanceOf[java.util.Map[_, _]].asScala.toMap)
    }
  }

  private def flattenNested[T: ClassTag](nestedSeq: Seq[_]): Seq[T] = {
    nestedSeq.flatMap {
      case x: T => Array(x)
      case s: Seq[_] =>
        flattenNested(s)
      case a: Array[_] =>
        flattenNested(a)
    }
  }

  private[onnx] def applyModel(session: OrtSession,
                               feedMap: Map[String, String],
                               fetchMap: Map[String, String],
                               inputSchema: StructType
                              )(rows: Iterator[Row]): Iterator[Row] = {
    val env = OrtEnvironment.getEnvironment

    rows.map {
      row =>
        // Each row contains a batch
        // Get the input tensors for each input node.
        val inputTensors = session.getInputInfo.asScala.map {
          case (inputName, inputNodeInfo) =>

            val batchedValues: Seq[Any] = row.getAs[Seq[Any]](feedMap(inputName))

            inputNodeInfo.getInfo match {
              case tensorInfo: TensorInfo => // Only supports tensor input.
                val tensor = createTensor(env, tensorInfo, batchedValues)
                (inputName, tensor)
              case other =>
                throw new NotImplementedError(s"Only tensor input type is supported, but got $other instead.")
            }
        }.toMap

        // Run the tensors through the ONNX runtime.
        val result = session.run(inputTensors.asJava)

        // Map the output tensors to batches.
        val outputBatches: Seq[Seq[Any]] = fetchMap.map {
          case (_, outputName) =>
            val i = session.getOutputInfo.asScala.keysIterator.indexOf(outputName)
            val outputValue: OnnxValue = result.get(i)
            mapOnnxValueToArray(outputValue)
        }.toSeq

        // Return a row for each output batch: original payload appended with model output.
        val data = inputSchema.map(f => row.getAs[Any](f.name))
        val resultRow = Row.fromSeq(data ++ outputBatches)

        if (rows.isEmpty) {
          session.close() // Cleanup the resource if there is no more rows.
        }

        resultRow
    }
  }

  private def createTensor(env: OrtEnvironment, tensorInfo: TensorInfo, batchedValues: Seq[_]) = {
    val classTag = ClassTag(tensorInfo.`type`.clazz)
    val flattened: Array[_] = flattenNested(batchedValues)(classTag).toArray

    val shape: Array[Long] = tensorInfo.getShape
    // the first dimension of the shape can be -1 when multiple inputs are allowed. Setting it to the real
    // input size. Otherwise we cannot create the tensor from the 1D array buffer.
    shape(0) = batchedValues.length

    tensorInfo.`type` match {
      case OnnxJavaType.FLOAT =>
        val buffer = FloatBuffer.wrap(flattened.map(_.asInstanceOf[Float]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.DOUBLE =>
        val buffer = DoubleBuffer.wrap(flattened.map(_.asInstanceOf[Double]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT8 =>
        val buffer = ByteBuffer.wrap(flattened.map(_.asInstanceOf[Byte]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT16 =>
        val buffer = ShortBuffer.wrap(flattened.map(_.asInstanceOf[Short]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT32 =>
        val buffer = IntBuffer.wrap(flattened.map(_.asInstanceOf[Int]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT64 =>
        val buffer = LongBuffer.wrap(flattened.map(_.asInstanceOf[Long]))
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.STRING =>
        OnnxTensor.createTensor(env, flattened.map(_.asInstanceOf[String]), shape)
      case other =>
        throw new NotImplementedError(s"Tensor input type $other not supported. " +
          s"Only FLOAT, DOUBLE, INT8, INT16, INT32, INT64, STRING types are supported.")
    }
  }

  /**
   * Returns true if the two data types are compatible. They are compatible if they share the same "shape", and
   * 1. The element types from both sides are numeric types, or
   * 2. The element types from both sides are the same.
   */
  @tailrec
  private def compatible(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (VectorType, right: ArrayType) =>
        compatible(DoubleType, right.elementType)
      case (left: ArrayType, right: ArrayType) =>
        compatible(left.elementType, right.elementType)
      case (_: NumericType, _: NumericType) => true
      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }
}

class ONNXModel(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with ONNXModelParams
    with Wrappable
    with BasicLogging {

  import ONNXModel._

  override protected lazy val pyInternalWrapper = true

  logClass()

  def this() = this(Identifiable.randomUID("ONNXModel"))

  @transient
  lazy val modelInput: Map[String, NodeInfo] = {
    using(initializeOrt(getModelPayload)) {
      session => session.getInputInfo.asScala.toMap
    }.get
  }

  @transient
  lazy val modelOutput: Map[String, NodeInfo] = {
    using(initializeOrt(getModelPayload)) {
      session => session.getOutputInfo.asScala.toMap
    }.get
  }

  override def transform(dataset: Dataset[_]): DataFrame = logTransform {
    val inputSchema = dataset.schema
    this.validateSchema(inputSchema)

    val modelOutputSchema = getModelOutputSchema(inputSchema)

    implicit val enc: Encoder[Row] = RowEncoder(
      StructType(modelOutputSchema.map(f => StructField(f.name, ArrayType(f.dataType))))
    )

    val modelPayloadBC = dataset.sparkSession.sparkContext.broadcast(this.getModelPayload)

    // The cache call is a workaround for GH issue 1075:
    //  https://github.com/Azure/mmlspark/issues/1075
    val batchedDF = getMiniBatcher.transform(dataset).cache()
    val (coerced, feedDict) = coerceBatchedDf(batchedDF)

    val outputDf = coerced.mapPartitions {
      rows: Iterator[Row] =>
        val session = initializeOrt(modelPayloadBC.value)
        applyModel(session, feedDict, getFetchDict, inputSchema)(rows)
    }

    // The cache call is a workaround for GH issue 1075:
    //  https://github.com/Azure/mmlspark/issues/1075
    val cacheAttempted = if (outputDf.isStreaming) outputDf else outputDf.cache()

    val flattenedDF = new FlattenBatch().transform(cacheAttempted)

    (softMaxTransform _ andThen argMaxTransform) (flattenedDF)
  }

  private def softMaxTransform(input: DataFrame): DataFrame = {
    this.getSoftMaxDict.foldLeft(input) {
      case (df, (input, output)) =>
        val softmaxCol = df.schema(input).dataType match {
          case ArrayType(_: NumericType, _) =>
            val softmaxUdf = UDFUtils.oldUdf({
              array: Seq[Double] =>
                val data = BDV(array: _*)
                (data - softmax(data)).mapValues(math.exp).toSpark
            }, VectorType)
            softmaxUdf(col(input).cast(ArrayType(DoubleType)))
          case MapType(_: NumericType, _: NumericType, _) =>
            val softmaxUdf = UDFUtils.oldUdf({
              map: Map[Double, Double] =>
                val data = BDV(map.toSeq.sortBy(_._1).map(_._2): _*)
                (data - softmax(data)).mapValues(math.exp).toSpark
            }, VectorType)
            softmaxUdf(col(input).cast(MapType(DoubleType, DoubleType)))
        }

        df.withColumn(output, softmaxCol)
    }
  }

  private def argMaxTransform(input: DataFrame): DataFrame = {
    this.getArgMaxDict.foldLeft(input) {
      case (df, (input, output)) =>
        val argmaxCol = df.schema(input).dataType match {
          case ArrayType(_: NumericType, _) =>
            val argmaxUdf = UDFUtils.oldUdf({
              array: Seq[Double] => argmax(array.toArray).toDouble
            }, DoubleType)
            argmaxUdf(col(input).cast(ArrayType(DoubleType)))
          case MapType(_: NumericType, _: NumericType, _) =>
            val argmaxUdf = UDFUtils.oldUdf({
              map: Map[Double, Double] =>
                map.maxBy(_._2)._1
            }, DoubleType)
            argmaxUdf(col(input).cast(MapType(DoubleType, DoubleType)))
        }

        df.withColumn(output, argmaxCol)
    }
  }

  private def coerceBatchedDf(df: DataFrame): (DataFrame, Map[String, String]) = {
    val toArray = UDFUtils.oldUdf({
      (vectors: Seq[Vector]) => vectors.map(_.toArray)
    }, ArrayType(ArrayType(DoubleType)))

    this.modelInput.mapValues(f => ArrayType(mapValueInfoToDataType(f.getInfo)))
      .foldLeft((df, this.getFeedDict)) {
        case ((accDf, feedDict), (onnxInputName, dataType)) =>
          val originalColName = this.getFeedDict(onnxInputName)
          val coercedColName = DatasetExtensions.findUnusedColumnName(originalColName, accDf)
          val originalCol = df.schema(originalColName).dataType match {
            case ArrayType(VectorType, _) => toArray(col(originalColName))
            case _ => col(originalColName)
          }

          (
            accDf.withColumn(coercedColName, originalCol.cast(dataType)),
            feedDict.updated(onnxInputName, coercedColName)
          )
      }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)

    val modelOutputSchema = getModelOutputSchema(schema)

    val softmaxFields = this.getSoftMaxDict.map {
      case (inputCol, outputCol) =>
        getSoftMaxOutputField(inputCol, outputCol, modelOutputSchema)
    }

    val argmaxFields = this.getArgMaxDict.map {
      case (inputCol, outputCol) =>
        getArgMaxOutputField(inputCol, outputCol, modelOutputSchema)
    }

    (softmaxFields ++ argmaxFields).foldLeft(modelOutputSchema)(_ add _)
  }

  private def validateSchema(schema: StructType): Unit = {
    // Validate that input schema matches with onnx model expected input types.
    this.modelInput.foreach {
      case (onnxInputName, onnxInputInfo) =>
        val colName = this.getFeedDict.getOrElse(
          onnxInputName,
          throw new IllegalArgumentException(
            s"Onnx model input $onnxInputName is not defined in onnxInputMap parameter."
          )
        )

        val inputDataType: DataType = schema(colName).dataType
        val onnxExpectedDataType: DataType = mapValueInfoToDataType(onnxInputInfo.getInfo)

        if (!compatible(inputDataType, onnxExpectedDataType)) {
          throw new IllegalArgumentException(
            s"Onnx model input $onnxInputName expects type ${onnxExpectedDataType.simpleString}, " +
              s"but got type ${inputDataType.simpleString}")
        }
    }

    // Validate the output col names do not conflict with input schema.
    (this.getFetchDict.keySet ++ this.getSoftMaxDict.values).foreach {
      colName =>
        if (schema.fieldNames.map(_.toLowerCase).contains(colName.toLowerCase)) {
          throw new IllegalArgumentException(s"Output field $colName already exists in the input schema.")
        }
    }
  }

  /**
   * Gets the output schema from the ONNX model
   */
  private def getModelOutputSchema(inputSchema: StructType): StructType = {
    // Get ONNX model output cols.
    val modelOutputFields = this.getFetchDict.map {
      case (colName, onnxOutputName) =>
        val onnxOutput = this.modelOutput.getOrElse(onnxOutputName,
          throw new IllegalArgumentException(s"""Onnx model does not have an output named "$onnxOutputName"""")
        )

        val dataType = mapValueInfoToDataType(onnxOutput.getInfo)

        StructField(colName, dataType)
    }

    StructType(inputSchema.fields ++ modelOutputFields)
  }

  private def getSoftMaxOutputField(inputCol: String, outputCol: String, schema: StructType) = {
    val inputField = schema(inputCol)

    val outputType = inputField.dataType match {
      case MapType(_: NumericType, _: NumericType, _) => SQLDataTypes.VectorType
      case ArrayType(_: NumericType, _) => SQLDataTypes.VectorType
      case t => throw new IllegalArgumentException(
        s"Input type for Softmax must be ArrayType(NumericType) or MapType(NumericType, NumericType), " +
          s"but got $t instead."
      )
    }

    StructField(outputCol, outputType)
  }

  private def getArgMaxOutputField(inputCol: String, outputCol: String, schema: StructType) = {
    val inputField = schema(inputCol)

    val outputType = inputField.dataType match {
      case ArrayType(_: NumericType, _) => DoubleType
      case MapType(_: NumericType, _: NumericType, _) => DoubleType
      case t => throw new IllegalArgumentException(
        s"Input type for Softmax must be ArrayType(NumericType) or MapType(NumericType, NumericType), " +
          s"but got $t instead."
      )
    }

    StructField(outputCol, outputType)
  }
}
