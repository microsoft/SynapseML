// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.onnx

import ai.onnxruntime.OrtSession.SessionOptions
import ai.onnxruntime.OrtSession.SessionOptions.OptLevel
import ai.onnxruntime._
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.HasFeedFetchMaps
import com.microsoft.ml.spark.stages.{FixedMiniBatchTransformer, FlattenBatch, HasMiniBatcher}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.{ByteArrayParam, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataType.equalsStructurally
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.nio._
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

trait ONNXModelParams extends Params with HasMiniBatcher with HasFeedFetchMaps {
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

  private[onnx] def applyModel(model: Broadcast[Array[Byte]],
                               feedMap: Map[String, String],
                               fetchMap: Map[String, String]
                              )(rows: Iterator[Row]): Iterator[Row] = {
    // initialize runtime
    val session: OrtSession = initializeOrt(model.value)
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
        Row.fromSeq(row.toSeq ++ outputBatches)
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
}

class ONNXModel(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with ONNXModelParams
    with Wrappable
    with BasicLogging {

  import ONNXModel._

  logClass()

  def this() = this(Identifiable.randomUID("ONNXModel"))

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

  @transient
  private lazy val model = initializeOrt(getModelPayload)

  @transient
  lazy val modelInput: Map[String, NodeInfo] = {
    this.model.getInputInfo.asScala.toMap
  }

  @transient
  lazy val modelOutput: Map[String, NodeInfo] = {
    this.model.getOutputInfo.asScala.toMap
  }

  override def transform(dataset: Dataset[_]): DataFrame = logTransform {

    val sparkContext = dataset.sparkSession.sparkContext

    val outputSchema = transformSchema(dataset.schema) // Check if the schema is correct

    val modelPayloadBC = sparkContext.broadcast(this.getModelPayload)

    val batchedDF = getMiniBatcher.transform(dataset)
    val enc = RowEncoder(StructType(outputSchema.map(f => StructField(f.name, ArrayType(f.dataType)))))

    val outputDF = batchedDF.toDF().mapPartitions {
      rows: Iterator[Row] => applyModel(modelPayloadBC, getFeedDict, getFetchDict)(rows)
    }(enc)

    // TODO: The cache call is a workaround for GH issue 1075:
    //  https://github.com/Azure/mmlspark/issues/1075
    val cacheAttempted = if (outputDF.isStreaming) outputDF else outputDF.cache()

    val flattenedDF = new FlattenBatch().transform(cacheAttempted)
    flattenedDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
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

        if (!equalsStructurally(inputDataType, onnxExpectedDataType, ignoreNullability = true)) {
          throw new IllegalArgumentException(
            s"Onnx model input $onnxInputName expects type ${onnxExpectedDataType.simpleString}, " +
              s"but got type ${inputDataType.simpleString}")
        }
    }

    // Validate the output col names do not conflict with input schema.
    this.getFetchDict.keySet.foreach {
      colName =>
        if (schema.fieldNames.map(_.toLowerCase).contains(colName.toLowerCase)) {
          throw new IllegalArgumentException(s"Output field $colName already exists in the input schema.")
        }
    }

    // Append output cols.
    val outputFields = this.getFetchDict.map {
      case (colName, onnxOutputName) =>
        val onnxOutput = this.modelOutput.getOrElse(onnxOutputName,
          throw new IllegalArgumentException(s"""Onnx model does not have an output named "$onnxOutputName"""")
        )

        val dataType = mapValueInfoToDataType(onnxOutput.getInfo)

        StructField(colName, dataType)
    }

    StructType(schema.fields ++ outputFields)
  }
}
