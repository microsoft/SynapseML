// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import ai.onnxruntime.OrtSession.SessionOptions.OptLevel
import ai.onnxruntime._
import breeze.linalg.{argmax, softmax, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.HasFeedFetchDicts
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.onnx.ONNXRuntime._
import com.microsoft.azure.synapse.ml.onnx.ONNXUtils._
import com.microsoft.azure.synapse.ml.param.{ByteArrayParam, StringStringMapParam}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch, HasMiniBatcher}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, TaskContext}

import java.util
import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

trait ONNXModelParams extends Params with HasMiniBatcher with HasFeedFetchDicts {

  val modelPayload: ByteArrayParam = new ByteArrayParam(
    this,
    "modelPayload",
    "Array of bytes containing the serialized ONNX model."
  )

  def getModelPayload: Array[Byte] = $(modelPayload)

  val softMaxDict: StringStringMapParam = new StringStringMapParam(
    this,
    "softMaxDict",
    "A map between output dataframe columns, where the value column will be computed from taking " +
      "the softmax of the key column. If the 'rawPrediction' column contains logits outputs, then one can " +
      "set softMaxDict to `Map(\"rawPrediction\" -> \"probability\")` to obtain the probability outputs."
  )

  def setSoftMaxDict(value: Map[String, String]): this.type = set(softMaxDict, value)

  def setSoftMaxDict(value: java.util.HashMap[String, String]): this.type = set(softMaxDict, value.asScala.toMap)

  def setSoftMaxDict(k: String, v: String): this.type = set(softMaxDict, Map(k -> v))

  def getSoftMaxDict: Map[String, String] = get(softMaxDict).getOrElse(Map.empty)

  val argMaxDict: StringStringMapParam = new StringStringMapParam(
    this,
    "argMaxDict",
    "A map between output dataframe columns, where the value column will be computed from taking " +
      "the argmax of the key column. This can be used to convert probability output to predicted label."
  )

  def setArgMaxDict(value: Map[String, String]): this.type = set(argMaxDict, value)

  def setArgMaxDict(value: java.util.HashMap[String, String]): this.type = set(argMaxDict, value.asScala.toMap)

  def setArgMaxDict(k: String, v: String): this.type = set(argMaxDict, Map(k -> v))

  def getArgMaxDict: Map[String, String] = get(argMaxDict).getOrElse(Map.empty)

  val deviceType: Param[String] = new Param[String](
    this,
    "deviceType",
    "Specify a device type the model inference runs on. Supported types are: CPU or CUDA." +
      "If not specified, auto detection will be used.",
    {x => Set("CPU", "CUDA")(x.toUpperCase())}
  )

  def getDeviceType: String = $(deviceType)

  def setDeviceType(value: String): this.type = set(deviceType, value)

  val optimizationLevel: Param[String] = new Param[String](
    this,
    "optimizationLevel",
    "Specify the optimization level for the ONNX graph optimizations. Details at " +
      "https://onnxruntime.ai/docs/resources/graph-optimizations.html#graph-optimization-levels. " +
      "Supported values are: NO_OPT; BASIC_OPT; EXTENDED_OPT; ALL_OPT. Default: ALL_OPT.",
    ParamValidators.inArray(Array("NO_OPT", "BASIC_OPT", "EXTENDED_OPT", "ALL_OPT"))
  )

  def getOptimizationLevel: String = $(optimizationLevel)

  def setOptimizationLevel(value: String): this.type = set(optimizationLevel, value)

  setDefault(
    optimizationLevel -> "ALL_OPT",
    miniBatcher -> new FixedMiniBatchTransformer().setBatchSize(10) //scalastyle:ignore magic.number
  )
}

/**
 * Object model for an ONNX model:
 * OrtSession
 * |-InputInfo: Map[String, NodeInfo]
 * |-OutputInfo: Map[String, NodeInfo]
 * OrtSession is the entry point for the object model. Most importantly it defines the InputInfo and OutputInfo maps.
 * ------------------------------------
 * NodeInfo
 * |-name: String
 * |-info: ValueInfo
 * Each NodeInfo is a name and ValueInfo tuple. ValueInfo has three implementations, explained below.
 * ------------------------------------
 * TensorInfo extends ValueInfo
 * |-shape: Array[Long]
 * |-type: OnnxJavaType
 * TensorInfo is the most common type of ValueInfo. It defines the type of the tensor elements, and the shape.
 * The first dimension of the tensor is assumed to be the batch size. For example, FLOAT[-1, 3, 224, 224]
 * could represent a unlimited batch size * 3 channels * 224 height * 224 width tensor, where each element is a float.
 * ------------------------------------
 * SequenceInfo extends ValueInfo
 * |-sequenceOfMaps: Boolean
 * |-sequenceType: OnnxJavaType
 * |-mapInfo: MapInfo
 * |-length: Int
 * SequenceInfo can be a sequence of values (value type specified by sequenceType) if sequenceOfMaps is false,
 *  or a sequence of MapInfo if sequenceOfMaps is true. Sequence of MapInfo is usually used for ZipMap type of output,
 *  where the sequence represent the batch, and each MapInfo represents probability or logits outcome per class for
 *  each observation.
 * ------------------------------------
 * MapInfo extends ValueInfo
 * |-keyType: OnnxJavaType
 * |-valueType: OnnxJavaType
 * |-size: Int
 * MapInfo defines keyType, valueType and size. It is usually used inside SequenceInfo.
 */
object ONNXModel extends ComplexParamsReadable[ONNXModel]

class ONNXModel(override val uid: String)
  extends Transformer
    with ComplexParamsWritable
    with ONNXModelParams
    with Wrappable
    with SynapseMLLogging {

  override protected lazy val pyInternalWrapper = true

  logClass(FeatureNames.DeepLearning)

  def this() = this(Identifiable.randomUID("ONNXModel"))

  def modelInput: Map[String, NodeInfo] = {
    using(OrtEnvironment.getEnvironment) {
      env =>
        using(createOrtSession(getModelPayload, env)) {
          session => session.getInputInfo.asScala.toMap
        }
    }.flatten.get
  }

  def modelInputJava: util.Map[String, NodeInfo] = {
    collection.mutable.Map(modelInput.toSeq: _*).asJava
  }

  def modelOutput: Map[String, NodeInfo] = {
    using(OrtEnvironment.getEnvironment) {
      env =>
        using(createOrtSession(getModelPayload, env)) {
          session => session.getOutputInfo.asScala.toMap
        }
    }.flatten.get
  }

  def modelOutputJava: util.Map[String, NodeInfo] = {
    collection.mutable.Map(modelOutput.toSeq: _*).asJava
  }

  private var broadcastedModelPayload: Option[Broadcast[Array[Byte]]] = None

  def setModelPayload(value: Array[Byte]): this.type = {
    this.broadcastedModelPayload.foreach(_.destroy)
    broadcastedModelPayload = None
    this.set(modelPayload, value)
  }

  def rebroadcastModelPayload(spark: SparkSession): Broadcast[Array[Byte]] = {
    val bc = spark.sparkContext.broadcast(getModelPayload)
    broadcastedModelPayload = Some(bc)
    bc
  }

  def setModelLocation(path: String): this.type = {
    val modelBytes = SparkContext.getOrCreate().binaryFiles(path).first()._2.toArray
    this.setModelPayload(modelBytes)
  }

  def sliceAtOutput(output: String): ONNXModel = {
    sliceAtOutputs(Array{output})
  }

  def sliceAtOutputs(outputs: Array[String]): ONNXModel = {
    sliceModelAtOutputs(this, outputs)
  }

  override def transform(dataset: Dataset[_]): DataFrame = logTransform ({
    val inputSchema = dataset.schema
    this.validateSchema(inputSchema)

    // If the fetch dictionary indicates a request to transform the entire model, just use this model.
    // Otherwise, we need to slice the model at the requested outputs before doing the transforming.
    // We do this automatically for the caller based on how they configure the fetchDict.
    // e.g., a fetchDict of ("rawFeatures" -> "some_intermediate_output") will slice the model
    // at the output "some_intermediate_output"
    val requestedOutputs = getFetchDict.values.toSeq.sorted
    val modelOutputs = modelOutput.keys.toSeq.sorted
    val actualModel =
      if (requestedOutputs == modelOutputs) this
      else sliceAtOutputs(getFetchDict.values.toArray)

    // Due to potential slicing of model, we either use this model or a sliced one to do the actual transform
    actualModel.transformInner(dataset, inputSchema)
  }, dataset.columns.length)

  def transformInner(dataset: Dataset[_], inputSchema: StructType): DataFrame = logTransform ({
    val modelOutputSchema = getModelOutputSchema(inputSchema)

    implicit val enc: Encoder[Row] = RowEncoder(
      StructType(modelOutputSchema.map(f => StructField(f.name, ArrayType(f.dataType))))
    )

    val batchedDF = getMiniBatcher.transform(dataset)
    val (coerced, feedDict) = coerceBatchedDf(batchedDF)
    val modelBc = broadcastedModelPayload.getOrElse(rebroadcastModelPayload(dataset.sparkSession))
    val (fetchDicts, devType, optLevel) = (getFetchDict, get(deviceType), OptLevel.valueOf(getOptimizationLevel))

    val outputDf = coerced.mapPartitions {
      rows =>
        val payload = modelBc.value
        val taskId = TaskContext.get().taskAttemptId()
        val gpuDeviceId = selectGpuDevice(devType)
        val env = OrtEnvironment.getEnvironment
        logInfo(s"Task:$taskId;DeviceType=$devType;DeviceId=$gpuDeviceId;OptimizationLevel=$optLevel")
        val session = createOrtSession(payload, env, optLevel, gpuDeviceId)
        applyModel(session, env, feedDict, fetchDicts, inputSchema)(rows)
    }

    val flattenedDF = new FlattenBatch().transform(outputDf)

    (softMaxTransform _ andThen argMaxTransform) (flattenedDF)
  }, dataset.columns.length)

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

  override def copy(extra: ParamMap): ONNXModel = defaultCopy(extra)

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
    (this.getFetchDict.keySet ++ this.getSoftMaxDict.values ++ this.getArgMaxDict.values).foreach {
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
        s"Input type for Argmax must be ArrayType(NumericType) or MapType(NumericType, NumericType), " +
          s"but got $t instead."
      )
    }

    StructField(outputCol, outputType)
  }
}
