// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import ai.onnx.proto.OnnxMl.{GraphProto, ModelProto, NodeProto, TensorShapeProto, TypeProto, ValueInfoProto}
import ai.onnxruntime._
import com.google.protobuf.ProtocolStringList
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._

import java.nio._
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag

object ONNXUtils {
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

  private[onnx] def createTensor(env: OrtEnvironment, tensorInfo: TensorInfo, batchedValues: Seq[_]): OnnxTensor = {
    val shape: Array[Long] = tensorInfo.getShape
    // the first dimension of the shape can be -1 when multiple inputs are allowed. Setting it to the real
    // input size. Otherwise we cannot create the tensor from the 1D array buffer.
    if (shape.tail.contains(-1))
      throw new Exception(s"The input tensor has shape [${shape.mkString}], " +
        s"but -1 is only allowed at the first dimension (batch size).")
    shape(0) = batchedValues.length

    val size = shape.product.toInt

    tensorInfo.`type` match {
      case OnnxJavaType.FLOAT =>
        val buffer = FloatBuffer.allocate(size)
        writeNestedSeqToBuffer[Float](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.DOUBLE =>
        val buffer = DoubleBuffer.allocate(size)
        writeNestedSeqToBuffer[Double](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT8 =>
        val buffer = ByteBuffer.allocate(size)
        writeNestedSeqToBuffer[Byte](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT16 =>
        val buffer = ShortBuffer.allocate(size)
        writeNestedSeqToBuffer[Short](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT32 =>
        val buffer = IntBuffer.allocate(size)
        writeNestedSeqToBuffer[Int](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.INT64 =>
        val buffer = LongBuffer.allocate(size)
        writeNestedSeqToBuffer[Long](batchedValues, buffer.put(_))
        buffer.rewind()
        OnnxTensor.createTensor(env, buffer, shape)
      case OnnxJavaType.STRING =>
        val flattened = writeNestedSeqToStringBuffer(batchedValues, size).toArray
        OnnxTensor.createTensor(env, flattened, shape)
      case other =>
        throw new NotImplementedError(s"Tensor input type $other not supported. " +
          s"Only FLOAT, DOUBLE, INT8, INT16, INT32, INT64, STRING types are supported.")
    }
  }

  private def writeNestedSeqToBuffer[T: ClassTag](nestedSeq: Seq[_], bufferWrite: T => Unit): Unit = {
    nestedSeq.foreach {
      case x: T => bufferWrite(x)
      case s: Seq[_] =>
        writeNestedSeqToBuffer(s, bufferWrite)
    }
  }

  private def writeNestedSeqToStringBuffer(nestedSeq: Seq[_], size: Int): ArrayBuffer[String] = {
    var i = 0
    val buffer = ArrayBuffer.fill[String](size)("")

    def innerWrite(nestedSeq: Seq[_]): Unit = {
      nestedSeq.foreach {
        case x: String =>
          buffer.update(i, x)
          i = i + 1
        case s: Seq[_] =>
          innerWrite(s)
      }
    }

    innerWrite(nestedSeq)
    buffer
  }

  /**
    * Returns true if the two data types are compatible. They are compatible if they share the same "shape", and
    * 1. The element types from both sides are numeric types, or
    * 2. The element types from both sides are the same.
    */
  @tailrec
  private[onnx] def compatible(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (VectorType, right: ArrayType) =>
        compatible(DoubleType, right.elementType)
      case (left: ArrayType, right: ArrayType) =>
        compatible(left.elementType, right.elementType)
      case (_: NumericType, _: NumericType) => true
      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  /*
   * Create a new ONNXModel from an existing model, but shrink to only include given outputs
   */
  private[onnx] def sliceModelAtOutputs(fullModel: ONNXModel, outputs: Array[String]): ONNXModel = {
    val fullProtobufModel = ModelProto.parseFrom(fullModel.getModelPayload)

    val (newNodes, newOutputs) = findUsedNodesForOutputs(fullProtobufModel, outputs)

    // Make a new model with the reduced set of nodes
    val slicedGraph = makeGraph(newNodes, newOutputs, fullProtobufModel.getGraph)
    val slicedProtobufModel = makeModel(slicedGraph, fullProtobufModel)

    // Return a new model with all the same parameters, but with a new protobuf model
    fullModel
      .copy(ParamMap.empty)
      .setModelPayload(slicedProtobufModel.toByteArray)
  }

  private def findUsedNodesForOutputs(model: ModelProto, newOutputNames: Array[String])
      : (Array[NodeProto], Array[ValueInfoProto]) = {
    val graph = model.getGraph
    val nodes = graph.getNodeList.toArray.map(_.asInstanceOf[NodeProto])

    val allInternalOutputs = nodes.flatMap(node => node.getNodeOutputNames)
    newOutputNames.foreach(out => {
      if (!allInternalOutputs.contains(out)) throw new IllegalArgumentException(s"Unknown output: $out")
    })

    // This is an array which will track which nodes are needed in the new model
    val nodeUsageStatus = collection.mutable.Map[String, Boolean]()
    nodes.foreach(node => nodeUsageStatus(node.getName) = false)

    val outputNameToNodeMap = nodes.flatMap(node => node.getNodeOutputNames.map(name => name -> node)).toMap

    // Recursive method to traverse graph backwards, marking nodes as used or not
    def markAsUsed(node: NodeProto): Unit = {
      // If the node is already marked, skip it
      if (!nodeUsageStatus(node.getName)) {
        nodeUsageStatus(node.getName) = true
        // This input might be an actual external input variable or initializer, which has no upstream node
        // Otherwise continue up the graph chain marking other upstream nodes as used.
        node.getNodeInputNames.foreach(in => {
          if (outputNameToNodeMap.contains(in)) markAsUsed(outputNameToNodeMap(in))
        })
      }
    }

    // Starting at the outputs we wish to slice at, mark all nodes as needed or not recursively
    newOutputNames.foreach(out => markAsUsed(outputNameToNodeMap(out)))

    val newNodes = nodes.filter(node => nodeUsageStatus(node.getName))
    val newOutputs = newOutputNames.map(out => ValueInfoProto.newBuilder().setName(out).build())
    (newNodes, newOutputs)
  }

  /*
   * TODO
   */
  private def batchify(source: ModelProto): ModelProto = {
    val model = ModelProto.newBuilder(source)
    val graph = GraphProto.newBuilder(model.getGraph)
    val input = ValueInfoProto.newBuilder(graph.getInput(0))
    val typeProto = TypeProto.newBuilder(input.getType)
    val tensorType = TypeProto.Tensor.newBuilder(typeProto.getTensorType)
    val shape = TensorShapeProto.newBuilder(tensorType.getShape)
    val dim0 = TensorShapeProto.Dimension.newBuilder(shape.getDim(0))
    if (dim0.hasDimValue && dim0.getDimValue == 1) {
      dim0.setDimParam("N")
      shape.setDim(0, dim0)
      tensorType.setShape(shape)
      typeProto.setTensorType(tensorType)
      input.setType(typeProto)
      graph.setInput(0, input)
      model.setGraph(graph)
      model.build()
    } else source
  }

  /*
   * Construct a GraphProto from a reference graph with a given set of nodes and outputs
   */
  private def makeGraph(nodes: Array[NodeProto],
                        outputs: Array[ValueInfoProto],
                        source: GraphProto): GraphProto = {
    val graph = GraphProto.newBuilder(source)
    graph.clearNode()
    nodes.foreach(node => graph.addNode(node))
    graph.clearOutput()
    outputs.foreach(output => graph.addOutput(output))
    graph.build()
  }

  /*
   * Construct a ModelProto from a given graph and reference model
   */
  private def makeModel(graph: GraphProto, source: ModelProto): ModelProto = {
    val model = ModelProto.newBuilder(source)
    model.setGraph(graph).build()
  }

  implicit class AugmentedProtocolStringList(list: ProtocolStringList) {
    def toStringArray: Array[String] = {
      list.toArray().map(_.asInstanceOf[String])
    }
  }

  implicit class AugmentedNodeProto(node: NodeProto) {
    def getNodeOutputNames: Array[String] = {
      node.getOutputList.toStringArray
    }

    def getNodeInputNames: Array[String] = {
      node.getInputList.toStringArray
    }
  }
}
