// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import ai.onnxruntime.OrtException.OrtErrorCode
import ai.onnxruntime.OrtSession.SessionOptions
import ai.onnxruntime.OrtSession.SessionOptions.OptLevel
import ai.onnxruntime._
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.utils.CloseableIterator
import com.microsoft.azure.synapse.ml.onnx.ONNXUtils._
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

/**
 * ONNXRuntime: A wrapper around the ONNX Runtime (ORT)
 */
object ONNXRuntime extends Logging {
  private[onnx] def createOrtSession(modelContent: Array[Byte],
                                     ortEnv: OrtEnvironment,
                                     optLevel: OptLevel = OptLevel.ALL_OPT,
                                     gpuDeviceId: Option[Int] = None): OrtSession = {
    val options = new SessionOptions()

    try {
      gpuDeviceId.foreach(options.addCUDA)
    } catch {
      case exp: OrtException if exp.getCode == OrtErrorCode.ORT_INVALID_ARGUMENT =>
        val err = s"GPU device is found on executor nodes with id ${gpuDeviceId.get}, " +
          s"but adding CUDA support failed. Most likely the ONNX runtime supplied to the cluster " +
          s"does not support GPU. Please install com.microsoft.onnxruntime:onnxruntime_gpu:{version} " +
          s"instead for optimal performance. Exception details: ${exp.toString}"
        logError(err)
    }

    options.setOptimizationLevel(optLevel)
    ortEnv.createSession(modelContent, options)
  }

  private[onnx] def selectGpuDevice(deviceType: Option[String]): Option[Int] = {
    deviceType match {
      case None | Some("CUDA") =>
        val gpuNum = TaskContext.get().resources().get("gpu").flatMap(_.addresses.map(_.toInt).headOption)
        gpuNum
      case Some("CPU") =>
        None
      case _ =>
        None
    }
  }

  private[onnx] def applyModel(session: OrtSession,
                               env: OrtEnvironment,
                               feedMap: Map[String, String],
                               fetchMap: Map[String, String],
                               inputSchema: StructType)(rows: Iterator[Row]): Iterator[Row] = {
    val results = rows.map {
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
        }

        // Run the tensors through the ONNX runtime.
        val outputBatches: Seq[Seq[Any]] = using(session.run(inputTensors.asJava)) {
          result =>
            // Map the output tensors to batches.
            fetchMap.map {
              case (_, outputName) =>
                val i = session.getOutputInfo.asScala.keysIterator.indexOf(outputName)
                val outputValue: OnnxValue = result.get(i)
                mapOnnxValueToArray(outputValue)
            }.toSeq
        }.get

        // Close the tensor and clean up native handles
        inputTensors.valuesIterator.foreach {
          _.close()
        }

        // Return a row for each output batch: original payload appended with model output.
        val data = inputSchema.map(f => row.getAs[Any](f.name))
        Row.fromSeq(data ++ outputBatches)
    }

    new CloseableIterator[Row](results, {
      session.close()
      env.close()
    })
  }
}
