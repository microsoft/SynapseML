// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

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
  // Extracted so createOrtSession's control flow fits scalastyle's method-length limit; the message
  // text/rationale is unchanged from what was previously inlined at each throw/log call site.
  private def noGpuResourceAssignedMessage: String =
    "deviceType=CUDA was explicitly requested, but no Spark \"gpu\" resource is assigned to this " +
      "executor/task, so there is no GPU device id to use. Configure Spark's GPU resource " +
      "allocation (for example spark.executor.resource.gpu.amount and " +
      "spark.task.resource.gpu.amount) so a gpu resource is assigned to this task, or set " +
      "deviceType=CPU to run on CPU intentionally."

  private def explicitCudaProviderFailedMessage(gpuDeviceId: Option[Int], exp: OrtException): String =
    s"deviceType=CUDA was explicitly requested and a GPU device (id ${gpuDeviceId.get}) was " +
      s"found on this executor, but adding CUDA support failed with error code ${exp.getCode}. " +
      s"Most likely the ONNX runtime supplied to the cluster is the default " +
      s"com.microsoft.onnxruntime:onnxruntime (CPU-only) artifact, or CUDA/cuDNN aren't " +
      s"installed on this node. Add com.microsoft.onnxruntime:onnxruntime_gpu:{version} " +
      s"(excluding the transitive onnxruntime CPU artifact) and a matching CUDA/cuDNN runtime " +
      s"for GPU acceleration, or set deviceType=CPU to run on CPU intentionally."

  private def autoFallbackProviderFailedMessage(gpuDeviceId: Option[Int], exp: OrtException): String =
    s"GPU device is found on executor nodes with id ${gpuDeviceId.get}, " +
      s"but adding CUDA support failed with error code ${exp.getCode}. Most likely the ONNX " +
      s"runtime supplied to the cluster is the default com.microsoft.onnxruntime:onnxruntime " +
      s"(CPU-only) artifact, or CUDA/cuDNN aren't installed on this node. Add " +
      s"com.microsoft.onnxruntime:onnxruntime_gpu:{version} (excluding the transitive onnxruntime " +
      s"CPU artifact) and a matching CUDA/cuDNN runtime for GPU acceleration. Falling back to CPU. " +
      s"Exception details: ${exp.toString}"

  private[onnx] def createOrtSession(modelContent: Array[Byte],
                                     ortEnv: OrtEnvironment,
                                     optLevel: OptLevel = OptLevel.ALL_OPT,
                                     gpuDeviceId: Option[Int] = None,
                                     explicitCudaRequested: Boolean = false): OrtSession = {
    // deviceType=CUDA is an explicit request for GPU acceleration. If Spark never assigned a "gpu"
    // resource to this executor/task, gpuDeviceId is None and addCUDA below would simply never be
    // attempted -- silently handing back a working CPU session with no error at all. That is the same
    // "silently broken GPU" failure mode this change must not reintroduce, so fail before creating any
    // session rather than let CPU inference proceed unannounced.
    if (explicitCudaRequested && gpuDeviceId.isEmpty) {
      throw new IllegalStateException(noGpuResourceAssignedMessage)
    }

    // SessionOptions owns a native handle that ONNX Runtime's own examples close via try-with-resources
    // right after createSession returns: the session copies what it needs from options during
    // construction and never needs the options object again, so closing it afterward is safe on every
    // path. Use this file's established using(...) resource-cleanup pattern (see applyModel below) so
    // SessionOptions is closed whether we fall through to a normal/auto-fallback session, or throw the
    // explicit-CUDA fail-fast error -- but never before createSession has actually consumed it.
    using(new SessionOptions()) { options =>
      try {
        gpuDeviceId.foreach(options.addCUDA)
      } catch {
        // A "gpu" resource was assigned (gpuDeviceId is defined) but adding CUDA support still failed.
        // Silently continuing on CPU here would produce a success-shaped result while hiding a severe,
        // hard-to-notice performance regression -- exactly the "silently broken GPU" failure mode this
        // dependency change must not reintroduce. Fail fast with an actionable error instead. There is
        // currently no parameter to opt in to a graceful CPU fallback for an explicit CUDA request; add
        // one deliberately before relaxing this if that behavior is ever needed -- do not silently
        // reinstate it here.
        case exp: OrtException if explicitCudaRequested =>
          throw new IllegalStateException(explicitCudaProviderFailedMessage(gpuDeviceId, exp), exp)
        // deviceType was left unset (auto-detection): a "gpu" Spark resource was found, but CUDA isn't
        // usable here. This wasn't an explicit ask for GPU, so log a clear, actionable error and
        // continue on CPU rather than failing an otherwise-working job.
        case exp: OrtException =>
          logError(autoFallbackProviderFailedMessage(gpuDeviceId, exp))
      }

      options.setOptimizationLevel(optLevel)
      ortEnv.createSession(modelContent, options)
    }.get
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
                outputValue.getInfo match {
                  case _: SequenceInfo => ONNXValueConverter.mapSequenceToArray(outputValue)
                  case _ => mapOnnxValueToArray(outputValue)
                }
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
