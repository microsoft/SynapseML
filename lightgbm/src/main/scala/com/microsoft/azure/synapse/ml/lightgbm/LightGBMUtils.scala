// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import com.microsoft.azure.synapse.ml.featurize.{Featurize, FeaturizeUtilities}
import com.microsoft.ml.lightgbm._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkEnv, TaskContext}

import java.util.Locale

/** Helper utilities for LightGBM learners */
object LightGBMUtils {
  private val DeviceParamNames = Set("device", "device_type")

  private def removeLightGBMQuotationSymbols(value: String): String = {
    def isQuote(char: Char): Boolean = char == '\'' || char == '"'
    value.dropWhile(isQuote).reverse.dropWhile(isQuote).reverse
  }

  private[lightgbm] def parseLightGBMParams(args: String): Map[String, String] = {
    args.split("[ \\t\\n\\r]+").iterator.filter(_.nonEmpty).foldLeft(Map.empty[String, String]) {
      case (params, token) =>
        val parts = token.split("=", -1).filter(_.nonEmpty)
        if (parts.length == 2) {
          val key = removeLightGBMQuotationSymbols(parts(0).trim)
          val value = removeLightGBMQuotationSymbols(parts(1).trim)
          if (key.nonEmpty && !params.contains(key)) params + (key -> value) else params
        } else {
          params
        }
    }
  }

  private[lightgbm] def hasDeviceParameter(parameters: String): Boolean =
    parseLightGBMParams(parameters).keys.exists(DeviceParamNames)

  private[lightgbm] def effectiveDeviceType(parameters: String): Option[String] = {
    val params = parseLightGBMParams(parameters)
    params.get("device_type").orElse(params.get("device")).map(_.toLowerCase(Locale.ROOT))
  }

  private[lightgbm] def boosterFailureGuidance(parameters: String): String = {
    effectiveDeviceType(parameters)
      .filter(device => device == LightGBMConstants.GPUDeviceType || device == LightGBMConstants.CUDADeviceType)
      .map { device =>
        s" Requested device_type=$device. SynapseML's bundled LightGBM native libraries are CPU-only; " +
          "GPU/CUDA training requires compatible custom lib_lightgbm and lib_lightgbm_swig libraries on " +
          "java.library.path for every Spark driver and executor before LightGBM is initialized."
      }
      .getOrElse("")
  }

  def validate(result: Int, component: String): Unit = {
    if (result == -1) {
      throw new Exception(component + " call failed in LightGBM with error: "
        + lightgbmlib.LGBM_GetLastError())
    }
  }

  def validateBooster(result: Int, parameters: String): Unit = {
    if (result == -1) {
      val nativeError = lightgbmlib.LGBM_GetLastError()
      val guidance = boosterFailureGuidance(parameters)
      throw new Exception(s"Booster call failed in LightGBM with error: $nativeError$guidance")
    }
  }

  def validateArray(result: SWIGTYPE_p_void, component: String): Unit = {
    if (result == null) {
      throw new Exception(component + " call failed in LightGBM with error: "
        + lightgbmlib.LGBM_GetLastError())
    }
  }

  /** Loads the native shared object binaries lib_lightgbm.so and lib_lightgbm_swig.so
    */
  def initializeNativeLibrary(): Unit = {
    val osPrefix = NativeLoader.getOSPrefix
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName(osPrefix + "_lightgbm")
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName(osPrefix + "_lightgbm_swig")
  }

  def getFeaturizer(dataset: Dataset[_], labelColumn: String, featuresColumn: String,
                    weightColumn: Option[String] = None,
                    groupColumn: Option[String] = None,
                    oneHotEncodeCategoricals: Boolean = true): PipelineModel = {
    // Create pipeline model to featurize the dataset
    val featureColumns = dataset.columns.filter(col => col != labelColumn &&
      !weightColumn.contains(col) && !groupColumn.contains(col)).toSeq
    new Featurize()
      .setOutputCol(featuresColumn)
      .setInputCols(featureColumns.toArray)
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumFeatures(FeaturizeUtilities.NumFeaturesTreeOrNNBased)
      .fit(dataset)
  }

  /** Returns an integer ID for the current worker.
    * @return In cluster, returns the executor id.  In local case, returns the partition id.
    */
  def getWorkerId: Int = {
    val executorId = SparkEnv.get.executorId
    val ctx = TaskContext.get
    val partId = ctx.partitionId
    // If driver, this is only in test scenario, make each partition a separate task
    val id = if (executorId == "driver") partId else executorId
    val idAsInt = id.toString.toInt
    idAsInt
  }

  /** Returns the partition ID for the spark Dataset.
    *
    * Used to make operations deterministic on same dataset.
    *
    * @return Returns the partition id.
    */
  def getPartitionId: Int = {
    val ctx = TaskContext.get
    ctx.partitionId
  }

  /** Returns the executor ID for the spark Dataset.
    *
    * @return Returns the executor id.
    */
  def getExecutorId: String = {
    SparkEnv.get.executorId
  }

  /** Returns true if spark is run in local mode.
    * @return True if spark is run in local mode.
    */
  def isLocalExecution: Boolean = {
    val executorId = SparkEnv.get.executorId
    executorId == "driver"
  }

  /** Returns a unique task Id for the current task run on the executor.
    * @return A unique task id.
    */
  def getTaskId: Long = {
    val ctx = TaskContext.get
    val taskId = ctx.taskAttemptId()
    taskId
  }
}
