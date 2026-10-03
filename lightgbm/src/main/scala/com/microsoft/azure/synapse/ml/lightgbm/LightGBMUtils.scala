// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import com.microsoft.azure.synapse.ml.featurize.{Featurize, FeaturizeUtilities}
import com.microsoft.ml.lightgbm._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkEnv, TaskContext}

import java.nio.file.{Files, Path, Paths}
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntBinaryOperator
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Try

private[lightgbm] sealed trait NativeOmpCallSite {
  def name: String
}

private[lightgbm] object NativeOmpCallSite {
  case object SampledColumnDataset extends NativeOmpCallSite {
    override val name: String = "LGBM_DatasetCreateFromSampledColumn"
  }

  case object SerializedReferenceDataset extends NativeOmpCallSite {
    override val name: String = "LGBM_DatasetCreateFromSerializedReference"
  }

  case object BoosterCreate extends NativeOmpCallSite {
    override val name: String = "LGBM_BoosterCreate"
  }

  case object BoosterResetParameter extends NativeOmpCallSite {
    override val name: String = "LGBM_BoosterResetParameter"
  }

  case object DenseDataset extends NativeOmpCallSite {
    override val name: String = "LGBM_DatasetCreateFromMat"
  }

  case object SparseDataset extends NativeOmpCallSite {
    override val name: String = "LGBM_DatasetCreateFromCSR"
  }

  val Values: Seq[NativeOmpCallSite] = Seq(
    SampledColumnDataset,
    SerializedReferenceDataset,
    BoosterCreate,
    BoosterResetParameter,
    DenseDataset,
    SparseDataset)
}

private[lightgbm] final class NativeOmpThreadRegistry {
  private val highWaterMark = new AtomicInteger(0)
  private val siteHighWaterMarks = new ConcurrentHashMap[NativeOmpCallSite, AtomicInteger]()
  private val maxValue = new IntBinaryOperator {
    override def applyAsInt(left: Int, right: Int): Int = math.max(left, right)
  }

  def register(site: NativeOmpCallSite, parameters: String): Int = {
    val requestedThreads = LightGBMUtils.positiveNumThreads(parameters).getOrElse(0)
    siteHighWaterMarks.computeIfAbsent(site, _ => new AtomicInteger(0))
      .accumulateAndGet(requestedThreads, maxValue)
    highWaterMark.accumulateAndGet(requestedThreads, maxValue)
  }

  def current: Int = highWaterMark.get()

  def current(site: NativeOmpCallSite): Int = Option(siteHighWaterMarks.get(site)).map(_.get()).getOrElse(0)
}

/** Helper utilities for LightGBM learners */
object LightGBMUtils {
  private val DeviceParamNames = Set("device", "device_type")
  private val TrueValues = Set("1", "+1", "true", "yes", "on")
  private val NativeOmpThreads = new NativeOmpThreadRegistry
  private[lightgbm] val MinStreamingOmpThreads: Int = 16

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

  private[lightgbm] def parameterValues(parameters: String, names: Set[String]): Map[String, String] =
    parseLightGBMParams(parameters).filter { case (name, _) => names.contains(name) }

  private[lightgbm] def positiveNumThreads(parameters: String): Option[Int] =
    parseLightGBMParams(parameters).get("num_threads")
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)

  private[lightgbm] def registerNativeOmpThreads(site: NativeOmpCallSite, parameters: String): Int =
    NativeOmpThreads.register(site, parameters)

  private[lightgbm] def nativeOmpThreadHighWaterMark: Int = NativeOmpThreads.current

  private[lightgbm] def nativeOmpThreadHighWaterMark(site: NativeOmpCallSite): Int = NativeOmpThreads.current(site)

  private[lightgbm] def firstOmpTeamSize(value: Option[String]): Option[Int] =
    value.flatMap(_.split(",", -1).headOption)
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(token => Try(token.toInt).toOption)
      .filter(_ > 0)

  private def parseCpuAffinityToken(token: String): Option[Long] = {
    val bounds = token.split("-", -1).map(_.trim)
    bounds.length match {
      case 1 => Try(bounds(0).toInt).toOption.filter(_ >= 0).map(_ => 1L)
      case 2 => for {
        start <- Try(bounds(0).toInt).toOption
        end <- Try(bounds(1).toInt).toOption
        if start >= 0 && end >= start
      } yield end.toLong - start.toLong + 1L
      case _ => None
    }
  }

  private[lightgbm] def parseCpuAffinityList(value: String): Option[Int] = {
    val counts = value.split(",", -1).iterator.map(_.trim).map(parseCpuAffinityToken).toSeq
    if (counts.nonEmpty && counts.forall(_.isDefined)) {
      val total = counts.flatten.sum
      if (total > 0 && total <= Int.MaxValue) Option(total.toInt) else None
    } else {
      None
    }
  }

  private[lightgbm] def linuxProcessAffinityCount(statusPath: Path = Paths.get("/proc/self/status")): Option[Int] =
    Try(Files.readAllLines(statusPath).asScala
      .find(_.startsWith("Cpus_allowed_list:"))
      .flatMap(line => parseCpuAffinityList(line.substring(line.indexOf(':') + 1).trim)))
      .toOption
      .flatten

  private def positiveProcessorCount(value: Option[String]): Option[Int] =
    value.map(_.trim).filter(_.nonEmpty)
      .flatMap(token => Try(token.toInt).toOption)
      .filter(_ > 0)

  private[lightgbm] def osReportedProcessorCount(osName: String,
                                                  windowsProcessorCount: Option[String],
                                                  macLogicalCpuCount: Option[String]): Option[Int] = {
    val normalizedName = osName.toLowerCase(Locale.ROOT)
    if (normalizedName.startsWith("windows")) {
      positiveProcessorCount(windowsProcessorCount)
    } else if (normalizedName.contains("mac") || normalizedName.contains("darwin")) {
      positiveProcessorCount(macLogicalCpuCount)
    } else {
      None
    }
  }

  private def firstCommandOutput(command: Seq[String]): Option[String] =
    Try {
      val process = new ProcessBuilder(command: _*).redirectErrorStream(true).start()
      val output = Source.fromInputStream(process.getInputStream)
      try {
        val firstLine = output.getLines().take(1).toSeq.headOption
        if (process.waitFor() == 0) firstLine else None
      } finally {
        output.close()
      }
    }.toOption.flatten

  private[lightgbm] def osReportedProcessorCount(): Option[Int] = {
    val osName = Option(System.getProperty("os.name")).getOrElse("")
    val normalizedName = osName.toLowerCase(Locale.ROOT)
    val macLogicalCpuCount = if (normalizedName.contains("mac") || normalizedName.contains("darwin")) {
      val sysctl = if (Files.isExecutable(Paths.get("/usr/sbin/sysctl"))) "/usr/sbin/sysctl" else "sysctl"
      firstCommandOutput(Seq(sysctl, "-n", "hw.logicalcpu"))
    } else {
      None
    }
    osReportedProcessorCount(
      osName,
      Option(System.getenv("NUMBER_OF_PROCESSORS")),
      macLogicalCpuCount)
  }

  private[lightgbm] def streamingOmpAllocationBound(externalThreads: Int,
                                                    configuredMaxThreads: Int,
                                                    configuredNumThreads: Int,
                                                    ompNumThreads: Option[String],
                                                    affinityCount: Option[Int],
                                                    osProcessorCount: Option[Int],
                                                    availableProcessors: Int,
                                                    registeredMaxThreads: Int,
                                                    warn: String => Unit): Int = {
    if (externalThreads == 1) {
      // The initializing thread is also the only pushing thread, so LightGBM can measure its exact team.
      -1
    } else {
      val defaultTeam = firstOmpTeamSize(ompNumThreads)
        .orElse(affinityCount.filter(_ > 0))
        .getOrElse {
          warn("Unable to prove the native OpenMP team from OMP_NUM_THREADS or Linux CPU affinity; " +
            "using the best-effort maximum of the OS-reported and JVM-reported processor counts " +
            "with the conservative streaming floor.")
          Seq(MinStreamingOmpThreads, osProcessorCount.getOrElse(0), availableProcessors)
            .filter(_ > 0).max
        }
      Seq(
        MinStreamingOmpThreads,
        configuredMaxThreads,
        configuredNumThreads,
        defaultTeam,
        registeredMaxThreads).filter(_ > 0).max
    }
  }

  private[lightgbm] def isEnabledParameterValue(value: String): Boolean =
    TrueValues.contains(value.toLowerCase(Locale.ROOT))

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
