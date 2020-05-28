// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import java.io._
import java.net.{ServerSocket, Socket}
import java.util.concurrent.Executors

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.core.env.NativeLoader
import com.microsoft.ml.spark.core.utils.ClusterUtil
import com.microsoft.ml.spark.featurize.{Featurize, FeaturizeUtilities}
import org.apache.spark.lightgbm.BlockManagerUtils
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.Logger

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/** Helper utilities for LightGBM learners */
object LightGBMUtils {
  def validate(result: Int, component: String): Unit = {
    if (result == -1) {
      throw new Exception(component + " call failed in LightGBM with error: "
        + lightgbmlib.LGBM_GetLastError())
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
    val featuresToHashTo = FeaturizeUtilities.NumFeaturesTreeOrNNBased
    val featureColumns = dataset.columns.filter(col => col != labelColumn &&
      !weightColumn.contains(col) && !groupColumn.contains(col)).toSeq
    val featurizer = new Featurize()
      .setFeatureColumns(Map(featuresColumn -> featureColumns))
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumberOfFeatures(featuresToHashTo)
    featurizer.fit(dataset)
  }

  def getBoosterPtrFromModelString(lgbModelString: String): SWIGTYPE_p_void = {
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    val numItersOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterLoadModelFromString(lgbModelString, numItersOut, boosterOutPtr),
      "Booster LoadFromString")
    lightgbmlib.voidpp_value(boosterOutPtr)
  }

  def getCategoricalIndexes(df: DataFrame,
                            featuresCol: String,
                            slotNames: Array[String],
                            categoricalColumnIndexes: Array[Int],
                            categoricalColumnSlotNames: Array[String]): Array[Int] = {
    val categoricalIndexes = if(slotNames.nonEmpty) {
      categoricalColumnSlotNames.map(slotNames.indexOf(_))
    } else {
      val categoricalSlotNamesSet = HashSet(categoricalColumnSlotNames: _*)
      val featuresSchema = df.schema(featuresCol)
      val metadata = AttributeGroup.fromStructField(featuresSchema)
      if (metadata.attributes.isEmpty) Array[Int]()
      else {
        metadata.attributes.get.zipWithIndex.flatMap {
          case (null, _) => Iterator()
          case (attr, idx) =>
            if (attr.name.isDefined && categoricalSlotNamesSet.contains(attr.name.get)) {
              Iterator(idx)
            } else {
              attr match {
                case _: NumericAttribute | UnresolvedAttribute => Iterator()
                // Note: it seems that BinaryAttribute is not considered categorical,
                // since all OHE cols are marked with this, but StringIndexed are always Nominal
                case _: BinaryAttribute => Iterator()
                case _: NominalAttribute => Iterator(idx)
              }
            }
        }
      }
    }

    categoricalColumnIndexes.union(categoricalIndexes).distinct
  }

  /**
    * Opens a socket communications channel on the driver, starts a thread that
    * waits for the host:port from the executors, and then sends back the
    * information to the executors.
    *
    * @param numWorkers The total number of training workers to wait for.
    * @return The address and port of the driver socket.
    */
  def createDriverNodesThread(numWorkers: Int, df: DataFrame,
                              log: Logger, timeout: Double,
                              barrierExecutionMode: Boolean,
                              driverServerPort: Int): (String, Int, Future[Unit]) = {
    // Start a thread and open port to listen on
    implicit val context: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    val driverServerSocket = new ServerSocket(driverServerPort)
    // Set timeout on socket
    val duration = Duration(timeout, SECONDS)
    if (duration.isFinite()) {
      driverServerSocket.setSoTimeout(duration.toMillis.toInt)
    }
    val f = Future {
      var emptyWorkerCounter = 0
      val hostAndPorts = ListBuffer[(Socket, String)]()
      if (barrierExecutionMode) {
        log.info(s"driver using barrier execution mode")
        var finished = false
        while (!finished) {
          log.info("driver accepting a new connection...")
          val driverSocket = driverServerSocket.accept()
          val reader = new BufferedReader(new InputStreamReader(driverSocket.getInputStream))
          val comm = reader.readLine()
          if (comm == LightGBMConstants.FinishedStatus) {
            log.info("driver received all workers from barrier stage")
            finished = true
          } else if (comm == LightGBMConstants.IgnoreStatus) {
            log.info("driver received ignore status from worker")
          } else {
            log.info(s"driver received socket from worker: $comm")
            val socketAndComm = (driverSocket, comm)
            hostAndPorts += socketAndComm
          }
        }
      } else {
        log.info(s"driver expecting $numWorkers connections...")
        while (hostAndPorts.size + emptyWorkerCounter < numWorkers) {
          log.info("driver accepting a new connection...")
          val driverSocket = driverServerSocket.accept()
          val reader = new BufferedReader(new InputStreamReader(driverSocket.getInputStream))
          val comm = reader.readLine()
          if (comm == LightGBMConstants.IgnoreStatus) {
            log.info("driver received ignore status from worker")
            emptyWorkerCounter += 1
          } else {
            log.info(s"driver received socket from worker: $comm")
            val socketAndComm = (driverSocket, comm)
            hostAndPorts += socketAndComm
          }
        }
      }
      // Concatenate with commas, eg: host1:port1,host2:port2, ... etc
      val allConnections = hostAndPorts.map(_._2).mkString(",")
      log.info(s"driver writing back to all connections: $allConnections")
      // Send data back to all threads on executors
      hostAndPorts.foreach(hostAndPort => {
        val writer = new BufferedWriter(new OutputStreamWriter(hostAndPort._1.getOutputStream))
        writer.write(allConnections + "\n")
        writer.flush()
      })
      log.info("driver closing all sockets and server socket")
      hostAndPorts.foreach(_._1.close())
      driverServerSocket.close()
    }
    val host = ClusterUtil.getDriverHost(df)
    val port = driverServerSocket.getLocalPort
    log.info(s"driver waiting for connections on host: $host and port: $port")
    (host, port, f)
  }

  /** Returns an integer ID for the current node.
    *
    * @return In cluster, returns the executor id.  In local case, returns the worker id.
    */
  def getId(): Int = {
    val executorId = SparkEnv.get.executorId
    val ctx = TaskContext.get
    val partId = ctx.partitionId
    // If driver, this is only in test scenario, make each partition a separate worker
    val id = if (executorId == "driver") partId else executorId
    val idAsInt = id.toString.toInt
    idAsInt
  }

  def generateData(numRows: Int, rowsAsDoubleArray: Array[Array[Double]]):
  (SWIGTYPE_p_void, SWIGTYPE_p_double) = {
    val numCols = rowsAsDoubleArray.head.length
    val data = lightgbmlib.new_doubleArray(numCols.toLong * numRows.toLong)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
      ri._1.zipWithIndex.foreach(value =>
        lightgbmlib.doubleArray_setitem(data, (value._2 + (ri._2 * numCols)).toLong, value._1)))
    (lightgbmlib.double_to_voidp_ptr(data), data)
  }

  def generateDenseDataset(numRows: Int, rowsAsDoubleArray: Array[Array[Double]],
                           referenceDataset: Option[LightGBMDataset],
                           featureNamesOpt: Option[Array[String]],
                           trainParams: TrainParams): LightGBMDataset = {
    val numCols = rowsAsDoubleArray.head.length
    val isRowMajor = 1
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = s"max_bin=${trainParams.maxBin} is_pre_partition=True " +
      s"bin_construct_sample_cnt=${trainParams.binSampleCount}" +
      (if (trainParams.categoricalFeatures.isEmpty) ""
      else s"categorical_feature=${trainParams.categoricalFeatures.mkString(",")}")
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
    var data: Option[(SWIGTYPE_p_void, SWIGTYPE_p_double)] = None
    try {
      data = Some(generateData(numRows, rowsAsDoubleArray))
      // Generate the dataset for features
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(
        data.get._1, data64bitType,
        numRows, numCols,
        isRowMajor, datasetParams, referenceDataset.map(_.dataset).orNull, datasetOutPtr),
        "Dataset create")
    } finally {
      if (data.isDefined) lightgbmlib.delete_doubleArray(data.get._2)
    }
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(datasetOutPtr))
    dataset.setFeatureNames(featureNamesOpt, numCols)
    dataset
  }

  /** Generates a sparse dataset in CSR format.
    *
    * @param sparseRows The rows of sparse vector.
    * @return
    */
  def generateSparseDataset(sparseRows: Array[SparseVector],
                            referenceDataset: Option[LightGBMDataset],
                            featureNamesOpt: Option[Array[String]],
                            trainParams: TrainParams): LightGBMDataset = {
    val numCols = sparseRows(0).size

    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = s"max_bin=${trainParams.maxBin} is_pre_partition=True " +
      (if (trainParams.categoricalFeatures.isEmpty) ""
       else s"categorical_feature=${trainParams.categoricalFeatures.mkString(",")}")
    // Generate the dataset for features
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromCSRSpark(
      sparseRows.asInstanceOf[Array[Object]],
      sparseRows.length,
      numCols, datasetParams, referenceDataset.map(_.dataset).orNull,
      datasetOutPtr),
      "Dataset create")
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(datasetOutPtr))
    dataset.setFeatureNames(featureNamesOpt, numCols)
    dataset
  }
}
