// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import com.microsoft.azure.synapse.ml.featurize.{Featurize, FeaturizeUtilities}
import com.microsoft.azure.synapse.ml.lightgbm.ConnectionState._
import com.microsoft.ml.lightgbm._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.Logger

import java.io._
import java.net.{ServerSocket, Socket}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    val featureColumns = dataset.columns.filter(col => col != labelColumn &&
      !weightColumn.contains(col) && !groupColumn.contains(col)).toSeq
    new Featurize()
      .setOutputCol(featuresColumn)
      .setInputCols(featureColumns.toArray)
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumFeatures(FeaturizeUtilities.NumFeaturesTreeOrNNBased)
      .fit(dataset)
  }

  def sendDataToExecutors(hostAndPorts: ListBuffer[(Socket, String)], allConnections: String): Unit = {
    hostAndPorts.foreach(hostAndPort => {
      val writer = new BufferedWriter(new OutputStreamWriter(hostAndPort._1.getOutputStream))
      writer.write(allConnections + "\n")
      writer.flush()
    })
  }

  def closeConnections(log: Logger, hostAndPorts: ListBuffer[(Socket, String)],
                       driverServerSocket: ServerSocket): Unit = {
    log.info("driver closing all sockets and server socket")
    hostAndPorts.foreach(_._1.close())
    driverServerSocket.close()
  }

  def addSocketAndComm(hostAndPorts: ListBuffer[(Socket, String)], log: Logger,
                       comm: String, driverSocket: Socket): Unit = {
    log.info(s"driver received socket from task: $comm")
    val socketAndComm = (driverSocket, comm)
    hostAndPorts += socketAndComm
  }

  /** Handles the connection to a task from the driver.
    *
    * @param driverServerSocket The driver socket.
    * @param log The log4j logger.
    * @param hostAndPorts A list of host and ports of connected tasks.
    * @param hostToMinPartition A list of host to the minimum partition id, used for determinism.
    * @return The connection status, can be finished for barrier mode, empty task or connected.
    */
  def handleConnection(driverServerSocket: ServerSocket, log: Logger,
                       hostAndPorts: ListBuffer[(Socket, String)],
                       hostToMinPartition: mutable.Map[String, String]): ConnectionState = {
    log.info("driver accepting a new connection...")
    val driverSocket = driverServerSocket.accept()
    val reader = new BufferedReader(new InputStreamReader(driverSocket.getInputStream))
    val comm = reader.readLine()
    if (comm == LightGBMConstants.FinishedStatus) {
      log.info("driver received all tasks from barrier stage")
      Finished
    } else if (comm.startsWith(LightGBMConstants.IgnoreStatus)) {
      log.info("driver received ignore status from task")
      val hostPartition = comm.split(":")
      val host = hostPartition(1)
      val partitionId = hostPartition(2)
      updateHostToMinPartition(hostToMinPartition, host, partitionId)
      EmptyTask
    } else {
      val hostPortPartition = comm.split(":")
      val host = hostPortPartition(0)
      val port = hostPortPartition(1)
      val partitionId = hostPortPartition(2)
      updateHostToMinPartition(hostToMinPartition, host, partitionId)
      addSocketAndComm(hostAndPorts, log, s"$host:$port", driverSocket)
      Connected
    }
  }

  def updateHostToMinPartition(hostToMinPartition: mutable.Map[String, String],
                               host: String, partitionId: String): Unit = {
    if (!hostToMinPartition.contains(host) || hostToMinPartition(host) > partitionId) {
      hostToMinPartition(host) = partitionId
    }
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
