// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark

import java.net.InetAddress

import org.apache.http.conn.util.InetAddressUtils
import org.apache.spark.sql.Dataset

object ClusterUtil {

  /** Get number of cores from dummy dataset for 1 executor.
    * Note: all executors have same number of cores,
    * and this is more reliable than getting value from conf.
    * @param dataset The dataset containing the current spark session.
    * @return The number of cores per executor.
    */
  def getNumCoresPerExecutor(dataset: Dataset[_]): Int = {
    val spark = dataset.sparkSession
    try {
      val confCores = spark.sparkContext.getConf
        .get("spark.executor.cores").toInt
      val confTaskCpus = spark.sparkContext.getConf
        .get("spark.task.cpus", "1").toInt
      confCores / confTaskCpus
    } catch {
      case _: NoSuchElementException =>
        // If spark.executor.cores is not defined, get the cores per JVM
        import spark.implicits._
        val numMachineCores = spark.range(0, 1)
          .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head
        numMachineCores
    }
  }

  def getDriverHost(dataset: Dataset[_]): String = {
    val blockManager = BlockManagerUtils.getBlockManager(dataset)
    blockManager.master.getMemoryStatus.toList.flatMap({ case (blockManagerId, _) =>
      if (blockManagerId.executorId == "driver") Some(getHostToIP(blockManagerId.host))
      else None
    }).head
  }

  def getHostToIP(hostname: String): String = {
    if (InetAddressUtils.isIPv4Address(hostname) || InetAddressUtils.isIPv6Address(hostname))
      hostname
    else
      InetAddress.getByName(hostname).getHostAddress
  }

  /** Returns a list of executor id and host.
    * @param dataset The dataset containing the current spark session.
    * @param numCoresPerExec The number of cores per executor.
    * @return List of executors as an array of (id,host).
    */
  def getExecutors(dataset: Dataset[_], numCoresPerExec: Int): Array[(Int, String)] = {
    val blockManager = BlockManagerUtils.getBlockManager(dataset)
    blockManager.master.getMemoryStatus.toList.flatMap({ case (blockManagerId, _) =>
      if (blockManagerId.executorId == "driver") None
      else Some((blockManagerId.executorId.toInt, getHostToIP(blockManagerId.host)))
    }).toArray
  }

  /** Returns the number of executors * number of cores.
    * @param dataset The dataset containing the current spark session.
    * @param numCoresPerExec The number of cores per executor.
    * @return The number of executors * number of cores.
    */
  def getNumExecutorCores(dataset: Dataset[_], numCoresPerExec: Int): Int = {
    val executors = getExecutors(dataset, numCoresPerExec)
    if (!executors.isEmpty) {
      executors.length * numCoresPerExec
    } else {
      val master = dataset.sparkSession.sparkContext.master
      val rx = "local(?:\\[(\\*|\\d+)(?:,\\d+)?\\])?".r
      master match {
        case rx(null)  => 1
        case rx("*")   => Runtime.getRuntime.availableProcessors()
        case rx(cores) => cores.toInt
        case _         => BlockManagerUtils.getBlockManager(dataset)
          .master.getMemoryStatus.size
      }
    }
  }
}