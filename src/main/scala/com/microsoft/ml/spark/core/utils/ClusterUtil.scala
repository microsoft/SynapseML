// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import java.net.InetAddress

import org.apache.http.conn.util.InetAddressUtils
import org.apache.spark.lightgbm.BlockManagerUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.Logger

object ClusterUtil {
  /** Get number of cores from dummy dataset for 1 executor.
    * Note: all executors have same number of cores,
    * and this is more reliable than getting value from conf.
    * @param dataset The dataset containing the current spark session.
    * @return The number of cores per executor.
    */
  def getNumCoresPerExecutor(dataset: Dataset[_], log: Logger): Int = {
    val spark = dataset.sparkSession
    val confTaskCpus = getTaskCpus(dataset, log)
    try {
      val confCores = spark.sparkContext.getConf
        .get("spark.executor.cores").toInt
      val coresPerExec = confCores / confTaskCpus
      log.info(s"ClusterUtils calculated num cores per executor as $coresPerExec from $confCores " +
        s"cores and $confTaskCpus task CPUs")
      coresPerExec
    } catch {
      case _: NoSuchElementException =>
        // If spark.executor.cores is not defined, get the cores based on master
        val defaultNumCores = getDefaultNumExecutorCores(dataset, log)
        val coresPerExec = defaultNumCores / confTaskCpus
        log.info(s"ClusterUtils calculated num cores per executor as $coresPerExec from " +
          s"default num cores($defaultNumCores) from master and $confTaskCpus task CPUs")
        coresPerExec
    }
  }

  def getDefaultNumExecutorCores(dataset: Dataset[_], log: Logger): Int = {
    val spark = dataset.sparkSession
    val master = try {
      val masterConf = spark.sparkContext.getConf.getOption("spark.master")
      if (masterConf.isDefined) {
        log.info(s"ClusterUtils detected spark.master config (spark.master: ${masterConf.get})")
      } else {
        log.info("ClusterUtils did not detect spark.master config set")
      }

      masterConf
    } catch {
      case _: NoSuchElementException => {
        log.info("spark.master config not set")
        None
      }
    }

    // ref: https://spark.apache.org/docs/latest/configuration.html
    if (master.isEmpty) {
      val numMachineCores = getJVMCPUs(spark)
      log.info("ClusterUtils did not detect spark.master config set" +
        s"So, the number of machine cores($numMachineCores) from JVM is used")
      numMachineCores
    } else if (master.get.startsWith("spark://") || master.get.startsWith("mesos://")) {
      // all the available cores on the worker in standalone and Mesos coarse-grained modes
      val numMachineCores = getJVMCPUs(spark)
      log.info(s"ClusterUtils detected the number of executor cores from $numMachineCores machine cores from JVM" +
        s"based on master address")
      numMachineCores
    } else if (master.get.startsWith("yarn") || master.get.startsWith("k8s://")) {
      // 1 in YARN mode
      log.info(s"ClusterUtils detected 1 as the number of executor cores based on master address")
      1
    } else {
      val numMachineCores = getJVMCPUs(spark)
      log.info(s"ClusterUtils did not detect master that has known default value." +
          s"So, the number of machine cores($numMachineCores) from JVM is used")
      numMachineCores
    }

  }

  def getTaskCpus(dataset: Dataset[_], log: Logger): Int = {
    val spark = dataset.sparkSession
    try {
      val taskCpusConfig = spark.sparkContext.getConf.getOption("spark.task.cpus")
      if (taskCpusConfig.isEmpty) {
        log.info("ClusterUtils did not detect spark.task.cpus config set, using default 1 instead")
      }
      taskCpusConfig.getOrElse("1").toInt
    } catch {
      case _: NoSuchElementException => {
        log.info("spark.task.cpus config not set, using default 1 instead")
        1
      }
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
    * @return List of executors as an array of (id,host).
    */
  def getExecutors(dataset: Dataset[_]): Array[(Int, String)] = {
    val blockManager = BlockManagerUtils.getBlockManager(dataset)
    blockManager.master.getMemoryStatus.toList.flatMap({ case (blockManagerId, _) =>
      if (blockManagerId.executorId == "driver") None
      else Some((blockManagerId.executorId.toInt, getHostToIP(blockManagerId.host)))
    }).toArray
  }

  def getJVMCPUs(spark: SparkSession): Int = {
    import spark.implicits._
    spark.range(0, 1)
      .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head
  }

  /** Returns the number of executors * number of cores.
    * @param dataset The dataset containing the current spark session.
    * @param numCoresPerExec The number of cores per executor.
    * @return The number of executors * number of cores.
    */
  def getNumExecutorCores(dataset: Dataset[_], numCoresPerExec: Int, log: Logger): Int = {
    val executors = getExecutors(dataset)
    log.info(s"Retrieving executors...")
    if (!executors.isEmpty) {
      log.info(s"Retrieved num executors ${executors.length} with num cores per executor ${numCoresPerExec}")
      executors.length * numCoresPerExec
    } else {
      log.info(s"Could not retrieve executors from blockmanager, trying to get from configuration...")
      val master = dataset.sparkSession.sparkContext.master
      val rx = "local(?:\\[(\\*|\\d+)(?:,\\d+)?\\])?".r
      master match {
        case rx(null)  => {
          log.info(s"Retrieved local() = 1 executor by default")
          1
        }
        case rx("*")   => {
          log.info(s"Retrieved local(*) = ${Runtime.getRuntime.availableProcessors()} executors")
          Runtime.getRuntime.availableProcessors()
        }
        case rx(cores) => {
          log.info(s"Retrieved local(cores) = $cores executors")
          cores.toInt
        }
        case _         => {
          val numExecutors = BlockManagerUtils.getBlockManager(dataset)
            .master.getMemoryStatus.size
          log.info(s"Using default case = $numExecutors executors")
          numExecutors
        }
      }
    }
  }
}
