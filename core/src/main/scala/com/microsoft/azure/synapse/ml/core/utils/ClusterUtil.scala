// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import java.net.InetAddress
import org.apache.http.conn.util.InetAddressUtils
import org.apache.spark.SparkContext
import org.apache.spark.injections.BlockManagerUtils
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.Logger

object ClusterUtil {
  /** Get number of tasks from dummy dataset for 1 executor.
    * Note: all executors have same number of cores,
    * and this is more reliable than getting value from conf.
    * @param spark The current spark session.
    * @param log The Logger.
    * @return The number of tasks per executor.
    */
  def getNumTasksPerExecutor(spark: SparkSession, log: Logger): Int = {
    val confTaskCpus = getTaskCpus(spark.sparkContext, log)
    try {
      val confCores = spark.sparkContext.getConf.get("spark.executor.cores").toInt
      val tasksPerExec = confCores / confTaskCpus
      log.info(s"ClusterUtils calculated num tasks per executor as $tasksPerExec from $confCores " +
        s"cores and $confTaskCpus task CPUs")
      tasksPerExec
    } catch {
      case _: NoSuchElementException =>
        // If spark.executor.cores is not defined, get the cores based on master
        val defaultNumCores = getDefaultNumExecutorCores(spark, log)
        val tasksPerExec = defaultNumCores / confTaskCpus
        log.info(s"ClusterUtils calculated num tasks per executor as $tasksPerExec from " +
          s"default num cores($defaultNumCores) from master and $confTaskCpus task CPUs")
        tasksPerExec
    }
  }

  /** Get number of rows per partition of a dataframe.  Note that this will execute a full
    * distributed Spark app query.
    * @param df The dataframe.
    * @return The number of rows per partition (where partitionId is the array index).
    */
  def getNumRowsPerPartition(df: DataFrame, labelCol: Column): Array[Long] = {
    val indexedRowCounts: Array[(Int, Long)] = df
      .select(typedLit(0.toByte))
      .rdd
      .mapPartitionsWithIndex({case (i,rows) => Iterator((i,rows.size.toLong))}, true)
      .collect()
    // Get an array where the index is implicitly the partition id
    indexedRowCounts.sortBy(pair => pair._1).map(pair => pair._2)
  }

  /** Get number of default cores from sparkSession(required) or master(optional) for 1 executor.
    * @param spark The current spark session. If master parameter is not set, the master in the spark session is used.
    * @param master This param is needed for unittest. If set, the function return the value for it.
    *               if not set, basically, master in spark (SparkSession) is used.
    * @return The number of default cores per executor based on master.
    */
  def getDefaultNumExecutorCores(spark: SparkSession, log: Logger, master: Option[String] = None): Int = {
    val masterOpt = master match {
      case Some(_) => master
      case None =>
        try {
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
    }

    // ref: https://spark.apache.org/docs/latest/configuration.html
    if (masterOpt.isEmpty) {
      val numMachineCores = getJVMCPUs(spark)
      log.info("ClusterUtils did not detect spark.master config set" +
        s"So, the number of machine cores($numMachineCores) from JVM is used")
      numMachineCores
    } else if (masterOpt.get.startsWith("spark://") || masterOpt.get.startsWith("mesos://")) {
      // all the available cores on the worker in standalone and Mesos coarse-grained modes
      val numMachineCores = getJVMCPUs(spark)
      log.info(s"ClusterUtils detected the number of executor cores from $numMachineCores machine cores from JVM" +
        s"based on master address")
      numMachineCores
    } else if (masterOpt.get.startsWith("yarn") || masterOpt.get.startsWith("k8s://")) {
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

  def getTaskCpus(sparkContext: SparkContext, log: Logger): Int = {
    try {
      val taskCpusConfig = sparkContext.getConf.getOption("spark.task.cpus")
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

  def getDriverHost(spark: SparkSession): String = {
    val blockManager = BlockManagerUtils.getBlockManager(spark)
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
    * @param spark The current spark session.
    * @return List of executors as an array of (id,host).
    */
  def getExecutors(spark: SparkSession): Array[(Int, String)] = {
    val blockManager = BlockManagerUtils.getBlockManager(spark)
    blockManager.master.getMemoryStatus.toList.flatMap({ case (blockManagerId, _) =>
      if (blockManagerId.executorId == "driver") None
      else if (blockManagerId.executorId == "fallback") None
      else Some((blockManagerId.executorId.toInt, getHostToIP(blockManagerId.host)))
    }).toArray
  }

  def getJVMCPUs(spark: SparkSession): Int = {
    import spark.implicits._
    spark.range(0, 1)
      .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head
  }

  /** Returns the number of executors * number of tasks.
    * @param spark The current spark session.
    * @param numTasksPerExec The number of tasks per executor.
    * @return The number of executors * number of tasks.
    */
  def getNumExecutorTasks(spark: SparkSession, numTasksPerExec: Int, log: Logger): Int = {
    val executors = getExecutors(spark)
    log.info(s"Retrieving executors...")
    if (!executors.isEmpty) {
      log.info(s"Retrieved num executors ${executors.length} with num tasks per executor $numTasksPerExec")
      executors.length * numTasksPerExec
    } else {
      log.info(s"Could not retrieve executors from blockmanager, trying to get from configuration...")
      val master = spark.sparkContext.master

      //TODO make this less brittle
      val rx = "local(?:\\[(\\*|\\d+)(?:,\\d+)?\\])?".r
      master match {
        case rx(null) =>  //scalastyle:ignore null
        log.info(s"Retrieved local() = 1 executor by default")
          1
        case rx("*")   =>
          log.info(s"Retrieved local(*) = ${Runtime.getRuntime.availableProcessors()} executors")
          Runtime.getRuntime.availableProcessors()
        case rx(cores) =>
          log.info(s"Retrieved local(cores) = $cores executors")
          cores.toInt
        case _         =>
          val numExecutors = BlockManagerUtils.getBlockManager(spark)
            .master.getMemoryStatus.size
          log.info(s"Using default case = $numExecutors executors")
          numExecutors
      }
    }
  }
}
