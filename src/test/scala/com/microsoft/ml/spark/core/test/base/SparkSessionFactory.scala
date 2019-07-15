// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.test.base

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// Convert configuration to JSON/ENV vars moving forward:
// 1. Logging Level
// 2. Warehouse directory
// 3. DiskBlockManager - currently defaults to USER TEMP it seems
// 3a. Does this derive from spark.local.dir? Should be configured as well?
// 4. Actual Session host instead of local
object SparkSessionFactory {

  // Default spark warehouse = ./spark-warehouse
  private val defaultWarehouseDirName = "spark-warehouse"
  private val testDir = System.currentTimeMillis.toString

  private lazy val localWarehousePath =
    "file:" +
    customNormalize(new File(currentDir, defaultWarehouseDirName)
                        .getAbsolutePath())
  val workingDir =
    "file:" +
    customNormalize(new File(currentDir, testDir)
                        .getAbsolutePath())
  // On NTFS-like systems, normalize path
  //   (solves the problem of sending a path from spark to hdfs on Windows)
  def customNormalize(path: String): String = {
    if (File.separator != "\\") path
    else path.replaceFirst("[A-Z]:", "").replace("\\", "/")
  }
  def currentDir(): String = System.getProperty("user.dir")

  def getSession(name: String, logLevel: String = "WARN", numRetries: Int): SparkSession = {
    val conf = new SparkConf()
        .setAppName(name)
        .setMaster(if (numRetries == 1){"local[*]"}else{s"local[*, $numRetries]"})
        .set("spark.logConf", "true")
        .set("spark.sql.shuffle.partitions", "20")
        .set("spark.sql.warehouse.dir", SparkSessionFactory.localWarehousePath)
    val sess = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    sess.sparkContext.setLogLevel(logLevel)
    sess
  }

}
