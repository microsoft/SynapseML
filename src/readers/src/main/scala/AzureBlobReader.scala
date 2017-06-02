// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark
import com.microsoft.ml.spark.FileFormat.FileFormat
import java.util.NoSuchElementException
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scala.util.parsing.json._

object AzureBlobReader {
  def read (accountName: String, accountKey: String, containerName: String, filePath: String,
            fileFormat: String, hasHeader: Boolean): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    val fileFormatEnum = ReaderUtils.getFileFormat(fileFormat)

    // Register the credential
    if (!ReaderUtils.isNullOrEmpty(accountKey)) {
      val config = spark.sparkContext.hadoopConfiguration
      val azureAccountKeyPrefix = "fs.azure.account.key."
      val azureAccountKeyPostfix = ".blob.core.windows.net"
      config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      config.set(azureAccountKeyPrefix.concat(accountName).concat(azureAccountKeyPostfix), accountKey)
    }

    // Generate the url
    var url: String = null
    if (!ReaderUtils.isNullOrEmpty(containerName) && !ReaderUtils.isNullOrEmpty(accountName)) {
      val urlPrefix = "wasbs://"
      val urlPostfix = ".blob.core.windows.net/"
      url = urlPrefix.concat(containerName).concat("@").concat(accountName).concat(urlPostfix).concat(filePath)
    } else {
      val urlPrefix = "wasbs:///"
      url = urlPrefix.concat(filePath)
    }

    // Populate the options
   val options = ReaderUtils.getOptionsForBlobReader(fileFormatEnum, true, hasHeader)

    // Get the file format
    var format = fileFormatEnum.toString
    if (format == "tsv") {
      format = "csv"
    }

    spark.read.format(format).options(options).load(url)
  }

  def read2 (jsonStr: String): DataFrame = {
    val parsedJsonStr = JSON.parseFull(jsonStr)
    var accountName = ""
    var accountKey = ""
    var containerName = ""
    var filePath = ""
    var fileFormat = ""
    var hasHeader = false;
    try {
      hasHeader = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("hasHeader").asInstanceOf[Boolean]
      fileFormat = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("fileFormat").asInstanceOf[String]
      filePath = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("filePath").asInstanceOf[String]
      containerName = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("containerName").asInstanceOf[String]
      accountKey = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("accountKey").asInstanceOf[String]
      accountName = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("accountName").asInstanceOf[String]
    } catch {
      case ex: NoSuchElementException => {
        throw new IllegalArgumentException("parameter not found or invalid Json format detected in the input.")
      }
    }

    read(accountName, accountKey, containerName, filePath, fileFormat, hasHeader)
  }
}
