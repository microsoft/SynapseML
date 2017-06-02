// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.NoSuchElementException
import com.microsoft.ml.spark.FileFormat.FileFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scala.util.parsing.json.JSON

object WasbReader {
  def read (url: String, fileFormat: String, hasHeader: Boolean): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    val fileFormatEnum = ReaderUtils.getFileFormat(fileFormat)

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
    var url = ""
    var fileFormat = ""
    var hasHeader = false

    try {
      hasHeader = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("hasHeader").asInstanceOf[Boolean]
      fileFormat  = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("fileFormat").asInstanceOf[String]
      url = parsedJsonStr.get.asInstanceOf[Map[String, Any]]("url").asInstanceOf[String]
    } catch {
      case ex: NoSuchElementException => {
        throw new IllegalArgumentException("parameter not found or invalid Json format detected in the input.")
      }
    }

    read(url, fileFormat, hasHeader)
  }
}
