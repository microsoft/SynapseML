// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.FileFormat.FileFormat

object ReaderUtils {
  def isNullOrEmpty(str: String): Boolean = {
    return str == null || str.trim.isEmpty
  }

  def getFileFormat(str: String): FileFormat = {
    if (isNullOrEmpty(str)) {
      throw new IllegalArgumentException("str is invalid.")
    }

    if (str.equalsIgnoreCase("csv")) {
      return FileFormat.Csv
    } else if (str.equalsIgnoreCase("tsv")) {
      return FileFormat.Tsv
    }else if (str.equalsIgnoreCase("json")) {
      return FileFormat.Json
    } else if (str.equalsIgnoreCase("parquet")) {
      return FileFormat.Parquet
    } else {
      throw new IllegalArgumentException("str is not valid file format.")
    }
  }

  def getOptionsForBlobReader(fileFormat: FileFormat, inferSchema: Boolean, hasHeader: Boolean): Map[String, String] = {
    var headerOpt = "false"
    if (hasHeader) {
      headerOpt = "true"
    }
    var schemaOpt = "false"
    if (inferSchema) {
      schemaOpt = "true"
    }
    var options = Map("inferSchema" -> schemaOpt, "header" -> headerOpt)
    if (fileFormat == FileFormat.Tsv) {
      options = options + ("delimiter" -> "\t")
    }

    return options
  }
}
