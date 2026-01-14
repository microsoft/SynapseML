// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.commons.io.IOUtils

import java.net.URL

object TestImageDownloader {

  /** Download content from the given URL column into a new binary column.
    *
    * This helper is intentionally simple and synchronous since it is only used in unit tests with
    * very small datasets.
    */
  def downloadFromUrls(df: DataFrame,
                       urlCol: String,
                       bytesCol: String,
                       connectTimeoutMs: Int = 10000,
                       readTimeoutMs: Int = 10000): DataFrame = {
    val fetchBytes = udf { urlStr: String =>
      if (urlStr == null) {
        null //scalastyle:ignore null
      } else {
        val connection = new URL(urlStr).openConnection()
        connection.setConnectTimeout(connectTimeoutMs)
        connection.setReadTimeout(readTimeoutMs)
        val stream = connection.getInputStream
        try {
          IOUtils.toByteArray(stream)
        } finally {
          stream.close()
        }
      }
    }
    df.withColumn(bytesCol, fetchBytes(col(urlCol)))
  }
}
