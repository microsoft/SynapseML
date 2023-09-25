// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.spark.SparkContext
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.sql.SparkSession

object SparkHadoopUtils {

  def getHadoopConfig(key: String, sc: Option[SparkContext]): String = {
    lazy val hadoopConfig: String = sc match{
      case Some(value) => value.hadoopConfiguration.get(key, "")
      case None => SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration.get(key, "")
    }
    if (hadoopConfig.isEmpty) {
      SynapseMLLogging.logMessage(s"UsageUtils.getHadoopConfig: Hadoop configuration $key is empty.")
      throw new IllegalArgumentException(s"UsageUtils.getHadoopConfig: Hadoop configuration $key is empty.")
    }
    hadoopConfig
  }
}
