// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.spark.SparkContext

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging

object SparkHadoopUtils {

  def getHadoopConfig(key: String, sc: SparkContext): String = {
      val value = sc.hadoopConfiguration.get(key, "")
      if (value.isEmpty) {
        SynapseMLLogging.logMessage(s"UsageUtils.getHadoopConfig: Hadoop configuration $key is empty.")
        throw new IllegalArgumentException(s"UsageUtils.getHadoopConfig: Hadoop configuration $key is empty.")
      }
      value
  }
}
