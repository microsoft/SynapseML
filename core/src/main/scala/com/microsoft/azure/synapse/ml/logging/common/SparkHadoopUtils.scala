// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.spark.SparkContext

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging

object SparkHadoopUtils {

  def getHadoopConfig(key: String, SC: SparkContext): String = {
    if (SC == null) {
      ""
    } else {
      val value = SC.hadoopConfiguration.get(key, "")
      if (value.isEmpty) {
        SynapseMLLogging.logMessage(s"UsageUtils.getHadoopConfig: Hadoop configuration $key is empty.")
      }
      value
    }
  }
}
