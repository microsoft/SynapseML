// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.spark.sql.SparkSession

object PlatformDetails {
  val PlatformSynapseInternal = "synapse_internal"
  val PlatformSynapse = "synapse"
  val PlatformBinder = "binder"
  val PlatformDatabricks = "databricks"
  val PlatformUnknown = "unknown"
  val SynapseProjectName = "Microsoft.ProjectArcadia"

  def currentPlatform(): String = {
    val azureService = sys.env.get("AZURE_SERVICE")
    azureService match {
      case Some(serviceName) if serviceName == SynapseProjectName =>
        val spark = SparkSession.builder.getOrCreate()
        val clusterType = spark.conf.get("spark.cluster.type")
        if (clusterType == "synapse") PlatformSynapse else PlatformSynapseInternal
      case _ if new java.io.File("/dbfs").exists() => PlatformDatabricks
      case _ if sys.env.get("BINDER_LAUNCH_HOST").isDefined => PlatformBinder
      case _ => PlatformUnknown
    }
  }

  def runningOnSynapseInternal(): Boolean = currentPlatform() == PlatformSynapseInternal

  def runningOnSynapse(): Boolean = currentPlatform() == PlatformSynapse

  def runningOnFabric(): Boolean = runningOnSynapseInternal
}
