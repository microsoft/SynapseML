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
  lazy val CurrentPlatform: String = currentPlatform()
  lazy val FabricRuntime: String = resolveFabricRuntime(
    runningOnFabric(),
    sparkVersion,
    sys.env.get("PYTHON_VERSION")
  )

  private[common] def sparkVersion: Option[String] = {
    try {
      Option(org.apache.spark.SPARK_VERSION).filter(_.nonEmpty)
    } catch {
      case _: LinkageError => None
    }
  }

  def currentPlatform(): String = {
    val azureService = sys.env.get("AZURE_SERVICE")
    azureService match {
      case _ if new java.io.File("/home/trusted-service-user/.trident-context").exists() => PlatformSynapseInternal
      // Note Below judgement doesn't work if you are not in main thread
      // In Fabric, existence of above file should always gives right judgement
      // In Synapse, hitting below condition has risks.
      case Some(serviceName) if serviceName == SynapseProjectName =>
        defineSynapsePlatform()
      case _ if new java.io.File("/dbfs").exists() => PlatformDatabricks
      case _ if sys.env.contains("BINDER_LAUNCH_HOST") => PlatformBinder
      case _ => PlatformUnknown
    }
  }

  def defineSynapsePlatform(): String = {
    val spark = SparkSession.getActiveSession
    if (spark.isDefined) {
      val clusterType = spark.get.conf.get("spark.cluster.type")
      if (clusterType == "synapse") PlatformSynapse else PlatformSynapseInternal
    } else {
      PlatformUnknown
    }
  }

  def runningOnSynapseInternal(): Boolean = CurrentPlatform == PlatformSynapseInternal

  def runningOnSynapse(): Boolean = CurrentPlatform == PlatformSynapse

  def runningOnFabric(): Boolean = runningOnSynapseInternal()

  private[common] def resolveFabricRuntime(
      isFabric: Boolean,
      sparkVersion: Option[String],
      pythonVersion: Option[String]): String = {
    if (!isFabric) {
      PlatformUnknown
    } else {
      sparkVersion
        .filter(_.nonEmpty)
        .map(version => s"fabric_spark_$version")
        .orElse(
          pythonVersion
            .filter(_.nonEmpty)
            .map(version => s"fabric_python_$version")
        )
        .getOrElse("fabric")
    }
  }
}
