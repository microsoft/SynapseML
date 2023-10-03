// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.fabric

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.fabric.HostEndpointUtils._
import com.microsoft.azure.synapse.ml.logging.fabric.TokenUtils.getAccessToken

import java.time.Instant
import java.util.UUID
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.DefaultJsonProtocol.StringJsonFormat

object UsageTelemetry extends FabricConstants with WebUtils {
  private val SC = SparkSession.builder().getOrCreate().sparkContext
  private lazy val CapacityId = SC.hadoopConfiguration.get("trident.capacity.id")
  private lazy val WorkspaceId: String = SC.hadoopConfiguration.get("trident.artifact.workspace.id")
  private lazy val PbiEnv = SC.getConf.get("spark.trident.pbienv", "").toLowerCase()

  private lazy val SharedHost = getMlflowSharedHost(PbiEnv)
  private val WlHost = getMlflowWorkloadHost(PbiEnv, CapacityId, WorkspaceId, Some(SharedHost))

  def reportUsage(featureName: String,
                  activityName: String,
                  attributes: Map[String, String]): Unit = {
    if (sys.env.getOrElse(emitUsage, "true").toLowerCase == "true") {
      try {
        reportUsageTelemetry(
          featureName,
          activityName,
          attributes)
      } catch {
        case runtimeError: Exception =>
          SynapseMLLogging.logMessage(s"UsageTelemetry::reportUsage: Hit issue emitting usage telemetry." +
            s" Exception = $runtimeError. (usage test)")
      }
    }
  }

  private def reportUsageTelemetry(featureName: String,
                                   activityName: String,
                                   attributes: Map[String, String]): Unit = {
    if (sys.env.getOrElse(fabricFakeTelemetryReportCalls, "false").toLowerCase == "false") {
      val attributesJson = attributes.toJson.compactPrint
      val data =
        s"""{
           |"timestamp":${Instant.now().getEpochSecond},
           |"feature_name":"$featureName",
           |"activity_name":"$activityName",
           |"attributes":$attributesJson
           |}""".stripMargin

      val mlAdminEndpoint = WlHost match {
        case Some(host) =>
          getMLWorkloadEndpoint(host, CapacityId, workloadEndpointAdmin, WorkspaceId)
        case None =>
          throw new IllegalArgumentException("Workload host name is missing.")
      }

      // Add the protocol and the route for the certified event telemetry endpoint
      val url = "https://" + mlAdminEndpoint + "telemetry"

      val headers = Map(
        "Content-Type" -> "application/json",
        "Authorization" -> s"""Bearer ${getAccessToken("pbi")}""".stripMargin,
        "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
      )
      usagePost(url, data, headers)
    }
  }
}
