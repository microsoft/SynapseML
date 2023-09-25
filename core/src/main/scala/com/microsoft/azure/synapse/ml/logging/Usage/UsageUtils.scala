// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.common.WebUtils
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.Usage.HostEndpointUtils._
import com.microsoft.azure.synapse.ml.logging.Usage.TokenUtils.getAccessToken
import com.microsoft.azure.synapse.ml.logging.common.SparkHadoopUtils

import java.time.Instant
import java.util.UUID
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.DefaultJsonProtocol.StringJsonFormat

object UsageTelemetry extends FabricConstants with SparkHadoopUtils with WebUtils {
  private val SC = SparkSession.builder().getOrCreate().sparkContext
  private val CapacityId = getHadoopConfig("trident.capacity.id", Some(SC))
  private val WorkspaceId: String = getHadoopConfig("trident.artifact.workspace.id", Some(SC))
  private val PbiEnv = SC.getConf.get("spark.trident.pbienv", "").toLowerCase()

  private val SharedHost = getMlflowSharedHost(PbiEnv)
  private val SharedEndpoint = f"{SharedHost}/metadata/workspaces/{WorkspaceId}/artifacts"
  private val WlHost = getMlflowWorkloadHost(PbiEnv, CapacityId, WorkspaceId, Some(SharedHost))

  def reportUsage(payload: FeatureUsagePayload): Unit = {
    if (sys.env.getOrElse(emitUsage, "true").toLowerCase == "true") {
      try {
        reportUsageTelemetry(payload.feature_name,
          payload.activity_name,
          payload.attributes)
      } catch {
        case runtimeError: Exception =>
          SynapseMLLogging.logMessage(s"UsageTelemetry::reportUsage: Hit issue emitting usage telemetry." +
            s" Exception = $runtimeError. (usage test)")
      }
    }
  }

  private def reportUsageTelemetry(featureName: String, activityName: String,
                                   attributes: Map[String,String] = Map()): Unit = {
    if (sys.env.getOrElse(fabricFakeTelemetryReportCalls,"false").toLowerCase == "false") {
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
        "Authorization" -> s"""Bearer ${getAccessToken}""".stripMargin,
        "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
      )
      usagePost(url, data, headers)
    }
  }
}
