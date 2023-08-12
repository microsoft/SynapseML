// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.common.SparkHadoopUtils.getHadoopConfig
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.{usageGet, usagePost}
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import com.microsoft.azure.synapse.ml.logging.Usage.TokenUtils.getAccessToken
import java.util.UUID
import java.time.Instant
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.DefaultJsonProtocol.StringJsonFormat

import com.microsoft.azure.synapse.ml.logging.Usage.HostEndpointUtils._

object UsageTelemetry {
  private val SC = SparkSession.builder().getOrCreate().sparkContext
  private val CapacityId = getHadoopConfig("trident.capacity.id", SC)
  val WorkspaceId: String = getHadoopConfig("trident.artifact.workspace.id", SC)
  private val PbiEnv = SC.getConf.get("spark.trident.pbienv", "").toLowerCase()

  private val SharedHost = getMlflowSharedHost(PbiEnv)
  val SharedEndpoint = f"{SharedHost}/metadata/workspaces/{WorkspaceId}/artifacts"
  private val WlHost = getMlflowWorkloadHost(PbiEnv, CapacityId, WorkspaceId, SharedHost)

  private val FabricFakeTelemetryReportCalls = "fabric_fake_usage_telemetry"
  def reportUsage(payload: FeatureUsagePayload): Unit = {
    if (sys.env.getOrElse(EmitUsage, "True") == "True") {
      try {
        reportUsageTelemetry(payload.feature_name.toString,
          payload.activity_name.toString.replace('_', '/'),
          payload.attributes)
      } catch {
        case runtimeError: Exception =>
          SynapseMLLogging.logMessage(s"UsageTelemetry::reportUsage: Hit issue emitting usage telemetry." +
            s" Exception = $runtimeError. (usage test)")
      }
    }
  }

  def reportUsageTelemetry(featureName: String, activityName: String, attributes: Map[String,String] = Map()): Unit = {
    if (sys.env.getOrElse(FabricFakeTelemetryReportCalls,"false") == "false") {
      val attributesJson = attributes.toJson.compactPrint
      val data =
        s"""{
           |"timestamp":${Instant.now().getEpochSecond},
           |"feature_name":"$featureName",
           |"activity_name":"${activityName.replace('0', '/')}",
           |"attributes":$attributesJson
           |}""".stripMargin

      val mlAdminEndpoint = getMLWorkloadEndpoint(WlHost, CapacityId, WorkloadEndpointAdmin, WorkspaceId)

      // Add the protocol and the route for the certified event telemetry endpoint
      val url = "https://" + mlAdminEndpoint + "telemetry"
      val driverAADToken = getAccessToken

      val headers = Map(
        "Content-Type" -> "application/json",
        "Authorization" -> s"""Bearer $driverAADToken""".stripMargin,
        "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
      )

      var response: JsValue = JsonParser("{}")
      try {
        response = usagePost(url, data, headers)
        /*if (response.asJsObject.fields("status_code").convertTo[String] != 200
          || response.asJsObject.fields("content").toString().isEmpty) {
          throw new Exception("Fetch access token error")
        }*/
      } catch {
        case e: Exception =>
          SynapseMLLogging.logMessage(s"UsageUtils.reportUsageTelemetry: Error occurred while emitting usage data. " +
            s"Exception = $e. (usage test)")
      }
    }
  }
}
