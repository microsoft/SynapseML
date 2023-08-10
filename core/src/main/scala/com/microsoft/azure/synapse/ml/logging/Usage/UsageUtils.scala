// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.{usageGet, usagePost}
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import com.microsoft.azure.synapse.ml.logging.Usage.TokenUtils.getAccessToken
import java.util.UUID
import java.time.Instant
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.DefaultJsonProtocol.StringJsonFormat

object UsageTelemetry {
  private val SC = SparkSession.builder().getOrCreate().sparkContext
  private val CapacityId = getHadoopConfig("trident.capacity.id")
  val WorkspaceId: String = getHadoopConfig("trident.artifact.workspace.id")
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
    SynapseMLLogging.logMessage(s"reportUsageTelemetry: feature_name: $featureName, " +
      s"activity_name: $activityName, attributes: $attributes")
    if (sys.env.getOrElse(FabricFakeTelemetryReportCalls,"false") == "false") {
      val attributesJson = attributes.toJson.compactPrint
      SynapseMLLogging.logMessage(s"reportUsageTelemetry: attributesJson = $attributesJson")
      val data =
        s"""{
           |"timestamp":${Instant.now().getEpochSecond},
           |"feature_name":"$featureName",
           |"activity_name":"${activityName.replace('0', '/')}",
           |"attributes":$attributesJson
           |}""".stripMargin

      val mlAdminEndpoint = getMLWorkloadEndpoint(WorkloadEndpointAdmin)

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

  def getHadoopConfig(key: String): String = {
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

  def getMlflowSharedHost(pbienv: String): String = {
    val pbiGlobalServiceEndpoints = Map(
      "public" -> "https://api.powerbi.com/",
      "fairfax" -> "https://api.powerbigov.us",
      "mooncake" -> "https://api.powerbi.cn",
      "blackforest" -> "https://app.powerbi.de",
      "msit" -> "https://api.powerbi.com/",
      "prod" -> "https://api.powerbi.com/",
      "int3" -> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
      "dxt" -> "https://powerbistagingapi.analysis.windows.net/",
      "edog" -> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
      "dev" -> "https://onebox-redirect.analysis.windows-int.net/",
      "console" -> "http://localhost:5001/",
      "daily" -> "https://dailyapi.powerbi.com/")


    val defaultGlobalServiceEndpoint: String = "https://api.powerbi.com/"
    val fetchClusterDetailUri: String = "powerbi/globalservice/v201606/clusterDetails"

    val url = pbiGlobalServiceEndpoints.getOrElse(pbienv, defaultGlobalServiceEndpoint) + fetchClusterDetailUri
    val headers = Map(
      "Authorization" -> s"Bearer ${TokenUtils.getAccessToken}",
      "RequestId" -> java.util.UUID.randomUUID().toString
    )
    try{
      val response: JsValue = usageGet(url, headers)
      response.asJsObject.fields("clusterUrl").convertTo[String]
    } catch {
      case e: Exception =>
        SynapseMLLogging.logMessage(s"getMlflowSharedHost: Can't get ml flow shared host. Exception = $e. (usage test)")
        ""
    }
  }

  def getMlflowWorkloadHost(pbienv: String, capacityId: String,
                            workspaceId: String,
                            sharedHost: String = ""): String = {
    val clusterUrl = if (sharedHost.isEmpty) {
      getMlflowSharedHost(pbienv)
    } else {
      sharedHost
    }
    try {
      val mwcToken: MwcToken = TokenUtils.getMWCToken(clusterUrl, workspaceId, capacityId, TokenUtils.MwcWorkloadTypeMl)
      if (mwcToken != null && mwcToken.TargetUriHost != null) {
        mwcToken.TargetUriHost
      } else {
        ""
      }
    } catch {
      case ex: Exception =>
        ""
    }
  }

  def getMLWorkloadEndpoint(endpoint: String): String = {
    val mlWorkloadEndpoint = s"${this.WlHost}/$WebApi/$Capacities/${this.CapacityId}/$WORKLOADS/" +
      s"$WorkloadEndpointMl/$endpoint/$WorkloadEndpointAutomatic/${WorkspaceID}/${this.WorkspaceId}/"
    mlWorkloadEndpoint
  }
}
