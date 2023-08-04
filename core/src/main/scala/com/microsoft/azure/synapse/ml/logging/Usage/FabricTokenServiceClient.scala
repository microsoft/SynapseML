// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import java.util.UUID
import java.net.URL
import java.net.InetAddress
import java.lang.management.ManagementFactory
import com.microsoft.azure.synapse.ml.logging.common.WebUtils._
import spray.json.DefaultJsonProtocol.StringJsonFormat
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import spray.json.{JsArray, JsObject, JsValue, _}
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.{usageGet}
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging

class FabricTokenServiceClient {
  val resourceMapping = Map(
    "https://storage.azure.com" -> "storage",
    "storage" -> "storage",
    "https://analysis.windows.net/powerbi/api" -> "pbi",
    "pbi" -> "pbi",
    "https://vault.azure.net" -> "keyvault",
    "keyvault" -> "keyvault",
    "https://kusto.kusto.windows.net" -> "kusto",
    "kusto" -> "kusto"
  )

  val hostname = InetAddress.getLocalHost.getHostName
  val processDetail = ManagementFactory.getRuntimeMXBean().getName()
  val processName = processDetail.substring(processDetail.indexOf('@') + 1)

  val fabricConbtext = FabricUtils.getFabricContext()
  val synapseTokenserviceEndpoint = fabricConbtext(SynapseTokenServiceEndpoint)
  val workloadEndpoint = fabricConbtext(TridentLakehouseTokenServiceEndpoint)
  val sessionToken = fabricConbtext(TridentSessionToken)
  val clusterIdentifier = fabricConbtext(SynapseClusterIdentifier)

  def getAccessToken(resourceParam: String): String = {
    if (!resourceMapping.contains(resourceParam)) {
      throw new Exception(s"$resourceParam not supported")
    }
    val resource = resourceMapping.getOrElse(resourceParam, "")
    val rid = UUID.randomUUID().toString()
    //to do workloadEndpoint
    val targetUrl = new URL(workloadEndpoint)
    var headers = Map(
      "x-ms-cluster-identifier" -> clusterIdentifier,
      "x-ms-workload-resource-moniker" -> clusterIdentifier,
      "Content-Type" -> "application/json;charset=utf-8",
      "x-ms-proxy-host" -> s"${targetUrl.getProtocol}://${targetUrl.getHost}",
      "x-ms-partner-token" -> sessionToken,
      "User-Agent" -> s"Trident Token Library - HostName:$hostname, ProcessName:$processName",
      "x-ms-client-request-id" -> rid
    )
    var url = s"$synapseTokenserviceEndpoint/api/v1/proxy${targetUrl.getPath}/access?resource=$resource"
    var response: JsValue = JsonParser("")
    try {
      response = usageGet(url, headers)
      if (response.asJsObject.fields("status_code").convertTo[String] != 200
        || response.asJsObject.fields("content").convertTo[String].isEmpty) {
        throw new Exception("Fetch access token error")
      }
    } catch {
      case e: Exception =>
        println(s"getAccessToken: Failed to fetch cluster details. Exception = $e. (usage test)")
        SynapseMLLogging.logMessage(s"getAccessToken: Failed to fetch cluster details. Exception = $e. (usage test)")
    }
    response.asJsObject.fields("content").toString().getBytes("UTF-8").toString()
  }
}
