// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import java.io.IOException
import java.lang.management.ManagementFactory
import java.net.URL
import java.net.InetAddress
import java.util.UUID
import spray.json.DefaultJsonProtocol.StringJsonFormat
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import spray.json.{JsArray, JsObject, JsValue, _}
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.usageGet
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.hc.client5.http.ClientProtocolException

class FabricTokenServiceClient {
  private val resourceMapping = Map(
    "https://storage.azure.com" -> "storage",
    "storage" -> "storage",
    "https://analysis.windows.net/powerbi/api" -> "pbi",
    "pbi" -> "pbi",
    "https://vault.azure.net" -> "keyvault",
    "keyvault" -> "keyvault",
    "https://kusto.kusto.windows.net" -> "kusto",
    "kusto" -> "kusto"
  )

  private val hostname = InetAddress.getLocalHost.getHostName
  private val processDetail = ManagementFactory.getRuntimeMXBean.getName
  private val processName = processDetail.substring(processDetail.indexOf('@') + 1)

  private val fabricContext = FabricUtils.getFabricContext
  private val synapseTokenServiceEndpoint: String = fabricContext(synapseTokenServiceEndpoint)
  private val workloadEndpoint = fabricContext(TridentLakehouseTokenServiceEndpoint)
  private val sessionToken = fabricContext(TridentSessionToken)
  private val clusterIdentifier = fabricContext(SynapseClusterIdentifier)

  def getAccessToken(resourceParam: String): String = {
    if (!resourceMapping.contains(resourceParam)) {
      throw new Exception(s"$resourceParam not supported")
    }
    val resource = resourceMapping.getOrElse(resourceParam, "")
    val rid = UUID.randomUUID().toString()
    val targetUrl = new URL(workloadEndpoint)
    val headers: Map[String, String] = Map(
      "x-ms-cluster-identifier" -> clusterIdentifier,
      "x-ms-workload-resource-moniker" -> clusterIdentifier,
      "Content-Type" -> "application/json;charset=utf-8",
      "x-ms-proxy-host" -> s"${targetUrl.getProtocol}://${targetUrl.getHost}",
      "x-ms-partner-token" -> sessionToken,
      "User-Agent" -> s"Trident Token Library - HostName:$hostname, ProcessName:$processName",
      "x-ms-client-request-id" -> rid
    )
    var url = s"$synapseTokenServiceEndpoint/api/v1/proxy${targetUrl.getPath}/access?resource=$resource"
    try {
      val response: JsValue = usageGet(url, headers)
      if (response.asJsObject.fields("status_code").convertTo[String] != 200
        || response.asJsObject.fields("content").convertTo[String].isEmpty) {
        throw new Exception("Fetch access token error")
      }
      response.asJsObject.fields("content").toString().getBytes("UTF-8").toString
    } catch {
      case e: IOException =>
        SynapseMLLogging.logMessage(s"getAccessToken: Failed to fetch cluster details. Problems in executing" +
          s" http request or the connection might have been aborted. Exception = $e.")
        ""
      case e: ClientProtocolException =>
        SynapseMLLogging.logMessage(s"getAccessToken: Failed to fetch cluster details. " +
          s"HTTP protocol error. Exception = $e.")
        ""
      case e: Exception =>
        SynapseMLLogging.logMessage(s"getAccessToken: Failed to fetch cluster details. Exception = $e.")
        ""
    }
  }
}
