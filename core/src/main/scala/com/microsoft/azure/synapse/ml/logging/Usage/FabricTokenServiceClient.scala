// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.common.CommonUtils
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import java.lang.management.ManagementFactory
import java.net.URL
import java.net.InetAddress
import java.util.UUID
import spray.json.{JsArray, JsObject, JsValue, _}

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
    val url = s"$synapseTokenServiceEndpoint/api/v1/proxy${targetUrl.getPath}/access?resource=$resource"

    CommonUtils.requestGet(url, headers, "content").toString().getBytes("UTF-8").toString
  }
}
