// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants.{Capacities, WorkloadEndpointAutomatic, Workloads}
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants.{WebApi, WorkloadEndpointMl, WorkspaceID}
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.usageGet
import spray.json.DefaultJsonProtocol.StringJsonFormat

object HostEndpointUtils {
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
    usageGet(url, headers).asJsObject.fields("clusterUrl").convertTo[String]
  }

  def getMlflowWorkloadHost(pbienv: String, capacityId: String, workspaceId: String,
                            sharedHost: Option[String] = None): Option[String] = {
    val clusterUrl = sharedHost match {
      case Some(value) =>
        value
      case None =>
        getMlflowSharedHost(pbienv)
    }

    val mwcToken: Option[MwcToken] = TokenUtils.getMwcToken(clusterUrl,
      workspaceId, capacityId, TokenUtils.MwcWorkloadTypeMl)
    mwcToken match {
      case Some(token) =>
        Some(token.TargetUriHost)
      case None =>
        None
    }
  }

  def getMLWorkloadEndpoint(wlHost: String, capacityId: String, endpoint: String,  workspaceID: String): String = {
    val mlWorkloadEndpoint = s"$wlHost/$WebApi/$Capacities/$capacityId/$Workloads/" +
      s"$WorkloadEndpointMl/$endpoint/$WorkloadEndpointAutomatic/${WorkspaceID}/$workspaceID/"
    mlWorkloadEndpoint
  }
}
