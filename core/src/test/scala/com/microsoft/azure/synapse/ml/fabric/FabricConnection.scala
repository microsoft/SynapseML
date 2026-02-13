// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.fabric.FabricTokenProvider.getAccessToken
import com.microsoft.azure.synapse.ml.fabric.FabricSchemas.ClusterDetails
import com.microsoft.azure.synapse.ml.fabric.FabricSchemas.JsonProtocols.ClusterDetailsFormat

trait HasFabricConnection {
  var fabricClientId: Option[String] = None
  var fabricRedirectUri: Option[String] = None
  var fabricEnv: Option[String] = None

  lazy val fabric: FabricConnection = createConnection()

  private def createConnection(): FabricConnection = {
    (fabricClientId, fabricRedirectUri) match {
      case (Some(cid), Some(uri)) =>
        println(s"Connecting to Fabric with ClientId: $cid and RedirectUri: $uri")
        new FabricConnection(cid, uri)
      case _ =>
        throw new IllegalArgumentException(
          "ClientId and RedirectUri must be provided when using FabricConnection")
    }
  }
}

trait HasFabricOperationsConnection extends HasFabricConnection {
  var fabricWorkspaceId: Option[String] = None
  override lazy val fabric: FabricOperations = createOperationsConnection()

  private def createOperationsConnection(): FabricOperations = {
    (fabricClientId, fabricRedirectUri, fabricWorkspaceId) match {
      case (Some(cid), Some(uri), Some(wid)) =>
        println(s"Connecting to Fabric with ClientId: $cid, RedirectUri: $uri, WorkspaceId: $wid")
        new FabricOperations(cid, uri, wid)
      case _ =>
        throw new IllegalArgumentException(
          "ClientId, RedirectUri and WorkspaceId must be provided when using FabricOperations")
    }
  }
}

private[fabric] class FabricConnection(clientId: String, redirectUri: String)
  extends FabricAuthenticatedHttpClient(clientId, redirectUri) {
  val uxHost: String = "http://app.powerbi.com"
  lazy val accessToken: String = getAccessToken(clientId, redirectUri)
  val sspHost: String = {
    var value = getRequest(
      "https://api.powerbi.com/powerbi/globalservice/v201606/clusterdetails"
    ).convertTo[ClusterDetails].clusterUrl
    if (!value.endsWith("/"))
      value += "/"
    value
  }
  val authHeader: String = s"Bearer $accessToken"
}

private[fabric] class FabricInternalConnection(
  clientId: String, redirectUri: String, workspaceIdGuid: String)
  extends FabricConnection(clientId, redirectUri) {
  val workspaceId: String = workspaceIdGuid
  val metadataUri: String = s"$sspHost/metadata"
  val artifactsUri: String = s"$metadataUri/workspaces/$workspaceIdGuid/artifacts"
  def getMWCToken(capacityId: String, workspaceId: String,
                  artifactId: String, workloadType: String): String =
    FabricTokenProvider.getMWCToken(
      clientId, redirectUri, metadataUri, capacityId, workspaceId, artifactId, workloadType)
}
