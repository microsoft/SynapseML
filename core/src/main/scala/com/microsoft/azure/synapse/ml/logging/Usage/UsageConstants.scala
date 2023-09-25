// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

trait FabricConstants {
  val emitUsage = "EmitUsage"
  val fabricFakeTelemetryReportCalls = "fabric_fake_usage_telemetry"

  val contextFilePath = "/home/trusted-service-user/.trident-context"
  val tokenServiceFilePath = "/opt/token-service/tokenservice.config.json"

  val synapseTokenServiceEndpoint = "synapse.tokenServiceEndpoint"
  val synapseClusterIdentifier = "synapse.clusterIdentifier"
  val synapseClusterType = "synapse.clusterType"
  val tridentLakehouseTokenServiceEndpoint = "trident.lakehouse.tokenservice.endpoint"
  val tridentSessionToken = "trident.session.token"
  val webApi = "webapi"
  val capacities = "Capacities"
  val workloads = "workloads"
  val workspaceID = "workspaceid"

  val workloadEndpointMl = "ML"
  val workloadEndpointAutomatic = "Automatic"
  val workloadEndpointAdmin = "MLAdmin"
}
