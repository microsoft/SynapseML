// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

object FabricConstants {
  val MlKustoTableName = "SynapseMLLogs"
  val EmitUsage = "EmitUsage"
  val FabricFakeTelemetryReportCalls = "fabric_fake_usage_telemetry"

  val ContextFilePath = "/home/trusted-service-user/.trident-context"
  val TokenServiceFilePath = "/opt/token-service/tokenservice.config.json"

  val SynapseTokenServiceEndpoint = "synapse.tokenServiceEndpoint"
  val SynapseClusterIdentifier = "synapse.clusterIdentifier"
  val SynapseClusterType = "synapse.clusterType"
  val TridentLakehouseTokenServiceEndpoint = "trident.lakehouse.tokenservice.endpoint"
  val TridentSessionToken = "trident.session.token"
  val WebApi = "webapi"
  val Capacities = "Capacities"
  val Workloads = "workloads"
  val WorkspaceID = "workspaceid"

  val WorkloadEndpointMl = "ML"
  val WorkloadEndpointLlmPlugin = "LlmPlugin"
  val WorkloadEndpointAutomatic = "Automatic"
  val WorkloadEndpointRegistry = "Registry"
  val WorkloadEndpointAdmin = "MLAdmin"
}
