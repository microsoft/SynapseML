// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage
object UsageFeatureNames extends Enumeration {
  type UsageFeatureNames = Value
  val Predict = Value(1)
}

object FeatureActivityName extends Enumeration {
  type FeatureActivityName = Value
  val API0Transform = Value(1)
  val API0SQL = Value(2)
  val API0UDF = Value(3)
}

object FabricConstants {
  val MlKustoTableName = "SynapseMLLogs"
  val EmitUsage = "EmitUsage"

  val ContextFilePath = "/home/trusted-service-user/.trident-context"
  val TokenServiceFilePath = "/opt/token-service/tokenservice.config.json"

  val SynapseTokenServiceEndpoint = "synapse.tokenServiceEndpoint"
  val SynapseClusterIdentifier = "synapse.clusterIdentifier"
  val SynapseClusterType = "synapse.clusterType"
  val TridentLakehouseTokenServiceEndpoint = "trident.lakehouse.tokenservice.endpoint"
  val TridentSessionToken = "trident.session.token"
  val WebApi = "webapi"
  val Capacities = "Capacities"
  val WORKLOADS = "workloads"
  val WorkspaceID = "workspaceid"

  val WorkloadEndpointMl = "ML"
  val WorkloadEndpointLlmPlugin = "LlmPlugin"
  val WorkloadEndpointAutomatic = "Automatic"
  val WorkloadEndpointRegistry = "Registry"
  val WorkloadEndpointAdmin = "MLAdmin"
}
