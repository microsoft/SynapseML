package com.microsoft.azure.synapse.ml.logging.Usage
object UsageFeatureNames extends Enumeration {
  type UsageFeatureNames = Value
  val Predict = Value(1)
}

object FeatureActivityName extends Enumeration {
  type FeatureActivityName = Value
  val API_Transform = Value(1)
  val API_SQL = Value(2)
  val API_UDF = Value(3)
}

object FabricConstants {
  val ML_KUSTO_TdABLE_NAME = "SynapseMLLogs"
  val EMIT_USAGE = "emit_usage"

  val CONTEXT_FILE_PATH = "/home/trusted-service-user/.trident-context"
  val TOKEN_SERVICE_FILE_PATH = "/opt/token-service/tokenservice.config.json"

  val SYNAPSE_TOKEN_SERVICE_ENDPOINT = "synapse.tokenServiceEndpoint"
  val SYNAPSE_CLUSTER_IDENTIFIER = "synapse.clusterIdentifier"
  val SYNAPSE_CLUSTER_TYPE = "synapse.clusterType"
  val TRIDENT_LAKEHOUSE_TOKEN_SERVICE_ENDPOINT = "trident.lakehouse.tokenservice.endpoint"
  val TRIDENT_SESSION_TOKEN = "trident.session.token"
  val WEB_API = "webapi"
  val CAPACITIES = "capacities"
  val WORKLOADS = "workloads"
  val WORKSPACE_ID = "workspaceid"

  val WORKLOAD_ENDPOINT_ML = "ML"
  val WORKLOAD_ENDPOINT_LLM_PLUGIN = "LlmPlugin"
  val WORKLOAD_ENDPOINT_AUTOMATIC = "Automatic"
  val WORKLOAD_ENDPOINT_REGISTRY = "Registry"
  val WORKLOAD_ENDPOINT_ADMIN = "MLAdmin"
}