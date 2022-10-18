package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension.Models

case class WorkloadPayload
(
  ExecutableFile: String,
  SparkVersion: String,
  DefaultLakehouseArtifactId: String
)
