package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension.Models

case class SparkJobDefinitionExecutionResponse
(
  statusString: String,
  artifactJobInstanceId: String,
  serviceExceptionJson: Option[String]
)
