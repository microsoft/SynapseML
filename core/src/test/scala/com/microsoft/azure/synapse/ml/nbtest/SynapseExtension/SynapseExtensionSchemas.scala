// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

case class Artifact (
  objectId: String,
  displayName: String
)

case class SparkJobDefinitionExecutionResponse (
  statusString: String,
  artifactJobInstanceId: String,
  serviceExceptionJson: Option[String]
)

