// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension.Models

case class WorkloadPayload
(
  ExecutableFile: String,
  SparkVersion: String,
  DefaultLakehouseArtifactId: String
)
