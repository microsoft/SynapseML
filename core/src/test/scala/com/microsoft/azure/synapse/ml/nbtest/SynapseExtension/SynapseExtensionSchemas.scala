// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

import java.time.LocalDateTime

case class Artifact (
  objectId: String,
  displayName: String,
  lastUpdatedDate: LocalDateTime
)

case class PowerBIDatasets (value: Seq[PowerBIDataset])

case class PowerBIDataset (name: String, id: String)

case class SparkJobDefinitionExecutionResponse (
  statusString: String,
  artifactJobInstanceId: String,
  serviceExceptionJson: Option[String]
)

case class ArtificatResponse(folderObjectId: String,
                             capacityObjectId: String,
                             displayName: String)

case class JobResponse(artifactJobInstanceId: String,
                       artifactJobHistoryProperties: Map[String, String],
                       tenantObjectId: String)

case class BatchLogResponse(errorInfo: Option[Seq[BatchLogErrorInfo]],
                            lastUpdatedTimestamp: String,
                            appId: Option[String],
                            id: String, // That's the Livy Id
                            log: Option[Seq[String]])

case class BatchLogErrorInfo(errorCode: String,
                             message: String,
                             source: String)

case class Activity(id: String)

case class ActivitiesResponse(items: Seq[Activity])