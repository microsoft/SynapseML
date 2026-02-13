// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import spray.json.{DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FabricSchemas {
  case class ClusterDetails(clusterUrl: String)

  case class Artifact (objectId: String,
                       displayName: String,
                       lastUpdatedDate: LocalDateTime)

  case class PowerBIDatasets (value: Seq[PowerBIDataset])

  case class PowerBIDataset (name: String, id: String)

  case class SparkJobDefinitionExecutionResponse (statusString: String,
                                                  artifactJobInstanceId: String,
                                                  serviceExceptionJson: Option[String])

  case class ArtifactResponse(folderObjectId: String,
                              capacityObjectId: String,
                              displayName: String)

  case class JobResponse(artifactJobInstanceId: String,
                         artifactJobHistoryProperties: Map[String, String],
                         tenantObjectId: String)

  case class BatchLogResponse(errorInfo: Option[Seq[BatchLogErrorInfo]],
                              lastUpdatedTimestamp: String,
                              appId: Option[String],
                              id: String,
                              log: Option[Seq[String]])

  case class BatchLogErrorInfo(errorCode: String,
                               message: String,
                               source: String)

  case class Activity(id: String)

  case class ActivitiesResponse(items: Seq[Activity])

  object JsonProtocols extends DefaultJsonProtocol {
    implicit object LocalDateTimeFormat extends RootJsonFormat[LocalDateTime] {
      def write(dt: LocalDateTime): JsValue = JsString(dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

      def read(value: JsValue): LocalDateTime =
        LocalDateTime.parse(value.toString().replaceAll("^\"+|\"+$", ""),
          DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }

    implicit val ClusterDetailsFormat: RootJsonFormat[ClusterDetails] =
      jsonFormat1(ClusterDetails.apply)

    implicit val ArtifactFormat: RootJsonFormat[Artifact] =
      jsonFormat3(Artifact.apply)

    implicit val SJDExecutionResponseFormat: RootJsonFormat[SparkJobDefinitionExecutionResponse] =
      jsonFormat3(SparkJobDefinitionExecutionResponse.apply)

    implicit val PowerBIDatasetFormat: RootJsonFormat[PowerBIDataset] =
      jsonFormat2(PowerBIDataset.apply)

    implicit val PowerBIDatasetsFormat: RootJsonFormat[PowerBIDatasets] =
      jsonFormat1(PowerBIDatasets.apply)

    implicit val ArtifactResponseFormat: RootJsonFormat[ArtifactResponse] =
      jsonFormat3(ArtifactResponse.apply)

    implicit val JobResponseFormat: RootJsonFormat[JobResponse] =
      jsonFormat3(JobResponse.apply)

    implicit val BatchLogErrorInfoFormat: RootJsonFormat[BatchLogErrorInfo] =
      jsonFormat3(BatchLogErrorInfo.apply)

    implicit val BatchLogResponseFormat: RootJsonFormat[BatchLogResponse] =
      jsonFormat5(BatchLogResponse.apply)

    implicit val ActivityFormat: RootJsonFormat[Activity] =
      jsonFormat1(Activity.apply)

    implicit val ActivitiesResponseFormat: RootJsonFormat[ActivitiesResponse] =
      jsonFormat1(ActivitiesResponse.apply)
  }
}
