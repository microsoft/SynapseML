// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class AMLExperiment(Resource: String,
                         ClientId: String,
                         ClientSecret: String,
                         TenantId: String,
                         SubscriptionId: String,
                         Region: String,
                         ResourceGroup: String,
                         Workspace: String,
                         ExperimentName: String,
                         RunFilePath: String)


case class AMLExperimentResponse(ExperimentId: String,
                                 Name: String,
                                 Description: String,
                                 CreatedUtc: String,
                                 Tags: Array[String],
                                 ArchivedTime: String,
                                 RetainForLifetimeOfWorkspace: Boolean
                                )

case class AMLModelCreatedBy(userObjectId: String,
                             userPuId: String,
                             userIdp: Option[String],
                             userAltSecId: Option[String],
                             userIss: String,
                             userTenantId: String,
                             userName: String)

case class AMLModelResponse(id: String,
                            framework: Option[String],
                            frameworkVersion: Option[String],
                            version: Option[Int],
                            tags: Option[List[String]],
                            datasets: Option[List[String]],
                            url: Option[String],
                            description: Option[String],
                            createdTime: Option[String],
                            modifiedTime: Option[String],
                            unpack: Option[Boolean],
                            parentModelId: Option[String],
                            experimentName: Option[String],
                            kvTags: Option[Map[String, String]],
                            properties: Option[Map[String, String]],
                            derivedModelIds: Option[String],
                            inputsSchema: Option[List[String]],
                            outputsSchema: Option[List[String]],
                            sampleInputData: Option[String],
                            sampleOutputData: Option[String],
                            resourceRequirements: Option[String],
                            createdBy: Option[AMLModelCreatedBy])

object AMLExperimentResponse extends SparkBindings[AMLExperimentResponse]

object AMLExperimentFormat extends DefaultJsonProtocol {
  implicit val AMLExperimentResponseFormat: RootJsonFormat[AMLExperimentResponse] =
    jsonFormat7(AMLExperimentResponse.apply)

  implicit val AMLExperimentFormat: RootJsonFormat[AMLExperiment] =
    jsonFormat10(AMLExperiment.apply)

  implicit val AMLModelCreatedByFormat: RootJsonFormat[AMLModelCreatedBy] =
    jsonFormat7(AMLModelCreatedBy.apply)

  implicit val AMLModelFormat: RootJsonFormat[AMLModelResponse] =
    jsonFormat22(AMLModelResponse.apply)

}
