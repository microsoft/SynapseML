// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class ContainerRegistry(address: String,
                             password: String,
                             username: String)

case class ModelDockerSection(arguments: Option[List[String]],
                              baseDockerFile: Option[String],
                              baseImage: Option[String],
                              baseImageRegistry: Option[ContainerRegistry],
                              enabled: Option[Boolean],
                              gpuSupport: Option[Boolean],
                              sharedVolumes: Option[Boolean],
                              shmSize: Option[String])

case class ModelPythonSection(baseCondaEnvironment: Option[String],
                              condaDependencies: Option[Map[String, String]],
                              interpreterPath: Option[String],
                              userManagedDependencies: Option[Boolean])

case class SparkMavenPackage(artifact: Option[String],
                             group: Option[String],
                             version: Option[String])

case class ModelSparkSection(packages: Option[SparkMavenPackage],
                             precachePackages: Option[Boolean],
                             repositories: Option[List[String]])

case class ModelEnvironmentDefinition(docker: Option[ModelDockerSection],
                                      environmentVariables: Option[Map[String, String]],
                                      inferencingStackVersion: Option[String],
                                      name: Option[String],
                                      python: Option[ModelPythonSection],
                                      spark: Option[ModelSparkSection],
                                      version: Option[String])

case class EnvironmentImageAsset(id: Option[String],
                                 mimeType: Option[String],
                                 unpack: Option[Boolean],
                                 url: Option[String])

case class EnvironmentImageRequest(assets: Option[List[EnvironmentImageAsset]],
                                   driverProgram: Option[String],
                                   environment: Option[ModelEnvironmentDefinition],
                                   modelIds: Option[List[String]])

case class AuthKeys(primaryKey: Option[String],
                    secondaryKey: Option[String])

case class AKSConfig(computeName: String,
                     computeType: String,
                     environmentImageRequest: Option[EnvironmentImageRequest],
                     keys: Option[AuthKeys],
                     location: Option[String],
                     name: String)

object AKSConfig extends SparkBindings[AKSConfig]

object AMLConfigFormats extends DefaultJsonProtocol {
  implicit val ContainerRegistryFormat: RootJsonFormat[ContainerRegistry] =
    jsonFormat3(ContainerRegistry.apply)

  implicit val ModelDockerSectionFormat: RootJsonFormat[ModelDockerSection] =
    jsonFormat8(ModelDockerSection.apply)

  implicit val ModelPythonSectionFormat: RootJsonFormat[ModelPythonSection] =
    jsonFormat4(ModelPythonSection.apply)

  implicit val SparkMavenPackageFormat: RootJsonFormat[SparkMavenPackage] =
    jsonFormat3(SparkMavenPackage.apply)

  implicit val ModelSparkSectionFormat: RootJsonFormat[ModelSparkSection] =
    jsonFormat3(ModelSparkSection.apply)

  implicit val ModelEnvironmentDefinitionFormat: RootJsonFormat[ModelEnvironmentDefinition] =
    jsonFormat7(ModelEnvironmentDefinition.apply)

  implicit val EnvironmentImageAssetFormat: RootJsonFormat[EnvironmentImageAsset] =
    jsonFormat4(EnvironmentImageAsset.apply)

  implicit val EnvironmentImageRequestFormat: RootJsonFormat[EnvironmentImageRequest] =
    jsonFormat4(EnvironmentImageRequest.apply)

  implicit val AuthKeysFormat: RootJsonFormat[AuthKeys] =
    jsonFormat2(AuthKeys.apply)

  implicit val AKSConfigFormat: RootJsonFormat[AKSConfig] =
    jsonFormat6(AKSConfig.apply)

}

