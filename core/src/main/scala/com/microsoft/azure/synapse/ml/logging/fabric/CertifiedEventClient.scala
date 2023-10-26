// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.fabric

import com.microsoft.azure.synapse.ml.fabric.TokenLibrary
import org.apache.spark.sql.SparkSession
import spray.json.DefaultJsonProtocol.{StringJsonFormat, _}
import spray.json._

import java.time.Instant
import java.util.UUID

import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails.runningOnFabric

object CertifiedEventClient extends RESTUtils {

  private val PbiGlobalServiceEndpoints = Map(
    "public" -> "https://api.powerbi.com/",
    "fairfax" -> "https://api.powerbigov.us",
    "mooncake" -> "https://api.powerbi.cn",
    "blackforest" -> "https://app.powerbi.de",
    "msit" -> "https://api.powerbi.com/",
    "prod" -> "https://api.powerbi.com/",
    "int3" -> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
    "dxt" -> "https://powerbistagingapi.analysis.windows.net/",
    "edog" -> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
    "dev" -> "https://onebox-redirect.analysis.windows-int.net/",
    "console" -> "http://localhost:5001/",
    "daily" -> "https://dailyapi.powerbi.com/")


  private lazy val CertifiedEventUri = getCertifiedEventUri

  private def getHeaders: Map[String, String] = {
    Map(
      "Authorization" -> s"Bearer ${TokenLibrary.getAccessToken}",
      "RequestId" -> UUID.randomUUID().toString,
      "Content-Type" -> "application/json",
      "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
    )
  }

  private def getCertifiedEventUri: String = {
    val sc = SparkSession.builder().getOrCreate().sparkContext
    val workspaceId = sc.hadoopConfiguration.get("trident.artifact.workspace.id")
    val capacityId = sc.hadoopConfiguration.get("trident.capacity.id")
    val pbiEnv = sc.getConf.get("spark.trident.pbienv").toLowerCase()

    val clusterDetailUrl = s"${PbiGlobalServiceEndpoints(pbiEnv)}powerbi/globalservice/v201606/clusterDetails"
    val headers = getHeaders

    val clusterUrl = usageGet(clusterDetailUrl, headers)
      .asJsObject.fields("clusterUrl").convertTo[String]
    val tokenUrl: String = s"$clusterUrl/metadata/v201606/generatemwctokenv2"

    val payload =
      s"""{
         |"capacityObjectId": "$capacityId",
         |"workspaceObjectId": "$workspaceId",
         |"workloadType": "ML"
         |}""".stripMargin


    val host = usagePost(tokenUrl, payload, headers)
      .asJsObject.fields("TargetUriHost").convertTo[String]

    s"https://$host/webapi/Capacities/$capacityId/workloads/ML/MLAdmin/Automatic/workspaceid/$workspaceId/telemetry"
  }


  private[ml] def logToCertifiedEvents(featureName: String,
                                       activityName: String,
                                       attributes: Map[String, String]): Unit = {

  if (runningOnFabric) {
      val payload =
        s"""{
           |"timestamp":${Instant.now().getEpochSecond},
           |"feature_name":"$featureName",
           |"activity_name":"$activityName",
           |"attributes":${attributes.toJson.compactPrint}
           |}""".stripMargin

      usagePost(CertifiedEventUri, payload, getHeaders)
    }
  }
}
