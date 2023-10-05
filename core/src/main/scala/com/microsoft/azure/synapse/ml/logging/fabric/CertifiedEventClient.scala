// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.fabric

import org.apache.spark.sql.SparkSession
import spray.json.DefaultJsonProtocol.{StringJsonFormat, _}
import spray.json._

import java.time.Instant
import java.util.UUID
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

object CertifiedEventClient extends RESTUtils {

  private val EmitUsage = "EmitUsage"

  private val FabricFakeTelemetryReportCalls = "fabric_fake_usage_telemetry"

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

  private def getAccessToken: String = {
    val objectName = "com.microsoft.azure.trident.tokenlibrary.TokenLibrary"
    val mirror = currentMirror
    val module = mirror.staticModule(objectName)
    val obj = mirror.reflectModule(module).instance
    val objType = mirror.reflect(obj).symbol.toType
    val methodName = "getAccessToken"
    val methodSymbols = objType.decl(TermName(methodName)).asTerm.alternatives
    val argType = typeOf[String]
    val selectedMethodSymbol = methodSymbols.find { m =>
      m.asMethod.paramLists match {
        case List(List(param)) => param.typeSignature =:= argType
        case _ => false
      }
    }.getOrElse(throw new NoSuchMethodException(s"Method $methodName with argument type $argType not found"))
    val methodMirror = mirror.reflect(obj).reflectMethod(selectedMethodSymbol.asMethod)
    methodMirror("pbi").asInstanceOf[String]
  }

  private def getHeaders: Map[String, String] = {
    Map(
      "Authorization" -> s"""Bearer $getAccessToken""".stripMargin,
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


  private[ml] def reportUsage(featureName: String,
                              activityName: String,
                              attributes: Map[String, String]): Unit = {

    val shouldReport = (sys.env.getOrElse(EmitUsage, "true").toLowerCase == "true") &&
      (sys.env.getOrElse(FabricFakeTelemetryReportCalls, "false").toLowerCase == "false")

    if (shouldReport) {
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
