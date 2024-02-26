// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json.JsValue

import java.net.{MalformedURLException, URL}
import java.util.UUID
import scala.io.Source

object FabricClient extends RESTUtils {
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

  private val WorkloadEndpointTypeML = "ML";
  private val WorkloadEndpointTypeLLMPlugin = "LlmPlugin"
  private val WorkloadEndpointTypeAutomatic = "Automatic"
  private val WorkloadEndpointTypeRegistry = "Registry"
  private val WorkloadEndpointTypeAdmin = "MLAdmin"
  private val ContextFilePath = "/home/trusted-service-user/.trident-context";
  private val SparkConfPath = "/opt/spark/conf/spark-defaults.conf";

  lazy val CapacityID: Option[String] = getCapacityID;
  lazy val WorkspaceID: Option[String] = getWorkspaceID;
  lazy val ArtifactID: Option[String] = getArtifactID;
  lazy val PbiEnv: String = getPbiEnv;
  lazy val FabricContext: Map[String, String] = getFabricContextFile;
  lazy val MLWorkloadHost: Option[String] = getMLWorkloadHost;

  lazy val PbiSharedHost: String = getPbiSharedHost;
  lazy val MLWorkloadEndpointML: String = getMLWorkloadEndpoint(WorkloadEndpointTypeML);
  lazy val MLWorkloadEndpointLLMPlugin: String = getMLWorkloadEndpoint(WorkloadEndpointTypeLLMPlugin);
  lazy val MLWorkloadEndpointAutomatic: String = getMLWorkloadEndpoint(WorkloadEndpointTypeAutomatic);
  lazy val MLWorkloadEndpointRegistry: String = getMLWorkloadEndpoint(WorkloadEndpointTypeRegistry);
  lazy val MLWorkloadEndpointAdmin: String = getMLWorkloadEndpoint(WorkloadEndpointTypeAdmin);
  lazy val MLWorkloadEndpointCognitive: String = s"${MLWorkloadEndpointML}cognitive/";
  lazy val MLWorkloadEndpointOpenAI: String = s"${MLWorkloadEndpointML}cognitive/openai/";

  private def extractSchemeAndHost(urlString: Option[String]): Option[String] = {
    try {
      urlString.map(url => {
        val urlObject = new URL(url)
        s"${urlObject.getProtocol}://${urlObject.getHost}"
      })
    } catch {
      case _: MalformedURLException => None
    }
  }

  private def getFabricContextFile: Map[String, String] = {
    readFabricContextFile() ++ readFabricSparkConfFile()
  }

  private def getCapacityID: Option[String] = {
    FabricContext.get("trident.capacity.id");
  }

  private def getWorkspaceID: Option[String] = {
    FabricContext.get("trident.artifact.workspace.id");
  }

  private def getArtifactID: Option[String] = {
    FabricContext.get("trident.artifact.id");
  }

  private def getPbiEnv: String = {
    FabricContext.getOrElse("spark.trident.pbienv", "public").toLowerCase();
  }

  private def getMLWorkloadHost: Option[String] = {
    extractSchemeAndHost(FabricContext.get("trident.lakehouse.tokenservice.endpoint"))
  }

  private def readFabricContextFile(): Map[String, String] = {
    val source = Source.fromFile(ContextFilePath)
    try {
      source.getLines().flatMap { line =>
        line.split("=", 2) match {
          case Array(_, value) if value.contains("=") =>
            None
          case Array(key, value) =>
            Some(key.trim -> value.trim)
          case _ =>
            None
        }
      }.toMap
    } finally {
      source.close()
    }
  }

  private def readFabricSparkConfFile(): Map[String, String] = {
    val source = Source.fromFile(SparkConfPath)
    try {
      source.getLines().map(_.trim).filterNot(_.startsWith("#")).flatMap { line =>
        line.split(" ", 2) match {
          case Array(_, value) if value.contains(" ") =>
            None
          case Array(key, value) =>
            Some(key.trim -> value.trim)
          case _ =>
            None // Handle lines without "=" or lines with more than one "="
        }
      }.toMap
    } finally {
      source.close()
    }
  }

  private def getHeaders: Map[String, String] = {
    Map(
      "Authorization" -> s"Bearer ${TokenLibrary.getAccessToken}",
      "RequestId" -> UUID.randomUUID().toString,
      "Content-Type" -> "application/json",
      "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
    )
  }

  private def getPbiSharedHost: String = {
    val clusterDetailUrl = s"${PbiGlobalServiceEndpoints(PbiEnv)}powerbi/globalservice/v201606/clusterDetails";
    val headers = getHeaders;
    usageGet(clusterDetailUrl, headers).asJsObject.fields("clusterUrl").convertTo[String];
  }

  private def getMLWorkloadEndpoint(endpointType: String): String = {
    s"${MLWorkloadHost.getOrElse("")}/webapi/capacities" +
      s"/${CapacityID.getOrElse("")}/workloads/ML/$endpointType/Automatic/workspaceid/${WorkspaceID.getOrElse("")}/"
  }

  def usagePost(url: String, body: String): JsValue = {
    usagePost(url, body, getHeaders);
  }
}
