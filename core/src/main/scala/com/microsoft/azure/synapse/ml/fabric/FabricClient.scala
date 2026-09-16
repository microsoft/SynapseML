// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import spray.json.DefaultJsonProtocol.{StringJsonFormat, mapFormat}
import spray.json._

import java.net.{MalformedURLException, URI, URL}
import java.util.{Locale, UUID}
import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.util.control.NonFatal

object FabricClient extends RESTUtils {
  private val WorkloadEndpointTypeML = "ML";
  private val WorkloadEndpointTypeLLMPlugin = "LlmPlugin"
  private val WorkloadEndpointTypeAutomatic = "Automatic"
  private val WorkloadEndpointTypeRegistry = "Registry"
  private val WorkloadEndpointTypeAdmin = "MLAdmin"
  private val ContextFilePath = "/home/trusted-service-user/.trident-context";
  private val SparkConfPath = "/opt/spark/conf/spark-defaults.conf";
  private val ClusterInfoPath = "/opt/health-agent/conf/cluster-info.json";
  private val CognitiveMwcRefreshLocks = TrieMap.empty[(String, String), AnyRef]
  private val AmbiguousPathEncoding = "(?i)%(?:25)*(?:2e|2f|5c)".r

  lazy val CapacityID: Option[String] = getCapacityID;
  lazy val WorkspaceID: Option[String] = getWorkspaceID;
  lazy val ArtifactID: Option[String] = getArtifactID;
  lazy val PbiEnv: String = getPbiEnv;
  lazy val FabricContext: Map[String, String] = getFabricContextFile;
  lazy val MLWorkloadHost: Option[String] = getMLWorkloadHost;
  lazy val WorkspacePeEnabled: Boolean = getWorkspacePeEnabled;

  lazy val PbiSharedHost: Option[String] = getPbiSharedHost;
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
    FabricContext.get("trident.artifact.workspace.id").orElse(FabricContext.get("trident.workspace.id"));
  }

  private def getArtifactID: Option[String] = {
    FabricContext.get("trident.artifact.id");
  }

  private def getPbiEnv: String = {
    FabricContext.getOrElse("spark.trident.pbienv", "public").toLowerCase();
  }

  private def getMLWorkloadHost: Option[String] = {
    if (WorkspacePeEnabled) {
      getMLWorkloadPEHost
    } else {
      extractSchemeAndHost(FabricContext.get("trident.lakehouse.tokenservice.endpoint"))
    }
  }

  private def getMLWorkloadPEHost: Option[String] = {
    WorkspaceID.map { wsId =>
      val cleanedWsId = wsId.toLowerCase.replace("-", "")
      val envMark = PbiEnv match {
        case "daily" | "dxt" | "msit" => s"$PbiEnv-"
        case _ => ""
      }
      s"https://${cleanedWsId}.z${cleanedWsId.take(2)}.${envMark}c.fabric.microsoft.com"
    }
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

  private def readClusterMetadata(): Map[String, String] = {
    val source = Source.fromFile(ClusterInfoPath)
    try {
      val jsonString = try source.mkString finally source.close()
      val jsValue = jsonString.parseJson
      val clusterMetadataJson = jsValue.asJsObject.fields("cluster_metadata")
      clusterMetadataJson.convertTo[Map[String, String]]
    } catch {
      case _: Exception => Map.empty[String, String]
    } finally {
      source.close()
    }
  }

  private def getWorkspacePeEnabled: Boolean = {
    val metadata = readClusterMetadata()
    metadata.get("workspace-pe-enabled").exists(_.equalsIgnoreCase("true"))
  }

  private def getHeaders: Map[String, String] = {
    Map(
      "Authorization" -> s"${getMLWorkloadAADAuthHeader}",
      "RequestId" -> UUID.randomUUID().toString,
      "Content-Type" -> "application/json"
    )
  }

  private def getPbiSharedHost: Option[String] = {
    if (WorkspacePeEnabled) {
      getPEPbiSharedHost
    } else {
      val endpoint = FabricContext.get("spark.trident.pbiHost") match {
        case Some(value) if value.nonEmpty =>
          value.replace("https://", "").replace("http://", "")
        case _ =>
          PbiEnv match {
            case "edog"  => "powerbiapi.analysis-df.windows.net"
            case "daily" => "dailyapi.fabric.microsoft.com"
            case "dxt"   => "dxtapi.fabric.microsoft.com"
            case "msit"  => "msitapi.fabric.microsoft.com"
            case _       => "api.fabric.microsoft.com"
          }
      }
      Some("https://" + endpoint)
    }
  }

  private def getPEPbiSharedHost: Option[String] = {
    WorkspaceID.map { wsId =>
      val cleanedWsId = wsId.toLowerCase.replace("-", "")
      val envMark = PbiEnv match {
        case "daily" | "dxt" | "msit" => PbiEnv
        case _ => ""
      }
      s"https://${cleanedWsId}.z${cleanedWsId.take(2)}.w.${envMark}api.fabric.microsoft.com"
    }
  }

  private def getMLWorkloadEndpoint(endpointType: String): String = {
    s"${MLWorkloadHost.getOrElse("")}/webapi/capacities" +
      s"/${CapacityID.getOrElse("")}/workloads/ML/$endpointType/Automatic/workspaceid/${WorkspaceID.getOrElse("")}/"
  }

  def usagePost(url: String, body: String): JsValue = {
    usagePost(url, body, getHeaders);
  }

  def getMLWorkloadAADAuthHeader: String = TokenLibrary.getMLWorkloadAADAuthHeader

  def getCognitiveMWCTokenAuthHeader: String = {
    TokenLibrary.getCognitiveMwcTokenAuthHeader(WorkspaceID.getOrElse(""), ArtifactID.getOrElse(""))
  }

  private[ml] def isEndpointUnder(requestUrl: String, endpointRoot: String): Boolean = {
    try {
      val request = new URL(requestUrl)
      val root = new URL(endpointRoot)
      val requestPort = if (request.getPort >= 0) request.getPort else request.getDefaultPort
      val rootPort = if (root.getPort >= 0) root.getPort else root.getDefaultPort
      val requestPath = trustedPath(new URI(requestUrl).getRawPath)
      val rootPath = trustedPath(new URI(endpointRoot).getRawPath)
      requestPath.exists(path =>
        rootPath.exists(rootPrefix =>
          request.getProtocol.equalsIgnoreCase("https") &&
            root.getProtocol.equalsIgnoreCase("https") &&
            request.getHost.equalsIgnoreCase(root.getHost) &&
            requestPort == rootPort &&
            path.startsWith(rootPrefix)))
    } catch {
      case _: MalformedURLException | _: java.net.URISyntaxException => false
    }
  }

  private def trustedPath(rawPath: String): Option[String] = {
    Option(rawPath).flatMap { path =>
      val lowerPath = path.toLowerCase(Locale.ROOT)
      val hasAmbiguousEncoding = AmbiguousPathEncoding.findFirstIn(lowerPath).nonEmpty
      val normalizedPath = path.replaceAll("/+", "/")
      val hasDotSegment = normalizedPath.split("/", -1).exists(segment => segment == "." || segment == "..")
      if (hasAmbiguousEncoding || path.contains("\\") || hasDotSegment) None else Some(normalizedPath)
    }
  }

  private[ml] def isOpenAIEndpoint(requestUrl: String): Boolean = {
    try {
      isEndpointUnder(requestUrl, MLWorkloadEndpointOpenAI)
    } catch {
      case NonFatal(_) => false
    }
  }

  def refreshCognitiveMWCTokenAuthHeader(rejectedAuthHeader: String): String = {
    val workspaceId = WorkspaceID.getOrElse("")
    val artifactId = ArtifactID.getOrElse("")
    val refreshLock = CognitiveMwcRefreshLocks.getOrElseUpdate((workspaceId, artifactId), new Object())
    refreshAuthHeader(
      rejectedAuthHeader,
      refreshLock,
      () => TokenLibrary.getCognitiveMwcTokenAuthHeader(workspaceId, artifactId),
      () => TokenLibrary.invalidateSparkMwcToken(workspaceId, artifactId))
  }

  private[ml] def refreshAuthHeader(
      rejectedAuthHeader: String,
      refreshLock: AnyRef,
      getCurrentAuthHeader: () => String,
      invalidateAuthHeader: () => Unit): String = {
    refreshLock.synchronized {
      val currentAuthHeader = getCurrentAuthHeader()
      if (currentAuthHeader != rejectedAuthHeader) {
        currentAuthHeader
      } else {
        invalidateAuthHeader()
        getCurrentAuthHeader()
      }
    }
  }
}
