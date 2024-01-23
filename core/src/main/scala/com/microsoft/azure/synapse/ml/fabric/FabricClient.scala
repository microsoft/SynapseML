package com.microsoft.azure.synapse.ml.fabric

import org.apache.log4j.Logger
import spray.json.DefaultJsonProtocol.StringJsonFormat

import java.util.UUID
import scala.io.Source
import scala.util.{Failure, Success, Try}
import spray.json.JsValue

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

  var CapacityID = "";
  var WorkspaceID = "";
  var ArtifactID = "";
  var PbiEnv = "";
  var FabricContext: Map[String, String] = Map[String, String]();

  private val WorkloadEndpointTypeML = "ML";
  private val WorkloadEndpointTypeLLMPlugin = "LlmPlugin"
  private val WorkloadEndpointTypeAutomatic = "Automatic"
  private val WorkloadEndpointTypeRegistry = "Registry"
  private val WorkloadEndpointTypeAdmin = "MLAdmin"

  lazy val PbiSharedHost: String = getPbiSharedHost;
  lazy val MLWorkloadHost: String = getMLWorkloadHost;
  lazy val MLWorkloadEndpointML: String = getMLWorkloadEndpoint(WorkloadEndpointTypeML);
  lazy val MLWorkloadEndpointLLMPlugin: String = getMLWorkloadEndpoint(WorkloadEndpointTypeLLMPlugin);
  lazy val MLWorkloadEndpointAutomatic: String = getMLWorkloadEndpoint(WorkloadEndpointTypeAutomatic);
  lazy val MLWorkloadEndpointRegistry: String = getMLWorkloadEndpoint(WorkloadEndpointTypeRegistry);
  lazy val MLWorkloadEndpointAdmin: String = getMLWorkloadEndpoint(WorkloadEndpointTypeAdmin);
  lazy val MLWorkloadEndpointCognitive: String = s"${MLWorkloadEndpointML}cognitive/";
  lazy val MLWorkloadEndpointOpenAI: String = s"${MLWorkloadEndpointML}cognitive/openai/";

  private val ContextFilePath = "/home/trusted-service-user/.trident-context";
  private val SparkConfPath = "/opt/spark/conf/spark-defaults.conf";

  lazy val MyLogger: Logger = Logger.getLogger(this.getClass.getName);

  readFabricContextFile();
  readFabricSparkConfFile();
  init();

  def init(): Unit ={
    this.CapacityID = this.FabricContext.getOrElse("trident.capacity.id", "");
    this.WorkspaceID = this.FabricContext.getOrElse("trident.artifact.workspace.id", "");
    this.ArtifactID = this.FabricContext.getOrElse("trident.artifact.id", "");
    this.PbiEnv = this.FabricContext.getOrElse("spark.trident.pbienv", "public").toLowerCase();
  }

  def readFabricContextFile(): Unit = {
    Try(Source.fromFile(ContextFilePath)) match {
      case Success(source) =>
        try {
          for (line <- source.getLines()) {
            val keyValue = line.split('=')
            if (keyValue.length == 2) {
              val Array(k, v) = keyValue.map(_.trim)
              FabricContext += (k -> v)
            }
          }
          source.close()
        } catch {
          case e: Exception =>
            MyLogger.error("Error reading Fabric context file", e)
        }
      case Failure(exception) =>
        MyLogger.error("Error opening Fabric context file", exception)
    }
  }

  def readFabricSparkConfFile(): Unit = {
    Try(Source.fromFile(SparkConfPath)) match {
      case Success(source) =>
        try {
          for (line <- source.getLines()) {
            val trimmedLine = line.trim();
            if (trimmedLine.nonEmpty && !trimmedLine.startsWith("#")) {
              val content = trimmedLine.split(' ');
              if (content.length == 2) {
                FabricContext += (content(0).trim() -> content(1).trim());
              }
            }
          }
          source.close()
        } catch {
          case e: Exception =>
            MyLogger.error("Error reading Fabric spark conf file", e)
        }
      case Failure(exception) =>
        MyLogger.error("Error opening Fabric spark conf file", exception)
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

  def getPbiSharedHost: String = {
    val clusterDetailUrl = s"${PbiGlobalServiceEndpoints(PbiEnv)}powerbi/globalservice/v201606/clusterDetails";
    val headers = getHeaders;
    usageGet(clusterDetailUrl, headers).asJsObject.fields("clusterUrl").convertTo[String];
  }

  def getMLWorkloadHost: String = {
    val payload =
      s"""{
         |"capacityObjectId": "$CapacityID",
         |"workspaceObjectId": "$WorkspaceID",
         |"workloadType": "ML"
         |}""".stripMargin

    val tokenUrl: String = s"$PbiSharedHost/metadata/v201606/generatemwctokenv2"

    val targetHost: String = usagePost(tokenUrl, payload, getHeaders)
      .asJsObject.fields("TargetUriHost").convertTo[String];

    s"https://$targetHost"
  }

  def getMLWorkloadEndpoint(endpointType: String): String = {
    s"$MLWorkloadHost/webapi/capacities/$CapacityID/workloads/ML/$endpointType/Automatic/workspaceid/$WorkspaceID/"
  }

  def usagePost(url: String, body: String): JsValue = {
    usagePost(url, body, getHeaders);
  }
}
