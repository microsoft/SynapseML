// Tenant setting status check by model name ("GPT-4", "gpt-35-turbo")
// eg: FabricTenant.getModelStatus("GPT-4")
// returnType: Boolean

// Part of the code are scala implementation of
// synapse.ml.internal_utils.session_utils
// synapse.ml.fabric.token_utils
// synapse.ml.mlflow.synapse_mlflow_utils

import spray.json._

import scala.io.Source
import scala.collection.immutable.Map
import scala.util.Try
import java.net.URI
import java.net.InetAddress
import StringifiedJsonProtocol._

import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils


object StringifiedJsonProtocol extends DefaultJsonProtocol {
  implicit object StringifiedMapFormat extends RootJsonFormat[Map[String, String]] {
    def write(map: Map[String, String]): JsValue = {
      JsObject(map.mapValues(JsString(_)))
    }

    def read(value: JsValue): Map[String, String] = value.asJsObject.fields.map {
      case (key, JsString(str)) => key -> str
      case (key, other) => key -> other.toString
    }
  }
}

object ConfigConstants {
  final val CONTEXT_FILE_PATH = "/home/trusted-service-user/.trident-context"
  final val TOKEN_PATH = "/opt/token-service/tokenservice.config.json"
  final val TRIDENT_LAKEHOUSE_TOKEN_SERVICE_ENDPOINT = "trident.lakehouse.tokenservice.endpoint"
  final val TRIDENT_SESSION_TOKEN = "trident.session.token"
}

object Configs {
  val pbiEnv = sc.getConf.get("spark.trident.pbienv").toLowerCase
  val tokenServiceEndpoint = sc.hadoopConfiguration.get("trident.lakehouse.tokenservice.endpoint")
  val capacityId = sc.hadoopConfiguration.get("trident.capacity.id")
  val workspaceId = sc.hadoopConfiguration.get("trident.artifact.workspace.id")
}

class FabricToken {

  def readJsonFileAsMap(filePath: String): Try[Map[String, String]] = {
    Try {
      val fileContent = Source.fromFile(filePath).getLines.mkString
      fileContent.parseJson.convertTo[Map[String, String]]//.asJsObject.fields
    }
  }

  val tokenServiceConfig = readJsonFileAsMap(ConfigConstants.TOKEN_PATH)
  val tokenServiceEndpoint = tokenServiceConfig.get("tokenServiceEndpoint")
  val clusterIdentifier = tokenServiceConfig.get("clusterName")


  def getTridentContext(contextFilePath: String): Map[String, String] = {
    var tridentContext = Map[String, String]()
    for (line <- Source.fromFile(contextFilePath).getLines()) {
      val parts = line.split('=').map(_.trim)
      if (parts.length == 2) {
        val (k, v) = (parts(0), parts(1))
        tridentContext += (k -> v)
      }
    }
    tridentContext
  }

  val tridentContext = getTridentContext(ConfigConstants.CONTEXT_FILE_PATH)
  val tridentLakehouseTokenServiceEndpoint = tridentContext.get(ConfigConstants.TRIDENT_LAKEHOUSE_TOKEN_SERVICE_ENDPOINT)
  val sessionToken = tridentContext.get(ConfigConstants.TRIDENT_SESSION_TOKEN).get

  def parseUrl(urlOption: Option[String]): Option[URI] = {
    urlOption match {
      case Some(url) =>
        try {
          Some(new URI(url))
        } catch {
          case e: Exception => None // Handle invalid URL or other exceptions
        }
      case None => None
    }
  }
  val urlOption: Option[String] = tridentLakehouseTokenServiceEndpoint
  val TargetUrl = parseUrl(urlOption)
  val hostname = InetAddress.getLocalHost.getHostName
  val scheme = TargetUrl.map(_.getScheme).get
  val host = TargetUrl.map(_.getHost).get
  val path = TargetUrl.map(_.getPath).get

  def getAADToekn(): String = {
    val url = s"${tokenServiceEndpoint}/api/v1/proxy${path}/access?resource=pbi"
    val httpGet = new HttpGet(url)
    httpGet.setHeader("x-ms-cluster-identifier", clusterIdentifier)
    httpGet.setHeader("x-ms-workload-resource-moniker", clusterIdentifier)
    httpGet.setHeader("Content-Type", "application/json;charset=utf-8")
    httpGet.setHeader("x-ms-proxy-host", s"${scheme}://${host}")
    httpGet.setHeader("x-ms-partner-token", sessionToken)
    httpGet.setHeader("User-Agent", s"SynapseML check tenant setting - HostName:${hostname}")
    val client = HttpClients.createDefault()
    val response = client.execute(httpGet)
    val entity = response.getEntity
    val responseString = EntityUtils.toString(entity, "UTF-8")
    response.close()
    client.close()
    responseString
  }
}

object FabricTenant extends FabricToken {
  val _DEFAULT_GLOBAL_SERVICE_ENDPOINT = "https://api.powerbi.com/"
  val _FETCH_CLUSTER_DETAIL_URI = "powerbi/globalservice/v201606/clusterDetails"
  val MWC_WORKLOAD_TYPE_ML = "ML"

  val aadToken = getAADToekn()

  val PbiGlobalServiceEndpoints = Map(
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

  def getMLFlowSharedHost(): String = {
    val url = PbiGlobalServiceEndpoints.getOrElse("msit",_DEFAULT_GLOBAL_SERVICE_ENDPOINT) + _FETCH_CLUSTER_DETAIL_URI
    val httpGet = new HttpGet(url)

    httpGet.setHeader("Authorization", s"Bearer ${aadToken}")
    val client = HttpClients.createDefault()
    val response = client.execute(httpGet)
    val entity = response.getEntity
    val content = EntityUtils.toString(entity)
    val jsonData = content.parseJson.convertTo[Map[String, String]]
    response.close()
    client.close()
    jsonData.getOrElse("clusterUrl","")

  }
  def getMLFlowWorkloadHost(): String = {
    val client = HttpClients.createDefault()

    val url = getMLFlowSharedHost + "/metadata/v201606/generatemwctokenv2"
    val httpPost = new HttpPost(url)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setHeader("Authorization", s"Bearer ${aadToken}")

    val payload = s"""{"capacityObjectId": "${Configs.capacityId}", "workspaceObjectId": "${Configs.workspaceId}", "workloadType": "${MWC_WORKLOAD_TYPE_ML}"}""""
    httpPost.setEntity(new StringEntity(payload, "UTF-8"))

    val response = client.execute(httpPost)
    val entity = response.getEntity
    val content = EntityUtils.toString(entity, "UTF-8")
    val jsonData = content.parseJson.convertTo[Map[String, String]]
    response.close()
    client.close()
    val targetUriHost = "https://" + jsonData.getOrElse("TargetUriHost", "")
    targetUriHost
  }

  def getMLFlowWorkloadEndpoint(): String = {
    val workloadHost = getMLFlowWorkloadHost
    val mlflowWorkloadEndpoint = s"${workloadHost}/webapi/capacities/${Configs.capacityId}/workloads/ML/ML/Automatic/workspaceid/${Configs.workspaceId}/"
    mlflowWorkloadEndpoint
  }

  def getModelStatus(modelName: String): Boolean = {

    val models = List(modelName)
    val jsonString = models.toJson.compactPrint

    val client = HttpClients.createDefault()
    val url = getMLFlowWorkloadEndpoint + "cognitive/openai/tenantsetting"
    val httpPost = new HttpPost(url)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setHeader("Authorization", s"Bearer ${aadToken}")
    httpPost.setEntity(new StringEntity(jsonString))

    val response = client.execute(httpPost)
    val responseString = EntityUtils.toString(response.getEntity)

    response.close()
    client.close()
    val responseField = responseString.parseJson.asJsObject.fields.get(modelName.toLowerCase).get
    val resultString: String = responseField match {
      case JsString(value) => value  // Directly extract the string value
    }
    // Allowed, Disallowed, DisallowedForCrossGeo, ModelNotFound, InvalidResult
    resultString == "Allowed"
  }
}