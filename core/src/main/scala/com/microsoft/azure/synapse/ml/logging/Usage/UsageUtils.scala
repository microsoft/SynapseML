package com.microsoft.azure.synapse.ml.logging.Usage
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.logging.common.WebUtils.{usageGet, usagePost}
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import java.util.UUID
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol.StringJsonFormat
import com.microsoft.azure.synapse.ml.logging.Usage.TokenUtils.getAccessToken
import com.microsoft.azure.synapse.ml.logging.Usage.TokenUtils
import com.microsoft.azure.synapse.ml.logging.Usage.MwcToken
import com.microsoft.azure.synapse.ml.logging.Usage.FeatureUsagePayload

object UsageTelemetry {
  // val sc = SparkSession.builder().getOrCreate().sparkContext
  val CapacityId = getHadoopConfig("trident.capacity.id")
  val WorkspaceId = getHadoopConfig("trident.artifact.workspace.id")
  val ArtifactId = getHadoopConfig("trident.artifact.id")
  val OnelakeEndpoint = getHadoopConfig("trident.onelake.endpoint")
  val Region = getHadoopConfig("spark.cluster.region")
  val PbiEnv = getHadoopConfig("spark.trident.pbienv").toLowerCase()

  val SharedHost = getMlflowSharedHost(PbiEnv)
  val shared_endpoint = f"{SharedHost}/metadata/workspaces/{WorkspaceId}/artifacts"
  val wlHost = getMlflowWorkloadHost(PbiEnv, CapacityId, WorkspaceId, SharedHost)

  val FabricFakeTelemetryReportCalls = "fabric_fake_usage_telemetry"
  def reportUsage(payload: FeatureUsagePayload): Unit = {
    if (sys.env.getOrElse(EMIT_USAGE, "True") != "True") {
      return
    }
    try {
      reportUsageTelemetry(payload.feature_name.toString, payload.activity_name.toString.replace('_', '/'), payload.attributes.toMap)
    } catch {
      case runtimeError: Exception =>
        SynapseMLLogging.logMessage(s"_report_usage_telemetry: usage telemetry error = $runtimeError")
    }
  }

  def reportUsageTelemetry(featureName: String, activityName: String, attributes: Map[String,String] = Map()): Unit = {
    SynapseMLLogging.logMessage(s"usage telemetry feature_name: $featureName, activity_name: $activityName, attributes: $attributes")
    if (sys.env.getOrElse(FabricFakeTelemetryReportCalls,"false") == "false") {
      val data =
        s"""{
           |"timestamp":${System.currentTimeMillis()},
           |"feature_name":"$featureName",
           |"activity_name":"${activityName.replace('.', '-')}",
           |"attributes":${attributes.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")},
           |"session_id":"${UUID.randomUUID().toString}"
           |}""".stripMargin

      val mlAdminEndpoint = getMLWorkloadEndpoint(WORKLOAD_ENDPOINT_ADMIN)
      val url = """{$mlAdminEndpoint}telemetry""".stripMargin

      val driverAADToken = getAccessToken()

      val headers = Map(
        "Content-Type" -> "application/json",
        "Authorization" -> s"""Bearer $driverAADToken""".stripMargin,
        "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
      )

      var response: JsValue = JsonParser("")
      try {
        response = usagePost(url, "", headers)
        if (response.asJsObject.fields("status_code").convertTo[String] != 200
          || response.asJsObject.fields("content").toString().isEmpty) {
          throw new Exception("Fetch access token error")
        }
      } catch {
        case e: Exception =>
          SynapseMLLogging.logMessage(s"sending $e")
      }
      response.asJsObject.fields("content").toString().getBytes("UTF-8")
    }
  }

  def getHadoopConfig(key: String): String = {
    if (sc == null) {
      ""
    } else {
      val value = sc.hadoopConfiguration.get(key, "")
      if (value.isEmpty) {
        throw new Exception(s"missing $key in hadoop config, mlflow failed to init")
      }
      value
    }
  }

  val PbiGlobalServiceEndpoints = Map (
    "public" -> "https://api.powerbi.com/",
    "fairfax" -> "https://api.powerbigov.us",
    "mooncake" -> "https://api.powerbi.cn",
    "blackforest" -> "https://app.powerbi.de",
    "msit" -> "https://api.powerbi.com/",
    "prod"-> "https://api.powerbi.com/",
    "int3"-> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
    "dxt" -> "https://powerbistagingapi.analysis.windows.net/",
    "edog" -> "https://biazure-int-edog-redirect.analysis-df.windows.net/",
    "dev" -> "https://onebox-redirect.analysis.windows-int.net/",
    "console" -> "http://localhost:5001/",
    "daily"-> "https://dailyapi.powerbi.com/")


  val DefaultGlobalServiceEndpoint: String = "https://api.powerbi.com/"
  val FetchClusterDetailUri: String = "powerbi/globalservice/v201606/clusterDetails"
  def getMlflowSharedHost(pbienv: String): String = {
    val url = PbiGlobalServiceEndpoints.getOrElse(pbienv, DefaultGlobalServiceEndpoint) + FetchClusterDetailUri
    //val sessionToken = FabricUtils.getFabricContext()(TRIDENT_SESSION_TOKEN)
    //todo: check if we need pbi token

    val headers = Map(
      "Authorization" -> s"Bearer ${TokenUtils.getAccessToken()}",
      "RequestId" -> java.util.UUID.randomUUID().toString
    )
    var response: JsValue = JsonParser("")
    try{
      response = usageGet(url, headers)
    }
    catch
    {
      case e: Exception =>
        SynapseMLLogging.logMessage(s"sending $e")
    }
    response.asJsObject.fields("clusterUrl").convertTo[String]
  }

  def getMlflowWorkloadHost(pbienv: String, capacityId: String, workspaceId: String, sharedHost: String = ""): String = {
    val clusterUrl = if (sharedHost.isEmpty) {
      getMlflowSharedHost(pbienv)
    } else {
      sharedHost
    }
    val mwcToken: MwcToken = TokenUtils.getMWCToken(clusterUrl, workspaceId, capacityId, TokenUtils.MWC_WORKLOAD_TYPE_ML)
    if (mwcToken != null && mwcToken.TargetUriHost != null) {
      mwcToken.TargetUriHost
    } else {
      ""
    }
  }

  def getMLWorkloadEndpoint(endpoint: String): String = {
    val ml_workload_endpoint = s"${this.wlHost}/$WEB_API/$CAPACITIES/${this.CapacityId}/$WORKLOADS/" +
      s"$WORKLOAD_ENDPOINT_ML/$endpoint/$WORKLOAD_ENDPOINT_AUTOMATIC/${WORKSPACE_ID}/${this.WorkspaceId}/"
    ml_workload_endpoint
  }
}
