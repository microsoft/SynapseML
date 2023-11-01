// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.SparkMavenRepositoryList
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import com.microsoft.azure.synapse.ml.nbtest.SharedNotebookE2ETestUtilities._
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicNameValuePair
import spray.json._

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}

object SynapseExtensionUtilities {

  import SynapseJsonProtocol._

  object Environment extends Enumeration {
    type Environment = Value
    val EDog, Daily, DXT, MSIT = Value

    def withNameOpt(s: String): Option[Value] = values.find(_.toString.toLowerCase == s.toLowerCase)
  }

  object Resource extends Enumeration {
    type Resource = Value
    val SSPHost, WorkspaceId, UxHost, TenantId, Password, AadAccessTokenResource, Login = Value

    def withNameOpt(s: String): Option[Value] = values.find(_.toString.toLowerCase == s.toLowerCase)
  }

  val TimeoutInMillis: Int = 60 * 60 * 1000
  val DefaultLogin = "login.microsoftonline.com"
  val PpeLogin = "login.windows-ppe.net"

  // TODO: Edog is not yet available.
  // Details: 401 Unauthorized upon creating the lakehouse
  lazy val EdogResources = HashMap(
    Resource.SSPHost -> Secrets.SynapseExtensionEdogSspHost,
    Resource.WorkspaceId -> Secrets.SynapseExtensionEdogWorkspaceId,
    Resource.UxHost -> Secrets.SynapseExtensionEdogUxHost,
    Resource.TenantId -> Secrets.SynapseExtensionEdogTenantId,
    Resource.Password -> Secrets.SynapseExtensionEdogPassword,
    Resource.AadAccessTokenResource -> "https://analysis.windows-int.net/powerbi/api",
    Resource.Login -> PpeLogin)

  lazy val DailyResources = HashMap(
    Resource.SSPHost -> Secrets.SynapseExtensionDailySspHost,
    Resource.WorkspaceId -> Secrets.SynapseExtensionDailyWorkspaceId,
    Resource.UxHost -> Secrets.SynapseExtensionDailyUxHost,
    Resource.TenantId -> Secrets.SynapseExtensionDailyTenantId,
    Resource.Password -> Secrets.SynapseExtensionDailyPassword,
    Resource.AadAccessTokenResource -> Secrets.AadResource,
    Resource.Login -> DefaultLogin)

  lazy val DxtResources = HashMap(
    Resource.SSPHost -> Secrets.SynapseExtensionDxtSspHost,
    Resource.WorkspaceId -> Secrets.SynapseExtensionDxtWorkspaceId,
    Resource.UxHost -> Secrets.SynapseExtensionDxtUxHost,
    Resource.TenantId -> Secrets.SynapseExtensionDxtTenantId,
    Resource.Password -> Secrets.SynapseExtensionDxtPassword,
    Resource.AadAccessTokenResource -> Secrets.AadResource,
    Resource.Login -> DefaultLogin)

  // TODO: MSIT is not yet available.
  // Details: We get PowerBiFeatureDisabled and a 404 upon creating the lakehouse
  lazy val MsitResources = HashMap(
    Resource.SSPHost -> Secrets.SynapseExtensionMsitSspHost,
    Resource.WorkspaceId -> Secrets.SynapseExtensionMsitWorkspaceId,
    Resource.UxHost -> Secrets.SynapseExtensionMsitUxHost,
    Resource.TenantId -> Secrets.SynapseExtensionMsitTenantId,
    Resource.Password -> Secrets.SynapseExtensionMsitPassword,
    Resource.AadAccessTokenResource -> Secrets.AadResource,
    Resource.Login -> DefaultLogin)

  val DefaultEnvironment = Environment.Daily
  val ResourceMap = getResources(DefaultEnvironment)
  val SSPHost: String = ResourceMap(Resource.SSPHost)
  val WorkspaceId: String = ResourceMap(Resource.WorkspaceId)
  val UxHost: String = ResourceMap(Resource.UxHost)
  val TenantId: String = ResourceMap(Resource.TenantId)
  val Password: String = ResourceMap(Resource.Password)
  val AadAccessTokenResource: String = ResourceMap(Resource.AadAccessTokenResource)
  val Login: String = ResourceMap(Resource.Login)

  val BaseUri: String = s"$SSPHost/metadata"
  val ArtifactsUri: String = s"$BaseUri/workspaces/$WorkspaceId/artifacts?"

  val AadAccessTokenClientId: String = "1950a258-227b-4e31-a9cf-717495945fc2"

  val Folder: String = s"build_${BuildInfo.version}/synapseextension/notebooks"
  val StorageAccount: String = "mmlsparkbuildsynapse"
  val StorageContainer: String = "synapse-extension"

  lazy val AccessToken: String = getAccessToken

  val Platform: String = Secrets.Platform.toUpperCase

  def createSJDArtifact(path: String): String = {
    createSJDArtifact(path, "SparkJobDefinition")
  }

  def updateSJDArtifact(path: String, artifactId: String, storeId: String): Artifact = {
    val eTag = getETagFromArtifact(artifactId)
    val store = Secrets.ArtifactStore.capitalize
    val sparkVersion = "3.4"
    val packages: String = "com.microsoft.azure:synapseml_2.12:" + BuildInfo.version

    val workloadPayload =
      s"""
         |"{
         |  'Default${store}ArtifactId': '$storeId',
         |  'ExecutableFile': '$path',
         |  'SparkVersion': '$sparkVersion',
         |  'SparkSettings': {
         |    'spark.jars.packages' : '$packages',
         |    'spark.jars.repositories' : '$SparkMavenRepositoryList',
         |    'spark.executorEnv.IS_$Platform': 'true',
         |    'spark.sql.extensions': 'com.microsoft.azure.synapse.ml.predict.PredictExtension',
         |    'spark.synapse.ml.predict.enabled': 'true',
         |    'spark.executor.heartbeatInterval': '60s',
         |   }
         |}"
    """.stripMargin

    val reqBody: String =
      s"""
         |{
         |  "workloadPayload": $workloadPayload
         |}
         |""".stripMargin

    val uri = s"$BaseUri/artifacts/$artifactId"
    patchRequest(uri, reqBody, eTag).convertTo[Artifact]
  }

  def createSJDArtifact(path: String, artifactType: String): String = {
    val runName = getBlobNameFromFilepath(path).replace(".py", "")

    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd-HH-mm-ss")
    val now = dtf.format(LocalDateTime.now)

    val reqBody: String =
      s"""
         |{
         |  "displayName": "$runName-$now",
         |  "description": "Synapse Spark Job Definition $artifactType",
         |  "artifactType": "$artifactType"
         |}
         |""".stripMargin
    val response = postRequest(ArtifactsUri, reqBody).asJsObject().convertTo[Artifact]
    println(s"Created SJD for $runName: ${getSparkJobDefinitionLink(response.objectId)}")
    response.objectId
  }

  def createStoreArtifact(): String = {
    val store = Secrets.ArtifactStore.capitalize
    val dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val now = dtf.format(LocalDateTime.now)

    val reqBody: String =
      s"""
         |{
         |  "displayName": "$store$now",
         |  "description": "SynapseML Test Infra $store",
         |  "artifactType": "$store"
         |}
         |""".stripMargin
    val response = postRequest(ArtifactsUri, reqBody).asJsObject().convertTo[Artifact]
    response.objectId
  }

  def deleteArtifact(artifactId: String): Unit = {
    val uri: String = s"$SSPHost/metadata/artifacts/$artifactId"
    deleteRequest(uri)
  }

  def submitJob(artifactId: String): String = {
    val uri: String = s"$BaseUri/artifacts/$artifactId/jobs/sparkjob"
    val createRequest = new HttpPost(uri)
    createRequest.setHeader("Authorization", s"$AccessToken")

    val response = postRequest(uri).asJsObject().convertTo[SparkJobDefinitionExecutionResponse]
    response.artifactJobInstanceId
  }

  def monitorJob(artifactId: String, jobInstanceId: String): Future[String] = {
    Future {
      pollExecutionStatus(
        artifactId, jobInstanceId,
        SynapseUtilities.TimeoutInMillis,
        System.currentTimeMillis())
    }(ExecutionContext.global)
  }

  @tailrec
  private def pollExecutionStatus(artifactId: String, jobInstanceId: String, timeout: Int, startTime: Long): String = {
    val state = getJobStatus(artifactId, jobInstanceId)
    if (state != null && state.statusString == "success") {
      state.statusString
    } else {
      if ((System.currentTimeMillis() - startTime) > timeout) {
        throw new TimeoutException(s"Job $jobInstanceId timed out.")
      }
      else if (state != null && Seq("Failed", "Dead", "Error").contains(state.statusString)) {
        println("Notebook failed: " + getSparkJobDefinitionLink(artifactId))
        throw new RuntimeException(state.statusString)
      }
      else if (state != null && state.statusString == "Completed") {
        println("Notebook completed: " + getSparkJobDefinitionLink(artifactId))
        state.statusString
      }
      else {
        blocking {
          Thread.sleep(8000)
        }
        pollExecutionStatus(artifactId, jobInstanceId, timeout, startTime)
      }
    }
  }

  def getJobStatus(artifactId: String, jobInstanceId: String): SparkJobDefinitionExecutionResponse = {
    val uri = s"$BaseUri/artifacts/$artifactId/jobs/$jobInstanceId"
    val response = getRequest(uri).asJsObject().convertTo[SparkJobDefinitionExecutionResponse]
    println(s"$uri: $response")
    response
  }

  def postRequest(uri: String, requestBody: String = ""): JsValue = {
    val createRequest = new HttpPost(uri)
    setRequestContentTypeAndAuthorization(createRequest)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(TimeoutInMillis)
      .setConnectTimeout(TimeoutInMillis)
      .setConnectionRequestTimeout(TimeoutInMillis)
      .build

    createRequest.setConfig(requestConfig)

    if (requestBody.nonEmpty) {
      createRequest.setEntity(new StringEntity(requestBody))
    }
    sendAndParseJson(createRequest)
  }

  def getRequest(uri: String): JsValue = {
    val getRequest = new HttpGet(uri)
    setRequestContentTypeAndAuthorization(getRequest)
    sendAndParseJson(getRequest)
  }

  def getETagFromArtifact(artifactId: String): String = {
    val uri = s"$BaseUri/artifacts/$artifactId"
    val getRequest = new HttpGet(uri)
    setRequestContentTypeAndAuthorization(getRequest)
    val response = safeSend(getRequest, close = false)
    val eTag = response.getFirstHeader("ETag").getElements.last.getValue
    response.close()
    eTag
  }

  def deleteRequest(uri: String): CloseableHttpResponse = {
    val deleteRequest = new HttpDelete(uri)
    setRequestContentTypeAndAuthorization(deleteRequest)
    safeSend(deleteRequest)
  }

  def patchRequest(uri: String, requestBody: String, etag: String): JsValue = {
    val patchRequest = new HttpPatch(uri)
    setRequestContentTypeAndAuthorization(patchRequest)
    patchRequest.setHeader("If-Match", etag)
    patchRequest.setEntity(new StringEntity(requestBody))
    sendAndParseJson(patchRequest)
  }

  def setRequestContentTypeAndAuthorization(request: HttpRequestBase): Unit = {
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", s"$AccessToken")
  }

  def uploadNotebookToAzure(notebook: File): String = {
    val dest = s"$Folder/${notebook.getName}"
    exec(s"az storage fs file upload " +
      s" -s ${notebook.getAbsolutePath} -p $dest -f $StorageContainer " +
      " --overwrite true " +
      s" --account-name $StorageAccount --account-key ${Secrets.SynapseStorageKey}")
    s"https://$StorageAccount.blob.core.windows.net/$StorageContainer/$dest"
  }

  def getBlobNameFromFilepath(filePath: String): String = {
    filePath.split(File.separatorChar).last
  }

  def listArtifacts(): Seq[Artifact] = {
    val uri: String = s"$SSPHost/metadata/workspaces/$WorkspaceId/artifacts"
    getRequest(uri).convertTo[Seq[Artifact]]
  }

  def getSparkJobDefinitionLink(SjdArtifactId: String): String = {
    val queryString = Platform.toLowerCase + "=1"
    s"$UxHost/groups/$WorkspaceId/sparkjobdefinitions/$SjdArtifactId?$queryString"
  }

  def getAccessToken: String = {
    val createRequest = new HttpPost(s"https://$Login/$TenantId/oauth2/token")
    createRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")
    createRequest.setEntity(
      new UrlEncodedFormEntity(
        List(
          ("resource", AadAccessTokenResource),
          ("client_id", AadAccessTokenClientId),
          ("grant_type", "password"),
          ("username", s"AdminUser@$TenantId"),
          ("password", Password),
          ("scope", "openid")
        ).map(p => new BasicNameValuePair(p._1, p._2)).asJava, "UTF-8")
    )
    "Bearer " + RESTHelpers.sendAndParseJson(createRequest).asJsObject()
      .fields("access_token").convertTo[String]
  }

  def getResources(defaultEnv: Environment.Value): HashMap[Resource.Value, String] = {
    val environment = getWorkingEnvironment(defaultEnv)
    environment match {
      case Environment.EDog => EdogResources
      case Environment.Daily => DailyResources
      case Environment.DXT => DxtResources
      case Environment.MSIT => MsitResources
    }
  }

  def getWorkingEnvironment(defaultEnv: Environment.Value): Environment.Value = {
    val undefined = ""
    val varName = "SYNAPSE_ENVIRONMENT"
    val userEnv = sys.env.get(varName).getOrElse(undefined)
    val envValue = Environment.withNameOpt(userEnv)
    if (userEnv != undefined && envValue == None) {
      println(s"WARNING: value of $varName ($userEnv) is not recognized")
    }
    val result = if (envValue != None) envValue.get else defaultEnv
    println(s"Using environment ${result.toString}")
    result
  }
}

object SynapseJsonProtocol extends DefaultJsonProtocol {
  implicit object LocalDateTimeFormat extends RootJsonFormat[LocalDateTime] {
    def write(dt: LocalDateTime): JsValue = JsString(dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

    def read(value: JsValue): LocalDateTime =
      LocalDateTime.parse(value.toString().replaceAll("^\"+|\"+$", ""),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }

  implicit val ArtifactFormat: RootJsonFormat[Artifact] =
    jsonFormat3(Artifact.apply)
  implicit val SparkJobDefinitionExecutionResponseFormat: RootJsonFormat[SparkJobDefinitionExecutionResponse] =
    jsonFormat3(SparkJobDefinitionExecutionResponse.apply)

}
