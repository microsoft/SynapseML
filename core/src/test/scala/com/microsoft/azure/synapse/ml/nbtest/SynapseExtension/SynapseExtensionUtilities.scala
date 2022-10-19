package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import com.microsoft.azure.synapse.ml.nbtest.SynapseExtension.Models._
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
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}
import scala.sys.process._

object SynapseExtensionUtilities {
  import SynapseJsonProtocol._

  val TimeoutInMillis: Int = 30 * 60 * 1000

  val SSPHost: String = "https://wabi-daily-us-east2-redirect.analysis.windows.net"
  val WorkspaceId: String = "8f02ac2a-92cb-4f52-975e-4d0fa4a5cafa"

  val BaseUri: String = s"$SSPHost/metadata"
  val ArtifactsUri: String = s"$BaseUri/workspaces/$WorkspaceId/artifacts"

  val TenantId: String = "pbidaily.onmicrosoft.com"
  val AadAccessTokenResource: String = "https://analysis.windows.net/powerbi/api"
  val AadAccessTokenClientId: String = "1950a258-227b-4e31-a9cf-717495945fc2"

  val Folder: String = s"build_${BuildInfo.version}/synapseextension/notebooks"
  val StorageAccount: String = "mmlsparkbuildsynapse"
  val StorageContainer: String = "synapse-extension"

  lazy val AccessToken: String = getAccessToken

  def getStorageOAuthToken: String = {
    val tokenFile = FileUtilities
      .join(BuildInfo.baseDirectory.getParent,
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseExtension/storagetoken")
      .getCanonicalFile
    FileUtilities.readFile(tokenFile)
  }

  def createSJDArtifact(path: String): String = {
    createSJDArtifact(path, "SparkJobDefinition")
  }

  def updateSJDArtifact(path: String, artifactId: String, lakehouseId: String): Artifact = {
    val eTag = getETagFromArtifact(artifactId)

    val excludes: String = "org.scala-lang:scala-reflect," +
      "org.apache.spark:spark-tags_2.12," +
      "org.scalactic:scalactic_2.12," +
      "org.scalatest:scalatest_2.12," +
      "org.slf4j:slf4j-api"
      //"io.netty:netty-tcnative-boringssl-static-2.0.43.Final-," +
    val reqBody: String =
      s"""
         |{
         |  "workloadPayload": "{
         |    \\"ExecutableFile\\": \\"$path\\",
         |    \\"DefaultLakehouseArtifactId\\": \\"$lakehouseId\\",
         |    \\"SparkVersion\\": \\"3.2\\",
         |    \\"SparkSettings\\": {
         |      \\"spark.jars.packages\\" : \\"com.microsoft.azure:synapseml_2.12:${BuildInfo.version}\\",
         |      \\"spark.jars.repositories\\" : \\"https://mmlspark.azureedge.net/maven\\",
         |      \\"spark.jars.excludes\\": \\"$excludes\\",
         |      \\"spark.dynamicAllocation.enabled\\": \\"false\\",
         |      \\"spark.driver.userClassPathFirst\\": \\"true\\",
         |      \\"spark.executor.userClassPathFirst\\": \\"true\\",
         |      \\"spark.executorEnv.IS_TRIDENT\\": \\"true\\"
         |    }
         |  }"
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
    response.objectId
  }

  def createLakehouseArtifact(): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val now = dtf.format(LocalDateTime.now)

    val reqBody: String =
      s"""
         |{
         |  "displayName": "Lakehouse$now",
         |  "description": "SynapseML Test Infra Lakehouse",
         |  "artifactType": "Lakehouse"
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
      else if (state != null && Seq("Failed", "Dead", "Error").contains(state.statusString))
      {
        throw new RuntimeException(state.statusString)
      }
      else if (state  != null && state.statusString == "Completed")
      {
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

  val TimeoutMillis: Int = 100000

  def postRequest(uri: String, requestBody: String  = ""): JsValue = {
    val createRequest = new HttpPost(uri)
    createRequest.setHeader("Content-Type", "application/json")
    createRequest.setHeader("Authorization", s"$AccessToken")

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(TimeoutMillis)
      .setConnectTimeout(TimeoutMillis)
      .setConnectionRequestTimeout(TimeoutMillis)
      .build

    createRequest.setConfig(requestConfig)

    if (requestBody.nonEmpty)
    {

      createRequest.setEntity(new StringEntity(requestBody))
    }
    sendAndParseJson(createRequest)
  }

  def getRequest(uri: String): JsValue  = {
    val getRequest = new HttpGet(uri)
    getRequest.setHeader("Content-Type", "application/json")
    getRequest.setHeader("Authorization", s"$AccessToken")
    sendAndParseJson(getRequest)
  }

  def getETagFromArtifact(artifactId: String): String = {
    val uri = s"$BaseUri/artifacts/$artifactId"
    val getRequest = new HttpGet(uri)
    getRequest.setHeader("Content-Type", "application/json")
    getRequest.setHeader("Authorization", s"$AccessToken")
    val response = safeSend(getRequest, close = false)
    val eTag = response.getFirstHeader("ETag").getElements.last.getValue
    response.close()
    eTag
  }

  def deleteRequest(uri: String): CloseableHttpResponse = {
    val deleteRequest = new HttpDelete(uri)
    deleteRequest.setHeader("Authorization", s"$AccessToken")
    safeSend(deleteRequest)
  }

  def patchRequest(uri: String, requestBody: String, etag: String): JsValue = {
    val patchRequest = new HttpPatch(uri)
    patchRequest.setHeader("Content-Type", "application/json")
    patchRequest.setHeader("If-Match", etag)
    patchRequest.setHeader("Authorization", s"$AccessToken")
    patchRequest.setEntity(new StringEntity(requestBody))
    sendAndParseJson(patchRequest)
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

  def listArtifacts(): Seq[Artifact] ={
    val uri: String = s"$SSPHost/metadata/workspaces/$WorkspaceId/artifacts"
    getRequest(uri).convertTo[Seq[Artifact]]
  }

  def getNotebookFilePath(artifactId: String, notebookBlobName: String): String =
  {
    s"abfss://$WorkspaceId@lake.trident.com/$artifactId/Main/$notebookBlobName"
  }

  def listPythonJobFiles(): Array[String] = {
    Option({
      val rootDirectory = FileUtilities
        .join(BuildInfo.baseDirectory.getParent, "notebooks/features")
        .getCanonicalFile

      FileUtilities.recursiveListFiles(rootDirectory)
        .filter(_.getAbsolutePath.endsWith(".py"))
        .filterNot(_.getAbsolutePath.contains("-"))
        .filterNot(_.getAbsolutePath.contains(" "))
        .map(file => file.getAbsolutePath)
    })
      .get
      .sorted
  }

  def getAccessToken: String = {
    val createRequest = new HttpPost(s"https://login.microsoftonline.com/$TenantId/oauth2/token")
    createRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")
    createRequest.setEntity(
      new UrlEncodedFormEntity(
        List(
          ("resource", s"$AadAccessTokenResource"),
          ("client_id", s"$AadAccessTokenClientId"),
          ("grant_type", "password"),
          ("username", s"SynapseMLE2ETestUser@pbidaily.onmicrosoft.com"),
          ("password", s"${Secrets.SynapseExtensionPassword}"),
          ("scope", "openid")
    ).map(p => new BasicNameValuePair(p._1, p._2)).asJava, "UTF-8")
    )
    "Bearer " + RESTHelpers.sendAndParseJson(createRequest).asJsObject()
      .fields("access_token").convertTo[String]
  }

  def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }
}

object SynapseJsonProtocol extends DefaultJsonProtocol {

  implicit val ApplicationFormat: RootJsonFormat[Artifact] =
    jsonFormat2(Artifact.apply)
  implicit val ApplicationsFormat: RootJsonFormat[SparkJobDefinitionExecutionResponse] =
    jsonFormat3(SparkJobDefinitionExecutionResponse.apply)
  implicit val SRRFormat: RootJsonFormat[WorkloadPayload] = jsonFormat3(WorkloadPayload.apply)

}
