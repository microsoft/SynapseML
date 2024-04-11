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
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods._
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.message.BasicNameValuePair
import spray.json.{RootJsonFormat, _}

import java.io.File
import java.net.URLEncoder
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}
import scala.util.Try

object SynapseExtensionUtilities {

  import SynapseJsonProtocol._

  val TimeoutInMillis: Int = 60 * 60 * 1000

  val WorkspaceId: String = "243bacab-cd76-48b7-8a29-82d8a74324fb"
  val UxHost: String = "http://app.powerbi.com"
  val TenantId: String = "SynapseMLFabricIntegration.onmicrosoft.com"
  val Password: String = Secrets.FabricIntegrationPassword
  val AadAccessTokenResource: String = "https://analysis.windows.net/powerbi/api"
  val Login: String = "login.windows.net"
  val SSPHost: String = "https://WABI-US-EAST-A-PRIMARY-redirect.analysis.windows.net"
  val BaseUri: String = s"$SSPHost/metadata"
  val ArtifactsUri: String = s"$BaseUri/workspaces/$WorkspaceId/artifacts"

  val AadAccessTokenClientId: String = "1950a258-227b-4e31-a9cf-717495945fc2"

  val Folder: String = s"build_${BuildInfo.version}/synapseextension/notebooks"
  val StorageAccount: String = "mmlsparkbuildsynapse"
  val StorageContainer: String = "synapse-extension"
  val StorageAccountData: String = "mmlspark"
  val StorageContainerPublic: String = "publicwasb"

  def getMwcToken(capacityId: String, workspaceId: String, artifactId: String, workloadType: String): String = {

    val url = s"$BaseUri/v201606/generatemwctoken"

    // workloadtype: Notebook vs SparkCore
    val reqBody: String =
      s"""
         |{
         |  "artifactObjectIds": ["$artifactId"],
         |  "capacityObjectId": "$capacityId",
         |  "type": "[Start] GetMWCToken",
         |  "workloadType": "$workloadType",
         |  "workspaceObjectId": "$workspaceId"
         |}
         |""".stripMargin

    val token = postRequest(url, reqBody)
      .asJsObject().fields("Token").convertTo[String]

    s"MwcToken $token"
  }

  val Platform: String = Secrets.Platform.toUpperCase

  def createSJDArtifact(path: String): String = {
    createSJDArtifact(path, "SparkJobDefinition")
  }

  def updateSJDArtifact(path: String, artifactId: String, storeId: String): Artifact = {
    val eTag = getETagFromArtifact(artifactId)
    val store = Secrets.ArtifactStore.capitalize
    val sparkVersion = "3.4"
    val packages: String ="com.microsoft.azure:synapseml-core_2.12:" + BuildInfo.version

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
         |    'spark.yarn.user.classpath.first': 'true'
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
    createRequest.setHeader("Authorization", AccessToken)

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

  def getArtifact(sjdArtifactId: String): ArtificatResponse =
    getRequest(s"$BaseUri/artifacts/$sjdArtifactId").convertTo[ArtificatResponse]

  def getJobs(sjdArtifactId: String): Seq[JobResponse] =
    getRequest(s"$BaseUri/artifacts/$sjdArtifactId/jobs").convertTo[Seq[JobResponse]]

  def getActivities(capacityId: String, livyId: String, mwcToken: String): String = {
    val hostname = capacityId.replace("-", "").toLowerCase

    // LivyId
    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/" +
      s"SparkCore/SparkCoreService/direct/v1/monitoring/workspaces/$WorkspaceId/jobs" +
      s"?%24filter=id+eq+%27$livyId%27+or+retriableBatchId+eq+%27$livyId%27&src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    sendAndParseJson(request)
      .convertTo[ActivitiesResponse]
      .items.head.id
  }

  def getLivyLog(sjdArtifactId: String, capacityId: String, activityId: String, mwcToken: String): String = {
    val hostname = capacityId.replace("-", "").toLowerCase

    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/Notebook/Data/" +
      s"Automatic/sparkui/api/v1/artifacts/$sjdArtifactId/activities/$activityId/livylog?src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    val response = RESTHelpers.safeSend(request, expectedCodes = Set(), close = false)
    val output = parseResult(response)
    response.close()

    output
  }

  def getJobLog(sjdArtifactId: String, batchId: String, capacityId: String, mwcToken: String): BatchLogResponse = {
    val hostname = capacityId.replace("-", "").toLowerCase

    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/Notebook/Data/" +
      s"Automatic/sparkui/api/versions/2019-01-01/artifacts/$sjdArtifactId/batches/$batchId?src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    sendAndParseJson(request)
      .convertTo[BatchLogResponse]
  }

  //noinspection ScalaStyle
  @tailrec
  private def pollExecutionStatus(artifactId: String, jobInstanceId: String, timeout: Int, startTime: Long): String = {
    val state = getJobStatus(artifactId, jobInstanceId)
    if (state != null && state.statusString == "success") {
      state.statusString
    } else {
      if ((System.currentTimeMillis() - startTime) > timeout) {
        throw new TimeoutException(s"Job $jobInstanceId timed out.")
      }
      else if (state != null && Seq("Failed", "Dead", "Error", "Cancelled").contains(state.statusString)) {
        val link = getSparkJobDefinitionLink(artifactId)

        println("Notebook failed: " + getSparkJobDefinitionLink(artifactId))

        // lets dig out the error log
        try {
          // get error log
          val artifact = getArtifact(artifactId)
          val jobResponses = getJobs(artifactId)
          val job = jobResponses.find(p => p.artifactJobInstanceId == jobInstanceId).get

          // the JobLog works w/ Notebook (and SparkCore), the activities + livy log need SparkCore
          val mwcToken = getMwcToken(artifact.capacityObjectId, WorkspaceId, artifactId, "SparkCore")

          val batchId = job.artifactJobHistoryProperties("2")

          // contains errors during notebook execution
          val jobLog = getJobLog(artifactId, batchId, artifact.capacityObjectId, mwcToken)

          // contains info if the jar file is missing (e.g. sbt publishBlob wasn't invoked)
          val activityId = getActivities(artifact.capacityObjectId, jobLog.id, mwcToken)

          val livyLog = getLivyLog(artifactId, artifact.capacityObjectId, activityId, mwcToken)

          val errorInfos = jobLog.errorInfo match {
            case Some(i) => i.map(x => s"${x.source} (${x.errorCode}): ${x.message}").mkString("\n")
            case None => ""
          }

          val logInfos = jobLog.log match {
            case Some(i) => i.mkString("\n")
            case None => ""
          }

          val msg =
            s"""
               |##### JOB DETAIL LOGS
               |$link
               |
               |${state.statusString}
               |
               |### Livy Log
               |$livyLog
               |
               |### Stdout/Stderr
               |$logInfos
               |
               |ErrorInfo
               |$errorInfos
               |
               |Last update: ${jobLog.lastUpdatedTimestamp}
               |AppId: ${jobLog.appId}
               |""".stripMargin

          println(msg)
        }
        catch {
          case t : Throwable =>
            println("### Log retrieval failed")
            println(t.getMessage)
            t.printStackTrace()
        }

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
    request.setHeader("Authorization", AccessToken)
  }

  def uploadNotebookToAzure(notebook: File): String = {
    val dest = s"$Folder/${notebook.getName}"
    exec(s"az storage fs file upload " +
      s" -s ${notebook.getAbsolutePath} -p $dest -f $StorageContainer " +
      " --overwrite true " +
      s" --account-name $StorageAccount --account-key ${Secrets.SynapseStorageKey}")
    s"https://$StorageAccount.blob.core.windows.net/$StorageContainer/$dest"
  }

  def getDatasets(): Seq[PowerBIDataset] = {
    val uri: String = s"$SSPHost/v1.0/myorg/groups/$WorkspaceId/datasets"

    getRequest(uri).convertTo[PowerBIDatasets].value
  }

  def downloadDatasetPBIX(datasetName: String): Path = {
    val tempFile = Files.createTempFile(datasetName, ".pbix")

    exec(s"az storage fs file download " +
      s"--account-name $StorageAccountData -f $StorageContainerPublic " +
      s"""-p "SemPy/pbi_data/${datasetName}.pbix" """ +
      s"""-d "${tempFile.toAbsolutePath}"""")

    tempFile
  }

  //noinspection ScalaStyle
  def importPbix(datasetName: String, datasetLocalPath: Path): String = {
    val url = s"$SSPHost/v1.0/myorg/groups/$WorkspaceId/imports" +
      s"?datasetDisplayName=${URLEncoder.encode(datasetName, "UTF-8")}" +
      "&nameConflict=CreateOrOverwrite&skipReport=true&overrideReportLabel=true&overrideModelLabel=true"

    val request = new HttpPost(url)
    request.setHeader("Content-Type", "multipart/form-data")
    request.setHeader("Authorization", AccessToken)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(TimeoutInMillis)
      .setConnectTimeout(TimeoutInMillis)
      .setConnectionRequestTimeout(TimeoutInMillis)
      .build

    request.setConfig(requestConfig)

    val multipartEntity = MultipartEntityBuilder
      .create
      .addBinaryBody(datasetName, datasetLocalPath.toFile, ContentType.APPLICATION_OCTET_STREAM, datasetName)
      .build

    request.setEntity(multipartEntity)

    // TODO: use safeSend(...) once the requestBody is truncated/filtered for binary content.
    //       This is a copy of safeSend w/o printing the requestBody in error cases.
    //       It's large and has binary characters that ruins the console.
    val response = retry(List(100, 500, 1000), { () =>
      val response = Client.execute(request)
      try {
        if (response.getStatusLine.getStatusCode.toString == "202") {
          response
        } else {
          val responseBodyOpt = Try(IOUtils.toString(response.getEntity.getContent, "UTF-8")).getOrElse("")

          throw new RuntimeException(
            s"Failed: " +
              s"\n\t response: $response " +
              s"\n\t requestUrl: ${request.getURI}" +
              s"\n\t responseBody: $responseBodyOpt")
        }
      } catch {
        case e: Exception =>
          response.close()
          throw e
      }
    })

    // parse response
    IOUtils
      .toString(response.getEntity.getContent, "utf-8")
      .parseJson
      .asJsObject().fields("id").convertTo[String]
  }

  def datasetToModelId(datasetId: String): Int = {
    val uri = s"$SSPHost/metadata/gallery/SharedDatasets/$datasetId"

    getRequest(uri)
      .asJsObject().fields("modelId").convertTo[Int]
  }

  def waitForStorageModeUpdate(modelId: Int): Unit = {
    val uri = s"$SSPHost/metadata/models/$modelId/storageModeConversionStatus"

    for (i <- 1 to 10) {
      if (getRequest(uri).asJsObject().fields("conversionStatus").convertTo[Int] == 1)
        return

      // wait a bit
      Thread.sleep(2000)
    }

    throw new Exception("Storage conversion for Power BI dataset was not successful")
  }

  def updateDatasetStorageMode(datasetId: String, targetStorageMode: Int): Unit = {
    val uri = s"$SSPHost/v1.0/myorg/datasets/$datasetId"

    val patchRequest = new HttpPatch(uri)
    setRequestContentTypeAndAuthorization(patchRequest)
    patchRequest.setEntity(new StringEntity(s"""{"targetStorageMode": $targetStorageMode}"""))
    safeSend(patchRequest)
  }

  def updateDatasetExportToOnelake(modelId: Int, export: Boolean = true): Unit = {
    val uri = s"$SSPHost/metadata/models/$modelId/settings"

    val createRequest = new HttpPost(uri)
    setRequestContentTypeAndAuthorization(createRequest)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(TimeoutInMillis)
      .setConnectTimeout(TimeoutInMillis)
      .setConnectionRequestTimeout(TimeoutInMillis)
      .build

    createRequest.setConfig(requestConfig)

    createRequest.setEntity(new StringEntity(s"""{"exportToOneLake": $export}"""))

    safeSend(createRequest)
  }

  def importPbix(): String = {
    val datasetName = "Customer Profitability Sample PBIX"

    val dataset = getDatasets().find(p => p.name == datasetName)

    // check if it's already there
    if (dataset.nonEmpty)
      dataset.get.id
    else {
      // download the PBIX
      val tempFile = downloadDatasetPBIX(datasetName)

      try {
        val datasetId = importPbix(datasetName, tempFile)

        val modelId = datasetToModelId(datasetId)

        // 2 = large (this is a prerequisite for export to OneLake)
        updateDatasetStorageMode(datasetId, 2)
        waitForStorageModeUpdate(modelId)

        // make sure the dataset exports all data to OneLake
        updateDatasetExportToOnelake(modelId)

        datasetId
      }
      finally {
        Files.delete(tempFile)
      }
    }
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

  lazy val AccessToken: String = {
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

    val token = RESTHelpers.sendAndParseJson(createRequest).asJsObject()
      .fields("access_token").convertTo[String]

    s"Bearer $token"
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

  implicit val PowerBIDatasetFormat: RootJsonFormat[PowerBIDataset] =
    jsonFormat2(PowerBIDataset.apply)

  implicit val PowerBIDatasetsFormat: RootJsonFormat[PowerBIDatasets] =
    jsonFormat1(PowerBIDatasets.apply)

  implicit val ArtificatResponseFormat: RootJsonFormat[ArtificatResponse] =
    jsonFormat3(ArtificatResponse.apply)

  implicit val JobResponseFormat: RootJsonFormat[JobResponse] =
    jsonFormat3(JobResponse.apply)

  implicit val BatchLogErrorInfoFormat: RootJsonFormat[BatchLogErrorInfo] =
    jsonFormat3(BatchLogErrorInfo.apply)

  implicit val BatchLogResponseFormat: RootJsonFormat[BatchLogResponse] =
    jsonFormat5(BatchLogResponse.apply)

  implicit val ActivityFormat: RootJsonFormat[Activity] =
    jsonFormat1(Activity.apply)

  implicit val ActivitiesResponseFormat: RootJsonFormat[ActivitiesResponse] =
    jsonFormat1(ActivitiesResponse.apply)
}
