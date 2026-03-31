// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

// scalastyle:off method.length

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.SparkMavenRepositoryList
import com.microsoft.azure.synapse.ml.fabric.FabricSchemas._
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import com.microsoft.azure.synapse.ml.nbtest.SharedNotebookE2ETestUtilities._
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import spray.json._

import java.io.File
import java.net.URLEncoder
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

private[fabric] class FabricOperations(clientId: String, redirectUri: String, workspaceId: String)
  extends FabricInternalConnection(clientId, redirectUri, workspaceId) {

  import JsonProtocols._

  val folder: String = s"build_${BuildInfo.version}/synapseextension/notebooks"
  val storageAccount: String = "mmlsparkbuildsynapse"
  val storageContainer: String = "synapse-extension"
  val storageAccountData: String = "mmlspark"
  val storageContainerPublic: String = "publicwasb"
  val platform: String = Secrets.Platform.toUpperCase

  def createSJDArtifact(path: String): String = {
    createSJDArtifact(path, "SparkJobDefinition")
  }

  def updateSJDArtifact(path: String, artifactId: String, storeId: String): Artifact = {
    updateSJDArtifact(path, artifactId, storeId, includePackages = true)
  }

  def updateSJDArtifact(path: String, artifactId: String, storeId: String,
                        includePackages: Boolean): Artifact = {
    val eTag = getETagFromArtifact(artifactId)
    val store = Secrets.ArtifactStore.capitalize

    val sparkSettingsEntries = scala.collection.mutable.ListBuffer(
      s"'spark.executorEnv.IS_$platform': 'true'",
      "'spark.executor.heartbeatInterval': '60s'"
    )

    if (includePackages) {
      val packages: String =
        "com.microsoft.azure:synapseml-core_2.12:" + BuildInfo.version
      sparkSettingsEntries.prepend(s"'spark.jars.packages' : '$packages'")
      sparkSettingsEntries.prepend(s"'spark.jars.repositories' : '$SparkMavenRepositoryList'")
    }

    val sparkSettings = sparkSettingsEntries.mkString(",\n    ")

    val workloadPayload =
      s"""
         |"{
         |  'Default${store}ArtifactId': '$storeId',
         |  'ExecutableFile': '$path',
         |  'SparkVersion': '3.5',
         |  'SparkSettings': {
         |    $sparkSettings,
         |   }
         |}"
    """.stripMargin

    val reqBody: String =
      s"""
         |{
         |  "workloadPayload": $workloadPayload
         |}
         |""".stripMargin

    val uri = s"$metadataUri/artifacts/$artifactId"
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
    val response = postRequest(artifactsUri, reqBody).asJsObject().convertTo[Artifact]
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
    val response = postRequest(artifactsUri, reqBody).asJsObject().convertTo[Artifact]
    response.objectId
  }

  def deleteArtifact(artifactId: String): Unit = {
    val uri: String = s"$metadataUri/artifacts/$artifactId"
    deleteRequest(uri)
  }

  def submitJob(artifactId: String): String = {
    val uri: String = s"$metadataUri/artifacts/$artifactId/jobs/sparkjob"
    val createRequest = new HttpPost(uri)
    createRequest.setHeader("Authorization", authHeader)

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

  def getArtifact(sjdArtifactId: String): ArtifactResponse =
    getRequest(s"$metadataUri/artifacts/$sjdArtifactId").convertTo[ArtifactResponse]

  def getJobs(sjdArtifactId: String): Seq[JobResponse] =
    getRequest(s"$metadataUri/artifacts/$sjdArtifactId/jobs").convertTo[Seq[JobResponse]]

  def getActivities(capacityId: String, livyId: String, mwcToken: String): String = {
    val hostname = capacityId.replace("-", "").toLowerCase

    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/" +
      s"SparkCore/SparkCoreService/direct/v1/monitoring/workspaces/$workspaceId/jobs" +
      s"?%24filter=id+eq+%27$livyId%27+or+retriableBatchId+eq+%27$livyId%27&src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    sendAndParseJson(request)
      .convertTo[ActivitiesResponse]
      .items.head.id
  }

  def getLivyLog(sjdArtifactId: String, capacityId: String,
                 activityId: String, mwcToken: String): String = {
    val hostname = capacityId.replace("-", "").toLowerCase

    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/Notebook/Data/" +
      s"Automatic/sparkui/api/v1/artifacts/$sjdArtifactId/activities/$activityId/livylog" +
      s"?src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    val response = RESTHelpers.safeSend(request, expectedCodes = Set(), close = false)
    val output = parseResult(response)
    response.close()

    output
  }

  def getJobLog(sjdArtifactId: String, batchId: String,
                capacityId: String, mwcToken: String): BatchLogResponse = {
    val hostname = capacityId.replace("-", "").toLowerCase

    val url = s"https://$hostname.pbidedicated.windows.net/webapi/capacities/$capacityId/workloads/Notebook/Data/" +
      s"Automatic/sparkui/api/versions/2019-01-01/artifacts/$sjdArtifactId/batches/$batchId" +
      s"?src=studio&artifactType=sjd"

    val request = new HttpGet(url)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", mwcToken)

    sendAndParseJson(request)
      .convertTo[BatchLogResponse]
  }

  @tailrec
  private def pollExecutionStatus(artifactId: String, jobInstanceId: String,
                                  timeout: Int, startTime: Long): String = {
    val state = getJobStatus(artifactId, jobInstanceId)
    if (state != null && state.statusString == "success") {
      state.statusString
    }
    else if ((System.currentTimeMillis() - startTime) > timeout) {
      throw new TimeoutException(s"Job $jobInstanceId timed out.")
    }
    else if (state != null && Seq("Failed", "Dead", "Error", "Cancelled").contains(state.statusString)) {
      handleFailure(state, artifactId, jobInstanceId)
    }
    else if (state != null && state.statusString == "Completed") {
      println("Notebook completed: " + getSparkJobDefinitionLink(artifactId))
      state.statusString
    }
    else {
      blocking {
        Thread.sleep(8000) //scalastyle:ignore
      }
      pollExecutionStatus(artifactId, jobInstanceId, timeout, startTime)
    }
  }

  private def handleFailure(state: SparkJobDefinitionExecutionResponse,
                            artifactId: String,
                            jobInstanceId: String): String = {
    val link = getSparkJobDefinitionLink(artifactId)
    println("Notebook failed: " + link)

    try {
      val artifact = getArtifact(artifactId)
      val mwcToken = getMWCToken(
        artifact.capacityObjectId, workspaceId, artifactId, "SparkCore")
      val batchId = getJobs(artifactId).find(p => p.artifactJobInstanceId == jobInstanceId).get
        .artifactJobHistoryProperties("2")

      val jobLog = getJobLog(artifactId, batchId, artifact.capacityObjectId, mwcToken)
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
      case t: Throwable =>
        println("### Log retrieval failed")
        println(t.getMessage)
        t.printStackTrace()
    }

    throw new RuntimeException(state.statusString)
  }

  def getJobStatus(artifactId: String,
                   jobInstanceId: String): SparkJobDefinitionExecutionResponse = {
    val uri = s"$metadataUri/artifacts/$artifactId/jobs/$jobInstanceId"
    val response = getRequest(uri).asJsObject().convertTo[SparkJobDefinitionExecutionResponse]
    println(s"$uri: $response")
    response
  }

  def getETagFromArtifact(artifactId: String): String = {
    val uri = s"$metadataUri/artifacts/$artifactId"
    val getReq = new HttpGet(uri)
    setRequestContentTypeAndAuthorization(getReq)
    val response = safeSend(getReq, close = false)
    val eTag = response.getFirstHeader("ETag").getElements.last.getValue
    response.close()
    eTag
  }

  def uploadNotebookToAzure(notebook: File): String = {
    val dest = s"$folder/${notebook.getName}"
    exec(s"az storage fs file upload " +
      s" -s ${notebook.getAbsolutePath} -p $dest -f $storageContainer " +
      " --overwrite true " +
      s" --account-name $storageAccount --auth-mode login")
    s"https://$storageAccount.blob.core.windows.net/$storageContainer/$dest"
  }

  def getDatasets(): Seq[PowerBIDataset] = {
    val uri: String = s"$sspHost/v1.0/myorg/groups/$workspaceId/datasets"
    getRequest(uri).convertTo[PowerBIDatasets].value
  }

  def downloadDatasetPBIX(datasetName: String): Path = {
    val tempFile = Files.createTempFile(datasetName, ".pbix")

    val blobPath = s"SemPy/pbi_data/${datasetName}.pbix"
    val encodedPath = URLEncoder.encode(blobPath, "UTF-8").replace("+", "%20").replace("%2F", "/")
    val url = s"https://$storageAccountData.blob.core.windows.net/$storageContainerPublic/$encodedPath"

    val cmd = Seq(
      "curl", "-L",
      s""""$url"""",
      "-o", s""""${tempFile.toAbsolutePath.toString}""""
    )
    val cmdStr = cmd.mkString(" ")
    exec(cmdStr)

    tempFile
  }

  def importPbix(datasetName: String, datasetLocalPath: Path): String = {
    val url = s"$sspHost/v1.0/myorg/groups/$workspaceId/imports" +
      s"?datasetDisplayName=${URLEncoder.encode(datasetName, "UTF-8")}" +
      "&nameConflict=CreateOrOverwrite&skipReport=true&overrideReportLabel=true&overrideModelLabel=true"

    val request = new HttpPost(url)
    request.setHeader("Content-Type", "multipart/form-data")
    request.setHeader("Authorization", authHeader)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(timeoutInMillis)
      .setConnectTimeout(timeoutInMillis)
      .setConnectionRequestTimeout(timeoutInMillis)
      .build

    request.setConfig(requestConfig)

    val multipartEntity = MultipartEntityBuilder
      .create
      .addBinaryBody(datasetName, datasetLocalPath.toFile,
        ContentType.APPLICATION_OCTET_STREAM, datasetName)
      .build

    request.setEntity(multipartEntity)

    val response = retry(List(100, 500, 1000), { () =>
      val response = Client.execute(request)
      try {
        if (response.getStatusLine.getStatusCode.toString == "202") {
          response
        } else {
          val responseBodyOpt = Try(
            IOUtils.toString(response.getEntity.getContent, "UTF-8")).getOrElse("")

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

    IOUtils
      .toString(response.getEntity.getContent, "utf-8")
      .parseJson
      .asJsObject().fields("id").convertTo[String]
  }

  def datasetToModelId(datasetId: String): Int = {
    val uri = s"$metadataUri/gallery/SharedDatasets/$datasetId"
    getRequest(uri)
      .asJsObject().fields("modelId").convertTo[Int]
  }

  def waitForStorageModeUpdate(modelId: Int): Unit = {
    val uri = s"$metadataUri/models/$modelId/storageModeConversionStatus"

    breakable {
      for (_ <- 1 to 10) {
        if (getRequest(uri).asJsObject().fields("conversionStatus").convertTo[Int] == 1)
          break()
        Thread.sleep(2000) //scalastyle:ignore
      }

      throw new Exception("Storage conversion for Power BI dataset was not successful")
    }
  }

  def updateDatasetStorageMode(datasetId: String, targetStorageMode: Int): Unit = {
    val uri = s"$sspHost/v1.0/myorg/datasets/$datasetId"

    val patchReq = new HttpPatch(uri)
    setRequestContentTypeAndAuthorization(patchReq)
    patchReq.setEntity(new StringEntity(s"""{"targetStorageMode": $targetStorageMode}"""))
    safeSend(patchReq)
  }

  def updateDatasetExportToOnelake(modelId: Int, export: Boolean = true): Unit = {
    val uri = s"$metadataUri/models/$modelId/settings"

    val createRequest = new HttpPost(uri)
    setRequestContentTypeAndAuthorization(createRequest)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(timeoutInMillis)
      .setConnectTimeout(timeoutInMillis)
      .setConnectionRequestTimeout(timeoutInMillis)
      .build

    createRequest.setConfig(requestConfig)
    createRequest.setEntity(new StringEntity(s"""{"exportToOneLake": $export}"""))
    safeSend(createRequest)
  }

  def importPbix(): String = {
    val datasetName = "Customer Profitability Sample PBIX"

    val dataset = getDatasets().find(p => p.name == datasetName)

    if (dataset.nonEmpty)
      dataset.get.id
    else {
      val tempFile = downloadDatasetPBIX(datasetName)

      try {
        val datasetId = importPbix(datasetName, tempFile)
        val modelId = datasetToModelId(datasetId)

        updateDatasetStorageMode(datasetId, 2)
        waitForStorageModeUpdate(modelId)
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
    val uri: String = s"$metadataUri/workspaces/$workspaceId/artifacts"
    getRequest(uri).convertTo[Seq[Artifact]]
  }

  def getSparkJobDefinitionLink(SjdArtifactId: String): String = {
    val queryString = platform.toLowerCase + "=1"
    s"$uxHost/groups/$workspaceId/sparkjobdefinitions/$SjdArtifactId?$queryString"
  }
}
