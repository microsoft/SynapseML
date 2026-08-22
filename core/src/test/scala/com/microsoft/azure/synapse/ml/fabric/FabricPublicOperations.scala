// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.fabric.FabricPublicOperations._
import com.microsoft.azure.synapse.ml.fabric.FabricSchemas._
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.{ContentType, StringEntity}
import spray.json._

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

private[fabric] object FabricPublicOperations {
  private val FabricApiBase = "https://api.fabric.microsoft.com/v1"
  private val DefaultRetryAfterSeconds = 5
  private val MinimumRetryAfterSeconds = 1
  private val CoreDigest = "^[0-9a-f]{64}$".r

  private[fabric] def resolveCorePackage(root: File,
                                         version: String,
                                         configuredPath: Option[String]): File = {
    val candidates = configuredPath match {
      case Some(path) => Seq(new File(path))
      case None =>
        Seq(new File(root, "target"), new File(root, "core/target"))
          .flatMap(childFiles)
          .filter(file => file.isDirectory && file.getName.startsWith("scala-"))
          .flatMap(childFiles)
          .filter(isCorePackage)
    }
    val currentVersion = candidates.filter(_.getName.contains(version))
    val resolved = if (currentVersion.nonEmpty) currentVersion else candidates
    require(
      resolved.size == 1 && resolved.head.isFile,
      "Expected exactly one current SynapseML core package jar. " +
        "Run 'sbt core/packageBin' or set SYNAPSEML_CORE_JAR; found: " +
        resolved.map(_.getAbsolutePath).mkString(", "))
    resolved.head.getCanonicalFile
  }

  private[fabric] def scriptSource(script: File, coreDigest: Option[String]): Array[Byte] = {
    require(script.isFile, s"Fabric batch script does not exist: ${script.getAbsolutePath}")
    val source = Files.readAllBytes(script.toPath)
    coreDigest match {
      case Some(digest) => provenancePreamble(digest) ++ source
      case None => source
    }
  }

  private[fabric] def batchSubmitCommand(cli: String,
                                         script: File,
                                         jobName: String,
                                         workspace: String,
                                         lakehouse: String,
                                         fabricEnvironment: String,
                                         outputDirectory: File,
                                         coreJar: Option[File]): Seq[String] = {
    require(cli.nonEmpty, "Fabric Spark CLI executable cannot be empty")
    require(script.isFile, s"Fabric batch script does not exist: ${script.getAbsolutePath}")
    require(workspace.nonEmpty, "Fabric workspace cannot be empty")
    require(lakehouse.nonEmpty, "Fabric Lakehouse cannot be empty")
    require(outputDirectory.isDirectory, s"Fabric output directory does not exist: $outputDirectory")
    coreJar.foreach(jar =>
      require(jar.isFile, s"SynapseML core package does not exist: ${jar.getAbsolutePath}"))

    Seq(
      cli,
      "batch",
      "submit",
      "--backend",
      "fabric",
      "--py",
      script.getAbsolutePath,
      "--name",
      jobName,
      "--workspace",
      workspace,
      "--lakehouse",
      lakehouse,
      "--no-create-lakehouse",
      "--env",
      fabricEnvironment,
      "--no-browser",
      "--download-log",
      "--output-dir",
      outputDirectory.getAbsolutePath
    ) ++ coreJar.toSeq.flatMap(jar => Seq("--extra-jars", jar.getAbsolutePath, "--no-m2"))
  }

  private[fabric] def batchCancelCommand(cli: String,
                                         jobName: String,
                                         workspace: String,
                                         lakehouse: String,
                                         fabricEnvironment: String): Seq[String] = {
    require(cli.nonEmpty, "Fabric Spark CLI executable cannot be empty")
    require(jobName.nonEmpty, "Fabric batch name cannot be empty")
    require(workspace.nonEmpty, "Fabric workspace cannot be empty")
    require(lakehouse.nonEmpty, "Fabric Lakehouse cannot be empty")

    Seq(
      cli,
      "batch",
      "cancel",
      "--backend",
      "fabric",
      "--name",
      jobName,
      "--yes",
      "--workspace",
      workspace,
      "--lakehouse",
      lakehouse,
      "--no-create-lakehouse",
      "--env",
      fabricEnvironment
    )
  }

  private def provenancePreamble(expectedDigest: String): Array[Byte] = {
    expectedDigest match {
      case CoreDigest() => ()
      case _ =>
        throw new IllegalArgumentException(
          s"SynapseML core digest is not a SHA-256 value: $expectedDigest")
    }
    val expectedDigestLiteral = JsString(expectedDigest).compactPrint
    s"""
       |from pyspark.sql import SparkSession as _SynapseMLSparkSession
       |import hashlib as _synapseml_hashlib
       |from urllib.parse import urlparse as _synapseml_urlparse
       |
       |_synapseml_spark = _SynapseMLSparkSession.builder.getOrCreate()
       |_synapseml_loader = (
       |    _synapseml_spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
       |)
       |_synapseml_class = _synapseml_loader.loadClass(
       |    "com.microsoft.azure.synapse.ml.build.BuildInfo$$"
       |)
       |_synapseml_origin = str(
       |    _synapseml_class.getProtectionDomain().getCodeSource().getLocation()
       |)
       |with open(_synapseml_urlparse(_synapseml_origin).path, "rb") as _synapseml_jar:
       |    _synapseml_digest = _synapseml_hashlib.sha256(_synapseml_jar.read()).hexdigest()
       |_synapseml_expected_digest = $expectedDigestLiteral
       |print(
       |    f"SYNAPSEML_CORE_PROVENANCE origin={_synapseml_origin} "
       |    f"sha256={_synapseml_digest}"
       |)
       |assert _synapseml_digest == _synapseml_expected_digest, (
       |    f"Expected SynapseML core sha256={_synapseml_expected_digest}, "
       |    f"loaded sha256={_synapseml_digest} from {_synapseml_origin}"
       |)
       |
       |""".stripMargin.getBytes(StandardCharsets.UTF_8)
  }

  private def childFiles(parent: File): Seq[File] = {
    Option(parent.listFiles()).map(_.toSeq).getOrElse(Seq.empty)
  }

  private def isCorePackage(file: File): Boolean = {
    file.isFile &&
      file.getName.startsWith("synapseml-core_") &&
      file.getName.endsWith(".jar") &&
      !file.getName.contains("-sources") &&
      !file.getName.contains("-javadoc") &&
      !file.getName.contains("-tests")
  }
}

private[fabric] class FabricPublicOperations(clientId: String,
                                              redirectUri: String,
                                              workspaceId: String)
  extends FabricOperations(clientId, redirectUri, workspaceId) {

  private case class PublicResponse(statusCode: Int,
                                    body: String,
                                    headers: Map[String, String]) {
    def header(name: String): Option[String] = {
      headers.collectFirst {
        case (key, value) if key.equalsIgnoreCase(name) => value
      }
    }

    def json: JsObject = {
      require(body.nonEmpty, s"Fabric returned an empty response body with HTTP $statusCode")
      body.parseJson.asJsObject
    }
  }

  private case class BatchSubmission(script: File,
                                     displayName: String,
                                     storeName: String,
                                     includePackages: Boolean)

  private class FabricCliInterruptedException(error: InterruptedException)
    extends RuntimeException("fabric-spark-cli was interrupted", error)

  private val fabricClient = new FabricScopedAuthenticatedHttpClient(
    clientId, redirectUri, FabricAzureCliTestConfiguration.DefaultFabricScope)
  private val requestConfig = RequestConfig
    .custom
    .setSocketTimeout(timeoutInMillis)
    .setConnectTimeout(timeoutInMillis)
    .setConnectionRequestTimeout(timeoutInMillis)
    .build
  private val batchLock = new Object
  private var batchNames: Map[String, String] = Map.empty
  private var batchSubmissions: Map[String, BatchSubmission] = Map.empty
  private var batchResults: Map[String, SparkJobDefinitionExecutionResponse] = Map.empty
  private var storeNames: Map[String, String] = Map.empty

  override def createSJDArtifact(path: String): String = {
    createSJDArtifact(path, "SparkJobDefinition")
  }

  override def createSJDArtifact(path: String, artifactType: String): String = {
    require(
      artifactType == "SparkJobDefinition",
      s"Public Fabric operations do not support artifact type '$artifactType'")
    val runName = getBlobNameFromFilepath(path).stripSuffix(".py")
    val displayName = FabricArtifactNames.sjd(runName)
    val batchId = UUID.randomUUID().toString
    batchLock.synchronized {
      batchNames += batchId -> displayName
    }
    println(s"Prepared direct Fabric batch for $runName: $batchId")
    batchId
  }

  override def createStoreArtifact(): String = {
    val store = "Lakehouse"
    val displayName = FabricArtifactNames.store(store)
    val body = JsObject(
      "displayName" -> JsString(displayName),
      "description" -> JsString(s"SynapseML Test Infra $store")
    )
    val storeId = createItem(s"$workspaceApi/lakehouses", body)
    batchLock.synchronized {
      storeNames += storeId -> displayName
    }
    storeId
  }

  override def updateSJDArtifact(path: String,
                                 artifactId: String,
                                 storeId: String,
                                 includePackages: Boolean): Artifact = {
    val script = new File(path)
    require(script.isFile, s"Fabric batch script does not exist: ${script.getAbsolutePath}")
    val (displayName, storeName) = batchLock.synchronized {
      val name = batchNames.getOrElse(
        artifactId,
        throw new IllegalArgumentException(s"Unknown direct Fabric batch: $artifactId"))
      val lakehouse = storeNames.getOrElse(
        storeId,
        throw new IllegalArgumentException(s"Unknown direct Fabric Lakehouse: $storeId"))
      (name, lakehouse)
    }
    batchLock.synchronized {
      batchSubmissions += artifactId ->
        BatchSubmission(script, displayName, storeName, includePackages)
    }
    Artifact(artifactId, displayName, LocalDateTime.now())
  }

  override def uploadNotebookToAzure(notebook: File): String = {
    require(notebook.isFile, s"Notebook script does not exist: ${notebook.getAbsolutePath}")
    notebook.getAbsolutePath
  }

  override def submitJob(artifactId: String): String = {
    val submission = batchLock.synchronized {
      batchSubmissions.getOrElse(
        artifactId,
        throw new IllegalArgumentException(s"Direct Fabric batch $artifactId is not configured"))
    }
    try {
      runBatch(artifactId, submission)
      batchLock.synchronized {
        batchResults += artifactId ->
          SparkJobDefinitionExecutionResponse("Completed", artifactId, None)
      }
      artifactId
    } catch {
      case NonFatal(error) =>
        batchLock.synchronized {
          batchResults += artifactId ->
            SparkJobDefinitionExecutionResponse("Failed", artifactId, Option(error.getMessage))
        }
        throw error
    }
  }

  override def getJobStatus(artifactId: String,
                            jobInstanceId: String): SparkJobDefinitionExecutionResponse = {
    require(
      artifactId == jobInstanceId,
      s"Direct Fabric batch ID mismatch: artifact=$artifactId, job=$jobInstanceId")
    batchLock.synchronized {
      batchResults.getOrElse(
        artifactId,
        SparkJobDefinitionExecutionResponse("NotStarted", artifactId, None))
    }
  }

  override def monitorJob(artifactId: String, jobInstanceId: String): Future[String] = {
    Future {
      val state = getJobStatus(artifactId, jobInstanceId)
      state.statusString match {
        case "Completed" => state.statusString
        case "Failed" =>
          throw new RuntimeException(
            s"Direct Fabric batch $jobInstanceId failed: " +
              state.serviceExceptionJson.getOrElse("no failure reason returned"))
        case status =>
          throw new IllegalStateException(
            s"Direct Fabric batch $jobInstanceId returned unexpected status '$status'")
      }
    }(ExecutionContext.global)
  }

  override def deleteArtifact(artifactId: String): Unit = {
    val directBatch = batchLock.synchronized {
      val exists = batchNames.contains(artifactId)
      batchNames -= artifactId
      batchSubmissions -= artifactId
      batchResults -= artifactId
      exists
    }
    if (directBatch) {
      println(s"Released direct Fabric batch definition: $artifactId")
    } else {
      deletePublicItem(artifactId)
      batchLock.synchronized {
        storeNames -= artifactId
      }
    }
  }

  override def listArtifacts(): Seq[Artifact] = {
    listArtifactsPage(s"$workspaceApi/items", ListBuffer.empty).toSeq
  }

  override def getSparkJobDefinitionLink(sjdArtifactId: String): String = {
    s"https://app.fabric.microsoft.com/groups/$workspaceId"
  }

  private def runBatch(artifactId: String, submission: BatchSubmission): Unit = {
    var temporaryScript: Option[Path] = None
    var failure: Option[Throwable] = None
    try {
      val outputDirectory = batchOutputDirectory(artifactId, submission.displayName)
      val corePackage = resolveSubmissionCorePackage(submission.includePackages)
      val digest = corePackage.map(sha256)
      val submittedScript = digest match {
        case Some(value) =>
          val file = Files.createTempFile(outputDirectory.toPath, "synapseml-provenance-", ".py")
          temporaryScript = Some(file)
          Files.write(file, scriptSource(submission.script, Some(value)))
          file.toFile
        case None =>
          submission.script
      }
      val command = batchSubmitCommand(
        sys.env.getOrElse("FABRIC_SPARK_CLI_PATH", "fabric-spark-cli"),
        submittedScript,
        submission.displayName.take(120),
        sys.env.get("FABRIC_E2E_WORKSPACE").filter(_.nonEmpty).getOrElse(workspaceId),
        submission.storeName,
        sys.env.getOrElse("FABRIC_E2E_ENV", "msit"),
        outputDirectory,
        corePackage)

      corePackage.zip(digest).foreach { case (jar, value) =>
        println(s"Submitting exact core package ${jar.getName}: sha256=$value")
      }
      println(s"Running direct Fabric batch ${submission.displayName}")
      try {
        runProcess(command)
      } catch {
        case NonFatal(error) =>
          cancelAfterFailure(submission, error)
      }
    } catch {
      case NonFatal(error) =>
        failure = Some(error)
        throw error
    } finally {
      cleanupTemporaryScript(temporaryScript, failure)
    }
  }

  private def resolveSubmissionCorePackage(includePackages: Boolean): Option[File] = {
    if (includePackages) {
      Some(resolveCorePackage(
        BuildInfo.baseDirectory,
        BuildInfo.version,
        sys.env.get("SYNAPSEML_CORE_JAR")))
    } else {
      None
    }
  }

  private def cleanupTemporaryScript(path: Option[Path], failure: Option[Throwable]): Unit = {
    path.foreach { script =>
      try {
        Files.deleteIfExists(script)
      } catch {
        case NonFatal(cleanupError) =>
          failure match {
            case Some(error) => error.addSuppressed(cleanupError)
            case None => throw cleanupError
          }
      }
    }
  }

  private def cancelAfterFailure(submission: BatchSubmission, error: Throwable): Nothing = {
    try {
      cancelBatch(submission)
    } catch {
      case NonFatal(cancelError) => error.addSuppressed(cancelError)
    }
    if (error.isInstanceOf[FabricCliInterruptedException]) {
      Thread.currentThread().interrupt()
    }
    throw error
  }

  private def cancelBatch(submission: BatchSubmission): Unit = {
    val command = batchCancelCommand(
      sys.env.getOrElse("FABRIC_SPARK_CLI_PATH", "fabric-spark-cli"),
      submission.displayName.take(120),
      sys.env.get("FABRIC_E2E_WORKSPACE").filter(_.nonEmpty).getOrElse(workspaceId),
      submission.storeName,
      sys.env.getOrElse("FABRIC_E2E_ENV", "msit"))
    println(s"Cancelling any running direct Fabric batch ${submission.displayName}")
    runProcess(command, TimeUnit.MINUTES.toMillis(2))
  }

  private def runProcess(command: Seq[String],
                         processTimeoutInMillis: Long = timeoutInMillis.toLong): Unit = {
    val process = new ProcessBuilder(command.asJava)
      .directory(BuildInfo.baseDirectory)
      .inheritIO()
      .start()
    val completed = try {
      process.waitFor(processTimeoutInMillis, TimeUnit.MILLISECONDS)
    } catch {
      case error: InterruptedException =>
        terminateProcess(process)
        throw new FabricCliInterruptedException(error)
    }
    if (!completed) {
      terminateProcess(process)
      throw new TimeoutException(
        s"fabric-spark-cli exceeded the ${processTimeoutInMillis / 1000}-second timeout")
    }
    require(
      process.exitValue() == 0,
      s"fabric-spark-cli command failed with exit code ${process.exitValue()}")
  }

  private def terminateProcess(process: Process): Unit = {
    process.destroy()
    if (!process.waitFor(10, TimeUnit.SECONDS)) {
      process.destroyForcibly()
    }
  }

  private def batchOutputDirectory(artifactId: String, displayName: String): File = {
    val root = sys.env
      .get("FABRIC_E2E_OUTPUT_DIR")
      .filter(_.nonEmpty)
      .map(new File(_))
      .getOrElse(new File(BuildInfo.baseDirectory, "target/fabric-e2e-logs"))
    val directory = new File(root, s"${displayName.take(80)}-${artifactId.take(8)}")
    Files.createDirectories(directory.toPath)
    directory
  }

  private def workspaceApi: String = s"$FabricApiBase/workspaces/$workspaceId"

  private def createItem(uri: String, body: JsObject): String = {
    val response = postJson(uri, body)
    response.statusCode match {
      case 201 => requiredString(response.json, "id")
      case 202 =>
        awaitOperation(response)
        val result = get(s"$FabricApiBase/operations/${requiredOperationId(response)}/result")
        requiredString(result.json, "id")
      case code =>
        throw new RuntimeException(s"Unexpected item creation response: HTTP $code")
    }
  }

  private def deletePublicItem(artifactId: String): Unit = {
    val request = new HttpDelete(s"$workspaceApi/items/$artifactId?hardDelete=true")
    val response = execute(request, fabricClient)
    response.statusCode match {
      case 200 | 204 => ()
      case 202 => awaitOperation(response)
      case code => throw new RuntimeException(s"Unexpected item deletion response: HTTP $code")
    }
  }

  private def postJson(uri: String, body: JsObject): PublicResponse = {
    val request = new HttpPost(uri)
    request.setEntity(new StringEntity(body.compactPrint, ContentType.APPLICATION_JSON))
    execute(request, fabricClient)
  }

  private def get(uri: String): PublicResponse = {
    execute(new HttpGet(uri), fabricClient)
  }

  private def execute(request: HttpRequestBase,
                      authenticatedClient: FabricAuthenticatedHttpClient): PublicResponse = {
    request.setConfig(requestConfig)
    authenticatedClient.setRequestContentTypeAndAuthorization(request)
    request.setHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType)
    val response = RESTHelpers.safeSend(request, close = false)
    try {
      val body = Option(response.getEntity)
        .map(entity => IOUtils.toString(entity.getContent, StandardCharsets.UTF_8))
        .getOrElse("")
      val headers = response.getAllHeaders.map(header => header.getName -> header.getValue).toMap
      PublicResponse(response.getStatusLine.getStatusCode, body, headers)
    } finally {
      response.close()
    }
  }

  private def requiredOperationId(response: PublicResponse): String = {
    response.header("x-ms-operation-id")
      .orElse(response.header("Location").map(operationIdFromLocation))
      .filter(_.nonEmpty)
      .getOrElse {
        throw new RuntimeException("Fabric long-running response did not include an operation ID")
      }
  }

  private def operationIdFromLocation(location: String): String = {
    new URI(location).getPath.split("/").filter(_.nonEmpty).lastOption.getOrElse("")
  }

  private def requiredString(json: JsObject, field: String): String = {
    json.fields.get(field) match {
      case Some(JsString(value)) if value.nonEmpty => value
      case _ => throw new RuntimeException(s"Fabric response did not contain string field '$field'")
    }
  }

  private def retryAfterSeconds(response: PublicResponse): Int = {
    response.header("Retry-After")
      .flatMap(value => Try(value.toInt).toOption)
      .map(value => math.max(MinimumRetryAfterSeconds, value))
      .getOrElse(DefaultRetryAfterSeconds)
  }

  private def awaitOperation(initialResponse: PublicResponse): Unit = {
    pollOperation(
      requiredOperationId(initialResponse),
      retryAfterSeconds(initialResponse),
      System.currentTimeMillis() + SynapseUtilities.TimeoutInMillis)
  }

  @tailrec
  private def pollOperation(operationId: String,
                            retryAfter: Int,
                            deadline: Long): Unit = {
    if (System.currentTimeMillis() > deadline) {
      throw new TimeoutException(s"Fabric operation $operationId timed out")
    }
    blocking {
      Thread.sleep(retryAfter.toLong * 1000L)
    }
    val response = get(s"$FabricApiBase/operations/$operationId")
    requiredString(response.json, "status") match {
      case "Succeeded" => ()
      case "Failed" | "Cancelled" =>
        throw new RuntimeException(
          s"Fabric operation $operationId failed: ${response.body}")
      case "NotStarted" | "Running" =>
        pollOperation(operationId, retryAfterSeconds(response), deadline)
      case status =>
        throw new RuntimeException(
          s"Fabric operation $operationId returned unsupported status '$status'")
    }
  }

  @tailrec
  private def listArtifactsPage(uri: String,
                                artifacts: ListBuffer[Artifact]): ListBuffer[Artifact] = {
    val response = get(uri).json
    response.fields.get("value") match {
      case Some(JsArray(items)) =>
        items.foreach {
          case item: JsObject =>
            val id = requiredString(item, "id")
            val displayName = requiredString(item, "displayName")
            val createdAt = FabricArtifactNames.createdAt(displayName).getOrElse(LocalDateTime.MAX)
            artifacts += Artifact(id, displayName, createdAt)
          case value =>
            throw new RuntimeException(s"Fabric items response contained a non-object: $value")
        }
      case _ =>
        throw new RuntimeException("Fabric items response did not contain an array field 'value'")
    }

    response.fields.get("continuationUri") match {
      case Some(JsString(nextUri)) if nextUri.nonEmpty =>
        listArtifactsPage(nextUri, artifacts)
      case Some(JsNull) | None =>
        artifacts
      case value =>
        throw new RuntimeException(s"Fabric items response contained invalid continuationUri: $value")
    }
  }

  private def sha256(file: File): String = {
    val stream = new FileInputStream(file)
    try {
      DigestUtils.sha256Hex(stream)
    } finally {
      stream.close()
    }
  }
}
