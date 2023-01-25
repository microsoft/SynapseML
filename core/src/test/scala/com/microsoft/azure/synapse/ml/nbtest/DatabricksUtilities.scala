// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.env.PackageUtils._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities.{TimeoutInMillis, monitorJob}
import com.microsoft.azure.synapse.ml.nbtest.SprayImplicits._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.sparkproject.guava.io.BaseEncoding
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsObject, JsValue, _}

import java.io.{File, FileInputStream}
import java.time.LocalDateTime
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

object DatabricksUtilities {

  // ADB Info
  val Region = "eastus"
  val PoolName = "synapseml-build-11.2"
  val GpuPoolName = "synapseml-build-11.2-gpu"
  val AdbRuntime = "11.2.x-scala2.12"
  // https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/11.2
  val AdbGpuRuntime = "11.2.x-gpu-ml-scala2.12"
  val NumWorkers = 5
  val AutoTerminationMinutes = 15

  lazy val Token: String = sys.env.getOrElse("MML_ADB_TOKEN", Secrets.AdbToken)
  lazy val AuthValue: String = "Basic " + BaseEncoding.base64()
    .encode(("token:" + Token).getBytes("UTF-8"))
  val BaseURL = s"https://$Region.azuredatabricks.net/api/2.0/"
  lazy val PoolId: String = getPoolIdByName(PoolName)
  lazy val GpuPoolId: String = getPoolIdByName(GpuPoolName)
  lazy val ClusterName = s"mmlspark-build-${LocalDateTime.now()}"
  lazy val GPUClusterName = s"mmlspark-build-gpu-${LocalDateTime.now()}"

  val Folder = s"/SynapseMLBuild/build_${BuildInfo.version}"
  val ScalaVersion: String = BuildInfo.scalaVersion.split(".".toCharArray).dropRight(1).mkString(".")

  val Libraries: String = List(
    Map("maven" -> Map("coordinates" -> PackageMavenCoordinate, "repo" -> PackageRepository)),
    Map("pypi" -> Map("package" -> "nltk")),
    Map("pypi" -> Map("package" -> "bs4")),
    Map("pypi" -> Map("package" -> "plotly")),
    Map("pypi" -> Map("package" -> "Pillow")),
    Map("pypi" -> Map("package" -> "onnxmltools==1.7.0")),
    Map("pypi" -> Map("package" -> "lightgbm")),
    Map("pypi" -> Map("package" -> "mlflow")),
    Map("pypi" -> Map("package" -> "openai"))
  ).toJson.compactPrint

  // TODO: install synapse.ml.dl wheel package here
  val GPULibraries: String = List(
    Map("maven" -> Map("coordinates" -> PackageMavenCoordinate, "repo" -> PackageRepository)),
    Map("pypi" -> Map("package" -> "pytorch-lightning==1.5.0")),
    Map("pypi" -> Map("package" -> "torchvision==0.12.0")),
    Map("pypi" -> Map("package" -> "transformers==4.15.0")),
    Map("pypi" -> Map("package" -> "petastorm==0.12.0"))
  ).toJson.compactPrint

  val GPUInitScripts: String = List(
    Map("dbfs" -> Map("destination" -> "dbfs:/FileStore/horovod-fix-commit/horovod_installation.sh"))
  ).toJson.compactPrint

  // Execution Params
  val TimeoutInMillis: Int = 40 * 60 * 1000

  val NotebookFiles: Array[File] = FileUtilities.recursiveListFiles(
    FileUtilities.join(
      BuildInfo.baseDirectory.getParent, "notebooks", "features").getCanonicalFile)

  val ParallelizableNotebooks: Seq[File] = NotebookFiles.filterNot(_.isDirectory)

  val CPUNotebooks: Seq[File] = ParallelizableNotebooks.filterNot(_.getAbsolutePath.contains("simple_deep_learning"))

  val GPUNotebooks: Seq[File] = ParallelizableNotebooks.filter(_.getAbsolutePath.contains("simple_deep_learning"))

  def databricksGet(path: String): JsValue = {
    val request = new HttpGet(BaseURL + path)
    request.addHeader("Authorization", AuthValue)
    RESTHelpers.sendAndParseJson(request)
  }

  //TODO convert all this to typed code
  def databricksPost(path: String, body: String): JsValue = {
    val request = new HttpPost(BaseURL + path)
    request.addHeader("Authorization", AuthValue)
    request.setEntity(new StringEntity(body))
    RESTHelpers.sendAndParseJson(request)
  }

  def getClusterIdByName(name: String): String = {
    val jsonObj = databricksGet("clusters/list")
    val cluster = jsonObj.select[Array[JsValue]]("clusters")
      .filter(_.select[String]("cluster_name") == name).head
    cluster.select[String]("cluster_id")
  }

  def getPoolIdByName(name: String): String = {
    val jsonObj = databricksGet("instance-pools/list")
    val cluster = jsonObj.select[Array[JsValue]]("instance_pools")
      .filter(_.select[String]("instance_pool_name") == name).head
    cluster.select[String]("instance_pool_id")
  }

  def workspaceMkDir(dir: String): Unit = {
    val body = s"""{"path": "$dir"}"""
    databricksPost("workspace/mkdirs", body)
    ()
  }

  def uploadNotebook(file: File, dest: String): Unit = {
    val content = BaseEncoding.base64().encode(
      IOUtils.toByteArray(new FileInputStream(file)))
    val body =
      s"""
         |{
         |  "content": "$content",
         |  "path": "$dest",
         |  "overwrite": true,
         |  "format": "JUPYTER"
         |}
       """.stripMargin
    databricksPost("workspace/import", body)
    ()
  }

  def uploadFileToDBFS(file: File, dest: String): Unit = {
    val content = BaseEncoding.base64().encode(
      IOUtils.toByteArray(new FileInputStream(file)))
    val body =
      s"""
         |{
         |  "contents": "$content",
         |  "path": "$dest",
         |  "overwrite": true
         |}
       """.stripMargin
    databricksPost("dbfs/put", body)
    ()
  }

  def workspaceRmDir(dir: String): Unit = {
    val body = s"""{"path": "$dir", "recursive":true}"""
    databricksPost("workspace/delete", body)
    ()
  }

  def createClusterInPool(clusterName: String,
                          sparkVersion: String,
                          numWorkers: Int,
                          poolId: String,
                          initScripts: String): String = {
    val body =
      s"""
         |{
         |  "cluster_name": "$clusterName",
         |  "spark_version": "$sparkVersion",
         |  "num_workers": $numWorkers,
         |  "autotermination_minutes": $AutoTerminationMinutes,
         |  "instance_pool_id": "$poolId",
         |  "spark_env_vars": {
         |     "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
         |   },
         |  "init_scripts": $initScripts
         |}
      """.stripMargin
    databricksPost("clusters/create", body).select[String]("cluster_id")
  }

  def installLibraries(clusterId: String, libraries: String): Unit = {
    databricksPost("libraries/install",
      s"""
         |{
         | "cluster_id": "$clusterId",
         | "libraries": $libraries
         |}
      """.stripMargin)
    ()
  }

  def uninstallLibraries(clusterId: String): Unit = {
    val body =
      s"""
         |{
         | "cluster_id": "$clusterId",
         | "libraries": $Libraries
         |}
      """.stripMargin
    println(body)
    databricksPost("libraries/uninstall", body)
    ()
  }

  def restartCluster(clusterId: String): Unit = {
    databricksPost("clusters/restart", s"""{"cluster_id":"$clusterId"}""")
    ()
  }

  def terminateCluster(clusterId: String): Unit = {
    databricksPost("clusters/delete", s"""{"cluster_id":"$clusterId"}""")
    ()
  }

  def permanentDeleteCluster(clusterId: String): Unit = {
    databricksPost("clusters/permanent-delete", s"""{"cluster_id":"$clusterId"}""")
    ()
  }

  def submitRun(clusterId: String, notebookPath: String): Int = {
    val body =
      s"""
         |{
         |  "run_name": "test1",
         |  "existing_cluster_id": "$clusterId",
         |  "timeout_seconds": ${TimeoutInMillis / 1000},
         |  "notebook_task": {
         |    "notebook_path": "$notebookPath",
         |    "base_parameters": []
         |  },
         |  "libraries": $Libraries
         |}
      """.stripMargin
    databricksPost("jobs/runs/submit", body).select[Int]("run_id")
  }

  def isClusterActive(clusterId: String): Boolean = {
    val clusterObj = databricksGet(s"clusters/get?cluster_id=$clusterId")
    val state = clusterObj.select[String]("state")
    println(s"Cluster State: $state")
    state == "RUNNING"
  }

  def areLibrariesInstalled(clusterId: String): Boolean = {
    val clusterObj = databricksGet(s"libraries/cluster-status?cluster_id=$clusterId")
    val libraryStatuses = clusterObj.asInstanceOf[JsObject].fields("library_statuses")
      .asInstanceOf[JsArray].elements
    if (libraryStatuses.exists(_.select[String]("status") == "FAILED")) {
      val error = libraryStatuses.filter(_.select[String]("status") == "FAILED").toJson.compactPrint
      throw new RuntimeException(s"Library Installation Failure: $error")
    }
    libraryStatuses.forall(_.select[String]("status") == "INSTALLED")
  }

  private def getRunStatuses(runId: Int): (String, Option[String]) = {
    val runObj = databricksGet(s"jobs/runs/get?run_id=$runId")
    val stateObj = runObj.select[JsObject]("state")
    val lifeCycleState = stateObj.select[String]("life_cycle_state")
    if (lifeCycleState == "TERMINATED") {
      val resultState = stateObj.select[String]("result_state")
      (lifeCycleState, Some(resultState))
    } else {
      (lifeCycleState, None)
    }
  }

  def getRunUrlAndNBName(runId: Int): (String, String) = {
    val runObj = databricksGet(s"jobs/runs/get?run_id=$runId").asJsObject()
    val url = runObj.select[String]("run_page_url")
      .replaceAll("westus", Region) //TODO this seems like an ADB bug
    val nbName = runObj.select[String]("task.notebook_task.notebook_path")
    (url, nbName)
  }

  //scalastyle:off cyclomatic.complexity
  def monitorJob(runId: Integer,
                 timeout: Int,
                 interval: Int = 8000,
                 logLevel: Int = 1): Future[Unit] = {
    Future {
      var finalState: Option[String] = None
      var lifeCycleState: String = "Not Started"
      val startTime = System.currentTimeMillis()
      val (url, nbName) = getRunUrlAndNBName(runId)
      if (logLevel >= 1) println(s"Started Monitoring notebook $nbName, url: $url")

      while (finalState.isEmpty &  //scalastyle:ignore while
        (System.currentTimeMillis() - startTime) < timeout &
        lifeCycleState != "INTERNAL_ERROR"
      ) {
        val (lcs, fs) = getRunStatuses(runId)
        finalState = fs
        lifeCycleState = lcs
        if (logLevel >= 2) println(s"Job $runId state: $lifeCycleState")
        blocking {
          Thread.sleep(interval.toLong)
        }
      }

      val error = finalState match {
        case Some("SUCCESS") =>
          if (logLevel >= 1) println(s"Notebook $nbName Succeeded")
          None
        case Some(state) =>
          Some(new RuntimeException(s"Notebook $nbName failed with state $state. " +
            s"For more information check the run page: \n$url\n"))
        case None if lifeCycleState == "INTERNAL_ERROR" =>
          Some(new RuntimeException(s"Notebook $nbName failed with state $lifeCycleState. " +
            s"For more information check the run page: \n$url\n"))
        case None =>
          Some(new TimeoutException(s"Notebook $nbName timed out after $timeout ms," +
            s" job in state $lifeCycleState, " +
            s" For more information check the run page: \n$url\n "))
      }

      error.foreach { error =>
        if (logLevel >= 1) print(error.getMessage)
        throw error
      }

    }(ExecutionContext.global)
  }
  //scalastyle:on cyclomatic.complexity

  def uploadAndSubmitNotebook(clusterId: String, notebookFile: File): DatabricksNotebookRun = {
    val destination: String = Folder + "/" + notebookFile.getName
    uploadNotebook(notebookFile, destination)
    val runId: Int = submitRun(clusterId, destination)
    val run: DatabricksNotebookRun = DatabricksNotebookRun(runId, notebookFile.getName)

    println(s"Successfully submitted job run id ${run.runId} for notebook ${run.notebookName}")

    run
  }

  def cancelRun(runId: Int): Unit = {
    println(s"Cancelling job $runId")
    databricksPost("jobs/runs/cancel", s"""{"run_id": $runId}""")
    ()
  }

  def listActiveJobs(clusterId: String): Vector[Int] = {
    //TODO this only gets the first 1k running jobs, full solution would page results
    databricksGet("jobs/runs/list?active_only=true&limit=1000")
      .asJsObject.fields.get("runs").map { runs =>
      runs.asInstanceOf[JsArray].elements.flatMap {
        case run if clusterId == run.select[String]("cluster_instance.cluster_id") =>
          Some(run.select[Int]("run_id"))
        case _ => None
      }
    }.getOrElse(Array().toVector: Vector[Int])
  }

  def listInstalledLibraries(clusterId: String): Vector[JsValue] = {
    databricksGet(s"libraries/cluster-status?cluster_id=$clusterId")
      .asJsObject.fields.get("library_statuses")
      .map(ls => ls.asInstanceOf[JsArray].elements)
      .getOrElse(Vector())
  }

  def uninstallAllLibraries(clusterId: String): Unit = {
    val libraries = listInstalledLibraries(clusterId)
      .map(l => l.asJsObject.fields("library"))
    if (libraries.nonEmpty) {
      val body =
        s"""
           |{
           |  "cluster_id":"$clusterId",
           |  "libraries": ${libraries.toJson.compactPrint}
           |}
      """.stripMargin
      databricksPost("libraries/uninstall", body)
    }
    ()
  }

  def cancelAllJobs(clusterId: String): Unit = {
    listActiveJobs(clusterId).foreach(cancelRun)
  }
}

abstract class DatabricksTestHelper extends TestBase {

  import DatabricksUtilities._

  def databricksTestHelper(clusterId: String,
                           libraries: String,
                           notebooks: Seq[File]): mutable.ListBuffer[Int] = {
    val jobIdsToCancel: mutable.ListBuffer[Int] = mutable.ListBuffer[Int]()

    println("Checking if cluster is active")
    tryWithRetries(Seq.fill(60 * 15)(1000).toArray) { () =>
      assert(isClusterActive(clusterId))
    }

    Thread.sleep(1000) // Ensure cluster is not overwhelmed
    println("Installing libraries")
    installLibraries(clusterId, libraries)
    tryWithRetries(Seq.fill(60 * 6)(1000).toArray) { () =>
      assert(areLibrariesInstalled(clusterId))
    }

    println(s"Creating folder $Folder")
    workspaceMkDir(Folder)

    println(s"Submitting jobs")
    val parNotebookRuns: Seq[DatabricksNotebookRun] = notebooks.map(uploadAndSubmitNotebook(clusterId, _))
    parNotebookRuns.foreach(notebookRun => jobIdsToCancel.append(notebookRun.runId))
    println(s"Submitted ${parNotebookRuns.length} for execution: ${parNotebookRuns.map(_.runId).toList}")
    assert(parNotebookRuns.nonEmpty)

    parNotebookRuns.foreach(run => {
      println(s"Testing ${run.notebookName}")
      test(run.notebookName) {
        val result = Await.ready(
          run.monitor(logLevel = 0),
          Duration(TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get

        if (!result.isSuccess) {
          throw result.failed.get
        }
      }
    })

    jobIdsToCancel
  }

  protected def afterAllHelper(jobIdsToCancel: mutable.ListBuffer[Int],
                               clusterId: String,
                               clusterName: String): Unit = {
    println("Suite test finished. Running afterAll procedure...")
    jobIdsToCancel.foreach(cancelRun)
    permanentDeleteCluster(clusterId)
    println(s"Deleted cluster with Id $clusterId, name $clusterName")
  }
}

case class DatabricksNotebookRun(runId: Int, notebookName: String) {
  def monitor(logLevel: Int = 2): Future[Any] = {
    monitorJob(runId, TimeoutInMillis, logLevel)
  }
}
