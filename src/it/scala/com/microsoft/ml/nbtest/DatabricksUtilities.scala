// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.nbtest

import java.io.{File, FileInputStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeoutException

import com.microsoft.ml.spark.core.env.StreamUtilities._
import com.microsoft.ml.nbtest.SprayImplicits._
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.spark_project.guava.io.BaseEncoding
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsObject, JsValue, _}

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.sys.process.Process
import com.microsoft.ml.spark.core.env.StreamUtilities._
import scala.concurrent.blocking

//noinspection ScalaStyle
object DatabricksUtilities {
  lazy val requestTimeout = 60000

  lazy val requestConfig: RequestConfig = RequestConfig.custom()
    .setConnectTimeout(requestTimeout)
    .setConnectionRequestTimeout(requestTimeout)
    .setSocketTimeout(requestTimeout)
    .build()

  lazy val client: CloseableHttpClient = HttpClientBuilder
    .create().setDefaultRequestConfig(requestConfig).build()

  // ADB Info
  val region = "southcentralus"
  val token: String = sys.env.getOrElse("MML_ADB_TOKEN", Secrets.adbToken)
  val authValue: String = "Basic " + BaseEncoding.base64()
    .encode(("token:" + token).getBytes("UTF-8"))
  val baseURL = s"https://$region.azuredatabricks.net/api/2.0/"
  val clusterName = "Test Cluster"
  lazy val clusterId: String = getClusterIdByName(clusterName)

  val folder = s"/MMLSparkBuild/build_${BuildInfo.version}"

  // MMLSpark info
  val truncatedScalaVersion: String = BuildInfo.scalaVersion
    .split(".".toCharArray.head).dropRight(1).mkString(".")
  val version = s"com.microsoft.ml.spark:${BuildInfo.name}_$truncatedScalaVersion:${BuildInfo.version}"
  val repository = "https://mmlspark.azureedge.net/maven"

  val libraries: String = List(
    Map("maven" -> Map("coordinates" -> version, "repo" -> repository)),
    Map("pypi" -> Map("package" -> "nltk"))
  ).toJson.compactPrint

  // Execution Params
  val timeoutInMillis: Int = 25 * 60 * 1000

  val notebookFiles: Array[File] = Option(
    FileUtilities.join(BuildInfo.baseDirectory, "notebooks", "samples").getCanonicalFile.listFiles()
  ).get

  def retry[T](backoffs: List[Int], f: () => T): T = {
    try {
      f()
    } catch {
      case t: Throwable =>
        val waitTime = backoffs.headOption.getOrElse(throw t)
        println(s"Caught error: $t with message ${t.getMessage}, waiting for $waitTime")
        blocking {Thread.sleep(waitTime.toLong)}
        retry(backoffs.tail, f)
    }
  }

  def databricksGet(path: String): JsValue = {
    retry(List(100, 500, 1000), { () =>
      val request = new HttpGet(baseURL + path)
      request.addHeader("Authorization", authValue)
      using(client.execute(request)) { response =>
        if (response.getStatusLine.getStatusCode != 200) {
          throw new RuntimeException(s"Failed: response: $response")
        }
        IOUtils.toString(response.getEntity.getContent).parseJson
      }.get
    })
  }

  //TODO convert all this to typed code
  def databricksPost(path: String, body: String): JsValue = {
    retry(List(100, 500, 1000), { () =>
      val request = new HttpPost(baseURL + path)
      request.addHeader("Authorization", authValue)
      request.setEntity(new StringEntity(body))
      using(client.execute(request)) { response =>
        if (response.getStatusLine.getStatusCode != 200) {
          val entity = IOUtils.toString(response.getEntity.getContent, "UTF-8")
          throw new RuntimeException(s"Failed:\n entity:$entity \n response: $response")
        }
        IOUtils.toString(response.getEntity.getContent).parseJson
      }.get
    })
  }

  def getClusterIdByName(name: String): String = {
    val jsonObj = databricksGet("clusters/list")
    val cluster = jsonObj.select[Array[JsValue]]("clusters")
      .filter(_.select[String]("cluster_name") == name).head
    cluster.select[String]("cluster_id")
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

  def workspaceRmDir(dir: String): Unit = {
    val body = s"""{"path": "$dir", "recursive":true}"""
    databricksPost("workspace/delete", body)
    ()
  }

  def installLibraries(clusterId: String): Unit = {
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
         | "libraries": $libraries
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

  def submitRun(notebookPath: String, timeout: Int = 10 * 60): Int = {
    val body =
      s"""
         |{
         |  "run_name": "test1",
         |  "existing_cluster_id": "$clusterId",
         |  "timeout_seconds": ${timeoutInMillis / 1000},
         |  "notebook_task": {
         |    "notebook_path": "$notebookPath",
         |    "base_parameters": []
         |  },
         |  "libraries": $libraries
         |}
      """.stripMargin
    databricksPost("jobs/runs/submit", body).select[Int]("run_id")
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
      .replaceAll("westus", region) //TODO this seems like an ADB bug
    val nbName = runObj.select[String]("task.notebook_task.notebook_path")
    (url, nbName)
  }

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

      while (finalState.isEmpty &
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
          if (logLevel >= 1) println(s"Notebook $nbName Suceeded")
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

  def uploadAndSubmitNotebook(notebookFile: File): Int = {
    uploadNotebook(notebookFile, folder + "/" + notebookFile.getName)
    submitRun(folder + "/" + notebookFile.getName)
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
