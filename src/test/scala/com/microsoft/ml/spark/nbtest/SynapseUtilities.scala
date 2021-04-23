package com.microsoft.ml.spark.nbtest

import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.cognitive.RESTHelpers
import com.microsoft.ml.spark.core.env.FileUtilities
import org.apache.commons.io.IOUtils
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicNameValuePair
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints, _}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.{File, InputStream}
import java.util
import scala.concurrent.{TimeoutException, blocking}
import scala.io.Source
import scala.sys.process.{Process, _}

case class LivyBatch(id: Int,
                     state: String,
                     appId: Option[String],
                     appInfo: Option[JObject],
                     log: Seq[String])

case class LivyLogs(id: Int,
                    from: Int,
                    total: Int,
                    log: Seq[String])

//noinspection ScalaStyle
object SynapseUtilities {

  implicit val Fmts: Formats = Serialization.formats(NoTypeHints)
  lazy val Token: String = getSynapseToken

  val Folder = s"build_${BuildInfo.version}/scripts"
  val TimeoutInMillis: Int = 20 * 60 * 1000
  val StorageAccount: String = "wenqxstorage"
  val StorageContainer: String = "gatedbuild"

  def listPythonFiles(): Array[String] = {
    Option(
      FileUtilities
        .join(BuildInfo.baseDirectory, "notebooks", "samples")
        .getCanonicalFile
        .listFiles()
        .filterNot(_.getAbsolutePath.contains("CyberML"))
        .filterNot(_.getAbsolutePath.contains("DeepLearning"))
        .filterNot(_.getAbsolutePath.contains("ConditionalKNN"))
        .filterNot(_.getAbsolutePath.contains("HyperParameterTuning"))
        .filter(_.getAbsolutePath.endsWith(".py"))
        .map(file => file.getAbsolutePath)
    ).get
  }

  def listNoteBookFiles(): Array[String] = {
    Option(
      FileUtilities
        .join(BuildInfo.baseDirectory, "notebooks", "samples")
        .getCanonicalFile
        .listFiles()
        .filter(_.getAbsolutePath.endsWith(".ipynb"))
        .map(file => file.getAbsolutePath)
    ).get
  }

  def getLogs(id: Int, livyUrl: String, backoffs: List[Int] = List(100, 500, 1000)): Seq[String] = {
    val getLogsRequest = new HttpGet(s"$livyUrl/$id/log?from=0&size=10000")
    getLogsRequest.setHeader("Authorization", s"Bearer $Token")
    val statsResponse = RESTHelpers.safeSend(getLogsRequest, backoffs = backoffs, close = false)
    val batchString = parse(IOUtils.toString(statsResponse.getEntity.getContent, "utf-8"))
    val batch = batchString.extract[LivyLogs]
    statsResponse.close()
    batch.log
  }

  def postMortem(batch: LivyBatch, livyUrl: String): LivyBatch = {
    getLogs(batch.id, livyUrl).foreach(println)
    write(batch)
    batch
  }

  def monitorJob(livyBatch: LivyBatch, livyUrl: String, timeout: Int = TimeoutInMillis): LivyBatch = {
    val startTime = System.currentTimeMillis()
    var livyBatchTmp: LivyBatch = livyBatch
    println(s"monitoring Livy Job: ${livyBatchTmp.id} ")
    while ( (livyBatchTmp.state == "not_started" || livyBatchTmp.state == "starting" )
      && (System.currentTimeMillis() - startTime) < timeout) {
      println(s"Livy Job ${livyBatchTmp.id} not started, waiting...")
      blocking {
        Thread.sleep(8000)
      }
      livyBatchTmp = poll(livyBatchTmp.id, livyUrl)
    }
    livyBatchTmp
  }

  def poll(id: Int, livyUrl: String, backoffs: List[Int] = List(100, 1000, 5000)): LivyBatch = {
    val getStatsRequest = new HttpGet(s"$livyUrl/$id")
    getStatsRequest.setHeader("Authorization", s"Bearer $Token")
    val statsResponse = RESTHelpers.safeSend(getStatsRequest, backoffs = backoffs, close = false)
    val batch = parse(IOUtils.toString(statsResponse.getEntity.getContent, "utf-8")).extract[LivyBatch]
    statsResponse.close()
    batch
  }

  def retry(id: Int, livyUrl: String, timeout: Int, startTime: Long): LivyBatch = {
    val batch = poll(id, livyUrl)
    println(s"batch state $id : ${batch.state}")
    if (batch.state == "success") {
      batch
    } else {
      if ((System.currentTimeMillis() - startTime) > timeout) {
        throw new TimeoutException(s"Job $id timed out.")
      }
      else if (batch.state == "dead") {
        postMortem(batch, livyUrl)
        throw new RuntimeException(s"Dead")
      }
      else {
        blocking {
          Thread.sleep(8000)
        }
        println(s"retrying id $id")
        retry(id, livyUrl, timeout, startTime)
      }
    }
  }

  def uploadAndSubmitNotebook(livyUrl: String, notebookPath: String): LivyBatch = {
    val convertedPyScript = new File(notebookPath)
    val abfssPath = uploadScript(convertedPyScript.getAbsolutePath, s"$Folder/${convertedPyScript.getName}")
    submitRun(livyUrl, abfssPath)
  }

  private def uploadScript(file: String, dest: String): String = {
    exec(s"az storage fs file upload " +
      s" -s $file -p $dest -f $StorageContainer " +
      s" --overwrite true " +
      s" --account-name $StorageAccount --account-key ${Secrets.SynapseStorageKey}")
    s"abfss://$StorageContainer@$StorageAccount.dfs.core.windows.net/$dest"
  }

  private def submitRun(livyUrl: String, path: String): LivyBatch = {
    // MMLSpark info
    val truncatedScalaVersion: String = BuildInfo.scalaVersion
      .split(".".toCharArray.head).dropRight(1).mkString(".")
    val deploymentBuild = s"com.microsoft.ml.spark:${BuildInfo.name}_$truncatedScalaVersion:${BuildInfo.version}"
    // val deploymentBuild = s"com.microsoft.ml.spark:${BuildInfo.name}_$truncatedScalaVersion:1.0.0-rc3-46-3b91af32-SNAPSHOT"
    val repository = "https://mmlspark.azureedge.net/maven"

    val sparkPackages: Array[String] = Array(deploymentBuild)
    val jobName = path.split('/').last.replace(".py", "")

    val livyPayload: String =
      s"""
         |{
         | "file" : "$path",
         | "name" : "$jobName",
         | "driverMemory" : "8g",
         | "driverCores" : 2,
         | "executorMemory" : "8g",
         | "executorCores" : 2,
         | "numExecutors" : 2,
         | "conf" :
         |      {
         |        "spark.jars.packages" : "${sparkPackages.map(s => s.trim).mkString(",")}",
         |        "spark.jars.repositories" : "$repository"
         |      }
         | }
      """.stripMargin

    val createRequest = new HttpPost(livyUrl)
    createRequest.setHeader("Content-Type", "application/json")
    createRequest.setHeader("Authorization", s"Bearer $Token")
    createRequest.setEntity(new StringEntity(livyPayload))
    val response = RESTHelpers.safeSend(createRequest, close = false)

    val content: String = IOUtils.toString(response.getEntity.getContent, "utf-8")
    val batch: LivyBatch = parse(content).extract[LivyBatch]
    val status: Int = response.getStatusLine.getStatusCode
    assert(status == 200)
    batch
  }

  def cancelRun(livyUrl: String, batchId: Int): Unit = {
    val createRequest = new HttpDelete(s"$livyUrl/$batchId")
    createRequest.setHeader("Authorization", s"Bearer $Token")
    val response = RESTHelpers.safeSend(createRequest, close = false)
    println(response.getEntity.getContent)
  }

  def convertNotebook() {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => {
        exec("conda activate mmlspark && jupyter nbconvert --to script .\\notebooks\\samples\\*.ipynb")
      }
      case _ => Process(
        "conda activate mmlspark && jupyter nbconvert --to script ./notebooks/samples/*.ipynb")
    }

    listPythonFiles().map(f=>{
      val newPath = f
        .replace(" ", "")
        .replace("-", "")
      new File(f).renameTo(new File(newPath))
    })
  }

  private def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  def getSynapseToken: String = {
    val tenantId: String = "72f988bf-86f1-41af-91ab-2d7cd011db47"
    val clientId: String = "85dde348-dd2b-43e5-9f5a-22262af45332"
    val spnKey: String = Secrets.SynapseSpnKey

    val uri: String = s"https://login.microsoftonline.com/$tenantId/oauth2/token"

    val createRequest = new HttpPost(uri)
    createRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")

    val bodyList: util.List[BasicNameValuePair] = new util.ArrayList[BasicNameValuePair]()
    bodyList.add(new BasicNameValuePair("grant_type", "client_credentials"))
    bodyList.add(new BasicNameValuePair("client_id", s"$clientId"))
    bodyList.add(new BasicNameValuePair("client_secret", s"$spnKey"))
    bodyList.add(new BasicNameValuePair("resource", "https://dev.azuresynapse.net/"))

    createRequest.setEntity(new UrlEncodedFormEntity(bodyList, "UTF-8"))

    val response = RESTHelpers.safeSend(createRequest, close = false)
    val inputStream: InputStream = response.getEntity.getContent
    val pageContent: String = Source.fromInputStream(inputStream).mkString
    pageContent.parseJson.asJsObject().fields("access_token").convertTo[String]
  }
}
