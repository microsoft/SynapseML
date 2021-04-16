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
  val NotebookFiles: Array[String] = Option(
    FileUtilities
      .join(BuildInfo.baseDirectory, "notebooks", "samples")
      .getCanonicalFile
      .listFiles()
      .filter(filePath => filePath.getAbsolutePath.endsWith(".py"))
      .map(file => file.getAbsolutePath)
  ).get

  val Folder = s"build_${BuildInfo.version}/scripts"
  val TimeoutInMillis: Int = 20 * 60 * 1000
  val StorageAccount: String = "wenqxstorage"
  val StorageContainer: String = "mmlsparkgatedbuild"

  def poll(id: Int, livyUrl: String, backoffs: List[Int] = List(100, 1000, 5000)): LivyBatch = {
    val getStatsRequest = new HttpGet(s"$livyUrl/batches/$id")
    val statsResponse = RESTHelpers.safeSend(getStatsRequest, backoffs = backoffs, close = false)
    val batch = parse(IOUtils.toString(statsResponse.getEntity.getContent, "utf-8")).extract[LivyBatch]
    statsResponse.close()
    batch
  }

  def getLogs(id: Int, livyUrl: String, backoffs: List[Int] = List(100, 500, 1000)): Seq[String] = {
    val getLogsRequest = new HttpGet(s"$livyUrl/batches/$id/log?from=0&size=10000")
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

  def retry(id: Int, livyUrl: String, timeout: Int, startTime: Long): LivyBatch = {
    val batch = poll(id, livyUrl)
    println(s"batch state $id : ${batch.state}")
    if (batch.state == "success") {
      batch
    } else {
      if ((System.currentTimeMillis() - startTime) < timeout) {
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
    // submitRun(livyUrl, abfssPath)

  }

  private def createSparkJob(jobCreateUrl: String, path: String): LivyBatch = {

  }


  private def submitRun(livyUrl: String, path: String): LivyBatch = {
    // MMLSpark info
    val truncatedScalaVersion: String = BuildInfo.scalaVersion
      .split(".".toCharArray.head).dropRight(1).mkString(".")
    val deploymentBuild = s"com.microsoft.ml.spark:${BuildInfo.name}_$truncatedScalaVersion:${BuildInfo.version}"
    val repository = "https://mmlspark.azureedge.net/maven"

    val sparkPackages: Array[String] = Array(deploymentBuild)
    val livyPayload: String =
      s"""
         |{
         | "file" : "$path",
         | "name" : "gatedBuild",
         | "driverMemory" : "2g",
         | "driverCores" : 2,
         | "executorMemory" : "2g",
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
    println(s"livy payload: $livyPayload")
    createRequest.setHeader("Content-Type", "application/json")
    createRequest.setHeader("Authorization", s"Bearer $Token")
    createRequest.setEntity(new StringEntity(livyPayload))
    val response = RESTHelpers.safeSend(createRequest, close = false)

    println(response.getEntity.getContent)
    val content: String = IOUtils.toString(response.getEntity.getContent, "utf-8")
    val batch: LivyBatch = parse(content).extract[LivyBatch]
    val status: Int = response.getStatusLine.getStatusCode
    assert(status != 201)
    batch
  }

  private def uploadScript(file: String, dest: String): String = {
    exec(s"az storage fs file upload " +
      s" -s $file -p $dest -f $StorageContainer " +
      s" --account-name $StorageAccount --account-key ${Secrets.SynapseStorageKey}")
    s"abfss://$StorageContainer@$StorageAccount.dfs.core.windows.net/$dest"
  }

  def cancelRun(livyUrl: String, batchId: Int): Unit = {
    val createRequest = new HttpDelete(s"$livyUrl/batches/$batchId")
    val response = RESTHelpers.safeSend(createRequest, close = false)
    println(response.getEntity.getContent)
  }

  private def convertNotebook(notebookPath: String): File = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => exec(
        s"conda activate mmlspark && jupyter nbconvert --to script $notebookPath")
      case _ => Process(
        s"conda init bash && conda activate mmlspark && jupyter nbconvert --to script $notebookPath")
    }
    new File(notebookPath.replace(".ipynb", ".py"))
  }

  private def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  private def getSynapseToken: String = {
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
