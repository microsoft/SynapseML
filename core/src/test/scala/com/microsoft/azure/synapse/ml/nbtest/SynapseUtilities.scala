// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.{SparkMavenPackageList, SparkMavenRepositoryList}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.{safeSend, sendAndParseJson}
import com.microsoft.azure.synapse.ml.nbtest.SharedNotebookE2ETestUtilities._
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities._
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicNameValuePair
import spray.json._

import java.io.File
import java.util.Calendar
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.language.postfixOps

case class LivyBatchData(id: Int,
                         state: String,
                         appId: Option[String],
                         log: Option[Seq[String]])

case class LivyBatch(data: LivyBatchData,
                     runName: String,
                     sparkPool: String) {

  import SynapseJsonProtocol._

  private def printStatus(data: LivyBatchData): Unit = {
    println(s"Job ${data.id} on pool $sparkPool has status ${data.state}")
    //batch.log.foreach(_.foreach(println))
  }

  def id: Int = data.id

  def state: String = data.state

  @tailrec
  private def pollLivyUrl(): LivyBatch = {
    val getStatusRequest = new HttpGet(s"${SynapseUtilities.livyUrl(sparkPool)}/${data.id}")
    getStatusRequest.setHeader("Authorization", s"Bearer $SynapseToken")
    val newData = sendAndParseJson(getStatusRequest).convertTo[LivyBatchData]
    printStatus(newData)
    if (newData.state == "success") {
      println(s"Success finishing job ${newData.id} on pool $sparkPool")
      LivyBatch(newData, runName, sparkPool)
    } else {
      if (Set("dead", "error")(newData.state.toLowerCase)) {
        throw new RuntimeException(s"Job ${newData.id} on pool $sparkPool ended with status ${newData.state}")
      } else {
        blocking {
          Thread.sleep(8000)
        }
        println(s"Polling Job ${newData.id} on pool $sparkPool")
        pollLivyUrl()
      }
    }
  }

  def monitor(): Future[LivyBatch] = {
    Future {
      pollLivyUrl()
    }(ExecutionContext.global)
  }

  def cancelRun(): Unit = {
    println(s"Cancelling run $id on pool $sparkPool")
    val cancelRequest = new HttpDelete(s"${livyUrl(sparkPool)}/$id")
    cancelRequest.setHeader("Authorization", s"Bearer $SynapseToken")
    safeSend(cancelRequest, backoffs = List(100, 500, 1000, 10000, 10000))
  }

  def jobStatusPage: String = {
    "https://web-staging.azuresynapse.net/en-us/monitoring/sparkapplication/" +
      runName +
      s"?workspace=%2Fsubscriptions%2F$SubscriptionId" +
      s"%2FresourceGroups%2F$ResourceGroupName" +
      s"%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2F$WorkspaceName" +
      s"&sparkPoolName=$sparkPool&livyId=$id"
  }

}

case class Application(state: String,
                       name: String,
                       livyId: String)

case class Applications(nJobs: Int,
                        sparkJobs: Option[Seq[Application]])

case class SynapseResourceValue(id: String,
                                name: String,
                                `type`: String,
                                location: String,
                                tags: Map[String, String])

case class SynapseResourceResponse(value: Seq[SynapseResourceValue])

object SynapseJsonProtocol extends DefaultJsonProtocol {

  implicit val LivyFormat: RootJsonFormat[LivyBatchData] = jsonFormat4(LivyBatchData.apply)
  implicit val ApplicationFormat: RootJsonFormat[Application] = jsonFormat3(Application.apply)
  implicit val ApplicationsFormat: RootJsonFormat[Applications] = jsonFormat2(Applications.apply)
  implicit val SRVFormat: RootJsonFormat[SynapseResourceValue] = jsonFormat5(SynapseResourceValue.apply)
  implicit val SRRFormat: RootJsonFormat[SynapseResourceResponse] = jsonFormat1(SynapseResourceResponse.apply)

}

object SynapseUtilities {

  import SynapseJsonProtocol._

  lazy val SynapseToken: String = getAccessToken(ClientId, Secrets.SynapseSpnKey,
    "https://dev.azuresynapse.net/")
  lazy val ArmToken: String = getAccessToken(ClientId, Secrets.SynapseSpnKey,
    "https://management.azure.com/")

  val LineSeparator: String = sys.props("line.separator").toLowerCase // Platform agnostic (\r\n:windows, \n:linux)
  val Folder = s"build_${BuildInfo.version}/scripts"
  val TimeoutInMillis: Int = 30 * 60 * 1000 // 30 minutes
  val StorageAccount: String = "mmlsparkbuildsynapse"
  val StorageContainer: String = "synapse"
  val TenantId: String = "72f988bf-86f1-41af-91ab-2d7cd011db47"
  val ClientId: String = "85dde348-dd2b-43e5-9f5a-22262af45332"
  val PoolNodeSize: String = "Small"
  val PoolLocation: String = "eastus2"
  val WorkspaceName: String = "mmlsparkbuild"
  val ResourceGroupName: String = "marhamil-mmlspark"
  val SubscriptionId: String = Secrets.SubscriptionID

  val ClusterPrefix = "tc"
  val ManagementUrlRoot: String = s"https://management.azure.com/subscriptions/" +
    s"$SubscriptionId/resourceGroups/$ResourceGroupName"
  val WorkspaceRoot = s"https://$WorkspaceName.dev.azuresynapse.net"

  def livyUrl(sparkPool: String): String = {
    s"$WorkspaceRoot/livyApi/versions/2019-11-01-preview/sparkPools/$sparkPool/batches"
  }

  def getQueuedJobs(poolName: String): Applications = {
    val uri: String =
      WorkspaceRoot +
        "/monitoring/workloadTypes/spark/applications" +
        "?api-version=2020-10-01-preview" +
        "&filter=(((state%20eq%20%27Queued%27)%20or%20(state%20eq%20%27Submitting%27))" +
        s"%20and%20(sparkPoolName%20eq%20%27$poolName%27))"
    val getRequest = new HttpGet(uri)
    getRequest.setHeader("Authorization", s"Bearer $SynapseToken")
    sendAndParseJson(getRequest).convertTo[Applications]
  }

  @tailrec
  def findAvailablePool(sparkPools: Seq[String]): String = {
    val readyPools = sparkPools.filter(getQueuedJobs(_).nJobs == 0)
    if (readyPools.nonEmpty) {
      val readyPool = readyPools.head
      println(s"Spark Pool: $readyPool is ready")
      readyPool
    } else {
      println(s"No spark pool is ready to submit a new job, waiting 10s")
      blocking {
        Thread.sleep(10000)
      }
      findAvailablePool(sparkPools)
    }
  }

  def uploadAndSubmitNotebook(poolName: String, notebook: File): LivyBatch = {
    val dest = s"$Folder/${notebook.getName}"
    exec(s"az storage fs file upload " +
      s" -s ${notebook.getAbsolutePath} -p $dest -f $StorageContainer " +
      " --overwrite true " +
      s" --account-name $StorageAccount --account-key ${Secrets.SynapseStorageKey}")
    val abfssPath = s"abfss://$StorageContainer@$StorageAccount.dfs.core.windows.net/$dest"

    val excludes: String = Seq(
      "org.scala-lang:scala-reflect",
      "org.apache.spark:spark-tags_2.12",
      "org.scalactic:scalactic_2.12",
      "org.scalatest:scalatest_2.12",
      "org.slf4j:slf4j-api").mkString(",")
    val runName = abfssPath.split('/').last.replace(".py", "")
    val livyPayload: String =
      s"""
         |{
         | "file" : "$abfssPath",
         | "name" : "$runName",
         | "driverMemory" : "28g",
         | "driverCores" : 4,
         | "executorMemory" : "28g",
         | "executorCores" : 4,
         | "numExecutors" : 2,
         | "conf" :
         |     {
         |         "spark.jars.packages" : "$SparkMavenPackageList",
         |         "spark.jars.repositories" : "$SparkMavenRepositoryList",
         |         "spark.jars.excludes": "$excludes",
         |         "spark.yarn.user.classpath.first": "true",
         |         "spark.sql.parquet.enableVectorizedReader":"false",
         |         "spark.sql.legacy.replaceDatabricksSparkAvro.enabled": "true"
         |     }
         | }
      """.stripMargin

    val createRequest = new HttpPost(livyUrl(poolName))
    createRequest.setHeader("Content-Type", "application/json")
    createRequest.setHeader("Authorization", s"Bearer $SynapseToken")
    createRequest.setEntity(new StringEntity(livyPayload))
    LivyBatch(sendAndParseJson(createRequest).convertTo[LivyBatchData], runName, poolName)
  }

  private def bigDataPoolBicepPayload(bigDataPoolName: String,
                                      poolLocation: String,
                                      poolNodeSize: String,
                                      createdAtTime: String): String = {
    val buildId: String = sys.env.getOrElse("AdoBuildId", "unknown")
    val buildNumber: String = sys.env.getOrElse("AdoBuildNumber", "unknown")
    s"""
       |{
       |  "name": "$bigDataPoolName",
       |  "location": "$poolLocation",
       |  "tags": {
       |    "createdBy": "SynapseE2E Tests",
       |    "createdAt": "$createdAtTime",
       |    "buildId": "$buildId",
       |    "buildNumber": "$buildNumber",
       |  },
       |  "properties": {
       |    "autoPause": {
       |      "delayInMinutes": "10",
       |      "enabled": "true"
       |    },
       |    "autoScale": {
       |      "enabled": "true",
       |      "maxNodeCount": "10",
       |      "minNodeCount": "3"
       |    },
       |    "cacheSize": "20",
       |    "dynamicExecutorAllocation": {
       |      "enabled": "true",
       |      "maxExecutors": "8",
       |      "minExecutors": "2"
       |    },
       |    "isComputeIsolationEnabled": "false",
       |    "nodeCount": "0",
       |    "nodeSize": "$poolNodeSize",
       |    "nodeSizeFamily": "MemoryOptimized",
       |    "provisioningState": "Succeeded",
       |    "sessionLevelPackagesEnabled": "true",
       |    "sparkVersion": "3.3"
       |  }
       |}
       |""".stripMargin
  }

  def tryDeleteOldSparkPools(): Unit = {
    println("Deleting stray old Apache Spark Pools...")
    val dayAgoTsInMillis: Long = Calendar.getInstance().getTimeInMillis - 24 * 60 * 60 * 1000 // Timestamp 24 hrs ago
    val getBigDataPoolsUri =
      s"""
         |$ManagementUrlRoot/resources?api-version=2021-04-01&
         |$$filter=substringof(name, \'$WorkspaceName/$ClusterPrefix\') and
         | resourceType eq \'Microsoft.Synapse/workspaces/bigDataPools\'
         |""".stripMargin.replaceAll(LineSeparator, "").replaceAll(" ", "%20")
    val getBigDataPoolRequest = new HttpGet(getBigDataPoolsUri)
    getBigDataPoolRequest.setHeader("Authorization", s"Bearer $ArmToken")
    val sparkPools = sendAndParseJson(getBigDataPoolRequest).convertTo[SynapseResourceResponse].value
    sparkPools.foreach(sparkPool => {
      val name = sparkPool.name.stripPrefix(s"$WorkspaceName/")
      if (sparkPool.tags.contains("createdAt") && sparkPool.tags.contains("createdBy")) {
        assert(name.stripPrefix(ClusterPrefix).length == dayAgoTsInMillis.toString.length)
        val creationTime = name.stripPrefix(ClusterPrefix).toLong
        if (creationTime <= dayAgoTsInMillis) {
          try {
            deleteSparkPool(name)
          } catch {
            case e: RuntimeException => println(s"Could not delete old spark cluster: ${e.getMessage}")
          }
        }
      }
    })
  }

  def createSparkPools(poolCount: Int): Seq[String] = {
    val timeStamp: String = Calendar.getInstance().getTime.toString
    (1 to poolCount).map { _ =>
      val triggerTime: String = Calendar.getInstance().getTimeInMillis.toString
      val bigDataPoolName = s"$ClusterPrefix$triggerTime"
      val deployUri = s"$ManagementUrlRoot/providers/Microsoft.Synapse/workspaces/" +
        s"$WorkspaceName/bigDataPools/$bigDataPoolName?api-version=2021-06-01-preview"

      // Create & Run the deployment Request
      val deployRequest = new HttpPut(deployUri)
      deployRequest.setHeader("Authorization", s"Bearer $ArmToken")
      deployRequest.setHeader("Content-Type", "application/json")
      deployRequest.setEntity(new StringEntity(
        bigDataPoolBicepPayload(bigDataPoolName, PoolLocation, PoolNodeSize, timeStamp)))
      println(s"Creating Apache Spark Pool: $bigDataPoolName...")
      safeSend(deployRequest)
      bigDataPoolName
    }
  }

  def deleteSparkPool(poolName: String): Unit = {
    val deleteUri = s"$ManagementUrlRoot/providers/Microsoft.Synapse/workspaces/" +
      s"$WorkspaceName/bigDataPools/$poolName?api-version=2021-06-01-preview"
    val deleteRequest = new HttpDelete(deleteUri)
    deleteRequest.setHeader("Authorization", s"Bearer $ArmToken")
    println(s"Deleting pool $poolName")
    safeSend(deleteRequest)
  }

  def getAccessToken(clientId: String, clientSecret: String, reqResource: String): String = {
    val createRequest = new HttpPost(s"https://login.microsoftonline.com/$TenantId/oauth2/token")
    createRequest.setHeader("Content-Type", "application/x-www-form-urlencoded")
    createRequest.setEntity(
      new UrlEncodedFormEntity(
        List(
          ("grant_type", "client_credentials"),
          ("client_id", clientId),
          ("client_secret", clientSecret),
          ("resource", reqResource)
        ).map(p => new BasicNameValuePair(p._1, p._2)).asJava, "UTF-8")
    )
    RESTHelpers.sendAndParseJson(createRequest).asJsObject()
      .fields("access_token").convertTo[String]
  }
}
