package com.microsoft.ml.spark.cognitive

import java.util.concurrent.Executors

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, ClientCredential}
import scalaj.http.{Http, HttpResponse, MultiPart}
import spray.json._
import java.nio.file.{Files, Paths}

class TestApp {
  lazy val resource: String = "https://login.microsoftonline.com"
  lazy val clientId: String = sys.env.getOrElse("CLIENT_ID", "")
  lazy val clientSecret: String = sys.env.getOrElse("CLIENT_SECRET", "")
  lazy val tenantId: String = sys.env.getOrElse("TENANT_ID", "")

  lazy val token: String = getAuth.getAccessToken
  lazy val headers: Seq[(String, String)] = Seq("Authorization" -> s"Bearer $token")

  lazy val subscriptionId: String = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"
  lazy val region: String = "eastus"
  lazy val resourceGroup: String = "extern2020"
  lazy val workspace: String = "exten-amls"

  lazy val hostUrl = s"https://$region.api.azureml.ms/"
  lazy val resourceBase: String = s"subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/" +
    s"Microsoft.MachineLearningServices/workspaces/$workspace/"

  lazy val experimentName = "new_experiment"
  lazy val runFilePath = System.getProperty("user.dir") + "/src/test/resources/testRun"

  def main(): Unit = {
    getOrCreateExperiment(experimentName)
    launchRun(experimentName, runFilePath)
    showModels
  }

  def getAuth: AuthenticationResult = {
    assert(clientId.length > 0, "Error: Set the CLIENT_ID environment variable.")
    assert(clientSecret.length > 0, "Error: Set the CLIENT_SECRET environment variable.")
    assert(tenantId.length > 0, "Error: Set the TENANT_ID environment variable.")

    val authority = s"$resource/$tenantId"
    val credential = new ClientCredential(clientId, clientSecret)

    val service = Executors.newFixedThreadPool(1)
    val context = new AuthenticationContext(authority, true, service)

    val future = context.acquireToken(resource, credential, null)
    future.get
  }

  def getOrCreateExperiment(experimentName: String): Unit = {
    println(s"TOKEN: $token")
    val historyBase = "history/v1.0/"
    val url = s"$hostUrl$historyBase$resourceBase" + s"experiments/$experimentName"

    // first try getting existing
    val response: HttpResponse[String] = Http(url)
      .headers(headers).asString

    if (response.code == 200) {
      println(s"Found $experimentName")
    } else {
      // otherwise create new experiment
      val response: HttpResponse[String] = Http(url)
        .postForm(Seq()).headers(headers).asString

      if (response.code == 200) {
        println(s"Created $experimentName")
      }
      else if (response.code == 401) {
        throw new Exception("Unauthorized request. Check your client ID, client secret, and tenant ID.")
      } else {
        println(response)
        throw new Exception
      }
    }
  }

  def getHTTPResponseField(response: HttpResponse[String], field: String): String = {
    response.body
      .parseJson.asJsObject
      .getFields(field).mkString
      .stripPrefix("\"").stripSuffix("\"")
  }

  def launchRun(experimentName: String, filePath: String, verbose: Boolean = true): Unit = {
    val executionBase = "execution/v1.0/"
    val url = s"$hostUrl$executionBase$resourceBase" + s"experiments/$experimentName/startrun"
    val path = if (filePath.substring(filePath.length - 1) != "/") (filePath + "/") else filePath

    val definitionBytes = Files.readAllBytes(Paths.get(path + "definition.json"))
    val projectBytes = Files.readAllBytes(Paths.get(path + "project.zip"))

    val response: HttpResponse[String] = Http(url)
      .postMulti(
        MultiPart("runDefinitionFile", path + "definition.json", "text/text", definitionBytes),
        MultiPart("projectZipFile", path + "project.zip", "application/zip", projectBytes)
      )
      .headers(headers).asString
    val runId = getHTTPResponseField(response, "runId")

    if (response.code == 200) {
      val successUrl = s"https://mlworkspacecanary.azure.ai/portal/subscriptions/" +
        s"$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.MachineLearningServices/" +
        s"workspaces/$workspace/experiments/$experimentName/runs/$runId"
      println(s"Congratulations, your job is running. You can check its progress at $successUrl")
    } else {
      println(response)
      throw new Exception
    }

  }

  def showModels(): Unit = {
    val modelHostUrl = "https://docs.microsoft.com/"
    val modelBase = "modelmanagement/v1.0/"
    val subscriptionId = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"

     val url = s"$modelHostUrl$modelBase/modelmanagement/v1.0/subscriptions/${subscriptionId}/" +
      s"resourceGroups/${resourceGroup}/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/${workspace}/models"

    val response: HttpResponse[String] = Http(url)
      .headers(headers)
      .param("runId", "AutoML_60f0a9c6-c7d4-4635-a02c-b5f0dde3ca54_ModelExplain")
      .asString
    println(response)
  }

}
