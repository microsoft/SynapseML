package com.microsoft.ml.spark.cognitive

import java.util.concurrent.Executors

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, ClientCredential}
import scalaj.http.{Http, HttpResponse, MultiPart}
import spray.json._
import java.nio.file.{Files, Paths}

class TestApp {
  lazy val resource = "https://login.microsoftonline.com"
  lazy val clientId = sys.env.getOrElse("CLIENT_ID", "")
  lazy val clientSecret = sys.env.getOrElse("CLIENT_SECRET", "")
  lazy val tenantId = sys.env.getOrElse("TENANT_ID", "")

  lazy val token = getAuth.getAccessToken
  lazy val headers = Seq("Authorization" -> s"Bearer $token")

  lazy val subscriptionId = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"
  lazy val region = "eastus"
  lazy val rg = "extern2020"
  lazy val ws = "exten-amls"

  lazy val hostUrl = s"https://$region.api.azureml.ms/"
  lazy val resourceBase = s"subscriptions/$subscriptionId/resourceGroups/$rg/providers/" +
    s"Microsoft.MachineLearningServices/workspaces/$ws/"

  def main(): Unit = {
    val experimentName = "new_experiment"
    getOrCreateExperiment(experimentName)
    val runFilePath = System.getProperty("user.dir") + "/src/test/resources/testRun"
    launchRun(experimentName, runFilePath)
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

    println(response)

    val runId = getHTTPResponseField(response, "runId")

    if (response.code == 200) {
      val successUrl = s"https://mlworkspacecanary.azure.ai/portal/subscriptions/" +
        s"$subscriptionId/resourceGroups/$rg/providers/Microsoft.MachineLearningServices/" +
        s"workspaces/$ws/experiments/$experimentName/runs/$runId"
      println(s"Congratulations, your job is running. You can check its progress at $successUrl")
    } else {
      throw new Exception
    }

  }
}
