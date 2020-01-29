// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.Executors

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, ClientCredential}
import com.microsoft.ml.spark.aml.AMLExperimentFormat._
import com.microsoft.ml.spark.cognitive.{HasServiceParams, SpeechResponse}
import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol, Wrappable}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.commons.io.IOUtils
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scalaj.http.{Http, HttpOptions, HttpResponse, MultiPart}
import spray.json._

import scala.io.Source

trait AMLParams extends Wrappable with DefaultParamsWritable {
  /*** @group param*/
  val resource = new Param[String](this, "resource", "URL to login resource")

  /** @group getParam */
  final def getResource: String = $(resource)

  /*** @group param*/
  val clientId = new Param[String](this, "clientId", "Client ID of application")

  /** @group getParam */
  final def getClientId: String = $(clientId)

  /*** @group param*/
  val clientSecret = new Param[String](this, "clientSecret", "Client secret of application")

  /** @group getParam */
  final def getClientSecret: String = $(clientSecret)

  /*** @group param*/
  val tenantId = new Param[String](this, "tenantId", "App tenant ID")

  /** @group getParam */
  final def getTenantId: String = $(tenantId)

  /*** @group param*/
  val subscriptionId = new Param[String](this, "subscriptionId", "App subscription ID")

  /** @group getParam */
  final def getSubscriptionId: String = $(subscriptionId)

  /*** @group param*/
  val region = new Param[String](this, "region", "User geographic region")

  /** @group getParam */
  final def getRegion: String = $(region)

  /*** @group param*/
  val resourceGroup = new Param[String](this, "resourceGroup", "App resource group")

  /** @group getParam */
  final def getResourceGroup: String = $(resourceGroup)

  /*** @group param*/
  val workspace = new Param[String](this, "workspace", "App workspace")

  /** @group getParam */
  final def getWorkspace: String = $(workspace)

  /*** @group param*/
  val experimentName = new Param[String](this, "experimentName", "Name of experiment")

  /** @group getParam */
  final def getExperimentName: String = $(experimentName)

  /*** @group param*/
  val runFilePath = new Param[String](this, "runFilePath",
    "Path to where definition JSON and project ZIP folder are located")

  /** @group getParam */
  final def getRunFilePath: String = $(runFilePath)

}

object AMLEstimator extends DefaultParamsReadable[AMLEstimator]

/**
  * @param uid The id of the module
  */
class AMLEstimator(override val uid: String)
  extends Estimator[AMLModel]
    with AMLParams with HasInputCol with HasOutputCol {
  def this() = this(Identifiable.randomUID("AMLEstimator"))

  setDefault(outputCol, uid + "_output")

  def setResource(value: String): this.type = set(resource, value)

  setDefault(resource -> "https://login.microsoftonline.com")

  def setClientId(value: String): this.type = set(clientId, value)

  def setClientSecret(value: String): this.type = set(clientSecret, value)

  def setTenantId(value: String): this.type = set(tenantId, value)

  def setSubscriptionId(value: String): this.type = set(subscriptionId, value)

  def setRegion(value: String): this.type = set(region, value)

  def setResourceGroup(value: String): this.type = set(resourceGroup, value)

  def setWorkspace(value: String): this.type = set(workspace, value)

  def setExperimentName(value: String): this.type = set(experimentName, value)

  def setRunFilePath(value: String): this.type = set(runFilePath, value)

  lazy val token: String = getAuth.getAccessToken
  lazy val headers: Seq[(String, String)] = Seq("Authorization" -> s"Bearer $token")
  lazy val hostUrl = s"https://$getRegion.api.azureml.ms/"
  lazy val resourceBase: String = s"subscriptions/$getSubscriptionId/resourceGroups/$getResourceGroup/providers/" +
    s"Microsoft.MachineLearningServices/workspaces/$getWorkspace/"

  def getAuth: AuthenticationResult = {
    assert(getClientId.length > 0, "Error: Set the CLIENT_ID environment variable.")
    assert(getClientSecret.length > 0, "Error: Set the CLIENT_SECRET environment variable.")
    assert(getTenantId.length > 0, "Error: Set the TENANT_ID environment variable.")

    val authority = s"$getResource/$getTenantId"
    val credential = new ClientCredential(getClientId, getClientSecret)

    val service = Executors.newFixedThreadPool(1)
    val context = new AuthenticationContext(authority, true, service)

    val future = context.acquireToken(getResource, credential, null)
    future.get
  }

  def getHTTPResponseField(response: HttpResponse[String], field: String): String = {
    response.body
      .parseJson.asJsObject
      .getFields(field).mkString
      .stripPrefix("\"").stripSuffix("\"")
  }

  def getOrCreateExperiment: String = {
    assert(token.length > 0, "Error: Access token not found. Check your credentials.")
    val historyBase = "history/v1.0/"
    val url = s"$hostUrl$historyBase$resourceBase" + s"experiments/$getExperimentName"

    // first try getting existing
    val response: HttpResponse[String] = Http(url)
      .headers(headers).asString
    println(response)

    if (response.code == 200) {
      println(s"Found $getExperimentName")
      getHTTPResponseField(response, "runId")
    } else {
      // otherwise create new experiment
      val response: HttpResponse[String] = Http(url)
        .postForm(Seq()).headers(headers).asString

      if (response.code == 200) {
        println(s"Created $experimentName")
        getHTTPResponseField(response, "runId")
      }
      else if (response.code == 401) {
        throw new Exception("Unauthorized request. Check your client ID, client secret, and tenant ID.")
      } else {
        println(response)
        throw new Exception
      }
    }
  }

  def launchRun: String = {
    val executionBase = "execution/v1.0/"
    val url = s"$hostUrl$executionBase$resourceBase" + s"experiments/$getExperimentName/startrun"
    val path = if (getRunFilePath.substring(getRunFilePath.length - 1) != "/") (getRunFilePath + "/")
              else getRunFilePath

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
        s"$getSubscriptionId/resourceGroups/$getResourceGroup/providers/Microsoft.MachineLearningServices/" +
        s"workspaces/$getWorkspace/experiments/$getExperimentName/runs/$runId"
      println(s"Congratulations, your job is running. You can check its progress at $successUrl")
      runId
    } else {
      println(response)
      throw new Exception
    }
  }

  def getTopModelResponse(runId: String): HttpResponse[String] = {
    val modelBase = "modelmanagement/v1.0/"
    val subscriptionId = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"

    val url = s"${hostUrl}${modelBase}subscriptions/${getSubscriptionId}/" +
      s"resourceGroups/${getResourceGroup}/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/${getWorkspace}/models"

    val response = Http(url)
      .headers(headers)
      .param("runId", runId)
      .param("count", "1")
      .option(HttpOptions.readTimeout(10000))
      .asString
    println(response)
    response
//    val topModel = response.body.parseJson.asJsObject
//        .getFields("value").toList.head
//        .convertTo[Array[AMLModelResponse]].head
//    topModel
  }

  def deployModel(modelId: String, computeType: String, container: String): HttpResponse[String] = {
    val possibleComputeTypes = List("ACI", "AKSENDPOINT", "IOT")
    assert(possibleComputeTypes.contains(computeType),
      s"""Error: Please enter a valid compute type. Possible values are ${
        possibleComputeTypes.mkString(", ")
      }"""
    )

    val serviceBase = "modelmanagement/v1.0/"

    val url = s"${hostUrl}${serviceBase}subscriptions/$getSubscriptionId/" +
      s"resourceGroups/$getResourceGroup/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/$getWorkspace/services"

    val response = Http(url).postForm(Seq(
      "computeType" -> "possibleComputeTypes",
      "name" -> "myServiceTEST"
    )).asString

    println(response)
    response
    // TODO: figure out params to pass into REST API call
    // ref: https://docs.microsoft.com/en-us/rest/api/azureml/modelsanddeployments/services/create#deploymenttype
    ???
  }

  def deployModel(configPath: String): HttpResponse[String] = {
    val serviceBase = "modelmanagement/v1.0/"

    val url = s"${hostUrl}${serviceBase}subscriptions/$getSubscriptionId/" +
      s"resourceGroups/$getResourceGroup/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/$getWorkspace/services"

    val config = Source.fromFile(configPath).getLines.mkString.parseJson.toString

    println(s"CONFIG: $config\n\n")
    Http(url)
      .header("content-type", "application/json")
      .header("Authorization", s"Bearer $token")
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
      .postData(config).asString
  }

  def predict(modelEndpoint: String, data: String): HttpResponse[String] = {
    val response = Http(modelEndpoint)
      .header("content-type", "application/json")
      .postData(data)
      .asString
    response
  }

  def uploadArtifact(path: String, container: String): HttpResponse[String] = {
    val artifactBase = "artifact/v1.0/"
    val origin = "LocalUpload"

    val url = s"$hostUrl${artifactBase}subscriptions/${getSubscriptionId}" +
      s"/resourceGroups/${getResourceGroup}/providers/Microsoft." +
      s"MachineLearningServices/workspaces/${getWorkspace}/artifacts/content/${origin}/${container}/${path}"

    val byteArray = IOUtils.toByteArray(new FileInputStream(new File(path)))

    Http(url)
      .header("Content-Type", "application/json; charset=utf-8")
      .header("Authorization", s"Bearer $token")
      .postData(byteArray).asString
  }

  def createAssets(artifact: String): HttpResponse[String] = {
    val modelBase = "modelmanagement/v1.0/"
    val url = s"$hostUrl${modelBase}subscriptions/${getSubscriptionId}/" +
      s"resourceGroups/${getResourceGroup}/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/${getWorkspace}/assets"

//    "artifacts": [{"prefix": "LocalUpload/200128T172310-de742f9e/my_c00l_classifier.joblib.tar.gz"}]
    val payload =
      s"""
        |{
        |"name": "stupid_model_asset",
        |"artifacts": [{"prefix": "$artifact"}]
        |}
        |""".stripMargin

    val response = Http(url)
      .header("content-type", "application/json")
      .header("Authorization", s"Bearer $token")
      .postData(payload)
      .asString
    response
  }

  override def fit(dataset: Dataset[_]): AMLModel = {
    getOrCreateExperiment
    val runId = launchRun
    val modelResponse = getTopModelResponse(runId)
    // TODO: deploy the model (call deployModel)
//    new AMLModel(modelResponse.id)
    ???
  }

  def deleteModel(id: String): Unit = {
    val serviceBase = "modelmanagement/v1.0/"

    val url = s"${hostUrl}${serviceBase}subscriptions/${getSubscriptionId}" +
      s"/resourceGroups/${getResourceGroup}/providers/Microsoft.MachineLearningServices/" +
      s"workspaces/${getWorkspace}/services/${id}"

    val response = Http(url)

    // TODO
    ???
  }

  override def copy(extra: ParamMap): this.type =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    schema.add("prediction", StringType)
  }

}

class AMLModel(val uid: String)
  extends Model[AMLModel] with DefaultParamsWritable with HasOutputCol
  with HasServiceParams with HasInputCol {
  override def copy(extra: ParamMap): AMLModel = defaultCopy(extra)

  val uri = new Param[String](this, "uri", "REST API endpoint")

  val chunkSize = new Param[Int](this, "chunkSize", "Number of chunks to split data")

  setDefault(chunkSize, 1)

  /** @group getParam */
  final def getUri: String = $(uri)

  /** @group getParam */
  final def getChunkSize: Int = $(chunkSize)

  def setUri(value: String): this.type = set(uri, value)

  def setChunkSize(value: String): this.type = set(chunkSize, value.toInt)

  def predict(data: String): HttpResponse[String] = {
    val response = Http(getUri)
      .header("content-type", "application/json")
      .postData(data)
      .asString
    response
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // call the deployed web service
    // https://docs.microsoft.com/en-us/azure/machine-learning/how-to-consume-web-service
    val df = dataset.toDF
    val schema = dataset.schema
    val sparkSession: SparkSession = df.sparkSession
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", dataset)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)

    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }

//    val inputCol = "input"

    df.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))
      .withColumn(
        getOutputCol,
        udf(predict(getInputCol), ArrayType(SpeechResponse.schema))(col(dynamicParamColName)))
      .drop(dynamicParamColName)

//    df.withColumn("prediction", udf(predict("input")))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add("prediction", StringType)
  }
}

object AMLModel extends DefaultParamsReadable[AMLModel]
