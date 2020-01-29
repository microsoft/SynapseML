// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import java.nio.file.{Files, Paths}

import com.microsoft.ml.spark.cognitive.{SpeechKey, SpeechToTextSDK}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import scalaj.http.Http

import scala.util.Random

class AMLSuite extends TransformerFuzzing[SpeechToTextSDK]
  with SpeechKey {
  import session.implicits._

  val resourcesDir: String = System.getProperty("user.dir") + "/src/test/resources/"
  val mnistEndpoint = "http://37284a91-944c-4443-bbcd-74838d158b75.eastus.azurecontainer.io/score"
  val mnistModel: AMLModel = new AMLModel("fakeuid1234")
    .setUri(mnistEndpoint)

  override def testSerialization(): Unit = {
    tryWithRetries(Array(0, 100, 100, 100, 100))(super.testSerialization)
  }

  lazy val aml: AMLEstimator = new AMLEstimator()
    .setClientId(sys.env.getOrElse("CLIENT_ID", ""))
    .setClientSecret(sys.env.getOrElse("CLIENT_SECRET", ""))
    .setRegion("eastus")
    .setResource("https://login.microsoftonline.com")
    .setTenantId(sys.env.getOrElse("TENANT_ID", ""))
    .setSubscriptionId("ce1dee05-8cf6-4ad6-990a-9c80868800ba")
    .setResourceGroup("extern2020")
    .setWorkspace("exten-amls")
    .setExperimentName("new_experiment")
    .setRunFilePath(resourcesDir + "testRun")

  test("Get token") {
    val token = aml.getAuth.getAccessToken
    println(s"Token: $token")
    assert(token.length > 0, "Access token not found")
  }

  test("Get existing experiment") {
    val runId = aml.getOrCreateExperiment
    println(runId)
  }

  /*
    TODO: Comment back in when support for removing experiments through API
     is supported (otherwise a new experiment will be created whenever tests
     are run)
   */
  /*
  test("Create new experiment") {
    def randomString(length: Int): String = {
      val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
      val sb = new StringBuilder
      for (i <- 1 to length) {
        val randomNum = util.Random.nextInt(chars.length)
        sb.append(chars(randomNum))
      }
      sb.toString
    }

    val randomExperimentName = s"fake_experiment_${randomString(6)}"
    println(s"Random Name: $randomExperimentName")
    val experiment = app.getOrCreateExperiment(randomExperimentName)
  }
  */

  test("Launch run hello") {
    aml.getOrCreateExperiment
    aml.launchRun
  }

  test("Launch run deploy model") {
    aml.setRunFilePath(resourcesDir + "testRunDeployModel")
    aml.getOrCreateExperiment
    aml.launchRun
  }

  test("Get model") {
    val runId = "AutoML_60f0a9c6-c7d4-4635-a02c-b5f0dde3ca54_97"
    val model = aml.getTopModelResponse(runId)
    assert(model.code == 200)
  }

  test("Get MNIST model") {
    val runId = "sklearn-mnist_1579893818_7eeca99c"
    val model = aml.getTopModelResponse(runId)
    assert(model.code == 200)
    println(model)

    val id = aml.getHTTPResponseField(model, "id")
    val url = aml.getHTTPResponseField(model, "url")
    val mimeType = aml.getHTTPResponseField(model, "mimeType")
    val unpack = aml.getHTTPResponseField(model, "unpack")
//    val asset =
//      s"""
//         |{
//         |
//         |""".stripMargin
//    assert(model.experimentName.get == "sklearn-mnist")
  }

  test("Get deployed model") {
    val endpoint = "http://d6ce4453-e327-4c79-89b0-22ca5bf65473.eastus.azurecontainer.io/score"
  }

  test("MNIST model predict basic") {
    val dataPath = resourcesDir + "mnist_smol.txt"

    val dataFromFile = Files.readAllLines(Paths.get(dataPath)).toString
    val dataList = dataFromFile.substring(1, dataFromFile.length - 1).split(", ")
    val label = dataList(0)
    val dataPoint = dataList.slice(1, dataList.size).mkString(", ")

    val data = s"""{"data": [[$dataPoint]]}"""

    val response = aml.predict(mnistEndpoint, data)
    assert(response.body == s"[$label]")
  }

  test("MNIST model predict batch") {
    val dataPath = resourcesDir + "mnist_batch.txt"
    val labelsPath = resourcesDir + "mnist_batch_labels.txt"

    val dataFromFile = Files.readAllLines(Paths.get(dataPath)).toString
    val labels = Files.readAllLines(Paths.get(labelsPath)).toString

    val data = s"""{"data": $dataFromFile}"""
    val response = aml.predict(mnistEndpoint, data)
    assert(response.body == s"$labels")
  }

  test("MNIST model predict DF") {
    val dataPath = resourcesDir + "mnist_batch.txt"
    val labelsPath = resourcesDir + "mnist_batch_labels.txt"

    val data = Files.readAllLines(Paths.get(dataPath))
      .toString.split("]")
      .map(string => string.substring(2))
    data.foreach(println)
    println(data.size)
    val labels = Files.readAllLines(Paths.get(labelsPath)).toString

    val mnistDf: DataFrame = Seq(data).toDF("input")
    println(mnistDf)
  }

  test("stupid test") {
    val url = "http://13.68.134.151:80/api/v1/service/mydumbservice2/score"
    val key = "04GtCCdEvT37WIXA0LOSDdmgouv0guzg"
    val data = s"""{"data": [14]}"""
    val response = Http(url)
      .header("content-type", "application/json")
      .header("Authorization", s"Bearer $key")
      .postData(data).asString
    println(response)
  }

  test("deployTest") {
    val configPath = resourcesDir + "model_config.json"
    val response = aml.deployModel(configPath)
    println(response.body)
    println(response.statusLine)
    println(response.code)
    println(response.headers)
  }

  test("artifact basic") {
    val filePath = resourcesDir + "score.py"
    val randomVal = new Random().nextDouble * 1000
//    val randomVal = 1010
    val container = s"random_string_${randomVal.toInt}"
    val response = aml.uploadArtifact(filePath, container)
    println(response)
    assert(response.code == 200)
  }

  test("deploy model") {
    val filePath = resourcesDir + "score.py"
    val randomVal = new Random().nextDouble * 1000
    val container = s"random_string_${randomVal.toInt}"
    val response = aml.uploadArtifact(filePath, container)
    println(response)
    val response2 = aml.deployModel(resourcesDir + "model_config.json")
    println(response2)
  }

  test("make assets") {
    val filePath = resourcesDir + "score.py"
    val randomVal = new Random().nextDouble * 1000
    val container = s"random_string_${randomVal.toInt}"
    val response = aml.uploadArtifact(filePath, container)
    val artifactId = aml.getHTTPResponseField(response, "artifactId")
    println(artifactId)
    val response2 = aml.createAssets(artifactId)
  }

  override def testObjects(): Seq[TestObject[SpeechToTextSDK]] =
    Seq()

  override def reader: MLReadable[_] = AMLEstimator
}
