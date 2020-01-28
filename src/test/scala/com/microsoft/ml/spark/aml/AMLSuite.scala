// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.cognitive.{SpeechKey, SpeechToTextSDK}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable

class AMLSuite extends TransformerFuzzing[SpeechToTextSDK]
  with SpeechKey {

  val resourcesDir = System.getProperty("user.dir") + "/src/test/resources/"
  val mnistEndpoint = "http://37284a91-944c-4443-bbcd-74838d158b75.eastus.azurecontainer.io/score"

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
    .setRunFilePath(System.getProperty("user.dir") + "/src/test/resources/testRun")

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
    TODO: Comment back in when support for removing experiments
    through API is supported (otherwise created a new experiment
    whenever tests are run)
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

  test("Launch run") {
    aml.getOrCreateExperiment
    aml.launchRun
  }

  test("Get model") {
    val runId = "AutoML_60f0a9c6-c7d4-4635-a02c-b5f0dde3ca54_97"
    val model = aml
      .getTopModelResponse(runId)
    assert(model.experimentName.get == "my-1st-automl-experiment")
  }

  test("Get MNIST model") {
    val runId = "sklearn-mnist_1579893818_7eeca99c"
    val model = aml
      .getTopModelResponse(runId)
    println(model)
    assert(model.experimentName.get == "sklearn-mnist")
  }

  test("Deploy model") {
//    aml.deployModel("model_id", "some invalid type")
  }

  test("Deploy model invalid") {
    assertThrows[AssertionError] {
      aml.deployModel("model_id", "some invalid type")
    }
  }

  test("Get deployed model") {
    val endpoint = "http://d6ce4453-e327-4c79-89b0-22ca5bf65473.eastus.azurecontainer.io/score"
  }

  test("Img to bytes") {
    import javax.imageio.ImageIO
    import java.awt.image.DataBufferByte
    import java.awt.image.WritableRaster
    import java.awt.image.BufferedImage
    import java.io.File

    val imgPath = resourcesDir + "mnist_2.jpg"
    val img = new File(imgPath)
    val bufferedImage: BufferedImage = ImageIO.read(img)

    val raster: WritableRaster = bufferedImage.getRaster
    val data: DataBufferByte = raster.getDataBuffer.asInstanceOf[DataBufferByte]
    println(data.getData.toString)
  }

  test("MNIST model predict basic") {
    import java.nio.file.{Files, Paths}
    val dataPath = resourcesDir + "mnist_smol.txt"

    val dataFromFile = Files.readAllLines(Paths.get(dataPath)).toString
    val dataList = dataFromFile.substring(1, dataFromFile.length - 1)split(", ")
    val label = dataList(0)
    val dataPoint = dataList.slice(1, dataList.size).mkString(", ")

    val data = s"""{"data": [[$dataPoint]]}"""
    println(data)

    val response = aml.predict(mnistEndpoint, data)
    assert(response.body == s"[$label]")
  }

  test("MNIST model predict batch") {
    import java.nio.file.{Files, Paths}
    val dataPath = resourcesDir + "mnist_batch.txt"
    val labelsPath = resourcesDir + "mnist_batch_labels.txt"

    val dataFromFile = Files.readAllLines(Paths.get(dataPath)).toString
    val labels = Files.readAllLines(Paths.get(labelsPath)).toString
    println(labels)

    val data = s"""{"data": $dataFromFile}"""
    val response = aml.predict(mnistEndpoint, data)
    assert(response.body == s"$labels")
  }

  override def testObjects(): Seq[TestObject[SpeechToTextSDK]] =
    Seq()

  override def reader: MLReadable[_] = AMLEstimator
}
