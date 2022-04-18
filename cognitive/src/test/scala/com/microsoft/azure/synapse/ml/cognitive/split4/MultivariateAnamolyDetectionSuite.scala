// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split4

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.cognitive.split1.AnomalyKey
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.benchmarks.DatasetUtils
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import spray.json.{DefaultJsonProtocol, _}

case class MADListModelsResponse(models: Seq[MADModel],
                                 currentCount: Int,
                                 maxCount: Int,
                                 nextLink: Option[String])

case class MADModel(modelId: String,
                    createdTime: String,
                    lastUpdatedTime: String,
                    status: String,
                    displayName: Option[String],
                    variablesCount: Int)

object MADListModelsProtocol extends DefaultJsonProtocol {
  implicit val MADModelEnc: RootJsonFormat[MADModel] = jsonFormat6(MADModel)
  implicit val MADLMRespEnc: RootJsonFormat[MADListModelsResponse] = jsonFormat4(MADListModelsResponse)
}

trait StorageCredentials {
  lazy val connectionString: String = sys.env.getOrElse("STORAGE_CONNECTION_STRING", Secrets.MADTestConnectionString)
  lazy val storageKey: String = sys.env.getOrElse("STORAGE_KEY", Secrets.MADTestStorageKey)
  lazy val storageSASToken: String = sys.env.getOrElse("STORAGE_SAS_TOKEN", Secrets.MADTestSASToken)
}

trait MADTestUtils extends TestBase with AnomalyKey with StorageCredentials {

  lazy val startTime: String = "2021-01-01T00:00:00Z"
  lazy val endTime: String = "2021-01-02T12:00:00Z"
  lazy val timestampColumn: String = "timestamp"
  lazy val inputColumns: Array[String] = Array("feature0", "feature1", "feature2")
  lazy val containerName: String = "madtest"
  lazy val intermediateSaveDir: String = "intermediateData"

  lazy val fileLocation: String = DatasetUtils.madTestFile("mad_example.csv").toString

  lazy val df: DataFrame = spark.read.format("csv").option("header", "true").load(fileLocation)
}


class FitMultivariateAnomalySuite extends EstimatorFuzzing[FitMultivariateAnomaly] with MADTestUtils {

  def simpleMultiAnomalyEstimator: FitMultivariateAnomaly = new FitMultivariateAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("result")
    .setStartTime(startTime)
    .setEndTime(endTime)
    .setContainerName(containerName)
    .setIntermediateSaveDir(intermediateSaveDir)
    .setTimestampCol(timestampColumn)
    .setInputCols(inputColumns)

  test("SimpleMultiAnomalyEstimator basic usage with connectionString") {

    val smae = simpleMultiAnomalyEstimator
      .setSlidingWindow(200)
      .setConnectionString(connectionString)
    val model = smae.fit(df)
    smae.cleanUpIntermediateData()
    val diagnosticsInfo = smae.getDiagnosticsInfo.get
    assert(diagnosticsInfo.variableStates.get.length.equals(3))

    val result = model
      .setStartTime(startTime)
      .setEndTime(endTime)
      .setOutputCol("result")
      .setTimestampCol(timestampColumn)
      .setInputCols(inputColumns)
      .transform(df)
      .collect()
    model.cleanUpIntermediateData()
    assert(result.length == df.collect().length)
  }

  test("SimpleMultiAnomalyEstimator basic usage with endpoint and sasToken") {

    val smae = simpleMultiAnomalyEstimator
      .setSlidingWindow(200)
      .setStorageName("anomalydetectiontest")
      .setStorageKey(storageKey)
      .setEndpoint("https://anomalydetectiontest.blob.core.windows.net/")
      .setSASToken(storageSASToken)
    val model = smae.fit(df)
    smae.cleanUpIntermediateData()
    val diagnosticsInfo = smae.getDiagnosticsInfo.get
    assert(diagnosticsInfo.variableStates.get.length.equals(3))

    val result = model
      .setStartTime(startTime)
      .setEndTime(endTime)
      .setOutputCol("result")
      .setTimestampCol(timestampColumn)
      .setInputCols(inputColumns)
      .transform(df)
      .collect()
    model.cleanUpIntermediateData()
    assert(result.length == df.collect().length)
  }

  test("Throw errors if alignMode is not set correctly") {
    val caught = intercept[IllegalArgumentException] {
      simpleMultiAnomalyEstimator.setSlidingWindow(200).setAlignMode("alignMode").fit(df)
    }
    assert(caught.getMessage.contains("alignMode must be either `inner` or `outer`."))
  }

  test("Throw errors if slidingWindow is not between 28 and 2880") {
    val caught = intercept[IllegalArgumentException] {
      simpleMultiAnomalyEstimator.setSlidingWindow(20).fit(df)
    }
    assert(caught.getMessage.contains("slidingWindow must be between 28 and 2880 (both inclusive)."))
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[Exception] {
      new FitMultivariateAnomaly()
        .setSubscriptionKey(anomalyKey)
        .setLocation("westus2")
        .setOutputCol("result")
        .fit(df)
    }
    assert(caught.getMessage.contains("You need to set either {connectionString, containerName} " +
      "or {storageName, storageKey, endpoint, sasToken, containerName} in order to access the blob container"))
  }

  test("Throw errors if start/end time is not ISO8601 format") {
    val caught = intercept[IllegalArgumentException] {
      val smae = simpleMultiAnomalyEstimator
        .setStartTime("2021-01-01 00:00:00")
        .setSlidingWindow(200)
        .setConnectionString(connectionString)
      smae.fit(df)
    }
    assert(caught.getMessage.contains("StartTime should be ISO8601 format."))

    val caught2 = intercept[IllegalArgumentException] {
      val smae = simpleMultiAnomalyEstimator
        .setEndTime("2021-01-01 00:00:00")
        .setSlidingWindow(200)
        .setConnectionString(connectionString)
      smae.fit(df)
    }
    assert(caught2.getMessage.contains("EndTime should be ISO8601 format."))
  }

  test("Expose correct error message during fitting") {
    val caught = intercept[RuntimeException] {
      val testDf = df.limit(50)
      val smae = simpleMultiAnomalyEstimator
        .setSlidingWindow(200)
        .setConnectionString(connectionString)
      smae.fit(testDf)
    }
    assert(caught.getMessage.contains("TrainFailed"))
  }

  test("Expose correct error message during inference") {
    val caught = intercept[RuntimeException] {
      val testDf = df.limit(50)
      val smae = simpleMultiAnomalyEstimator
        .setSlidingWindow(200)
        .setConnectionString(connectionString)
      val model = smae.fit(df)
      smae.cleanUpIntermediateData()
      val diagnosticsInfo = smae.getDiagnosticsInfo.get
      assert(diagnosticsInfo.variableStates.get.length.equals(3))

      model.setStartTime(startTime)
        .setEndTime(endTime)
        .setOutputCol("result")
        .setTimestampCol(timestampColumn)
        .setInputCols(inputColumns)
        .transform(testDf)
        .collect()
    }
    assert(caught.getMessage.contains("Not enough data."))
  }

  test("Expose correct error message for invalid modelId") {
    val caught = intercept[RuntimeException] {
      val detectMultivariateAnomaly = new DetectMultivariateAnomaly()
        .setModelId("FAKE_MODEL_ID")
        .setConnectionString(connectionString)
        .setSubscriptionKey(anomalyKey)
        .setLocation("westus2")
        .setContainerName(containerName)
        .setIntermediateSaveDir(intermediateSaveDir)
      detectMultivariateAnomaly
        .setStartTime(startTime)
        .setEndTime(endTime)
        .setOutputCol("result")
        .setTimestampCol(timestampColumn)
        .setInputCols(inputColumns)
        .transform(df)
        .collect()
    }
    assert(caught.getMessage.contains("Encounter error while fetching model"))
  }

  test("return modelId after retries and get model status before inference") {
    val caught = intercept[RuntimeException] {
      val smae = simpleMultiAnomalyEstimator
        .setMaxPollingRetries(1)
        .setSlidingWindow(200)
        .setConnectionString(connectionString)
      val model = smae.fit(df)
      smae.cleanUpIntermediateData()

      model.setStartTime(startTime)
        .setEndTime(endTime)
        .setOutputCol("result")
        .setTimestampCol(timestampColumn)
        .setInputCols(inputColumns)
        .transform(df)
        .collect()
      model.cleanUpIntermediateData()
    }
    assert(caught.getMessage.contains("not ready yet"))
  }

  ignore("Clean up all models"){
    var modelsLeft = true
    while (modelsLeft){

      val models = MADUtils.madListModels(anomalyKey)
        .parseJson.asJsObject().fields("models").asInstanceOf[JsArray].elements
        .map(modelJson => modelJson.asJsObject.fields("modelId").asInstanceOf[JsString].value)

      modelsLeft = models.nonEmpty

      models.foreach{modelId =>
        println(s"Deleting $modelId")
        MADUtils.madDelete(modelId, anomalyKey)
      }
    }

  }

  override def testSerialization(): Unit = {
    println("ignore the Serialization Fuzzing test because fitting process takes more than 3 minutes")
  }

  override def testExperiments(): Unit = {
    println("ignore the Experiment Fuzzing test because fitting process takes more than 3 minutes")
  }

  override def afterAll(): Unit = {
    MADUtils.cleanUpAllModels(anomalyKey)
    super.afterAll()
  }

  override def testObjects(): Seq[TestObject[FitMultivariateAnomaly]] =
    Seq(new TestObject(simpleMultiAnomalyEstimator.setSlidingWindow(200), df))

  override def reader: MLReadable[_] = FitMultivariateAnomaly

  override def modelReader: MLReadable[_] = DetectMultivariateAnomaly
}

