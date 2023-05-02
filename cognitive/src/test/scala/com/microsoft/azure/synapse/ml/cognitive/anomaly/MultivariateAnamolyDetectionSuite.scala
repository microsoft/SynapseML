// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.anomaly

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.benchmarks.DatasetUtils
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import spray.json.{DefaultJsonProtocol, _}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable


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

  lazy val storageKey: String = sys.env.getOrElse("STORAGE_KEY", Secrets.MADTestStorageKey)
  lazy val storageAccount = "anomalydetectiontest"
  lazy val containerName = "madtest"

}

trait MADTestUtils extends TestBase with AnomalyKey with StorageCredentials {

  lazy val startTime: String = "2021-01-01T00:00:00Z"
  lazy val endTime: String = "2021-01-02T12:00:00Z"
  lazy val timestampColumn: String = "timestamp"
  lazy val inputColumns: Array[String] = Array("feature0", "feature1", "feature2")
  lazy val intermediateSaveDir: String =
    s"wasbs://$containerName@$storageAccount.blob.core.windows.net/intermediateData"
  lazy val fileLocation: String = DatasetUtils.madTestFile("mad_example.csv").toString
  lazy val fileSchema: StructType = StructType(Array(
    StructField(timestampColumn, StringType, nullable = true)
  ) ++ inputColumns.map(inputCol => StructField(inputCol, DoubleType, nullable = true)))
  lazy val df: DataFrame = spark.read.format("csv")
    .option("header", "true").schema(fileSchema).load(fileLocation)

}

class SimpleFitMultivariateAnomalySuite extends EstimatorFuzzing[SimpleFitMultivariateAnomaly] with MADTestUtils {

  def simpleMultiAnomalyEstimator: SimpleFitMultivariateAnomaly = new SimpleFitMultivariateAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation(anomalyLocation)
    .setOutputCol("result")
    .setStartTime(startTime)
    .setEndTime(endTime)
    .setIntermediateSaveDir(intermediateSaveDir)
    .setTimestampCol(timestampColumn)
    .setInputCols(inputColumns)

  test("SimpleFitMultivariateAnomaly basic usage") {
    val smae = simpleMultiAnomalyEstimator.setSlidingWindow(50)
    val model = smae.fit(df)
    smae.cleanUpIntermediateData()

    // model might not be ready
    tryWithRetries(Array(100, 500, 1000)) { () =>
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
  }

  test("Throw errors if alignMode is not set correctly") {
    val caught = intercept[IllegalArgumentException] {
      simpleMultiAnomalyEstimator.setAlignMode("alignMode").fit(df)
    }
    assert(caught.getMessage.contains("alignMode must be either `inner` or `outer`."))
  }

  test("Throw errors if slidingWindow is not between 28 and 2880") {
    val caught = intercept[IllegalArgumentException] {
      simpleMultiAnomalyEstimator.setSlidingWindow(20).fit(df)
    }
    assert(caught.getMessage.contains("slidingWindow must be between 28 and 2880 (both inclusive)."))
  }

  test("Throw errors if authentication is not provided") {
    val caught = intercept[IllegalAccessError] {
      new SimpleFitMultivariateAnomaly()
        .setSubscriptionKey(anomalyKey)
        .setLocation(anomalyLocation)
        .setIntermediateSaveDir(s"wasbs://$containerName@notreal.blob.core.windows.net/intermediateData")
        .setOutputCol("result")
        .setInputCols(Array("feature0"))
        .fit(df)
    }
    assert(caught.getMessage.contains("Could not find the storage account credentials."))
  }

  test("Throw errors if start/end time is not ISO8601 format") {
    val caught = intercept[IllegalArgumentException] {
      val smae = simpleMultiAnomalyEstimator
        .setStartTime("2021-01-01 00:00:00")
      smae.fit(df)
    }
    assert(caught.getMessage.contains("StartTime should be ISO8601 format."))

    val caught2 = intercept[IllegalArgumentException] {
      val smae = simpleMultiAnomalyEstimator
        .setEndTime("2021-01-01 00:00:00")
      smae.fit(df)
    }
    assert(caught2.getMessage.contains("EndTime should be ISO8601 format."))
  }

  test("Expose correct error message during fitting") {
    val caught = intercept[RuntimeException] {
      val testDf = df.limit(50)
      simpleMultiAnomalyEstimator
        .fit(testDf)
    }
    assert(caught.getMessage.contains("TrainFailed"))
  }

  test("Expose correct error message during inference") {
    val caught = intercept[RuntimeException] {
      val testDf = df.limit(50)
      val smae = simpleMultiAnomalyEstimator
      val model = smae.fit(df)
      smae.cleanUpIntermediateData()
      assert(model.getDiagnosticsInfo.variableStates.get.length.equals(3))

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
      val detectMultivariateAnomaly = new SimpleDetectMultivariateAnomaly()
        .setModelId("FAKE_MODEL_ID")
        .setSubscriptionKey(anomalyKey)
        .setLocation(anomalyLocation)
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

  override def testSerialization(): Unit = {
    println("ignore the Serialization Fuzzing test because fitting process takes more than 3 minutes")
  }

  override def testExperiments(): Unit = {
    println("ignore the Experiment Fuzzing test because fitting process takes more than 3 minutes")
  }

  override def afterAll(): Unit = {
    MADUtils.cleanUpAllModels(anomalyKey, anomalyLocation)
    super.afterAll()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val hc = spark.sparkContext.hadoopConfiguration
    hc.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hc.set(s"fs.azure.account.keyprovider.$storageAccount.blob.core.windows.net",
      "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    hc.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", storageKey)
    cleanOldModels()
  }

  override def testObjects(): Seq[TestObject[SimpleFitMultivariateAnomaly]] =
    Seq(new TestObject(simpleMultiAnomalyEstimator.setSlidingWindow(200), df))

  def stringToTime(dateString: String): ZonedDateTime = {
    val tsFormat = "yyyy-MM-dd'T'HH:mm:ssz"
    val formatter = DateTimeFormatter.ofPattern(tsFormat)
    ZonedDateTime.parse(dateString, formatter)
  }

  def cleanOldModels(): Unit = {
    val url = simpleMultiAnomalyEstimator.setLocation(anomalyLocation).getUrl + "/"
    val twoDaysAgo = ZonedDateTime.now().minusDays(2)
    val modelSet: mutable.HashSet[String] = mutable.HashSet()
    var modelDeleted: Boolean = false

    // madListModels doesn't necessarily return all models, so just in case,
    // if we delete any models, we loop around to see if there are more to check.
    // scalastyle:off while
    do {
      modelDeleted = false
      val models = MADUtils.madListModels(anomalyKey, anomalyLocation)
        .parseJson.asJsObject().fields("models").asInstanceOf[JsArray].elements
        .map(modelJson => modelJson.asJsObject.fields("modelId").asInstanceOf[JsString].value)
      models.foreach { modelId =>
        if (!modelSet.contains(modelId)) {
          modelSet += modelId
          val lastUpdated =
            MADUtils.madGetModel(url, modelId, anomalyKey).parseJson.asJsObject.fields("lastUpdatedTime")
          val lastUpdatedTime = stringToTime(lastUpdated.toString().replaceAll("\"", ""))
          if (lastUpdatedTime.isBefore(twoDaysAgo)) {
            println(s"Deleting $modelId")
            MADUtils.madDelete(modelId, anomalyKey, anomalyLocation)
            modelDeleted = true
          }
        }
      }
    } while (modelDeleted)
    // scalastyle:on while
  }

  override def reader: MLReadable[_] = SimpleFitMultivariateAnomaly

  override def modelReader: MLReadable[_] = SimpleDetectMultivariateAnomaly
}

class DetectLastMultivariateAnomalySuite extends TransformerFuzzing[DetectLastMultivariateAnomaly]
  with MADTestUtils {

  lazy val sfma: SimpleFitMultivariateAnomaly = {
    val hc: Configuration = spark.sparkContext.hadoopConfiguration
    hc.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hc.set(s"fs.azure.account.keyprovider.$storageAccount.blob.core.windows.net",
      "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    hc.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", storageKey)

    new SimpleFitMultivariateAnomaly()
      .setSubscriptionKey(anomalyKey)
      .setLocation(anomalyLocation)
      .setOutputCol("result")
      .setStartTime(startTime)
      .setEndTime(endTime)
      .setIntermediateSaveDir(intermediateSaveDir)
      .setTimestampCol(timestampColumn)
      .setInputCols(inputColumns)
      .setSlidingWindow(50)
  }

  lazy val modelId: String = {
    val model: SimpleDetectMultivariateAnomaly = sfma.fit(df)
    MADUtils.CreatedModels += model.getModelId
    model.getModelId
  }

  lazy val dlma: DetectLastMultivariateAnomaly = new DetectLastMultivariateAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation(anomalyLocation)
    .setModelId(modelId)
    .setInputVariablesCols(inputColumns)
    .setOutputCol("result")
    .setTimestampCol(timestampColumn)

  test("Basic Usage") {
    val result = dlma.setBatchSize(50)
      .transform(df.limit(100))
      .collect()
    assert(result(0).get(6) == null)
    assert(!result(50).getAs[Boolean]("isAnomaly"))
    assert(result(68).getAs[Boolean]("isAnomaly"))
  }

  test("Error if batch size is smaller than sliding window") {
    val result = dlma.setBatchSize(10).transform(df.limit(50))
    result.show(50, truncate = false)
    assert(result.collect().head.getAs[StringType](dlma.getErrorCol).toString.contains("NotEnoughData"))
  }

  override def afterAll(): Unit = {
    MADUtils.cleanUpAllModels(anomalyKey, anomalyLocation)
    sfma.cleanUpIntermediateData()
    super.afterAll()
  }

  override def testSerialization(): Unit = {
    println("ignore the Serialization Fuzzing test because fitting process takes more than 3 minutes")
  }

  override def testObjects(): Seq[TestObject[DetectLastMultivariateAnomaly]] =
    Seq(new TestObject(dlma, df))

  override def reader: MLReadable[_] = DetectLastMultivariateAnomaly
}
