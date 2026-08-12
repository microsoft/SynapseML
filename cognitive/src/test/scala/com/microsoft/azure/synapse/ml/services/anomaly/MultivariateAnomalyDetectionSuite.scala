// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.anomaly

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.hadoop.fs.RemoteIterator
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import spray.json._

class MultivariateAnomalyDetectionSuite extends TestBase {

  import MADJsonProtocol._

  private class ExposedSimpleFitMultivariateAnomaly(uid: String)
    extends SimpleFitMultivariateAnomaly(uid) {
    def requestBody(dataSource: String): String = entityToBody(prepareEntity(dataSource).get)
  }

  private class ExposedSimpleDetectMultivariateAnomaly(uid: String)
    extends SimpleDetectMultivariateAnomaly(uid) {
    def requestBody(dataSource: String): String = entityToBody(prepareEntity(dataSource).get)
  }

  private class ExposedDetectLastMultivariateAnomaly(uid: String)
    extends DetectLastMultivariateAnomaly(uid) {
    def requestBody(row: Row): String = entityToBody(prepareEntity(row).get)
    def requestUrl(row: Row): String = prepareUrl(row)
  }

  private class TestRemoteIterator(values: Seq[Int]) extends RemoteIterator[Int] {
    private val underlying = values.iterator
    override def hasNext: Boolean = underlying.hasNext
    override def next(): Int = underlying.next()
  }

  private def entityToBody(entity: AbstractHttpEntity): String = EntityUtils.toString(entity)

  test("MADUtils.madUrl creates expected endpoint") {
    assert(MADUtils.madUrl("westus2") ==
      "https://westus2.api.cognitive.microsoft.com/anomalydetector/v1.1/multivariate/")
  }

  test("start and end times accept valid ISO instant format") {
    val fit = new SimpleFitMultivariateAnomaly("fit-time-test")
      .setStartTime("2024-01-01T00:00:00Z")
      .setEndTime("2024-01-02T00:00:00Z")

    assert(fit.getStartTime == "2024-01-01T00:00:00Z")
    assert(fit.getEndTime == "2024-01-02T00:00:00Z")
  }

  test("invalid start time throws deterministic validation error") {
    val exception = intercept[IllegalArgumentException] {
      new SimpleFitMultivariateAnomaly("fit-invalid-time").setStartTime("2024/01/01")
    }
    assert(exception.getMessage.contains("StartTime should be ISO8601 format"))
  }

  test("intermediateSaveDir validates accepted and rejected schemes") {
    val fit = new SimpleFitMultivariateAnomaly("fit-save-dir")
      .setIntermediateSaveDir("wasbs://container@account.blob.core.windows.net/datasets")
    assert(fit.getIntermediateSaveDir.startsWith("wasbs://"))

    val exception = intercept[IllegalArgumentException] {
      fit.setIntermediateSaveDir("file:///tmp")
    }
    assert(exception.getMessage.contains("improper HDFS loacation"))
  }

  test("fit parameter setters normalize and validate values") {
    val fit = new SimpleFitMultivariateAnomaly("fit-params")
      .setSlidingWindow(28)
      .setAlignMode("inner")
      .setFillNAMethod("fixed")

    assert(fit.getSlidingWindow == 28)
    assert(fit.getAlignMode == "Inner")
    assert(fit.getFillNAMethod == "Fixed")

    intercept[IllegalArgumentException] {
      fit.setSlidingWindow(27)
    }
    intercept[IllegalArgumentException] {
      fit.setAlignMode("bad-mode")
    }
    intercept[IllegalArgumentException] {
      fit.setFillNAMethod("bad-method")
    }
  }

  test("SimpleFitMultivariateAnomaly prepareEntity uses deterministic defaults") {
    val fit = new ExposedSimpleFitMultivariateAnomaly("fit-default-entity")
      .setStartTime("2024-01-01T00:00:00Z")
      .setEndTime("2024-01-02T00:00:00Z")
    val request = fit.requestBody("https://storage/path.csv").parseJson.convertTo[MAERequest]

    assert(request.dataSource == "https://storage/path.csv")
    assert(request.dataSchema == "OneTable")
    assert(request.slidingWindow.contains(300))
    assert(request.alignPolicy.exists(_.alignMode.contains("Outer")))
    assert(request.alignPolicy.exists(_.fillNAMethod.contains("Linear")))
    assert(request.alignPolicy.exists(_.paddingValue.isEmpty))
    assert(request.displayName.isEmpty)
  }

  test("SimpleFitMultivariateAnomaly prepareEntity reflects configured values") {
    val fit = new ExposedSimpleFitMultivariateAnomaly("fit-custom-entity")
      .setStartTime("2024-01-01T00:00:00Z")
      .setEndTime("2024-01-02T00:00:00Z")
      .setSlidingWindow(120)
      .setAlignMode("inner")
      .setFillNAMethod("fixed")
      .setPaddingValue(9)
      .setDisplayName("demo-model")

    val request = fit.requestBody("https://storage/path.csv").parseJson.convertTo[MAERequest]
    assert(request.slidingWindow.contains(120))
    assert(request.alignPolicy.contains(AlignPolicy(Some("Inner"), Some("Fixed"), Some(9))))
    assert(request.displayName.contains("demo-model"))
  }

  test("SimpleDetectMultivariateAnomaly prepareEntity respects topContributorCount") {
    val detect = new ExposedSimpleDetectMultivariateAnomaly("detect-entity")
      .setStartTime("2024-01-01T00:00:00Z")
      .setEndTime("2024-01-02T00:00:00Z")
      .setTopContributorCount(7)

    val request = detect.requestBody("https://storage/path.csv").parseJson.convertTo[DMARequest]
    assert(request.dataSource == "https://storage/path.csv")
    assert(request.topContributorCount.contains(7))
  }

  test("DetectLastMultivariateAnomaly prepareEntity builds expected variables payload") {
    import spark.implicits._
    val row = Seq(
      (Seq("2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"), Seq(1.0, 2.0), Seq(5.0, 6.0))
    ).toDF("timestamp_list", "varA_list", "varB_list").head()

    val detectLast = new ExposedDetectLastMultivariateAnomaly("detect-last-entity")
      .setInputVariablesCols(Array("varA", "varB"))
      .setTopContributorCount(3)

    val request = detectLast.requestBody(row).parseJson.convertTo[DLMARequest]
    assert(request.topContributorCount == 3)
    assert(request.variables.map(_.variable) == Seq("varA", "varB"))
    assert(request.variables.forall(_.timestamps == Seq("2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z")))
    assert(request.variables.find(_.variable == "varA").exists(_.values == Seq(1.0, 2.0)))
  }

  test("DetectLastMultivariateAnomaly prepareUrl appends model and detect-last path") {
    val detectLast = new ExposedDetectLastMultivariateAnomaly("detect-last-url")
      .setLocation("westus2")
      .setModelId("model-123")

    assert(detectLast.requestUrl(Row.empty) ==
      "https://westus2.api.cognitive.microsoft.com/anomalydetector/v1.1/multivariate/models/model-123:detect-last")
  }

  test("transformSchema appends deterministic output, error and isAnomaly fields") {
    val inputSchema = StructType(Seq(StructField("timestamp", StringType)))

    val fitSchema = new SimpleFitMultivariateAnomaly("fit-schema")
      .setOutputCol("fitOutput")
      .setErrorCol("fitError")
      .transformSchema(inputSchema)
    assert(fitSchema("fitOutput").dataType == DMAResponse.schema)
    assert(fitSchema("fitError").dataType == DMAError.schema)
    assert(fitSchema("isAnomaly").dataType == BooleanType)

    val detectLastSchema = new DetectLastMultivariateAnomaly("detect-last-schema")
      .setOutputCol("detectOutput")
      .setErrorCol("detectError")
      .transformSchema(inputSchema)
    assert(detectLastSchema("detectOutput").dataType == DLMAResponse.schema)
    assert(detectLastSchema("detectError").dataType == DMAError.schema)
    assert(detectLastSchema("isAnomaly").dataType == BooleanType)
  }

  test("remote iterator conversion wraps Hadoop iterator deterministically") {
    import Conversions._
    val iterator: Iterator[Int] = new TestRemoteIterator(Seq(1, 2, 3))
    assert(iterator.toSeq == Seq(1, 2, 3))
  }

}
