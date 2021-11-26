// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpDelete, HttpEntityEnclosingRequestBase, HttpGet, HttpRequestBase}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import spray.json._

import java.net.URI

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

import com.microsoft.azure.synapse.ml.cognitive.split1.MADListModelsProtocol._

object MADUtils extends AnomalyKey {

  import com.microsoft.azure.synapse.ml.cognitive.RESTHelpers._

  def madSend(request: HttpRequestBase, path: String,
              params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", anomalyKey)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(s"Failed: response: $response " + s"requestUrl: ${request.getURI}" +
            s"requestBody: $bodyOpt")
        }
        if (response.getStatusLine.getReasonPhrase == "No Content") {
          ""
        }
        else if (response.getStatusLine.getReasonPhrase == "Created") {
          response.getHeaders("Location").head.getValue
        }
        else {
          IOUtils.toString(response.getEntity.getContent, "UTF-8")
        }
      }.get
    })
  }

  def madDelete(path: String, params: Map[String, String] = Map()): String = {
    madSend(new HttpDelete(), "https://westus2.api.cognitive.microsoft.com/anomalydetector/" +
      "v1.1-preview/multivariate/models/" + path, params)
  }

  def madListModels(params: Map[String, String] = Map()): String = {
    madSend(new HttpGet(), "https://westus2.api.cognitive.microsoft.com/anomalydetector/" +
      "v1.1-preview/multivariate/models", params)
  }
}

trait MADUtils extends TestBase with AnomalyKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/sample_data_5_3000.zip"
  ).toDF("source")

  lazy val startTime: String = "2021-01-01T00:00:00Z"

  lazy val endTime: String = "2021-01-02T12:00:00Z"
}

class MultivariateAnomalyModelSuite extends EstimatorFuzzing[MultivariateAnomalyModel] with MADUtils {

  import MADUtils._

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  def mad: MultivariateAnomalyModel = new MultivariateAnomalyModel()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("result")
    .setSourceCol("source")
    .setStartTime(startTime)
    .setEndTime(endTime)
    .setConcurrency(5)

  test("Basic Usage") {
    val mam = mad.setSlidingWindow(200)
    val model = mam.fit(df)
    val diagnosticsInfo = mam.getDiagnosticsInfo
    assert(diagnosticsInfo.variableStates.get.length.equals(5))

    val result = model
      .setSourceCol("source").setStartTime(startTime).setEndTime(endTime)
      .setOutputCol("result")
      .transform(df)
      .withColumn("status", col("result.summary.status"))
      .withColumn("variableStates", col("result.summary.variableStates"))
      .select("status", "variableStates")
      .collect()

    assert(result.head.getString(0).equals("READY"))
    assert(result.head.getSeq(1).length.equals(5))

    madDelete(model.getModelId)
  }

  test("Throw errors if slidingWindow is not between 28 and 2880") {
    val caught = intercept[IllegalArgumentException] {
      mad.setSlidingWindow(20).fit(df)
    }
    assert(caught.getMessage.contains("parameter slidingWindow given invalid value"))
  }

  test("Throw errors if required fields not set") {
    val caught = intercept[AssertionError] {
      new MultivariateAnomalyModel()
        .setSubscriptionKey(anomalyKey)
        .setLocation("westus2")
        .setOutputCol("result")
        .fit(df)
    }
    assert(caught.getMessage.contains("Missing required params"))
    assert(caught.getMessage.contains("slidingWindow"))
    assert(caught.getMessage.contains("source"))
    assert(caught.getMessage.contains("startTime"))
    assert(caught.getMessage.contains("endTime"))
  }

  override def afterAll(): Unit = {
    val models = madListModels().parseJson.convertTo[MADListModelsResponse].models.map(_.modelId)
    for (modelId <- models) {
      madDelete(modelId)
    }
    super.afterAll()
  }

  override def testObjects(): Seq[TestObject[MultivariateAnomalyModel]] =
    Seq(new TestObject(mad.setSlidingWindow(200), df))

  override def reader: MLReadable[_] = MultivariateAnomalyModel

  override def modelReader: MLReadable[_] = DetectMultivariateAnomaly
}

