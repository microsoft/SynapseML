// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.Secrets.getAccessToken
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIAPIKey, OpenAIPrompt}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import spray.json.DefaultJsonProtocol.IntJsonFormat

trait TestGlobalParamsTrait extends HasGlobalParams {
  val testParam: Param[Double] = new Param[Double](
    this, "TestParam", "Test Param")

  GlobalParams.registerParam(testParam, TestParamKey)

  def getTestParam: Double = getGlobalParam(testParam)

  def setTestParam(v: Double): this.type = set(testParam, v)

  val testServiceParam = new ServiceParam[Int](
    this, "testServiceParam", "Test Service Param", isRequired = false)

  GlobalParams.registerServiceParam(testServiceParam, TestServiceParamKey)

  def getTestServiceParam: Int = getGlobalServiceParamScalar(testServiceParam)

  def setTestServiceParam(v: Int): this.type = setScalarParam(testServiceParam, v)

  def getTestServiceParamCol: String = getVectorParam(testServiceParam)

  def setTestServiceParamCol(v: String): this.type = setVectorParam(testServiceParam, v)
}

class TestGlobalParams extends TestGlobalParamsTrait {

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override val uid: String = Identifiable.randomUID("TestGlobalParams")
}

class GlobalParamSuite extends Flaky with OpenAIAPIKey {

  import spark.implicits._

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }

  GlobalParams.setGlobalParam(TestParamKey, 12.5)
  GlobalParams.setGlobalServiceParam(TestServiceParamKey, 1)

  val testGlobalParams = new TestGlobalParams()

  test("Basic Usage") {
    assert(testGlobalParams.getTestParam == 12.5)
    assert(testGlobalParams.getTestServiceParam == 1)
  }

  test("Test Changing Value") {
    assert(testGlobalParams.getTestParam == 12.5)
    GlobalParams.setGlobalParam("TestParam", 19.853)
    assert(testGlobalParams.getTestParam == 19.853)
    assert(testGlobalParams.getTestServiceParam == 1)
    GlobalParams.setGlobalServiceParam("TestServiceParam", 4)
    assert(testGlobalParams.getTestServiceParam == 4)
  }

  test("Using wrong setters and getters") {
    assertThrows[AssertionError] {
      GlobalParams.setGlobalServiceParam("TestParam", 8.8888)
    }
    assertThrows[AssertionError] {
      GlobalParams.getParam(testGlobalParams.testServiceParam)
    }
  }

  GlobalParams.setGlobalServiceParam("OpenAIDeploymentName", deploymentName)

  lazy val prompt: OpenAIPrompt = new OpenAIPrompt()
    .setSubscriptionKey(openAIAPIKey)
    .setCustomServiceName(openAIServiceName)
    .setOutputCol("outParsed")
    .setTemperature(0)

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes"),
    (null, "none") //scalastyle:ignore null
  ).toDF("text", "category")

  test("OpenAIPrompt w Globals") {
    val nonNullCount = prompt
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)
  }
}

