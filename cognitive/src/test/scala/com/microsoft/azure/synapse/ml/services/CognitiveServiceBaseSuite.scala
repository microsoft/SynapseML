// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._

private class ServiceParamHarness(override val uid: String = "serviceParamHarness")
  extends Params with HasServiceParams {

  val requiredText: ServiceParam[String] =
    new ServiceParam[String](this, "requiredText", "required text", isRequired = true)

  val optionalText: ServiceParam[String] =
    new ServiceParam[String](this, "optionalText", "optional text")

  val batchedText: ServiceParam[Seq[String]] =
    new ServiceParam[Seq[String]](this, "batchedText", "batched text")

  val urlVersion: ServiceParam[String] =
    new ServiceParam[String](this, "urlVersion", "url version", isURLParam = true)

  def vectorParamMap: Map[String, String] = getVectorParamMap

  def requiredParamNames: Set[String] = getRequiredParams.map(_.name).toSet

  def urlParamNames: Set[String] = getUrlParams.map(_.name).toSet

  def shouldSkipRow(row: Row): Boolean = shouldSkip(row)

  def valueMap(row: Row, excludes: Set[ServiceParam[_]] = Set()): Map[String, Any] = getValueMap(row, excludes)

  def valueAnyOpt(row: Row, p: ServiceParam[_]): Option[Any] = getValueAnyOpt(row, p)

  def valueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = getValueOpt(row, p)

  override def copy(extra: ParamMap): Params = this
}

private class LocationHarness(override val uid: String = "locationHarness")
  extends Params with HasSetLocation {

  override def urlPath: String = "/v1/resource"

  override def copy(extra: ParamMap): Params = this
}

private class CustomDomainHarness(override val uid: String = "customDomainHarness")
  extends Params with HasCustomCogServiceDomain {

  override def urlPath: String = "/deployments/chat/completions"

  override private[ml] def internalServiceType: String = "openai"

  override def copy(extra: ParamMap): Params = this
}

private class LinkedServiceHarness(override val uid: String = "linkedServiceHarness")
  extends Params with HasSetLinkedService {

  override def urlPath: String = "/analyze"

  override def copy(extra: ParamMap): Params = this
}

private class LinkedServiceLocationHarness(override val uid: String = "linkedServiceLocationHarness")
  extends Params with HasSetLinkedServiceUsingLocation {

  override def urlPath: String = "/analyze"

  override def copy(extra: ParamMap): Params = this
}

private class CognitiveInputHarness(override val uid: String = "cognitiveInputHarness")
  extends Params with HasCognitiveServiceInput {

  val apiVersion: ServiceParam[String] =
    new ServiceParam[String](this, "apiVersion", "api version", isURLParam = true)

  val requiredText: ServiceParam[String] =
    new ServiceParam[String](this, "requiredText", "required text", isRequired = true)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = _ => None

  def buildUrl(row: Row): String = prepareUrl(row)

  def headers(row: Row, addContentType: Boolean = true): Map[String, String] = getHeaders(row, addContentType)

  def shouldSkipRow(row: Row): Boolean = shouldSkip(row)

  override def copy(extra: ParamMap): Params = this
}

class CognitiveServiceBaseSuite extends TestBase {

  import spark.implicits._

  test("setLocation maps cloud domains deterministically") {
    val service = new LocationHarness()
      .setLocation("eastus")
    assert(service.getUrl == "https://eastus.api.cognitive.microsoft.com/v1/resource")

    service.setLocation("usgovarizona")
    assert(service.getUrl == "https://usgovarizona.api.cognitive.microsoft.us/v1/resource")

    service.setLocation("chinanorth")
    assert(service.getUrl == "https://chinanorth.api.cognitive.microsoft.cn/v1/resource")
  }

  test("custom domain helpers build deterministic urls") {
    val service = new CustomDomainHarness()
      .setCustomServiceName("contoso")
    assert(service.getUrl == "https://contoso.cognitiveservices.azure.com/deployments/chat/completions")

    service.setEndpoint("https://custom.endpoint/")
    assert(service.getUrl == "https://custom.endpoint/deployments/chat/completions")

    val internal = new CustomDomainHarness()
      .setDefaultInternalEndpoint("https://fabric")
    assert(internal.getOrDefault(internal.url) == "https://fabric/cognitive/openai/deployments/chat/completions")
  }

  test("linked service setter resolves endpoint and key locally") {
    val service = new LinkedServiceHarness()
      .setLinkedService("demo")

    assert(service.getUrl == "https://demo.endpoint/analyze")
    assert(service.getSubscriptionKey == "key-demo")
  }

  test("linked service location setter resolves domain and key locally") {
    val service = new LinkedServiceLocationHarness()
      .setLinkedService("gov")

    assert(service.getUrl == "https://usgovvirginia.api.cognitive.microsoft.us/analyze")
    assert(service.getSubscriptionKey == "key-gov")
  }

  test("service param helper methods are deterministic") {
    val harness = new ServiceParamHarness()
    harness.setVectorParam(harness.requiredText, "requiredCol")
    harness.setVectorParam("urlVersion", "versionCol")
    harness.setScalarParam("optionalText", "fallback")

    val row = Seq(("hello", "2024-10-01")).toDF("requiredCol", "versionCol").head()
    assert(harness.vectorParamMap == Map("requiredText" -> "requiredCol", "urlVersion" -> "versionCol"))
    assert(harness.requiredParamNames == Set("requiredText"))
    assert(harness.urlParamNames == Set("urlVersion"))
    assert(!harness.shouldSkipRow(row))
    assert(harness.valueAnyOpt(row, harness.urlVersion).contains("2024-10-01"))
    assert(
      harness.valueMap(row, Set(harness.urlVersion)) ==
        Map("requiredText" -> "hello", "optionalText" -> "fallback")
    )

    val missingRequired = Seq((Option.empty[String], "2024-10-01")).toDF("requiredCol", "versionCol").head()
    assert(harness.shouldSkipRow(missingRequired))
  }

  test("cognitive input helper methods build urls and headers locally") {
    val input = new CognitiveInputHarness()
    input.setUrl("https://example.test/root")
    input.setVectorParam(input.requiredText, "textCol")
    input.setVectorParam(input.apiVersion, "versionCol")

    val row = Seq(("hello", "2024-10-01")).toDF("textCol", "versionCol").head()
    assert(input.buildUrl(row) == "https://example.test/root?apiVersion=2024-10-01")
    assert(!input.shouldSkipRow(row))

    input.setCustomUrlRoot("https://override.test/root")
    assert(input.buildUrl(row) == "https://override.test/root")

    val subscriptionHeaders = new CognitiveInputHarness().setSubscriptionKey("sub-key").headers(Row.empty)
    assert(subscriptionHeaders("Ocp-Apim-Subscription-Key") == "sub-key")
    assert(subscriptionHeaders("Content-Type") == "application/json")
    assert(!subscriptionHeaders.contains("Authorization"))

    val aadHeaders = new CognitiveInputHarness().setAADToken("aad-token").headers(Row.empty)
    assert(aadHeaders("Authorization") == "Bearer aad-token")

    val customHeaders = new CognitiveInputHarness()
      .setCustomAuthHeader("Shared custom-auth")
      .setCustomHeaders(Map("X-Test" -> "1"))
      .headers(Row.empty)
    assert(customHeaders("Authorization") == "Shared custom-auth")
    assert(customHeaders("X-Test") == "1")
    assert(customHeaders.contains("x-ai-telemetry-properties"))
  }

  test("cognitive input helper methods resolve automatically batched string headers") {
    val keyInput = new CognitiveInputHarness()
    keyInput.setSubscriptionKeyCol("keys")
    val keyRow = Seq(Seq(
      Option.empty[String], Some(""), Some("sub-key"), Some("other-key")
    )).toDF("keys").head()
    assert(keyInput.headers(keyRow)("Ocp-Apim-Subscription-Key") == "sub-key")

    val aadInput = new CognitiveInputHarness()
    aadInput.setAADTokenCol("tokens")
    val aadRow = Seq(Seq(
      Option.empty[String], Some(""), Some("aad-token"), Some("other-token")
    )).toDF("tokens").head()
    assert(aadInput.headers(aadRow)("Authorization") == "Bearer aad-token")

    val customAuthInput = new CognitiveInputHarness()
    customAuthInput.setCustomAuthHeaderCol("authHeaders")
    val customAuthRow = Seq(Seq(
      Option.empty[String], Some(""), Some("Shared custom-auth"), Some("Shared other-auth")
    ))
      .toDF("authHeaders")
      .head()
    assert(customAuthInput.headers(customAuthRow)("Authorization") == "Shared custom-auth")
  }

  test("subscription key columns work through public batching and flattening") {
    val input = Seq(
      ("first", "first-key"),
      ("second", "second-key")
    ).toDF("text", "key")
    val batched = new FixedMiniBatchTransformer().setBatchSize(10).transform(input)
    val inputBuilder = new CognitiveInputHarness().setSubscriptionKeyCol("key")

    assert(inputBuilder.headers(batched.head())("Ocp-Apim-Subscription-Key") == "first-key")

    val restored = new FlattenBatch().transform(batched)
      .select("text", "key")
      .as[(String, String)]
      .collect()
      .toSeq
    assert(restored == Seq("first" -> "first-key", "second" -> "second-key"))
  }

  test("cognitive input helper methods resolve automatically batched map headers") {
    val input = new CognitiveInputHarness()
    input.setVectorParam(input.customHeaders, "customHeadersCol")
    input.setVectorParam(input.telemHeaders, "telemHeadersCol")

    val row = Seq((
      Seq(Map.empty[String, String], Map("X-Test" -> "1")),
      Seq(Map.empty[String, String], Map("X-Telemetry" -> "2"))
    )).toDF("customHeadersCol", "telemHeadersCol").head()

    val headers = input.headers(row)
    assert(headers("X-Test") == "1")
    assert(headers("X-Telemetry") == "2")
  }

  test("empty automatically batched credentials do not fail header resolution") {
    val input = new CognitiveInputHarness()
    input.setSubscriptionKeyCol("keys")
    input.setAADTokenCol("tokens")
    input.setCustomAuthHeaderCol("authHeaders")

    val row = Seq((
      Seq(Option.empty[String], Some("")),
      Seq(Option.empty[String], Some("")),
      Seq(Option.empty[String], Some(""))
    )).toDF("keys", "tokens", "authHeaders").head()

    val headers = input.headers(row)
    assert(!headers.contains("Ocp-Apim-Subscription-Key"))
    assert(!headers.contains("Authorization"))
  }

  test("invalid automatically batched credential element types fail clearly") {
    val input = new CognitiveInputHarness().setSubscriptionKeyCol("keys")
    val row = Seq(Seq(1, 2)).toDF("keys").head()

    val error = intercept[IllegalArgumentException](input.headers(row))
    assert(error.getMessage.contains("subscriptionKey"))
    assert(error.getMessage.contains("String or array<string>"))

    val mapInput = new CognitiveInputHarness()
    mapInput.setVectorParam(mapInput.customHeaders, "customHeadersCol")
    val invalidMapRow = Seq(Seq("not-a-map")).toDF("customHeadersCol").head()

    val mapError = intercept[IllegalArgumentException](mapInput.headers(invalidMapRow))
    assert(mapError.getMessage.contains("customHeaders"))
    assert(mapError.getMessage.contains("map<string,string> or array<map<string,string>>"))
  }

  test("batch-aware header resolution does not change payload service parameters") {
    val harness = new ServiceParamHarness()
    harness.setVectorParam(harness.batchedText, "textCol")
    val values = Seq("first", "second")
    val row = Seq(values).toDF("textCol").head()

    assert(harness.valueOpt(row, harness.batchedText).contains(values))
  }
}
