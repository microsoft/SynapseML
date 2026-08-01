// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

import java.io.{PrintWriter, StringWriter}

class AzureSearchAuthSuite extends AnyFunSuite {

  private val serviceName = "test-search-service"
  private val apiVersion = "test-api-version"
  private val indexName = "test-index"

  private def header(request: HttpRequestBase, name: String): Option[String] = {
    Option(request.getFirstHeader(name)).map(_.getValue)
  }

  private def requests(auth: AzureSearchAuth): Seq[HttpRequestBase] = Seq(
    AzureSearchRequests.listIndexes(auth, serviceName, apiVersion),
    AzureSearchRequests.getIndex(auth, serviceName, indexName, apiVersion),
    AzureSearchRequests.createIndex(auth, serviceName, "{}", apiVersion),
    AzureSearchRequests.getStatistics(auth, serviceName, indexName, apiVersion)
  )

  test("subscription key headers are used by every index API") {
    requests(AzureSearchAuth.fromSubscriptionKey("test-subscription-key")).foreach { request =>
      assert(header(request, "api-key").contains("test-subscription-key"))
      assert(header(request, "Authorization").isEmpty)
    }
  }

  test("AAD headers are used by every index API") {
    requests(AzureSearchAuth.fromAADToken("test-aad-token")).foreach { request =>
      assert(header(request, "Authorization").contains("Bearer test-aad-token"))
      assert(header(request, "api-key").isEmpty)
    }
  }

  test("shared auth precedence and custom headers match cognitive service requests") {
    val auth = AzureSearchAuth(
      subscriptionKey = Some("test-subscription-key"),
      aadToken = Some("test-aad-token"),
      customAuthHeader = Some("Custom test-auth"),
      customHeaders = Map("x-test-header" -> "test-value"))

    val headers = auth.headers(addContentType = true)
    assert(headers("api-key") == "test-subscription-key")
    assert(!headers.contains("Authorization"))
    assert(headers("x-test-header") == "test-value")
    assert(headers("Content-Type") == "application/json")
  }

  test("custom authorization and custom headers work without a key or AAD token") {
    val auth = AzureSearchAuth(
      customAuthHeader = Some("Custom test-auth"),
      customHeaders = Map("x-test-header" -> "test-value"))

    requests(auth).foreach { request =>
      assert(header(request, "Authorization").contains("Custom test-auth"))
      assert(header(request, "x-test-header").contains("test-value"))
    }
  }

  test("an Authorization custom header can be the sole credential") {
    val auth = AzureSearchAuth(customHeaders = Map(
      "Authorization" -> "Custom test-auth",
      "x-test-header" -> "test-value"))

    requests(auth).foreach { request =>
      assert(header(request, "Authorization").contains("Custom test-auth"))
      assert(header(request, "x-test-header").contains("test-value"))
    }
  }

  test("auth values are redacted from diagnostic strings") {
    val rendered = AzureSearchAuth(
      subscriptionKey = Some("test-subscription-key"),
      aadToken = Some("test-aad-token"),
      customAuthHeader = Some("Custom test-auth"),
      customHeaders = Map("x-test-header" -> "test-value")).toString

    assert(!rendered.contains("test-subscription-key"))
    assert(!rendered.contains("test-aad-token"))
    assert(!rendered.contains("Custom test-auth"))
    assert(!rendered.contains("test-value"))
    assert(rendered.contains("x-test-header"))
  }

  test("missing credentials fail before writer preparation or an index request") {
    val writerError = intercept[IllegalArgumentException] {
      AzureSearchAuth.fromOptions(Map.empty)
    }
    val requestError = intercept[IllegalArgumentException] {
      AzureSearchRequests.listIndexes(AzureSearchAuth(), serviceName, apiVersion)
    }

    assert(writerError.getMessage.contains("authentication"))
    assert(requestError.getMessage.contains("authentication"))
  }

  test("writer options configure key, AAD, custom authorization, and custom headers") {
    val keyWriter = AzureSearchWriter.configureAuthentication(
      new AddDocuments(),
      AzureSearchAuth.fromOptions(Map("subscriptionKey" -> "test-subscription-key")))
    assert(keyWriter.getSubscriptionKey == "test-subscription-key")

    val aadAuth = AzureSearchAuth.fromOptions(Map(
      "AADToken" -> "test-aad-token",
      "customHeaders" -> "{\"x-test-header\":\"test-value\"}"))
    val aadWriter = AzureSearchWriter.configureAuthentication(new AddDocuments(), aadAuth)
    assert(aadWriter.getAADToken == "test-aad-token")
    assert(aadWriter.getOrDefault(aadWriter.customHeaders).left.get("x-test-header") == "test-value")

    val customWriter = AzureSearchWriter.configureAuthentication(
      new AddDocuments(),
      AzureSearchAuth.fromOptions(Map(
        "customAuthHeader" -> "Custom test-auth",
        "customHeaders" -> "{\"x-test-header\":\"test-value\"}")))
    assert(customWriter.getCustomAuthHeader == "Custom test-auth")
    assert(customWriter.getOrDefault(customWriter.customHeaders).left.get("x-test-header") == "test-value")
  }

  test("malformed custom header options are rejected") {
    val error = intercept[IllegalArgumentException] {
      AzureSearchAuth.fromOptions(Map(
        "customAuthHeader" -> "Custom test-auth",
        "customHeaders" -> "not-json"))
    }
    assert(error.getMessage.contains("customHeaders"))
  }

  test("legacy subscription-key APIs remain source compatible") {
    val compileOnly = () => {
      val lister = new IndexLister {}
      val getter = new IndexJsonGetter {}
      lister.getExisting("key", "service")
      lister.getExisting("key", "service", "version")
      getter.getIndexJsonFromExistingIndex("key", "service", "index")
      getter.getIndexJsonFromExistingIndex("key", "service", "index", "version")
      SearchIndex.createIfNoneExists("key", "service", "{}")
      SearchIndex.createIfNoneExists("key", "service", "{}", "version")
      SearchIndex.getStatistics("index", "key", "service")
      SearchIndex.getStatistics("index", "key", "service", "version")
    }

    assert(compileOnly != null)
  }

  private def renderExceptionChain(t: Throwable): String = {
    val writer = new StringWriter()
    t.printStackTrace(new PrintWriter(writer))
    val causeChain = Iterator.iterate(t: Throwable)(_.getCause)
      .takeWhile(_ != null)
      .map(e => s"${e.getClass.getName}: ${Option(e.getMessage).getOrElse("")}")
      .mkString(" | ")
    writer.toString + " || " + causeChain
  }

  private def headerNameValuePairs(request: HttpRequestBase): Seq[(String, String)] = {
    request.getAllHeaders.map(h => h.getName -> h.getValue).toSeq.sorted
  }

  test("mixed-case api-key and Authorization custom headers cannot bypass precedence or duplicate") {
    val auth = AzureSearchAuth(
      subscriptionKey = Some("test-subscription-key"),
      customHeaders = Map(
        "AUTHORIZATION" -> "custom-should-not-win",
        "Api-Key" -> "custom-should-not-win-either",
        "x-generic" -> "generic-value"))
    requests(auth).foreach { request =>
      assert(header(request, "api-key").contains("test-subscription-key"))
      assert(request.getHeaders("api-key").length == 1)
      assert(request.getHeaders("Authorization").isEmpty)
      assert(header(request, "x-generic").contains("generic-value"))
      assert(request.getAllHeaders.forall(h => !h.getValue.contains("custom-should-not-win")))
    }
  }

  test("a mixed-case Authorization custom header is the sole credential and is canonicalized") {
    val auth = AzureSearchAuth(customHeaders = Map(
      "authorization" -> "Custom test-auth",
      "x-generic" -> "generic-value"))
    requests(auth).foreach { request =>
      assert(header(request, "Authorization").contains("Custom test-auth"))
      assert(request.getHeaders("Authorization").length == 1)
      assert(request.getAllHeaders.count(_.getName == "authorization") == 0)
      assert(header(request, "x-generic").contains("generic-value"))
    }
  }

  test("explicit credentials outrank an auth entry embedded in custom headers") {
    val auth = AzureSearchAuth(
      customAuthHeader = Some("Custom explicit-auth"),
      customHeaders = Map("Authorization" -> "custom-should-not-win"))
    requests(auth).foreach { request =>
      assert(header(request, "Authorization").contains("Custom explicit-auth"))
      assert(request.getHeaders("Authorization").length == 1)
      assert(request.getAllHeaders.forall(h => !h.getValue.contains("custom-should-not-win")))
    }
  }

  test("writer and management index APIs apply identical auth headers") {
    val auth = AzureSearchAuth(
      subscriptionKey = Some("test-subscription-key"),
      customHeaders = Map(
        "authorization" -> "custom-should-not-win",
        "x-generic" -> "generic-value"))
    val sharedHeaders = auth.headers(addContentType = true)

    val managementRequest = AzureSearchRequests.getIndex(auth, serviceName, indexName, apiVersion)

    val addHeaderRequest = new HttpGet("https://example.com")
    sharedHeaders.foreach { case (name, value) => addHeaderRequest.addHeader(name, value) }
    val setHeaderRequest = new HttpGet("https://example.com")
    sharedHeaders.foreach { case (name, value) => setHeaderRequest.setHeader(name, value) }

    assert(headerNameValuePairs(addHeaderRequest) == headerNameValuePairs(setHeaderRequest))
    assert(headerNameValuePairs(managementRequest) == headerNameValuePairs(addHeaderRequest))

    assert(managementRequest.getHeaders("Authorization").isEmpty)
    assert(managementRequest.getHeaders("api-key").length == 1)
    assert(header(managementRequest, "api-key").contains("test-subscription-key"))
    assert(managementRequest.getAllHeaders.forall(h => !h.getValue.contains("custom-should-not-win")))
  }

  test("malformed custom header JSON is rejected without leaking secrets in the exception chain") {
    val secretValue = "canary-9c3f-not-a-real-secret"
    val malformedMarker = "totally-not-valid-json"
    val malformedCustomHeaders = "{\"api-key\": \"" + secretValue + "\" " + malformedMarker + "}"

    val rawParserMessage = intercept[Exception](malformedCustomHeaders.parseJson).getMessage
    assert(rawParserMessage.contains(secretValue))

    val sanitized = intercept[IllegalArgumentException] {
      AzureSearchAuth.fromOptions(Map("customHeaders" -> malformedCustomHeaders))
    }

    assert(sanitized.getCause == null)
    assert(sanitized.getMessage.contains("customHeaders"))
    val rendered = renderExceptionChain(sanitized)
    assert(!rendered.contains(secretValue))
    assert(!rendered.contains(malformedMarker))
    assert(!rendered.toLowerCase.contains("spray"))
  }
}
