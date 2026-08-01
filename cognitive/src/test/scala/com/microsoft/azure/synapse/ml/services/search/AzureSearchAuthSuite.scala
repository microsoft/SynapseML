// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.spark.sql.Row
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

  test("a blank custom api-key does not suppress a valid Authorization custom header") {
    val auth = AzureSearchAuth(customHeaders = Map(
      "api-key" -> "   ",
      "Authorization" -> "Custom valid-auth",
      "x-generic" -> "generic-value"))
    requests(auth).foreach { request =>
      assert(header(request, "Authorization").contains("Custom valid-auth"))
      assert(request.getHeaders("Authorization").length == 1)
      assert(request.getHeaders("api-key").isEmpty)
      assert(header(request, "x-generic").contains("generic-value"))
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

  test("writer header preparation ranks an embedded credential above the Fabric fallback") {
    // Exercise the real AddDocuments/HasCognitiveServiceInput header path (not a synthetic
    // request.addHeader reconstruction). The Fabric fallback token is injected through the shared
    // seam so the writer path is covered without a live Fabric environment or any secret.
    val fabricFallback = Some("Bearer fabric-fallback-token")

    val embeddedWriter = new AddDocuments().setCustomHeaders(Map(
      "api-key" -> "embedded-key",
      "x-generic" -> "generic-value"))
    val embeddedHeaders =
      embeddedWriter.buildServiceAuthHeaders(Row.empty, addContentType = false, fabricFallback)
    assert(embeddedHeaders("api-key") == "embedded-key")
    assert(!embeddedHeaders.contains("Authorization"))
    assert(embeddedHeaders("x-generic") == "generic-value")
    assert(!embeddedHeaders.values.exists(_.contains("fabric-fallback-token")))

    // The management path never synthesizes a Fabric fallback; both converge on the embedded key.
    val managementHeaders = AzureSearchAuth(customHeaders = Map(
      "api-key" -> "embedded-key",
      "x-generic" -> "generic-value")).headers()
    assert(managementHeaders("api-key") == embeddedHeaders("api-key"))
    assert(managementHeaders.get("Authorization") == embeddedHeaders.get("Authorization"))
  }

  test("writer header preparation uses the Fabric fallback when no other credential exists") {
    val headers = new AddDocuments().buildServiceAuthHeaders(
      Row.empty, addContentType = false, Some("Bearer fabric-fallback-token"))
    assert(headers("Authorization") == "Bearer fabric-fallback-token")
    assert(!headers.contains("api-key"))
  }

  test("writer explicit subscription key outranks an embedded credential and the Fabric fallback") {
    val headers = new AddDocuments()
      .setSubscriptionKey("explicit-key")
      .setCustomHeaders(Map("api-key" -> "embedded-key"))
      .buildServiceAuthHeaders(Row.empty, addContentType = false, Some("Bearer fabric-fallback-token"))
    assert(headers("api-key") == "explicit-key")
    assert(!headers.contains("Authorization"))
    assert(!headers.values.exists(_.contains("fabric-fallback-token")))
  }

  test("mixed-case duplicate api-key custom headers resolve deterministically to one header") {
    val auth = AzureSearchAuth(customHeaders = Map(
      "API-KEY" -> "first-key",
      "Api-Key" -> "second-key"))
    val first = auth.headers()
    assert(first == auth.headers()) // case-insensitive resolution is deterministic
    assert(first.keys.count(_.equalsIgnoreCase("api-key")) == 1)
    assert(Set("first-key", "second-key").contains(first("api-key")))
    assert(first("api-key") == "first-key") // sorted by raw name: "API-KEY" precedes "Api-Key"
    assert(!first.contains("Authorization"))
  }

  test("custom headers with only blank auth values are rejected as missing credentials") {
    val error = intercept[IllegalArgumentException] {
      AzureSearchAuth(customHeaders = Map("api-key" -> "  ", "Authorization" -> "   ")).validated
    }
    assert(error.getMessage.contains("authentication"))
  }

  test("automatic Fabric fallback eligibility treats blank explicit credentials as absent") {
    // Exercises the real getFabricFallbackAuthHeader credential gate (the production decision), not a
    // fallback value injected straight into buildServiceAuthHeaders. A blank or whitespace
    // subscription key, AAD token, or custom auth header must NOT mark the Fabric fallback
    // ineligible: ServiceAuthHeaders.build discards those blank values, so suppressing the fallback
    // would leave the writer and non-Search cognitive consumers unauthenticated on Fabric. The same
    // non-blank guard also treats a null value as absent.
    assert(new AddDocuments().lacksExplicitAuthCredential(Row.empty))
    assert(new AddDocuments().setCustomAuthHeader("   ").lacksExplicitAuthCredential(Row.empty))
    assert(new AddDocuments().setSubscriptionKey("   ").lacksExplicitAuthCredential(Row.empty))
    assert(new AddDocuments().setAADToken("   ").lacksExplicitAuthCredential(Row.empty))

    // A non-blank explicit credential (any of the three) makes the fallback ineligible so it never
    // fetches a Fabric token or outranks the supplied credential.
    assert(!new AddDocuments().setSubscriptionKey("explicit-key").lacksExplicitAuthCredential(Row.empty))
    assert(!new AddDocuments().setAADToken("explicit-token").lacksExplicitAuthCredential(Row.empty))
    assert(!new AddDocuments().setCustomAuthHeader("Custom explicit-auth").lacksExplicitAuthCredential(Row.empty))
  }

  test("writer header preparation never evaluates the Fabric fallback when an embedded credential is present") {
    // The production writer path (getHeaders -> buildServiceAuthHeaders) supplies the Fabric fallback
    // by-name; on Fabric that supplier acquires a token and can throw. An embedded api-key/Authorization
    // in customHeaders outranks the fallback, so preparation must succeed WITHOUT ever evaluating the
    // supplier -- a throwing supplier that is nonetheless invoked reproduces the eager-evaluation bug.
    var throwingEvaluations = 0
    def throwingFallback: Option[String] = {
      throwingEvaluations += 1
      throw new RuntimeException("Fabric fallback must not be evaluated when a credential is present")
    }

    val embeddedWriter = new AddDocuments().setCustomHeaders(Map(
      "api-key" -> "embedded-key",
      "x-generic" -> "generic-value"))
    val headers = embeddedWriter.buildServiceAuthHeaders(Row.empty, addContentType = false, throwingFallback)

    assert(throwingEvaluations == 0)
    assert(headers("api-key") == "embedded-key")
    assert(!headers.contains("Authorization"))
    assert(headers("x-generic") == "generic-value")

    // When no higher-priority credential exists the fallback IS evaluated (exactly once) and applied.
    var fallbackEvaluations = 0
    def countingFallback: Option[String] = {
      fallbackEvaluations += 1
      Some("fabric-fallback-token")
    }
    val fallbackHeaders =
      new AddDocuments().buildServiceAuthHeaders(Row.empty, addContentType = false, countingFallback)
    assert(fallbackEvaluations == 1)
    assert(fallbackHeaders("Authorization") == "fabric-fallback-token")
    assert(!fallbackHeaders.contains("api-key"))
  }
}
