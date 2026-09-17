// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import org.apache.http.client.methods.HttpPost
import org.apache.spark.sql.Row

class OpenAIFabricAuthRetrySuite extends TestBase {

  private class AuthProvenanceProbe(fallbackAuthHeader: Option[String]) extends OpenAIChatCompletion {
    var fallbackCalls = 0

    override protected[openai] def runningOnFabric: Boolean = true

    override protected[openai] def usingDefaultOpenAIEndpoint: Boolean = true

    override protected def getFabricFallbackAuthHeader(row: Row): Option[String] = {
      fallbackCalls += 1
      fallbackAuthHeader
    }

    def requestData: HTTPRequestData = {
      val request = new HttpPost("https://example.test/openai")
      addHeaders(request, Row.empty, addContentType = false)
      new HTTPRequestData(request)
    }
  }

  test("only implicit Fabric authentication marks a request for refresh") {
    val implicitProbe = new AuthProvenanceProbe(Some("MwcToken implicit"))
    val implicitRequestData = implicitProbe.requestData
    val implicitHttpRequest = implicitRequestData.toHTTPCore

    assert(implicitProbe.fallbackCalls === 1)
    assert(implicitRequestData.usesFabricAuth)
    assert(implicitHttpRequest.getFirstHeader("Authorization").getValue === "MwcToken implicit")
    assert(Option(implicitHttpRequest.getFirstHeader(HTTPRequestData.FabricAuthMarkerHeader)).isEmpty)

    val explicitProbe = new AuthProvenanceProbe(Some("MwcToken implicit"))
      .setCustomAuthHeader("MwcToken explicit")
      .setCustomHeaders(Map(HTTPRequestData.FabricAuthMarkerHeader -> "true"))
      .asInstanceOf[AuthProvenanceProbe]
    val explicitRequestData = explicitProbe.requestData

    assert(explicitProbe.fallbackCalls === 0)
    assert(!explicitRequestData.usesFabricAuth)
    assert(explicitRequestData.toHTTPCore.getFirstHeader("Authorization").getValue === "MwcToken explicit")

    val apiKeyProbe = new AuthProvenanceProbe(Some("MwcToken implicit"))
      .setSubscriptionKey("explicit-key")
      .asInstanceOf[AuthProvenanceProbe]
    val apiKeyRequestData = apiKeyProbe.requestData

    assert(apiKeyProbe.fallbackCalls === 0)
    assert(!apiKeyRequestData.usesFabricAuth)
    assert(apiKeyRequestData.toHTTPCore.getFirstHeader("api-key").getValue === "explicit-key")
  }
}
