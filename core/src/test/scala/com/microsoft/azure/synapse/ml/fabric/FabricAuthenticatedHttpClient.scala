// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.{safeSend, sendAndParseJson}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import spray.json.JsValue

object FabricAuthenticatedHttpClient {
  def apply(clientId: String, redirectUri: String): FabricAuthenticatedHttpClient =
    new FabricAuthenticatedHttpClient(clientId, redirectUri)
}

class FabricAuthenticatedHttpClient(clientId: String, redirectUri: String) {
  val timeoutInMillis: Int = 60 * 60 * 1000

  def setRequestContentTypeAndAuthorization(request: HttpRequestBase): Unit = {
    request.setHeader("Content-Type", "application/json")
    request.setHeader("Authorization", s"Bearer ${FabricTokenProvider.getAccessToken(clientId, redirectUri)}")
  }

  def postRequest(uri: String, requestBody: String = ""): JsValue = {
    val request = new HttpPost(uri)
    setRequestContentTypeAndAuthorization(request)

    val requestConfig = RequestConfig
      .custom
      .setSocketTimeout(timeoutInMillis)
      .setConnectTimeout(timeoutInMillis)
      .setConnectionRequestTimeout(timeoutInMillis)
      .build

    request.setConfig(requestConfig)

    if (requestBody.nonEmpty) {
      request.setEntity(new StringEntity(requestBody))
    }
    sendAndParseJson(request)
  }

  def getRequest(uri: String): JsValue = {
    val request = new HttpGet(uri)
    setRequestContentTypeAndAuthorization(request)
    sendAndParseJson(request)
  }

  def deleteRequest(uri: String): CloseableHttpResponse = {
    val deleteRequest = new HttpDelete(uri)
    setRequestContentTypeAndAuthorization(deleteRequest)
    safeSend(deleteRequest)
  }

  def patchRequest(uri: String, requestBody: String, etag: String): JsValue = {
    val patchRequest = new HttpPatch(uri)
    setRequestContentTypeAndAuthorization(patchRequest)
    patchRequest.setHeader("If-Match", etag)
    patchRequest.setEntity(new StringEntity(requestBody))
    sendAndParseJson(patchRequest)
  }
}
