// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import spray.json.{JsObject, JsValue, JsonParser}

trait RESTUtils {
  def usagePost(url: String, body: String, headers: Map[String, String]): JsValue = {
    val request = new HttpPost(url)

    for ((k, v) <- headers)
      request.addHeader(k, v)

    request.setEntity(new StringEntity(body))

    val response = RESTHelpers.safeSend(request, close = false)
    val parsedResponse = parseResponse(response)
    response.close()
    parsedResponse
  }

  def usageGet(url: String, headers: Map[String, String]): JsValue = {
    val request = new HttpGet(url)
    for ((k, v) <- headers)
      request.addHeader(k, v)

    val response = RESTHelpers.safeSend(request, close = false)
    val result = parseResponse(response)
    response.close()
    result
  }

  private def parseResponse(response: CloseableHttpResponse): JsValue = {
    val content: String = IOUtils.toString(response.getEntity.getContent, "utf-8")
    if (content.nonEmpty) {
      JsonParser(content)
    } else {
      JsObject()
    }
  }

}
