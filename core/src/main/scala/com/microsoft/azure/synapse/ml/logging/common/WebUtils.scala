// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import scala.util.{Success, Try}
import spray.json.DefaultJsonProtocol.{IntJsonFormat, StringJsonFormat}
import spray.json.{JsObject, JsValue, _}

trait WebUtils {
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
      content.parseJson
    } else {
      JsObject()
    }
  }

  def requestGet(url: String, headers: Map[String, String], property: String): JsValue = {
    val response: JsValue = usageGet(url, headers)

    val statusCode = Try(response.asJsObject.fields("status_code").convertTo[Int])
    val propertyValue = Try(response.asJsObject.fields(property).convertTo[String])

    (statusCode, propertyValue) match {
      case (Success(code), Success(value)) if code == 200 && !value.isEmpty => response.asJsObject.fields(property)
      case _ => throw new Exception(s"CommonUtils.requestGet: Failed with " +
        s"code=$statusCode. Property looked for was = $property")
    }
  }
}
