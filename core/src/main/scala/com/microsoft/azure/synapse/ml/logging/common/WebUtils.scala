// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import spray.json.{JsArray, JsObject, JsValue, _}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging

object WebUtils {
  def usagePost(url: String, body: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpPost(url)

    for ((k, v) <- headerPayload)
        request.addHeader(k, v)

    request.setEntity(new StringEntity(body))
    RESTHelpers.sendAndParseJson(request)
  }

  def usageGet(url: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpGet(url)
    try {
      for ((k, v) <- headerPayload)
        request.addHeader(k, v)
    } catch {
      case e: IllegalArgumentException =>
        SynapseMLLogging.logMessage(s"WebUtils::usageGet: Getting error setting in the request header. Exception = $e")
    }
    RESTHelpers.sendAndParseJson(request)
  }
}
