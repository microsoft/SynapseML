// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import spray.json.{JsArray, JsObject, JsValue, _}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers


import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
object WebUtils {

  val Region: String = "eastus"
  val BaseURL: String = s"https://$Region.azuredatabricks.net/api/2.0/"

  def usagePost(url: String, body: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpPost(url)
    try {
      for ((k, v) <- headerPayload)
        request.addHeader(k, v)
    }
    catch {
      case e: IllegalArgumentException =>
        SynapseMLLogging.logMessage(s"WebUtils::usagePost: Getting error setting in the request header. Exception = $e")
    }
    request.setEntity(new StringEntity(body))
    RESTHelpers.sendAndParseJson(request)
  }

  def usageGet(url: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpGet(url)
    try {
      for ((k, v) <- headerPayload)
        request.addHeader(k, v)
    }
    catch
    {
      case e: IllegalArgumentException =>
        SynapseMLLogging.logMessage(s"WebUtils::usageGet: Getting error setting in the request header. Exception = $e")
    }
    RESTHelpers.sendAndParseJson(request)
  }
}
