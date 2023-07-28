package com.microsoft.azure.synapse.ml.logging.common

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import spray.json.{JsArray, JsObject, JsValue, _}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
object WebUtils {

  val Region: String = "eastus"
  val BaseURL: String = s"https://$Region.azuredatabricks.net/api/2.0/"

  def usagePost(url: String, body: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpPost(url)
    for ((k,v) <- headerPayload)
      request.addHeader(k, v)
    request.setEntity(new StringEntity(body))
    RESTHelpers.sendAndParseJson(request)
  }

  def usageGet(url: String, headerPayload: Map[String, String]): JsValue = {
    val request = new HttpGet(url)
    for ((k, v) <- headerPayload)
      request.addHeader(k, v)
    RESTHelpers.sendAndParseJson(request)
  }
}
