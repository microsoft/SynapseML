// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.ServiceAuthHeaders
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.{ContentType, StringEntity}
import spray.json._

final case class AzureSearchAuth(subscriptionKey: Option[String] = None,
                                 aadToken: Option[String] = None,
                                 customAuthHeader: Option[String] = None,
                                 customHeaders: Map[String, String] = Map.empty) {

  private def nonBlank(value: String): Boolean = value.trim.nonEmpty

  private def normalized: AzureSearchAuth = copy(
    subscriptionKey = subscriptionKey.filter(nonBlank),
    aadToken = aadToken.filter(nonBlank),
    customAuthHeader = customAuthHeader.filter(nonBlank))

  private[search] def validated: AzureSearchAuth = {
    val auth = normalized
    val customCredential = auth.customHeaders.exists { case (name, value) =>
      (name.equalsIgnoreCase("api-key") || name.equalsIgnoreCase("Authorization")) && nonBlank(value)
    }
    require(
      auth.subscriptionKey.nonEmpty || auth.aadToken.nonEmpty || auth.customAuthHeader.nonEmpty || customCredential,
      "Azure Search authentication requires subscriptionKey, AADToken, CustomAuthHeader, " +
        "or an api-key/Authorization custom header")
    auth
  }

  override def toString: String = {
    val customHeaderNames = customHeaders.keys.toSeq.sorted.mkString("[", ",", "]")
    s"AzureSearchAuth(subscriptionKey=<redacted>, aadToken=<redacted>, " +
      s"customAuthHeader=<redacted>, customHeaders=$customHeaderNames)"
  }

  private[search] def headers(addContentType: Boolean = false): Map[String, String] = {
    val auth = validated
    ServiceAuthHeaders.build(
      auth.subscriptionKey,
      "api-key",
      "Authorization",
      auth.aadToken,
      auth.customAuthHeader,
      Option(auth.customHeaders).filter(_.nonEmpty),
      None,
      if (addContentType) Some("application/json") else None)
  }
}

object AzureSearchAuth {
  def fromSubscriptionKey(subscriptionKey: String): AzureSearchAuth = {
    AzureSearchAuth(subscriptionKey = Some(subscriptionKey)).validated
  }

  def fromAADToken(aadToken: String): AzureSearchAuth = {
    AzureSearchAuth(aadToken = Some(aadToken)).validated
  }

  private def optionValue(options: Map[String, String], names: Seq[String]): Option[String] = {
    val values = names.flatMap(options.get).distinct
    require(values.size <= 1, s"Conflicting Azure Search options: ${names.mkString(" and ")}")
    values.headOption
  }

  private def parseCustomHeaders(value: String): Map[String, String] = {
    try {
      value.parseJson match {
        case JsObject(fields) => fields.map {
          case (name, JsString(headerValue)) => name -> headerValue
          case (name, _) => throw new IllegalArgumentException(
            s"customHeaders value for '$name' must be a JSON string")
        }
        case _ => throw new IllegalArgumentException("customHeaders must be a JSON object")
      }
    } catch {
      case error: Exception =>
        throw new IllegalArgumentException(
          "customHeaders must be a JSON object whose values are strings", error)
    }
  }

  private[search] def fromOptions(options: Map[String, String]): AzureSearchAuth = {
    AzureSearchAuth(
      subscriptionKey = options.get("subscriptionKey"),
      aadToken = optionValue(options, Seq("AADToken", "aadToken")),
      customAuthHeader = optionValue(options, Seq("CustomAuthHeader", "customAuthHeader")),
      customHeaders = options.get("customHeaders").map(parseCustomHeaders).getOrElse(Map.empty)
    ).validated
  }
}

private[search] object AzureSearchRequests {

  private def addHeaders(request: HttpRequestBase,
                         auth: AzureSearchAuth,
                         addContentType: Boolean = false): Unit = {
    auth.headers(addContentType).foreach { case (name, value) => request.setHeader(name, value) }
  }

  def listIndexes(auth: AzureSearchAuth,
                  serviceName: String,
                  apiVersion: String): HttpGet = {
    val request = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion&$$select=name")
    addHeaders(request, auth)
    request
  }

  def getIndex(auth: AzureSearchAuth,
               serviceName: String,
               indexName: String,
               apiVersion: String): HttpGet = {
    val request = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName?api-version=$apiVersion")
    addHeaders(request, auth, addContentType = true)
    request
  }

  def createIndex(auth: AzureSearchAuth,
                  serviceName: String,
                  indexJson: String,
                  apiVersion: String): HttpPost = {
    val request = new HttpPost(
      s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion")
    addHeaders(request, auth, addContentType = true)
    request.setEntity(new StringEntity(indexJson, ContentType.APPLICATION_JSON))
    request
  }

  def getStatistics(auth: AzureSearchAuth,
                    serviceName: String,
                    indexName: String,
                    apiVersion: String): HttpGet = {
    val request = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName/stats?api-version=$apiVersion")
    addHeaders(request, auth)
    request
  }
}
