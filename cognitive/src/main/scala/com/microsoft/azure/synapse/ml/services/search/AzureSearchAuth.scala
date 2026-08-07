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

  private def nonBlank(value: String): Boolean = value != null && value.trim.nonEmpty

  // Java callers can pass a null customHeaders map or entries with a null header name or value. Reuse
  // the shared cognitive-services normalizer so a null map collapses to empty and null-named or
  // null-valued entries are dropped with identical rules to setCustomHeaders and ServiceAuthHeaders
  // .build, keeping validation, toString, and header assembly null-safe and free of null generics.
  private def sanitizedCustomHeaders: Map[String, String] =
    ServiceAuthHeaders.sanitizeHeaderMap(customHeaders)

  // Public/Java callers can pass a null Option container (not just Some(null)) for any credential,
  // e.g. AzureSearchAuth(null, Some(aad), None, Map.empty). Option(_).flatten collapses a null
  // container to None before filtering (so .filter never NPEs) while preserving Some(null), which
  // nonBlank then drops -- a null/blank higher-priority credential never suppresses a valid lower one,
  // and validated/toString/header assembly read this normalized copy so they stay null-safe.
  private def normalized: AzureSearchAuth = copy(
    subscriptionKey = Option(subscriptionKey).flatten.filter(nonBlank),
    aadToken = Option(aadToken).flatten.filter(nonBlank),
    customAuthHeader = Option(customAuthHeader).flatten.filter(nonBlank),
    customHeaders = sanitizedCustomHeaders)

  private[search] def validated: AzureSearchAuth = {
    val auth = normalized
    val customCredential = auth.customHeaders.exists { case (name, value) =>
      (name.equalsIgnoreCase("api-key") || name.equalsIgnoreCase("Authorization")) && nonBlank(value)
    }
    require(
      auth.subscriptionKey.nonEmpty || auth.aadToken.nonEmpty || auth.customAuthHeader.nonEmpty || customCredential,
      "Azure AI Search authentication requires subscriptionKey, AADToken, CustomAuthHeader, " +
        "or an api-key/Authorization custom header")
    auth
  }

  override def toString: String = {
    val customHeaderNames = sanitizedCustomHeaders.keys.toSeq.sorted.mkString("[", ",", "]")
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
      None, // Azure AI Search management requests have no automatic Fabric fallback
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
    // Drop null/blank alias values before the conflict check so a blank alias (treated as absent
    // everywhere else) never conflicts with a valid sibling. Values are compared verbatim -- never
    // trimmed -- and the failure names only the option keys, never their (credential) values.
    val values = names.flatMap(options.get).filter(ServiceAuthHeaders.nonBlank).distinct
    require(values.size <= 1, s"Conflicting Azure AI Search options: ${names.mkString(" and ")}")
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
      // Never chain the spray-json parser exception: its message echoes the raw customHeaders
      // input, which can contain credential values. Surface a sanitized error with no cause.
      case _: Exception =>
        throw new IllegalArgumentException(
          "customHeaders must be a JSON object whose values are strings")
    }
  }

  private[search] def fromOptions(options: Map[String, String]): AzureSearchAuth = {
    AzureSearchAuth(
      subscriptionKey = optionValue(options, Seq("subscriptionKey")),
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
    // Mirror the shared cognitive writer path (HasCognitiveServiceInput.addHeaders), which uses
    // addHeader, so the document writer and these management index APIs apply the deduplicated,
    // canonical auth map from ServiceAuthHeaders.build with identical semantics.
    auth.headers(addContentType).foreach { case (name, value) => request.addHeader(name, value) }
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
