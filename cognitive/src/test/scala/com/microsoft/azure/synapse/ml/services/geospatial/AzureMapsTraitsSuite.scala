// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.geospatial

import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData, RequestLineData}
import org.apache.http._
import org.apache.http.client.methods.{CloseableHttpResponse, HttpRequestBase}
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.{BasicHeader, BasicHeaderIterator, BasicStatusLine}
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext
import org.scalatest.funsuite.AnyFunSuite

import java.net.{URI, URISyntaxException}
import java.util.Locale

class AzureMapsTraitsSuite extends AnyFunSuite {

  private class TestableMapsAsyncReply extends AddressGeocoder("test-maps-async") {

    def extract(request: HTTPRequestData): Map[String, String] =
      extractHeaderValuesForPolling(request)

    def query(headers: Map[String, String],
              client: CloseableHttpClient,
              location: URI): Option[HTTPResponseData] =
      queryForResult(headers, client, location)
  }

  private class RecordingHttpClient(responseFactory: () => CloseableHttpResponse)
    extends CloseableHttpClient {
    @volatile var lastRequestUri: Option[URI] = None

    override def close(): Unit = {}

    override def getConnectionManager: ClientConnectionManager = null

    override def getParams: HttpParams = null

    override protected def doExecute(target: HttpHost,
                                     request: HttpRequest,
                                     context: HttpContext): CloseableHttpResponse = {
      request match {
        case base: HttpRequestBase => lastRequestUri = Option(base.getURI)
        case other =>
          lastRequestUri = try {
            Option(new URI(other.getRequestLine.getUri))
          } catch {
            case _: URISyntaxException => None
          }
      }
      responseFactory()
    }
  }

  private class StubCloseableHttpResponse(statusCode: Int,
                                          headers: Seq[(String, String)] = Seq.empty,
                                          entity: Option[HttpEntity] = None)
    extends CloseableHttpResponse {

    private val statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, statusCode, "OK")
    private val headerObjects: Array[Header] = headers.map { case (n, v) => new BasicHeader(n, v) }.toArray

    override def close(): Unit = {}

    override def getStatusLine: StatusLine = statusLine

    override def setStatusLine(statusline: StatusLine): Unit = {}

    override def setStatusLine(ver: ProtocolVersion, code: Int): Unit = {}

    override def setStatusLine(ver: ProtocolVersion, code: Int, reason: String): Unit = {}

    override def setStatusCode(code: Int): Unit = {}

    override def setReasonPhrase(reason: String): Unit = {}

    override def getEntity: HttpEntity = entity.orNull

    override def setEntity(entity: HttpEntity): Unit = {}

    override def getLocale: Locale = Locale.US

    override def setLocale(loc: Locale): Unit = {}

    override def getProtocolVersion: ProtocolVersion = statusLine.getProtocolVersion

    override def containsHeader(name: String): Boolean =
      headerObjects.exists(_.getName.equalsIgnoreCase(name))

    override def getHeaders(name: String): Array[Header] =
      headerObjects.filter(_.getName.equalsIgnoreCase(name))

    override def getAllHeaders: Array[Header] = headerObjects

    override def addHeader(header: Header): Unit = {}

    override def addHeader(name: String, value: String): Unit = {}

    override def setHeader(header: Header): Unit = {}

    override def setHeader(name: String, value: String): Unit = {}

    override def setHeaders(headers: Array[Header]): Unit = {}

    override def removeHeader(header: Header): Unit = {}

    override def removeHeaders(name: String): Unit = {}

    override def getFirstHeader(name: String): Header =
      headerObjects.find(_.getName.equalsIgnoreCase(name)).orNull

    override def getLastHeader(name: String): Header =
      headerObjects.reverse.find(_.getName.equalsIgnoreCase(name)).orNull

    override def headerIterator(): HeaderIterator =
      new BasicHeaderIterator(headerObjects, null)

    override def headerIterator(name: String): HeaderIterator =
      new BasicHeaderIterator(headerObjects, name)

    override def getParams: HttpParams = null

    override def setParams(params: HttpParams): Unit = {}
  }

  test("extractHeaderValuesForPolling preserves encoded Azure Maps query params") {
    val helper = new TestableMapsAsyncReply
    val request = HTTPRequestData(
      RequestLineData(
        "GET",
        "https://example.com/maps?foo=bar&subscription-key=abc%2B123%3D&api-version=2024-01-01",
        None),
      Array.empty,
      None)

    val params = helper.extract(request)
    assert(params == Map("subscription-key" -> "abc%2B123%3D", "api-version" -> "2024-01-01"))
  }

  test("queryForResult appends auth params without double encoding or dropping fragments") {
    val helper = new TestableMapsAsyncReply
    val entity = new StringEntity("""{"status":"succeeded"}""", "UTF-8")
    val client = new RecordingHttpClient(() => new StubCloseableHttpResponse(200, Seq.empty, Some(entity)))

    val location = new URI("https://example.com/path?existing=1#frag")
    val headers = Map("subscription-key" -> "abc%2B123%3D", "api-version" -> "2024-01-01")

    val response = helper.query(headers, client, location)
    assert(response.nonEmpty)
    assert(response.get.statusLine.statusCode == 200)

    val recordedUri = client.lastRequestUri.get
    assert(recordedUri.getFragment == "frag")

    val queryParts = recordedUri.getRawQuery.split("&").toSet
    assert(queryParts.contains("existing=1"))
    assert(queryParts.contains("subscription-key=abc%2B123%3D"))
    assert(queryParts.contains("api-version=2024-01-01"))
  }

}
