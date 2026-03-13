// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.http.ProtocolVersion
import org.apache.http.message.BasicHeader

class VerifyHTTPSchema extends TestBase {

  test("HeaderData stores name and value") {
    val header = HeaderData("Content-Type", "application/json")
    assert(header.name === "Content-Type")
    assert(header.value === "application/json")
  }

  test("HeaderData.toHTTPCore creates BasicHeader") {
    val header = HeaderData("Authorization", "Bearer token123")
    val httpHeader = header.toHTTPCore
    assert(httpHeader.getName === "Authorization")
    assert(httpHeader.getValue === "Bearer token123")
  }

  test("HeaderData can be created from HTTP Header") {
    val basicHeader = new BasicHeader("X-Custom", "custom-value")
    val headerData = new HeaderData(basicHeader)
    assert(headerData.name === "X-Custom")
    assert(headerData.value === "custom-value")
  }

  test("ProtocolVersionData stores protocol info") {
    val pvd = ProtocolVersionData("HTTP", 1, 1)
    assert(pvd.protocol === "HTTP")
    assert(pvd.major === 1)
    assert(pvd.minor === 1)
  }

  test("ProtocolVersionData.toHTTPCore creates ProtocolVersion") {
    val pvd = ProtocolVersionData("HTTP", 2, 0)
    val pv = pvd.toHTTPCore
    assert(pv.getProtocol === "HTTP")
    assert(pv.getMajor === 2)
    assert(pv.getMinor === 0)
  }

  test("ProtocolVersionData can be created from ProtocolVersion") {
    val pv = new ProtocolVersion("HTTP", 1, 0)
    val pvd = new ProtocolVersionData(pv)
    assert(pvd.protocol === "HTTP")
    assert(pvd.major === 1)
    assert(pvd.minor === 0)
  }

  test("StatusLineData stores status info") {
    val pvd = ProtocolVersionData("HTTP", 1, 1)
    val sld = StatusLineData(pvd, 200, "OK")
    assert(sld.protocolVersion === pvd)
    assert(sld.statusCode === 200)
    assert(sld.reasonPhrase === "OK")
  }

  test("RequestLineData stores request info") {
    val pvd = Some(ProtocolVersionData("HTTP", 1, 1))
    val rld = RequestLineData("GET", "https://example.com", pvd)
    assert(rld.method === "GET")
    assert(rld.uri === "https://example.com")
    assert(rld.protocolVersion === pvd)
  }

  test("RequestLineData works without protocol version") {
    val rld = RequestLineData("POST", "/api/data", None)
    assert(rld.method === "POST")
    assert(rld.uri === "/api/data")
    assert(rld.protocolVersion.isEmpty)
  }

  test("EntityData stores content info") {
    val content = "test content".getBytes
    val entity = EntityData(
      content = content,
      contentEncoding = None,
      contentLength = Some(content.length.toLong),
      contentType = Some(HeaderData("Content-Type", "text/plain")),
      isChunked = false,
      isRepeatable = true,
      isStreaming = false
    )
    assert(entity.content === content)
    assert(entity.contentLength === Some(content.length.toLong))
    assert(entity.isChunked === false)
    assert(entity.isRepeatable === true)
  }

  test("HTTPResponseData stores response info") {
    val pvd = ProtocolVersionData("HTTP", 1, 1)
    val statusLine = StatusLineData(pvd, 200, "OK")
    val response = HTTPResponseData(
      headers = Array(HeaderData("Content-Type", "application/json")),
      entity = None,
      statusLine = statusLine,
      locale = "en-US"
    )
    assert(response.headers.length === 1)
    assert(response.statusLine.statusCode === 200)
    assert(response.locale === "en-US")
    assert(response.entity.isEmpty)
  }

  test("HTTPRequestData stores request info") {
    val requestLine = RequestLineData("GET", "https://api.example.com/data", None)
    val request = HTTPRequestData(
      requestLine = requestLine,
      headers = Array(HeaderData("Accept", "application/json")),
      entity = None
    )
    assert(request.requestLine.method === "GET")
    assert(request.headers.length === 1)
    assert(request.entity.isEmpty)
  }

  test("HTTPRequestData with entity") {
    val content = """{"key": "value"}""".getBytes
    val entity = EntityData(
      content = content,
      contentEncoding = None,
      contentLength = Some(content.length.toLong),
      contentType = Some(HeaderData("Content-Type", "application/json")),
      isChunked = false,
      isRepeatable = true,
      isStreaming = false
    )
    val requestLine = RequestLineData("POST", "/api/submit", None)
    val request = HTTPRequestData(
      requestLine = requestLine,
      headers = Array(),
      entity = Some(entity)
    )
    assert(request.entity.isDefined)
    assert(request.entity.get.content === content)
  }

  test("HTTPSchema.Response has schema") {
    val schema = HTTPSchema.Response
    assert(schema != null)
  }

  test("HTTPSchema.Request has schema") {
    val schema = HTTPSchema.Request
    assert(schema != null)
  }

  test("HTTPSchema.stringToResponse creates response") {
    val response = HTTPSchema.stringToResponse("test body", 200, "OK")
    assert(response.statusLine.statusCode === 200)
    assert(response.statusLine.reasonPhrase === "OK")
    assert(response.entity.isDefined)
  }

  test("HTTPSchema.emptyResponse creates response without entity") {
    val response = HTTPSchema.emptyResponse(404, "Not Found")
    assert(response.statusLine.statusCode === 404)
    assert(response.statusLine.reasonPhrase === "Not Found")
    assert(response.entity.isEmpty)
  }

  test("HTTPSchema.binaryToResponse creates response with binary content") {
    val content = Array[Byte](1, 2, 3, 4, 5)
    val response = HTTPSchema.binaryToResponse(content, 200, "OK")
    assert(response.entity.isDefined)
    assert(response.entity.get.content === content)
  }

  test("HeaderValues.PlatformInfo returns a string") {
    val platformInfo = HeaderValues.PlatformInfo
    assert(platformInfo != null)
    assert(platformInfo.nonEmpty)
  }
}
