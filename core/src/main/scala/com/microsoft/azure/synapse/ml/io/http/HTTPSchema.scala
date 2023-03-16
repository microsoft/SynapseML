// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.sun.net.httpserver.HttpExchange
import org.apache.commons.io.IOUtils
import org.apache.http._
import org.apache.http.client.methods._
import org.apache.http.entity.{ByteArrayEntity, StringEntity}
import org.apache.http.message.BasicHeader
import org.apache.spark.injections.UDFUtils
import org.apache.spark.internal.{Logging => SLogging}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, typedLit}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, Row}

import java.net.{SocketException, URI}
import scala.collection.JavaConverters._

case class HeaderData(name: String, value: String) {

  def this(h: Header) = {
    this(h.getName, h.getValue)
  }

  def toHTTPCore: Header = new BasicHeader(name, value)

}

object HeaderData extends SparkBindings[HeaderData]

case class EntityData(content: Array[Byte],
                      contentEncoding: Option[HeaderData],
                      contentLength: Option[Long],
                      contentType: Option[HeaderData],
                      isChunked: Boolean,
                      isRepeatable: Boolean,
                      isStreaming: Boolean) {

  def this(e: HttpEntity) = {
    this(
      try {
        IOUtils.toByteArray(e.getContent)
      } catch {
        case _: SocketException => Array() //TODO investigate why sockets fail sometimes
      },
      Option(e.getContentEncoding).map(new HeaderData(_)),
      Option(e.getContentLength),
      Option(e.getContentType).map(new HeaderData(_)),
      e.isChunked,
      e.isRepeatable,
      e.isStreaming)
  }

  def toHttpCore: HttpEntity = {
    val e = new ByteArrayEntity(content)
    contentEncoding.foreach { ce => e.setContentEncoding(ce.toHTTPCore) }
    contentLength.foreach { cl => assert(e.getContentLength == cl) }
    contentType.foreach(h => e.setContentType(h.toHTTPCore))
    e.setChunked(isChunked)
    assert(e.isRepeatable == isRepeatable)
    assert(e.isStreaming == isStreaming)
    e
  }

}

object EntityData extends SparkBindings[EntityData]

case class StatusLineData(protocolVersion: ProtocolVersionData,
                          statusCode: Int,
                          reasonPhrase: String) {

  def this(s: StatusLine) = {
    this(new ProtocolVersionData(s.getProtocolVersion),
      s.getStatusCode,
      s.getReasonPhrase)
  }

}

object StatusLineData extends SparkBindings[StatusLineData]

case class HTTPResponseData(headers: Array[HeaderData],
                            entity: Option[EntityData],
                            statusLine: StatusLineData,
                            locale: String) {

  def this(response: CloseableHttpResponse) = {
    this(response.getAllHeaders.map(new HeaderData(_)),
      Option(response.getEntity).map(new EntityData(_)),
      new StatusLineData(response.getStatusLine),
      response.getLocale.toString)
  }

  def respondToHTTPExchange(request: HttpExchange): Unit = {
    val responseHeaders = request.getResponseHeaders
    val headersToAdd = headers ++ Seq(
      entity.flatMap(_.contentType),
      entity.flatMap(_.contentEncoding)).flatten
    if (headersToAdd.nonEmpty) {
      headersToAdd.foreach(h => responseHeaders.add(h.name, h.value))
    }
    try {
      request.sendResponseHeaders(statusLine.statusCode,
        entity.flatMap(_.contentLength).getOrElse(0L))
    } catch {
      case e: java.io.IOException =>
        HTTPResponseData.warn(s"Could not write headers: ${e.getMessage}")
    }

    try {
      entity.foreach(entity => using(request.getResponseBody) {
        _.write(entity.content)
      }.get)
    } catch {
      case e: java.io.IOException =>
        HTTPResponseData.warn(s"Could not send bytes: ${e.getMessage}")
    }
  }

}

object HTTPResponseData extends SparkBindings[HTTPResponseData] with SLogging {
  def warn(msg: => String): Unit = logWarning(msg)
}

case class ProtocolVersionData(protocol: String, major: Int, minor: Int) {

  def this(v: ProtocolVersion) = {
    this(v.getProtocol, v.getMajor, v.getMinor)
  }

  def toHTTPCore: ProtocolVersion = {
    new ProtocolVersion(protocol, major, minor)
  }

}

object ProtocolVersionData extends SparkBindings[ProtocolVersionData]

case class RequestLineData(method: String,
                           uri: String,
                           protocolVersion: Option[ProtocolVersionData]) {

  def this(l: RequestLine) = {
    this(l.getMethod,
      l.getUri,
      Some(new ProtocolVersionData(l.getProtocolVersion)))
  }

}

object RequestLineData extends SparkBindings[RequestLineData]

object HeaderValues {
  lazy val PlatformInfo: String = sys.env.get("MMLSPARK_PLATFORM_INFO").map(" " + _).getOrElse(" no-platform-info")
}

case class HTTPRequestData(requestLine: RequestLineData,
                           headers: Array[HeaderData],
                           entity: Option[EntityData]) {

  def this(r: HttpRequestBase) = {
    this(new RequestLineData(r.getRequestLine),
      r.getAllHeaders.map(new HeaderData(_)),
      r match {
        case re: HttpEntityEnclosingRequestBase => Option(re.getEntity).map(new EntityData(_))
        case _ => None
      })
  }

  //scalastyle:off cyclomatic.complexity
  def toHTTPCore: HttpRequestBase = {
    val request = requestLine.method.toUpperCase match {
      case "GET" => new HttpGet()
      case "HEAD" => new HttpHead()
      case "DELETE" => new HttpDelete()
      case "OPTIONS" => new HttpOptions()
      case "TRACE" => new HttpTrace()
      case "POST" => new HttpPost()
      case "PUT" => new HttpPut()
      case "PATCH" => new HttpPatch()
    }
    request match {
      case re: HttpEntityEnclosingRequestBase =>
        entity.foreach(e => re.setEntity(e.toHttpCore))
      case _ if entity.isDefined =>
        throw new IllegalArgumentException(s"Entity is defined but method is ${requestLine.method}")
      case _ =>
    }
    request.setURI(new URI(requestLine.uri))
    requestLine.protocolVersion.foreach(pv =>
      request.setProtocolVersion(pv.toHTTPCore))
    request.setHeaders(headers.map(_.toHTTPCore) ++
      Array(new BasicHeader(
        "User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")))
    request
  }
  //scalastyle:on cyclomatic.complexity
}

object HTTPRequestData extends SparkBindings[HTTPRequestData] {
  def fromHTTPExchange(httpEx: HttpExchange): HTTPRequestData = {
    val requestHeaders = httpEx.getRequestHeaders
    val isChunked = Option(requestHeaders.getFirst("Transfer-Encoding") == "chunked").getOrElse(false)
    HTTPRequestData(
      RequestLineData(
        httpEx.getRequestMethod,
        httpEx.getRequestURI.toString,
        Option(httpEx.getProtocol).map { p =>
          val Array(v, n) = p.split("/".toCharArray.head)
          val Array(major, minor) = n.split(".".toCharArray.head)
          ProtocolVersionData(v, major.toInt, minor.toInt)
        }),
      httpEx.getRequestHeaders.asScala.flatMap {
        case (k, vs) => vs.asScala.map(v => HeaderData(k, v))
      }.toArray,
      Some(EntityData(
        IOUtils.toByteArray(httpEx.getRequestBody),
        Option(requestHeaders.getFirst("Content-Encoding")).map(HeaderData("Content-Encoding", _)),
        Option(requestHeaders.getFirst("Content-Length")).map(_.toLong),
        Option(requestHeaders.getFirst("Content-Type")).map(HeaderData("Content-Type", _)),
        isChunked = isChunked,
        isRepeatable = false,
        isStreaming = isChunked
      ))
    )
  }
}

object HTTPSchema {

  val Response: DataType = HTTPResponseData.schema
  val Request: DataType = HTTPRequestData.schema

  //Convenience Functions for making and parsing HTTP objects
  //scalastyle:off

  private def stringToEntity(s: String): EntityData = {
    new EntityData(new StringEntity(s, "UTF-8"))
  }

  private def binaryToEntity(arr: Array[Byte]): EntityData = {
    new EntityData(new ByteArrayEntity(arr))
  }

  private def entityToString(e: EntityData): Option[String] = {
    if (e.content.isEmpty) {
      None
    } else {
      Some(IOUtils.toString(e.content,
        e.contentEncoding.map(h => h.value).getOrElse("UTF-8")))
    }
  }

  private def entity_to_string_udf: UserDefinedFunction = {
    val fromRow = EntityData.makeFromRowConverter
    UDFUtils.oldUdf({ x: Row =>
      val sOpt = Option(x).flatMap(r => entityToString(fromRow(r)))
      sOpt.orNull
    }, StringType)
  }

  def entity_to_string(c: Column): Column = entity_to_string_udf(c)

  private val string_to_entity_udf: UserDefinedFunction =
    UDFUtils.oldUdf({ x: String => stringToEntity(x) }, EntityData.schema)

  def string_to_entity(c: Column): Column = string_to_entity_udf(c)

  private def request_to_string_udf: UserDefinedFunction = {
    val fromRow = HTTPRequestData.makeFromRowConverter
    UDFUtils.oldUdf({ x: Row =>
      val sOpt = Option(x)
        .flatMap(r => fromRow(r).entity)
        .map(entityToString)
      sOpt.orNull
    }, StringType)
  }

  def request_to_string(c: Column): Column = request_to_string_udf(c)

  def stringToResponse(x: String, code: Int, reason: String): HTTPResponseData = {
    HTTPResponseData(
      Array(),
      Some(stringToEntity(x)),
      StatusLineData(null, code, reason),
      "en")
  }

  private val string_to_response_udf: UserDefinedFunction =
    UDFUtils.oldUdf(stringToResponse _, HTTPResponseData.schema)

  def string_to_response(str: Column, code: Column = lit(200), reason: Column = lit("Success")): Column =
    string_to_response_udf(str, code, reason)

  def emptyResponse(code: Int, reason: String): HTTPResponseData = {
    HTTPResponseData(
      Array(),
      None,
      StatusLineData(null, code, reason),
      "en")
  }

  private val empty_response_udf: UserDefinedFunction =
    UDFUtils.oldUdf(emptyResponse _, HTTPResponseData.schema)

  def empty_response(code: Column = lit(200), reason: Column = lit("Success")): Column =
    empty_response_udf(code, reason)

  def binaryToResponse(x: Array[Byte], code: Int, reason: String): HTTPResponseData = {
    HTTPResponseData(
      Array(),
      Some(binaryToEntity(x)),
      StatusLineData(null, code, reason),
      "en")
  }

  private val binary_to_response_udf: UserDefinedFunction =
    UDFUtils.oldUdf(binaryToResponse _, HTTPResponseData.schema)

  def binary_to_response(ba: Column, code: Column = lit(200), reason: Column = lit("Success")): Column =
    binary_to_response_udf(ba, code, reason)

  def to_http_request(urlCol: Column, headersCol: Column, methodCol: Column, jsonEntityCol: Column): Column = {
    val pvd: Option[ProtocolVersionData] = None
    struct(
      struct(
        methodCol.alias("method"),
        urlCol.alias("uri"),
        typedLit(pvd).alias("protocolVersion")).alias("requestLine"),
      headersCol.alias("headers"),
      string_to_entity(jsonEntityCol).alias("entity")
    ).cast(Request)
  }

  def to_http_request(urlCol: String, headersCol: String, methodCol: String, jsonEntityCol: String): Column = {
    to_http_request(col(urlCol), col(headersCol), col(methodCol), col(jsonEntityCol))
  }

  //scalastyle:on
}
