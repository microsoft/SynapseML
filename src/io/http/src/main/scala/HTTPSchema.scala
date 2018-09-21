// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.{SocketException, URI}

import com.microsoft.ml.spark.schema.SparkBindings
import com.microsoft.ml.spark.StreamUtilities.using
import com.sun.net.httpserver.HttpExchange
import org.apache.commons.io.IOUtils
import org.apache.http._
import org.apache.http.client.methods._
import org.apache.http.entity.{ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, typedLit, udf}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, Row}

import collection.JavaConverters._
import collection.JavaConversions._

case class HeaderData(name: String, value: String) {

  def this(h: Header) = {
    this(h.getName, h.getValue)
  }

  def toHTTPCore: Header = new BasicHeader(name, value)

}

object HeaderData  extends SparkBindings[HeaderData]

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
    contentLength.foreach { cl => assert(e.getContentLength == cl)}
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
    request.sendResponseHeaders(statusLine.statusCode,
      entity.flatMap(_.contentLength).getOrElse(0L))
    entity.foreach(entity =>using(request.getResponseBody) {
      _.write(entity.content)
    }.get)
  }

}

object HTTPResponseData extends SparkBindings[HTTPResponseData]

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
                           protoclVersion: Option[ProtocolVersionData]) {

  def this(l: RequestLine) = {
    this(l.getMethod,
         l.getUri,
         Some(new ProtocolVersionData(l.getProtocolVersion)))
  }

}

object RequestLineData extends SparkBindings[RequestLineData]

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

  def toHTTPCore: HttpRequestBase = {
    val request = requestLine.method.toUpperCase match {
      case "GET"     => new HttpGet()
      case "HEAD"    => new HttpHead()
      case "DELETE"  => new HttpDelete()
      case "OPTIONS" => new HttpOptions()
      case "TRACE"   => new HttpTrace()
      case "POST"    => new HttpPost()
      case "PUT"     => new HttpPut()
      case "PATCH"   => new HttpPatch()
    }
    request match {
      case re: HttpEntityEnclosingRequestBase =>
        entity.foreach(e => re.setEntity(e.toHttpCore))
      case _ if entity.isDefined =>
        throw new IllegalArgumentException(s"Entity is defined but method is ${requestLine.method}")
      case _ =>
    }
    request.setURI(new URI(requestLine.uri))
    requestLine.protoclVersion.foreach(pv =>
      request.setProtocolVersion(pv.toHTTPCore))
    request.setHeaders(headers.map(_.toHTTPCore))
    request
  }

}

object HTTPRequestData extends SparkBindings[HTTPRequestData] {
  def fromHTTPExchange(httpe: HttpExchange): HTTPRequestData = {
    val requestHeaders = httpe.getRequestHeaders
    val isChunked = Option(requestHeaders.getFirst("Transfer-Encoding")=="chunked").getOrElse(false)
    HTTPRequestData(
      RequestLineData(
        httpe.getRequestMethod,
        httpe.getRequestURI.toString,
        Option(httpe.getProtocol).map{p =>
          val Array(v, n) = p.split("/".toCharArray.head)
          val Array(major, minor) = n.split(".".toCharArray.head)
          ProtocolVersionData(v, major.toInt, minor.toInt)
        }),
      httpe.getRequestHeaders.asScala.flatMap {
        case (k, vs) => vs.map(v => HeaderData(k,v))
      }.toArray,
      Some(EntityData(
        IOUtils.toByteArray(httpe.getRequestBody),
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

  val response: DataType = HTTPResponseData.schema
  val request: DataType = HTTPRequestData.schema

  //Convenience Functions for making and parsing HTTP objects

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

  private val entity_to_string_udf: UserDefinedFunction = {
    val fromRow = EntityData.makeFromRowConverter
    udf({ x: Row =>
      val sOpt = Option(x).flatMap(r => entityToString(fromRow(r)))
      sOpt.orNull
    }, StringType)
  }

  def entity_to_string(c: Column): Column = entity_to_string_udf(c)

  private val string_to_entity_udf: UserDefinedFunction =
    udf({ x: String => stringToEntity(x) }, EntityData.schema)

  def string_to_entity(c: Column): Column = string_to_entity_udf(c)

  private val request_to_string_udf: UserDefinedFunction = {
    val fromRow = HTTPRequestData.makeFromRowConverter
    udf({ x: Row =>
      val sOpt = Option(x)
        .flatMap(r => fromRow(r).entity)
        .map(entityToString)
      sOpt.orNull
    }, StringType)
  }

  def request_to_string(c: Column): Column = request_to_string_udf(c)

  def stringToResponse(x: String): HTTPResponseData = {
    HTTPResponseData(
      Array(),
      Some(stringToEntity(x)),
      StatusLineData(null, 200, "Success"),
      "en")
  }

  private val string_to_response_udf: UserDefinedFunction =
    udf(stringToResponse _, HTTPResponseData.schema)

  def string_to_response(c: Column): Column = string_to_response_udf(c)

  private val binary_to_response_udf: UserDefinedFunction =
    udf({ x: Array[Byte] =>
      HTTPResponseData(
        Array(),
        Some(binaryToEntity(x)),
        StatusLineData(null, 200, "Success"),
        "en")
    }, HTTPResponseData.schema)

  def binary_to_response(c: Column): Column = binary_to_response_udf(c)

  def to_http_request(urlCol: Column, headersCol: Column, methodCol: Column, jsonEntityCol: Column): Column = {
    val pvd: Option[ProtocolVersionData] = None
    struct(
      struct(
        methodCol.alias("method"),
        urlCol.alias("uri"),
        typedLit(pvd).alias("protocolVersion")).alias("requestLine"),
      headersCol.alias("headers"),
      string_to_entity(jsonEntityCol).alias("entity")
    ).cast(request)
  }

  def to_http_request(urlCol: String, headersCol: String, methodCol: String, jsonEntityCol: String): Column = {
    to_http_request(col(urlCol), col(headersCol), col(methodCol), col(jsonEntityCol))
  }

}
