// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.{SocketException, URI}

import com.microsoft.ml.spark.schema.SparkBindings
import org.apache.commons.io.IOUtils
import org.apache.http._
import org.apache.http.client.methods._
import org.apache.http.entity.{ByteArrayEntity, StringEntity}
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, typedLit, udf}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, Row}

case class HeaderData(name: String, value: String) {

  def this(h: Header) = {
    this(h.getName, h.getValue)
  }

  def toHTTPCore: Header = new BasicHeader(name, value)

}

object HeaderData  extends SparkBindings[HeaderData]

case class EntityData(content: Array[Byte],
                      contentEncoding: Option[HeaderData],
                      contentLength: Long,
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
         e.getContentLength,
         Option(e.getContentType).map(new HeaderData(_)),
         e.isChunked,
         e.isRepeatable,
         e.isStreaming)
  }

  def toHttpCore: HttpEntity = {
    val e = new ByteArrayEntity(content)
    contentEncoding.foreach { ce => e.setContentEncoding(ce.toHTTPCore) }
    assert(e.getContentLength == contentLength)
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
                            entity: EntityData,
                            statusLine: StatusLineData,
                            locale: String) {

  def this(response: CloseableHttpResponse) = {
    this(response.getAllHeaders.map(new HeaderData(_)),
         new EntityData(response.getEntity),
         new StatusLineData(response.getStatusLine),
         response.getLocale.toString)
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

object HTTPRequestData extends SparkBindings[HTTPRequestData]

object HTTPSchema {

  val response: DataType = ScalaReflection.schemaFor[HTTPResponseData].dataType
  val request: DataType = ScalaReflection.schemaFor[HTTPRequestData].dataType

  def to_http(urlCol: String, headersCol: String, methodCol: String, jsonEntityCol: String): Column = {
    to_http(col(urlCol), col(headersCol), col(methodCol), col(jsonEntityCol))
  }

  private def stringToEntity(s: String): EntityData = {
    new EntityData(new StringEntity(s))
  }

  private def entityToString(e: EntityData): Option[String] = {
    if (e.content.isEmpty) {
      None
    } else {
      Some(IOUtils.toString(e.content,
        e.contentEncoding.map(h => h.value).getOrElse("UTF-8")))
    }
  }
  val entityToStringUDF: UserDefinedFunction = {
    val fromRow = EntityData.makeFromRowConverter
    udf({ x: Row =>
      val sOpt = Option(x).map(r => entityToString(fromRow(r)))
      sOpt.orNull
    }, StringType)
  }

  val stringToEntityUDF: UserDefinedFunction = udf({ x: String => stringToEntity(x) },
    ScalaReflection.schemaFor[EntityData].dataType
  )

  def to_http(urlCol: Column, headersCol: Column, methodCol: Column, jsonEntityCol: Column): Column = {
    val pvd: Option[ProtocolVersionData] = None
    struct(
      struct(
        methodCol.alias("method"),
        urlCol.alias("uri"),
        typedLit(pvd).alias("protocolVersion")).alias("requestLine"),
      headersCol.alias("headers"),
      stringToEntityUDF(jsonEntityCol).alias("entity")
    ).cast(request)
  }

}
