// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase, HttpRequestBase}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.blocking
import scala.util.Try

object RESTHelpers {
  lazy val RequestTimeout = 60000

  lazy val RequestConfigVal: RequestConfig = RequestConfig.custom()
    .setConnectTimeout(RequestTimeout)
    .setConnectionRequestTimeout(RequestTimeout)
    .setSocketTimeout(RequestTimeout)
    .build()

  lazy val ConnectionManager: PoolingHttpClientConnectionManager = {
    val cm = new PoolingHttpClientConnectionManager()
    cm.setDefaultMaxPerRoute(Int.MaxValue)
    cm.setMaxTotal(Int.MaxValue)
    cm
  }

  lazy val Client: CloseableHttpClient = HttpClientBuilder
    .create().setConnectionManager(ConnectionManager)
    .setDefaultRequestConfig(RequestConfigVal).build()

  @tailrec
  def retry[T](backoffs: List[Int], f: () => T): T = {
    try {
      f()
    } catch {
      case t: Throwable =>
        val waitTime = backoffs.headOption.getOrElse(throw t)
        println(s"Caught error: $t with message ${t.getMessage}, waiting for $waitTime")
        blocking {
          Thread.sleep(waitTime.toLong)
        }
        retry(backoffs.tail, f)
    }
  }

  def safeSend(request: HttpRequestBase,
               backoffs: List[Int] = List(100, 500, 1000), //scalastyle:ignore magic.number
               expectedCodes: Set[Int] = Set(),
               close: Boolean = true): CloseableHttpResponse = {
    safeSendImpl(request, backoffs, expectedCodes, close, redactErrorBodies = false)
  }

  def safeSendRedactingBodies(
      request: HttpRequestBase,
      backoffs: List[Int] = List(100, 500, 1000), //scalastyle:ignore magic.number
      expectedCodes: Set[Int] = Set(),
      close: Boolean = true): CloseableHttpResponse = {
    safeSendImpl(request, backoffs, expectedCodes, close, redactErrorBodies = true)
  }

  private def safeSendImpl(request: HttpRequestBase,
                           backoffs: List[Int],
                           expectedCodes: Set[Int],
                           close: Boolean,
                           redactErrorBodies: Boolean): CloseableHttpResponse = {
    retry(backoffs, { () =>
      val response = Client.execute(request)
      try {
        if (response.getStatusLine.getStatusCode.toString.startsWith("2") ||
          expectedCodes(response.getStatusLine.getStatusCode)
        ) {
          response
        } else {
          val requestBodyOpt = if (redactErrorBodies) {
            "<redacted>"
          } else {
            Try(request match {
              case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
              case _ => ""
            }).get
          }

          val responseBodyOpt = if (redactErrorBodies) {
            "<redacted>"
          } else {
            Try(IOUtils.toString(response.getEntity.getContent, "UTF-8")).getOrElse("")
          }

          throw new RuntimeException(
            s"Failed: " +
              s"\n\t response: $response " +
              s"\n\t requestUrl: ${request.getURI}" +
              s"\n\t requestBody: $requestBodyOpt" +
              s"\n\t responseBody: $responseBodyOpt")
        }
      } catch {
        case e: Exception =>
          response.close()
          throw e
      } finally {
        if (close) {
          response.close()
        }
      }
    })
  }

  def parseResult(result: CloseableHttpResponse): String = {
    IOUtils.toString(result.getEntity.getContent, "utf-8")
  }

  def sendAndParseJson(request: HttpRequestBase,
                       expectedCodes: Set[Int] = Set(),
                       backoffs: List[Int] = List(100, 500, 1000) //scalastyle:ignore magic.number
                      ): JsValue = {
    sendAndParseJsonImpl(request, expectedCodes, backoffs, redactErrorBodies = false)
  }

  def sendAndParseJsonRedactingBodies(
      request: HttpRequestBase,
      expectedCodes: Set[Int] = Set(),
      backoffs: List[Int] = List(100, 500, 1000) //scalastyle:ignore magic.number
      ): JsValue = {
    sendAndParseJsonImpl(request, expectedCodes, backoffs, redactErrorBodies = true)
  }

  private def sendAndParseJsonImpl(request: HttpRequestBase,
                                   expectedCodes: Set[Int],
                                   backoffs: List[Int],
                                   redactErrorBodies: Boolean): JsValue = {
    val response = safeSendImpl(
      request,
      expectedCodes = expectedCodes,
      close = false,
      backoffs = backoffs,
      redactErrorBodies = redactErrorBodies)
    val output = parseResult(response).parseJson
    response.close()
    output
  }

}
