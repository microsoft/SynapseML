// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpRequestBase}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.internal.{Logging => SparkLogging}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait Handler {

  def handle(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData

}

private[ml] trait HTTPClient extends BaseClient
  with AutoCloseable with Handler {

  override protected type Client = CloseableHttpClient
  override type ResponseType = HTTPResponseData
  override type RequestType = HTTPRequestData

  protected val internalClient: Client = HttpClientBuilder.create().build()

  override def close(): Unit = {
    internalClient.close()
  }

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext = {
    ResponseWithContext(handle(internalClient, request.request), request.context)
  }

}

object HandlingUtils extends SparkLogging {
  private[ml] def convertAndClose(response: CloseableHttpResponse): HTTPResponseData = {
    val rData = new HTTPResponseData(response)
    response.close()
    rData
  }

  type HandlerFunc = (CloseableHttpClient, HTTPRequestData) => HTTPResponseData

  private[ml] def sendWithRetries(client: CloseableHttpClient,
                              request: HttpRequestBase,
                              retriesLeft: Array[Int]): CloseableHttpResponse = {
    val response = client.execute(request)
    val code = response.getStatusLine.getStatusCode
    val suceeded = code match {
      case 200 => true
      case 201 => true
      case 202 => true
      case 429 =>
        Option(response.getFirstHeader("Retry-After"))
          .foreach{h =>
            logInfo(s"waiting ${h.getValue} on ${request match {
              case p:HttpPost => p.getURI + "   " + IOUtils.toString(p.getEntity.getContent,"UTF-8")
              case _ => request.getURI
            }}")
            Thread.sleep(h.getValue.toLong * 1000)}
        false
      case 400 =>
        true
      case _ =>
        logWarning(s"got error  $code: ${response.getStatusLine.getReasonPhrase} on ${request match {
          case p:HttpPost => p.getURI + "   " + IOUtils.toString(p.getEntity.getContent,"UTF-8")
          case _ => request.getURI
        }}")
        false
    }
    if (suceeded || retriesLeft.isEmpty) {
      response
    } else {
      response.close()
      Thread.sleep(retriesLeft.head.toLong)
      sendWithRetries(client, request, retriesLeft.tail)
    }
  }

  def advanced(retryTimes: Int*)(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData = {
    val req = request.toHTTPCore
    val message = req match {
      case r: HttpPost => IOUtils.toString(r.getEntity.getContent, "UTF-8")
      case r => r.getURI
    }
    logInfo(s"sending $message")
    val start = System.currentTimeMillis()
    val resp = sendWithRetries(client, req, retryTimes.toArray)
    logInfo(s"finished sending (${System.currentTimeMillis()-start}ms) $message")
    req.releaseConnection()
    convertAndClose(resp)
  }

  def advancedUDF(retryTimes: Int*): UserDefinedFunction =
    udf(advanced(retryTimes:_*) _, StringType)

  def basic(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData =
    convertAndClose(client.execute(request.toHTTPCore))

  def basicUDF: UserDefinedFunction = udf(basic _, StringType)
}

class AsyncHTTPClient(val handler: HandlingUtils.HandlerFunc,
                               override val concurrency: Int,
                               override val timeout: Duration)
                              (override implicit val ec: ExecutionContext)
  extends AsyncClient(concurrency, timeout)(ec) with HTTPClient {
  override def handle(client: CloseableHttpClient,
                      request: HTTPRequestData): HTTPResponseData = handler(client, request)
}

class SingleThreadedHTTPClient(val handler: HandlingUtils.HandlerFunc)
  extends HTTPClient with SingleThreadedClient {
  override def handle(client: CloseableHttpClient,
                      request: HTTPRequestData): HTTPResponseData = handler(client, request)
}
