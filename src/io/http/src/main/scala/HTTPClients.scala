// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.AdvancedHTTPHandling.{retryTimes, sendWithRetries}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpRequestBase, HttpUriRequest}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BufferedHeader

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait HTTPTypeAliases {
  protected type Client = CloseableHttpClient
  protected type ResponseType = CloseableHttpResponse
  protected type RequestType = HttpRequestBase

  private[ml] val internalClient: Client = HttpClientBuilder.create().build()
}

private[ml] trait HTTPClient extends BaseClient[HTTPRequestData, HTTPResponseData]
    with AutoCloseable with Unbuffered[HTTPRequestData, HTTPResponseData] with HTTPTypeAliases {

  override def close(): Unit = {
    internalClient.close()
  }

  private[ml] def toRequest(s: ContextIn): Request = {
    Request(s.in.toRequest, s.context)
  }

  private[ml] def fromResponse(response: Response): ContextOut  = {
    ContextOut(new HTTPResponseData(response.response), response.context)
  }

  def handle(client: CloseableHttpClient, request: HttpRequestBase): CloseableHttpResponse

  override def sendInternalRequest(request: Request): Response = {
    Response(handle(internalClient, request.request), request.context)
  }

}

object AdvancedHTTPHandling {

  val retryTimes = Seq(100L, 1000L, 2000L, 4000L)

  protected def sendWithRetries(client: CloseableHttpClient,
                                request: HttpRequestBase,
                                retriesLeft: Seq[Long]): CloseableHttpResponse = {
    val response = client.execute(request)
    val code = response.getStatusLine.getStatusCode
    val suceeded = code match {
        case 200 => true
        case 429 =>
          val waitTime = response.headerIterator("Retry-After")
            .nextHeader().asInstanceOf[BufferedHeader]
            .getBuffer.toString.split(" ").last.toInt
          Thread.sleep(waitTime.toLong * 1000)
          false
        case _ => false
      }
    if (suceeded) {
      response
    } else {
      Thread.sleep(retriesLeft.head)
      sendWithRetries(client, request, retriesLeft.tail)
    }
  }

  def handle(client: CloseableHttpClient, request: HttpRequestBase): CloseableHttpResponse =
    sendWithRetries(client, request, retryTimes)

}

object BasicHTTPHandling {

  def handle(client: CloseableHttpClient, request: HttpRequestBase): CloseableHttpResponse =
    client.execute(request)

}

abstract class AsyncHTTPClient(override val concurrency: Int, override val timeout: Duration)
                              (override implicit val ec: ExecutionContext)
    extends HTTPClient
    with Asynchrony[HTTPRequestData, HTTPResponseData]

abstract class SingleThreadedHTTPClient
    extends HTTPClient
    with SingleThreaded[HTTPRequestData, HTTPResponseData]
