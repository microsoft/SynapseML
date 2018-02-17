// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.http.client.methods.{CloseableHttpResponse, HttpUriRequest}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BufferedHeader

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

private[ml] trait HTTPClient extends BaseClient[HTTPRequestData, HTTPResponseData]
  with AutoCloseable {
  protected type Client = CloseableHttpClient
  protected type ResponseType = CloseableHttpResponse
  protected type RequestType = HttpUriRequest

  private[ml] val internalClient: Client = HttpClientBuilder.create().build()

  override def close(): Unit = {
    internalClient.close()
  }

  private[ml] def toRequest(s: ContextIn): Request = {
    Request(s.in.toRequest, s.context)
  }

  private[ml] def fromResponse(response: Response): ContextOut  = {
    ContextOut(new HTTPResponseData(response.response), response.context)
  }

}

private[ml] trait AdvancedHTTPHandling extends HTTPClient {

  val retryTimes = Seq(100L, 1000L, 2000L, 4000L)

  protected def sendWithRetries(request: Request,
                                retriesLeft: Seq[Long]
                               ): Response = {
    val originalResponse = Response(internalClient.execute(request.request), request.context)
    val response = handle(originalResponse)
    response.getOrElse({
      if (retriesLeft.isEmpty) {
        throw new IllegalArgumentException(
          s"No longer retrying. " +
            s"Code: ${originalResponse.response.getStatusLine.getStatusCode}, " +
            s"Message: $originalResponse")
      }
      Thread.sleep(retriesLeft.head)
      sendWithRetries(request, retriesLeft.tail)
    })
  }

  private[ml] override def sendInternalRequest(request: Request): Response =
    sendWithRetries(request, retryTimes)

  protected def handle(responseWithContext: Response): Option[Response] = {
    val response = responseWithContext.response
    val code = response.getStatusLine.getStatusCode
    if (code == 429) {
      val waitTime = response.headerIterator("Retry-After")
        .nextHeader().asInstanceOf[BufferedHeader]
        .getBuffer.toString.split(" ").last.toInt
      //logger.warn(s"429 response code, waiting for $waitTime s." +
      //  s" Consider tuning batch interval")
      Thread.sleep(waitTime.toLong * 1000)
      None
    } else if (code == 503) {
      //logger.warn(s"503 response code")
      None
    }else{
      assert(response.getStatusLine.getStatusCode == 200, response.toString)
      Some(responseWithContext)
    }
  }

}

private[ml] trait BasicHTTPHandling extends HTTPClient {
  private[ml] override def sendInternalRequest(request: Request): Response = {
    Response(internalClient.execute(request.request), request.context)
  }
}

abstract class AsyncHTTPClient(override val concurrency: Int, override val timeout: Duration)
                              (override implicit val ec: ExecutionContext)
  extends HTTPClient
  with Unbuffered[HTTPRequestData, HTTPResponseData]
  with Asynchrony[HTTPRequestData, HTTPResponseData]

abstract class SingleThreadedHTTPClient
  extends HTTPClient
    with Unbuffered[HTTPRequestData, HTTPResponseData]
    with SingleThreaded[HTTPRequestData, HTTPResponseData]
