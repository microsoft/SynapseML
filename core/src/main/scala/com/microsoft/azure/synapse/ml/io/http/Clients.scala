// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.utils.AsyncUtils
import org.apache.log4j.{LogManager, Logger}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

private[ml] trait BaseClient {

  protected type Context = Option[Any]

  protected type Client
  protected type ResponseType
  protected type RequestType

  case class ResponseWithContext(response: Option[ResponseType], context: Context) {
    def this(response: Option[ResponseType]) = this(response, None)
  }

  case class RequestWithContext(request: Option[RequestType], context: Context) {
    def this(request: Option[RequestType]) = this(request, None)
  }

  protected lazy val logger: Logger = LogManager.getLogger("BaseClient")

  protected val internalClient: Client

  def sendRequestsWithContext
  (requests: Iterator[RequestWithContext]): Iterator[ResponseWithContext]

}

private[ml] trait SingleThreadedClient extends BaseClient {

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext

  override def sendRequestsWithContext
  (requests: Iterator[RequestWithContext]): Iterator[ResponseWithContext] = {
    requests.map(sendRequestWithContext)
  }

}

abstract class AsyncClient(val concurrency: Int,
                           val timeout: Duration)
                          (implicit val ec: ExecutionContext)
  extends BaseClient {

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext

  override def sendRequestsWithContext
  (requests: Iterator[RequestWithContext]): Iterator[ResponseWithContext] = {
    val futureResponses = requests.map(r => Future {
      sendRequestWithContext(r)
    }(ExecutionContext.global)
    )
    AsyncUtils.bufferedAwait(futureResponses, concurrency, timeout)
  }
}
