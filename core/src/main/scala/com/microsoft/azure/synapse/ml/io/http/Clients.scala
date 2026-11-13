// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.utils.AsyncUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.TaskContext

import java.util.concurrent.CancellationException
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

object AsyncClient {
  private val Owner = new ThreadLocal[TaskContext]()

  private[ml] def currentTaskContext: Option[TaskContext] =
    Option(Owner.get()).orElse(Option(TaskContext.get()))

  private[ml] def isTaskCancelled: Boolean =
    currentTaskContext.exists(task => task.isInterrupted() || task.isCompleted())

  private def withTaskContext[T](task: Option[TaskContext])(body: => T): T = {
    val previous = Owner.get()
    Owner.set(task.orNull)
    try {
      if (isTaskCancelled) throw new CancellationException("The owning Spark task has ended.")
      body
    } catch {
      case error: InterruptedException =>
        // Future does not complete its promise when InterruptedException escapes.
        Thread.interrupted()
        throw new CancellationException("The asynchronous request was interrupted.").initCause(error)
    } finally {
      if (previous == null) Owner.remove() else Owner.set(previous)
    }
  }
}

abstract class AsyncClient(val concurrency: Int,
                           val timeout: Duration)
                          (implicit val ec: ExecutionContext)
  extends BaseClient {

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext

  override def sendRequestsWithContext
  (requests: Iterator[RequestWithContext]): Iterator[ResponseWithContext] = {
    val task = AsyncClient.currentTaskContext
    val futureResponses = requests.map(r => Future {
      AsyncClient.withTaskContext(task) {
        sendRequestWithContext(r)
      }
    }(ec)
    )
    AsyncUtils.bufferedAwait(futureResponses, concurrency, timeout)
  }
}
