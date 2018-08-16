// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import org.apache.log4j.{LogManager, Logger}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BatchIterator[T](val it: Iterator[T],
                       maxNum: Int = Integer.MAX_VALUE)
  extends Iterator[List[T]] {

  val queue: BlockingQueue[T] = new LinkedBlockingQueue[T](maxNum)
  var hasStarted = false
  val finishedLatch = new CountDownLatch(1)

  private val thread: Thread = new Thread {
    override def run(): Unit = {
      while (it.synchronized(it.hasNext)) {
        val datum = it.synchronized(it.next())
        queue.put(datum)
      }
      finishedLatch.countDown()
    }
  }

  override def hasNext: Boolean = {
    if (!hasStarted) {
      it.hasNext
    } else {
      it.synchronized(it.hasNext) ||
      !queue.isEmpty ||
      { finishedLatch.await()
        !queue.isEmpty }
      // Final clause needed to ensure the fetching thread
      // can finish before the iterator is exhausted.
      // This blocking should be kept in the final clause
      // To optimize performance
    }
  }

  def start(): Unit = {
    hasStarted = true
    thread.start()
  }

  def close(): Unit = {
    thread.interrupt()
  }

  override def next(): List[T] = {
    if (!hasStarted) start()
    assert(hasNext)
    val results = new java.util.ArrayList[T]()
    queue.drainTo(results)
    if (results.isEmpty) List(queue.take()) else results.toList
  }

}

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
    })
    AsyncUtils.bufferedAwait(futureResponses, concurrency, timeout)
  }
}
