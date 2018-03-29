// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.entity.{ByteArrayEntity, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BufferedHeader
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.collection.JavaConversions._
import scala.collection.mutable
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

private[ml] trait ClientTypeAliases {

  protected type Context = Option[Any]

  protected type Client
  protected type ResponseType
  protected type RequestType

  case class Response(response: ResponseType, context: Context) {
    def this(response: ResponseType) = this(response, None)
  }

  case class Request(request: RequestType, context: Context) {
    def this(request: RequestType) = this(request, None)
  }

}

private[ml] trait BaseClient[In, Out] extends ClientTypeAliases {

  case class ContextIn(in: In, context: Context) {
    def this(in: In) = this(in, None)
  }

  case class ContextOut(out: Out, context: Context) {
    def this(out: Out) = this(out, None)
  }

  protected lazy val logger: Logger = LogManager.getLogger("BaseClient")

  private[ml] val internalClient: Client

  private[ml] def sendRequests(requests: Iterator[Request]): Iterator[Response]

  private[ml] def sendInternalRequest(request: Request): Response

  def send(messages: Iterator[ContextIn]): Iterator[ContextOut] = {
    fromResponses(sendRequests(toRequests(messages)))
  }

  private[ml] def toRequests(inputs: Iterator[ContextIn]): Iterator[Request]

  private[ml] def fromResponses(responses: Iterator[Response]): Iterator[ContextOut]

}

private[ml] trait Unbuffered[In, Out] extends BaseClient[In, Out] {

  private[ml] def toRequest(message: ContextIn): Request

  private[ml] def fromResponse(response: Response): ContextOut

  private[ml] def toRequests(messages: Iterator[ContextIn]): Iterator[Request] = {
    messages.map(toRequest)
  }

  private[ml] def fromResponses(responses: Iterator[Response]): Iterator[ContextOut] = {
    responses.map(fromResponse)
  }

}

private[ml] trait Asynchrony[In, Out] extends BaseClient[In, Out] {

  protected def bufferedAwait[T](it: Iterator[Future[T]],
                                 concurrency: Int,
                                 timeout: Duration)
                                (implicit ec: ExecutionContext): Iterator[T] = {
    if (concurrency > 1) {
      val slidingIterator = it.sliding(concurrency - 1).withPartial(true)
      // `hasNext` will auto start the nth future in the batch
      val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
      initIterator.map(futureBatch => Await.result(futureBatch.head, timeout)) ++
        tailIterator.flatMap(lastBatch => Await.result(Future.sequence(lastBatch), timeout))
    } else if (concurrency == 1) {
      it.map(f => Await.result(f, timeout))
    } else {
      throw new IllegalArgumentException(
        s"Concurrency needs to be at least 1, got: $concurrency")
    }
  }

  val concurrency: Int
  val timeout: Duration
  implicit val ec: ExecutionContext

  override def sendRequests(requests: Iterator[Request]): Iterator[Response] = {
    val futureResponses = requests.map(r => Future {
      sendInternalRequest(r)
    })
    bufferedAwait(futureResponses, concurrency, timeout)
  }

}

private[ml] trait SingleThreaded[In, Out] extends BaseClient[In, Out] {

  override private[ml] def sendRequests(requests: Iterator[Request]): Iterator[Response] = {
    requests.map(sendInternalRequest)
  }

}

private[ml] trait ColumnSchema {

  def verifyInput(input: DataType): Unit

  def transformDataType(input: DataType): DataType

}


abstract class SimpleClient[In, Out] extends
    SingleThreaded[In, Out] with Unbuffered[In, Out]

abstract class AsyncClient[In, Out](override val concurrency: Int,
                                    override val timeout: Duration)
                                   (override implicit val ec: ExecutionContext)
    extends Asynchrony[In, Out] with Unbuffered[In, Out]
