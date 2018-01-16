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

  protected type Context = mutable.Map[String, Any]

  case class Response(response: CloseableHttpResponse, context: Context) {
    def this(response: CloseableHttpResponse) = this(response, mutable.Map())
  }

  case class Request(request: HttpUriRequest, context: Context) {
    def this(request: HttpUriRequest) = this(request, mutable.Map())
  }

  protected type Client = CloseableHttpClient

}

private[ml] trait BaseClient[In, Out] extends AutoCloseable with ClientTypeAliases {

  protected def sendWithRetries(request: Request,
                                retriesLeft: Seq[Long]): Response = {
    val originalResponse = Response(client.execute(request.request), request.context)
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

  protected def handle(responseWithContext: Response): Option[Response] = {
    val response = responseWithContext.response
    val code = response.getStatusLine.getStatusCode
    if (code == 429) {
      val waitTime = response.headerIterator("Retry-After")
        .nextHeader().asInstanceOf[BufferedHeader]
        .getBuffer.toString.split(" ").last.toInt
      logger.warn(s"429 response code, waiting for $waitTime s." +
        s" Consider tuning batch interval")
      Thread.sleep(waitTime.toLong * 1000)
      None
    } else if (code == 503) {
      logger.warn(s"503 response code")
      None
    } else {
      assert(response.getStatusLine.getStatusCode == 200, response.toString)
      Some(responseWithContext)
    }
  }

  protected lazy val logger: Logger = LogManager.getLogger("BatchedHTTPClient")

  protected lazy val client: Client = HttpClientBuilder.create().build()

  private[ml] def sendRequests(requests: Iterator[Request]): Iterator[Response]

  def send(messages: Iterator[In]): Iterator[Out] = {
    fromResponses(sendRequests(toRequests(messages)))
  }

  private[ml] def toRequests(inputs: Iterator[In]): Iterator[Request]

  private[ml] def fromResponses(responses: Iterator[Response]): Iterator[Out]

  override def close(): Unit = {
    client.close()
  }

  private var url: Option[String] = None

  def setUrl(value: String): Unit = {
    url = Some(value)
  }

  def getUrl: String = url.get

  private var method: Option[String] = None

  def setMethod(value: String): Unit = {
    method = Some(value)
  }

  def getMethod: String = method.get

}

private[ml] trait Unbuffered[In, Out] extends ClientTypeAliases {

  private[ml] def toRequest(message: In): Request

  private[ml] def fromResponse(response: Response): Out

  private[ml] def toRequests(messages: Iterator[In]): Iterator[Request] = {
    messages.map(toRequest)
  }

  private[ml] def fromResponses(responses: Iterator[Response]): Iterator[Out] = {
    responses.map(fromResponse)
  }

}

private[ml] trait BufferedBatching[In, Out] extends ClientTypeAliases {

  private[ml] def toBatchRequest(messages: Seq[In]): Request

  private[ml] def fromBatchResponse(response: Response): Seq[Out]

  private[ml] def toRequests(messages: Iterator[In]): Iterator[Request] = {
    val bit = new BatchIterator(messages)
    bit.start()
    bit.map(messages => if (messages.isEmpty) None else Some(toBatchRequest(messages)))
       .flatten
  }

  private[ml] def fromResponses(responses: Iterator[Response]): Iterator[Out] = {
    responses.map(r => fromBatchResponse(r)).flatten
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
      sendWithRetries(r, Seq(100L, 1000L, 2000L, 4000L))
    })
    bufferedAwait(futureResponses, concurrency, timeout)
  }

}

private[ml] trait SingleThreaded[In, Out] extends BaseClient[In, Out] {

  override private[ml] def sendRequests(requests: Iterator[Request]): Iterator[Response] = {
    requests.map(sendWithRetries(_, Seq(100L, 1000L, 2000L, 4000L)))
  }

}

private[ml] trait ColumnSchema {

  def verifyInput(input: DataType): Unit

  def transformDataType(input: DataType): DataType

}

private[ml] trait JsonInput[Out] extends BaseClient[String, Out] with ColumnSchema {

  setMethod("POST")

  private[ml] def toRequest(s: String): Request = {
    val post = getMethod match {
      case "POST" =>
        val p = new HttpPost(getUrl)
        p.setHeader("Content-type", "application/json")
        p.setEntity(new StringEntity(s))
        p
    }
    new Request(post)
  }

  private[ml] def toBatchRequest(strings: Seq[String]): Request = {
    val post = getMethod match {
      case "POST" =>
        val json = "[" + strings.mkString(",\n") + "]"
        val p = new HttpPost(getUrl)
        p.setHeader("Content-type", "application/json")
        p.setEntity(new StringEntity(json))
        p
    }
    new Request(post)
  }

  override def verifyInput(input: DataType): Unit = assert(input == StringType)

}

private[ml] trait JsonOutput[In] extends BaseClient[In, String] with ColumnSchema {

  private[ml] def fromResponse(response: Response): String = {
    val newString = IOUtils.toString(response.response.getEntity.getContent)
    newString
  }

  private[ml] def fromBatchResponse(response: Response): Seq[String] = {
    val responseString = IOUtils.toString(response.response.getEntity.getContent)
    responseString.stripPrefix("[").stripSuffix("]").split(",")
  }

  def transformDataType(input: DataType): DataType = {
    verifyInput(input)
    StringType
  }

}

private[ml] trait BinaryInput[Out] extends BaseClient[Array[Byte], Out] with ColumnSchema {

  override def verifyInput(input: DataType): Unit = assert(input == BinaryType)

  private[ml] def toRequest(message: Array[Byte]) = {
    val p = new HttpPost(getUrl)
    p.setEntity(new ByteArrayEntity(message))
    new Request(p)
  }

}

private[ml] trait TextOutput[In] extends BaseClient[In, String] with ColumnSchema {

  private[ml] def fromResponse(response: Response): String = {
    val newString = IOUtils.toString(response.response.getEntity.getContent)
    newString
  }

  def transformDataType(input: DataType): DataType = {
    verifyInput(input)
    StringType
  }

}

// Syntactic sugars for simple developer interface
private[ml] trait JsonClient extends JsonInput[String] with JsonOutput[String]

abstract class SimpleClient[In, Out] extends SingleThreaded[In, Out] with Unbuffered[In, Out]

abstract class BatchedClient[In, Out] extends SingleThreaded[In, Out] with BufferedBatching[In, Out]

abstract class AsyncClient[In, Out](override val concurrency: Int, override val timeout: Duration)
                                   (override implicit val ec: ExecutionContext)
  extends Asynchrony[In, Out] with Unbuffered[In, Out]

abstract class BatchedAsyncClient[In, Out](override val concurrency: Int,
                                           override val timeout: Duration)
                                          (override implicit val ec: ExecutionContext)
  extends Asynchrony[In, Out] with BufferedBatching[In, Out]
