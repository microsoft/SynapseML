// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.fabric.FabricClient
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.commons.io.IOUtils
import org.apache.http.{Header, HttpEntity, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.utils.DateUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, BasicHttpEntity, ByteArrayEntity, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHttpResponse
import org.apache.spark.injections.UDFUtils
import org.apache.spark.internal.{Logging => SparkLogging}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CancellationException, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.{Random, Try}
import scala.util.control.NonFatal

trait Handler {

  def handle(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData

}

private[ml] trait HTTPClient extends BaseClient
  with AutoCloseable with Handler {

  override protected type Client = CloseableHttpClient
  override type ResponseType = HTTPResponseData
  override type RequestType = HTTPRequestData

  protected val requestTimeout: Int

  protected val requestConfig: RequestConfig = RequestConfig.custom()
    .setConnectTimeout(requestTimeout)
    .setConnectionRequestTimeout(requestTimeout)
    .setSocketTimeout(requestTimeout)
    .build()

  protected val connectionManager: PoolingHttpClientConnectionManager = {
    val cm = new PoolingHttpClientConnectionManager()
    cm.setDefaultMaxPerRoute(Int.MaxValue) // Spark will handle the threading to avoid going over limits
    cm.setMaxTotal(Int.MaxValue)
    cm
  }

  protected val internalClient: Client = HttpClientBuilder.create()
    .setConnectionManager(connectionManager)
    .setDefaultRequestConfig(requestConfig).build()

  private val cancellableClient: Client = HttpClientBuilder.create()
    .setConnectionManager(connectionManager)
    .setConnectionManagerShared(true)
    .setDefaultRequestConfig(RequestConfig.copy(requestConfig).setNormalizeUri(false).build())
    .disableAutomaticRetries()
    .disableCookieManagement()
    .disableRedirectHandling().build()

  override def close(): Unit = {
    try cancellableClient.close() finally internalClient.close()
  }

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext =
    sendRequestWithClient(request, internalClient)

  private def sendRequestWithClient(request: RequestWithContext, client: Client): ResponseWithContext = {
    request.request.map(req =>
      ResponseWithContext(Some(handle(client, req)), request.context)
    ).getOrElse(ResponseWithContext(None, request.context))
  }

  // Outside a Spark task, callers must close the iterator if they do not consume it fully.
  private[ml] def sendCancellableRequestsWithContext(requests: Iterator[RequestWithContext],
                                                     concurrency: Int,
                                                     timeout: Duration): Iterator[ResponseWithContext]
    with AutoCloseable =
    CancellableRequests.map(requests, concurrency, timeout)(request =>
      sendRequestWithClient(request, cancellableClient))

}

object HandlingUtils extends SparkLogging {
  private class BufferedResponseEntity(val bytes: Array[Byte]) extends ByteArrayEntity(bytes)

  private[ml] def convertAndClose(response: CloseableHttpResponse): HTTPResponseData = {
    try {
      response.getEntity match {
        case entity: BufferedResponseEntity =>
          HTTPResponseData(response.getAllHeaders.map(new HeaderData(_)),
            Some(EntityData(entity.bytes, Option(entity.getContentEncoding).map(new HeaderData(_)),
              Some(entity.getContentLength), Option(entity.getContentType).map(new HeaderData(_)),
              entity.isChunked, entity.isRepeatable, entity.isStreaming)),
            new StatusLineData(response.getStatusLine), response.getLocale.toString)
        case _ => new HTTPResponseData(response)
      }
    } finally response.close()
  }

  type HandlerFunc = (CloseableHttpClient, HTTPRequestData) => HTTPResponseData

  private def retryPause(milliseconds: Long, boundedPolicy: Boolean): Unit = {
    if (!boundedPolicy) Thread.sleep(milliseconds) else {
      require(milliseconds >= 0, "Retry delay cannot be negative")
      val duration = TimeUnit.MILLISECONDS.toNanos(milliseconds)
      val started = System.nanoTime()
      @tailrec
      def waitRemaining(): Unit = {
        CancellableRequests.check()
        val remaining = duration - (System.nanoTime() - started)
        if (remaining > 0) {
          TimeUnit.NANOSECONDS.sleep(remaining)
          waitRemaining()
        }
      }
      waitRemaining()
    }
  }

  private def keepTrying(client: CloseableHttpClient,
                         request: HttpRequestBase,
                         retriesLeft: Array[Int],
                         e: Throwable,
                         extraCodesToRetry: Set[Int] = Set(),
                         backoff429Ms: Long = 0,
                         boundedPolicy: Boolean = false,
                         quiet: Boolean = false,
                         refreshAuth: Option[String => String] = None): CloseableHttpResponse = {
    if (retriesLeft.isEmpty) {
      throw e
    } else {
      retryPause(retriesLeft.head.toLong, boundedPolicy)
      sendWithRetryPolicy(client, request, retriesLeft.tail, extraCodesToRetry, backoff429Ms,
        boundedPolicy, quiet, refreshAuth)
    }
  }

  private val MaxBackoffMs: Long = 60000L // 1 minute cap for 429 backoff
  private val MaxRetryAfterLength = 64
  private val MaxResponseInspectionBytes = 1024 * 1024L

  private def copyResponseEntityMetadata(source: HttpEntity, target: AbstractHttpEntity): Unit = {
    Option(source.getContentEncoding).foreach(target.setContentEncoding)
    Option(source.getContentType).foreach(target.setContentType)
    target.setChunked(source.isChunked)
  }

  private def preserveResponseReadFailure(buffered: BasicHttpResponse, redirect: Boolean): Unit = {
    val bodyRead = buffered.getEntity != null
    val message = if (bodyRead) {
      "Could not read the HTTP response metadata; preserving its buffered body and status."
    } else if (redirect) {
      "Could not read the HTTP redirect response body; preserving its status without retrying."
    } else {
      "Could not read the HTTP error response body; preserving its status and available retry headers."
    }
    logWarning(message)
    buffered.setLocale(java.util.Locale.ROOT)
    if (!bodyRead) buffered.setEntity(new StringEntity(message, StandardCharsets.UTF_8))
  }

  private def bufferAndClose(response: CloseableHttpResponse): CloseableHttpResponse = {
    try {
      val buffered = new BasicHttpResponse(response.getStatusLine) with CloseableHttpResponse {
        override def close(): Unit = ()
      }
      val code = buffered.getStatusLine.getStatusCode
      val redirect = code >= HttpStatus.SC_MULTIPLE_CHOICES && code < HttpStatus.SC_BAD_REQUEST
      var retryAfter = Option.empty[Header]
      try {
        retryAfter = Option(response.getFirstHeader("Retry-After"))
          .filter(h => code >= HttpStatus.SC_BAD_REQUEST &&
            retryAfterMillis(h.getValue, System.currentTimeMillis()).isDefined)
        buffered.setHeaders(response.getAllHeaders)
        buffered.setLocale(response.getLocale)
        Option(response.getEntity).foreach { entity =>
          val bytes = new BufferedResponseEntity(IOUtils.toByteArray(entity.getContent))
          buffered.setEntity(bytes)
          copyResponseEntityMetadata(entity, bytes)
        }
      } catch {
        case _: java.io.IOException if redirect || code >= HttpStatus.SC_BAD_REQUEST =>
          CancellableRequests.check()
          retryAfter.foreach(buffered.setHeader)
          preserveResponseReadFailure(buffered, redirect)
      }
      buffered
    } finally {
      try response.close() catch {
        case _: java.io.IOException => logWarning("Could not close the HTTP response.")
      }
    }
  }

  private def replayResponseEntity(response: CloseableHttpResponse,
                                   source: HttpEntity,
                                   bytes: Array[Byte],
                                   input: InputStream): Unit = {
    val replay = new BasicHttpEntity()
    replay.setContent(new SequenceInputStream(new ByteArrayInputStream(bytes), input))
    replay.setContentLength(source.getContentLength)
    copyResponseEntityMetadata(source, replay)
    response.setEntity(replay)
  }

  private def closeInspectionInput(input: InputStream): Unit = {
    try {
      input.close()
    } catch {
      case NonFatal(error) =>
        logWarning("Could not close the HTTP response inspection stream.", error)
    }
  }

  private[ml] def responseBodyForInspection(response: CloseableHttpResponse): Option[String] = {
    Option(response.getEntity).flatMap {
      case entity: BufferedResponseEntity =>
        Some(new String(entity.bytes, 0,
          math.min(entity.bytes.length.toLong, MaxResponseInspectionBytes).toInt, StandardCharsets.UTF_8))
      case entity =>
      val output = new ByteArrayOutputStream()
      var input = Option.empty[InputStream]
      var keepInputOpen = false
      try {
        val responseInput = entity.getContent
        input = Some(responseInput)
        IOUtils.copyLarge(responseInput, output, 0, MaxResponseInspectionBytes + 1)
        val bytes = output.toByteArray
        if (bytes.length > MaxResponseInspectionBytes) {
          replayResponseEntity(response, entity, bytes, responseInput)
          keepInputOpen = true
          Some(new String(bytes, 0, MaxResponseInspectionBytes.toInt, StandardCharsets.UTF_8))
        } else {
          val bufferedEntity = new ByteArrayEntity(bytes)
          copyResponseEntityMetadata(entity, bufferedEntity)
          response.setEntity(bufferedEntity)
          Some(new String(bytes, StandardCharsets.UTF_8))
        }
      } catch {
        case NonFatal(error) =>
          logWarning("Could not inspect the HTTP response body; preserving it for retry handling.", error)
          input.foreach { responseInput =>
            try {
              replayResponseEntity(response, entity, output.toByteArray, responseInput)
              keepInputOpen = true
            } catch {
              case NonFatal(replayError) =>
                error.addSuppressed(replayError)
                logWarning("Could not reconstruct the partially inspected HTTP response body.", replayError)
            }
          }
          None
      } finally {
        if (!keepInputOpen) {
          input.foreach(closeInspectionInput)
        }
      }
    }
  }

  private def capacityLimitExceeded(response: CloseableHttpResponse): Boolean =
    responseBodyForInspection(response).exists(_.contains("CapacityLimitExceeded"))

  private[ml] def previewMessage(previewRequest: HttpRequestBase): String = {
    try {
      previewRequest match {
        case request: HttpPost =>
          Option(request.getEntity).map { entity =>
            Try {
              val input = entity.getContent
              try {
                IOUtils.toString(input, "UTF-8")
              } finally {
                input.close()
              }
            }.getOrElse("")
          }.getOrElse("")
        case request =>
          request.getURI.toString
      }
    } finally {
      previewRequest.releaseConnection()
    }
  }

  //scalastyle:off cyclomatic.complexity
  //scalastyle:off method.length
  private[ml] def sendWithRetries(client: CloseableHttpClient,
                                  request: HttpRequestBase,
                                  retriesLeft: Array[Int],
                                  extraCodesToRetry: Set[Int] = Set(),
                                  backoff429Ms: Long = 0
                                 ): CloseableHttpResponse =
    sendWithRetryPolicy(client, request, retriesLeft, extraCodesToRetry, backoff429Ms,
      boundedPolicy = false, quiet = false)

  private[ml] def retryAfterMillis(value: String, nowMillis: Long): Option[Long] =
    Option(value).filter(_.length <= MaxRetryAfterLength).flatMap { hint =>
      Try(BigDecimal(hint) * 1000).toOption.filter(_ >= 0)
        .map { millis =>
          if (millis == 0) 0L
          else millis.max(BigDecimal(1)).min(BigDecimal(Long.MaxValue))
            .setScale(0, BigDecimal.RoundingMode.CEILING).toLong
        }
        .orElse(Try(Option(DateUtils.parseDate(hint))).toOption.flatten
          .map(date => math.max(0L, date.getTime - nowMillis)))
    }

  private[ml] def sendWithRetryPolicy(client: CloseableHttpClient,
                                  request: HttpRequestBase,
                                  retriesLeft: Array[Int],
                                  extraCodesToRetry: Set[Int],
                                  backoff429Ms: Long,
                                  boundedPolicy: Boolean,
                                  quiet: Boolean,
                                  refreshAuth: Option[String => String] = None): CloseableHttpResponse = {
    require(boundedPolicy || refreshAuth.isEmpty, "Authentication replay requires the bounded retry policy")
    if (boundedPolicy) CancellableRequests.check()
    var executingRequest = true
    try {
      val received = client.execute(request)
      // Read failures belong to this attempt's retry budget, not to successful response decoding.
      val response = if (boundedPolicy) bufferAndClose(received) else received
      if (boundedPolicy) CancellableRequests.check()
      executingRequest = false
      val code = response.getStatusLine.getStatusCode
      lazy val boundedRetryAfterMs = if (boundedPolicy) {
        Option(response.getFirstHeader("Retry-After"))
          .flatMap(h => retryAfterMillis(h.getValue, System.currentTimeMillis()))
      } else None
      //scalastyle:off magic.number
      if (code == 401 && refreshAuth.isDefined && retriesLeft.nonEmpty) {
        if (boundedRetryAfterMs.exists(_ > MaxBackoffMs)) {
          response
        } else {
          response.close()
          boundedRetryAfterMs.foreach(delay => retryPause(delay, boundedPolicy))
          val rejected = Option(request.getFirstHeader("Authorization")).map(_.getValue).getOrElse("")
          val refreshed = try Some(refreshAuth.get(rejected)) catch {
            case canceled: CancellationException => throw canceled
            case NonFatal(_) if boundedPolicy =>
              CancellableRequests.check()
              logWarning("Fabric authentication refresh failed; returning the rejected HTTP response")
              None
          }
          refreshed match {
            case Some(auth) =>
              request.setHeader("Authorization", auth)
              sendWithRetryPolicy(client, request, retriesLeft.tail, extraCodesToRetry, backoff429Ms,
                boundedPolicy, quiet, None)
            case None => response
          }
        }
      } else {
        val dontRetry = code match {
          case code if boundedPolicy && code >= 200 && code < 400 => true
          case 200 => true
          case 201 => true
          case 202 => true
          case 429 =>
            // Inspect body to distinguish capacity errors from transient rate limits.
            if (capacityLimitExceeded(response)) {
              // Fabric capacity-exceeded 429s are NOT transient rate limits —
              // retrying will not help and causes hangs
              if (!quiet) logWarning(s"Capacity limit exceeded (non-retryable 429) on ${request.getURI}")
              true
            } else {
              Option(response.getFirstHeader("Retry-After")).filter(_ => !quiet)
                .foreach { h =>
                  logInfo(s"waiting ${h.getValue} on ${
                    request match {
                      case p: HttpPost => p.getURI + "   " +
                        Try(IOUtils.toString(p.getEntity.getContent, "UTF-8")).getOrElse("")
                      case _ => request.getURI
                    }
                  }")
                }
              false
            }
          case code =>
            if (!quiet) logWarning(s"got error  $code: ${response.getStatusLine.getReasonPhrase} on ${
              request match {
                case p: HttpPost => p.getURI + "   " +
                  Try(IOUtils.toString(p.getEntity.getContent, "UTF-8")).getOrElse("")
                case _ => request.getURI
              }
            }")

            if (extraCodesToRetry(code)) {
              false
            } else {
              code.toString.startsWith("4") // Retry only when code isn't a 4XX
            }
        }
        //scalastyle:on magic.number
        if (dontRetry || retriesLeft.isEmpty) {
          response
        } else {
          // Bounded requests honor seconds and HTTP dates, terminating rather than retrying a long hint early.
          // Legacy 429 handling caps numeric hints and falls back to exponential backoff for HTTP dates.
          // Retry-After: 0 means "retry immediately" (RFC 7231).
          val retryAfterMs = if (boundedPolicy) {
            boundedRetryAfterMs
          } else if (code == 429) {
            Option(response.getFirstHeader("Retry-After"))
              .flatMap(h => Try(h.getValue.toLong * 1000).toOption)
              .filter(_ >= 0)
              .map(math.min(_, MaxBackoffMs))
          } else None
          if (boundedPolicy && retryAfterMs.exists(_ > MaxBackoffMs)) {
            response
          } else {
            response.close()
            if (code == 429) { // Legacy callers do not count rate limiting in number of failures.
              val baseBackoff = retryAfterMs.getOrElse {
                // No usable Retry-After header; use exponential backoff
                val current = math.max(backoff429Ms, retriesLeft.head.toLong)
                math.min(current * 2, MaxBackoffMs)
              }
              // Add jitter (up to 10% of base) to prevent thundering herd
              val jitter = Random.nextInt(math.max((baseBackoff / 10).toInt, 1))
              val sleepMs = math.min(baseBackoff + jitter, MaxBackoffMs)
              val source = if (retryAfterMs.isDefined) "Retry-After header" else "exponential backoff"
              if (!quiet) logInfo(s"429 rate-limited on ${request.getURI}: " +
                s"sleeping ${sleepMs}ms ($source, jitter=${jitter}ms)")
              retryPause(sleepMs, boundedPolicy)
              sendWithRetryPolicy(client, request, if (boundedPolicy) retriesLeft.tail else retriesLeft,
                extraCodesToRetry, baseBackoff, boundedPolicy, quiet, refreshAuth)
            } else {
              retryPause(if (boundedPolicy) retryAfterMs.getOrElse(retriesLeft.head.toLong)
                else retriesLeft.head.toLong, boundedPolicy)
              sendWithRetryPolicy(client, request, retriesLeft.tail, extraCodesToRetry, 0,
                boundedPolicy, quiet, refreshAuth)
            }
          }
        }
      }
    } catch {
      case e: java.io.IOException if executingRequest || !boundedPolicy =>
        if (quiet) logWarning("HTTP connection failed") else logError("Encountering a connection error", e)
        keepTrying(client, request, retriesLeft, e, extraCodesToRetry, backoff429Ms,
          boundedPolicy, quiet, refreshAuth)
    }
  }
  //scalastyle:on method.length
  //scalastyle:on cyclomatic.complexity

  //scalastyle:off cyclomatic.complexity
  //scalastyle:off method.length
  //scalastyle:off magic.number
  private[ml] def sendWithFabricAuthRetries(
      client: CloseableHttpClient,
      requestData: HTTPRequestData,
      retriesLeft: Array[Int],
      extraCodesToRetry: Set[Int] = Set(),
      getAuthHeader: () => String = () => FabricClient.getCognitiveMWCTokenAuthHeader,
      refreshAuthHeader: String => String = FabricClient.refreshCognitiveMWCTokenAuthHeader,
      backoff429Ms: Long = 0,
      authRetryUsed: Boolean = false,
      authOverride: Option[String] = None): (CloseableHttpResponse, HttpRequestBase) = {
    val request = requestData.toHTTPCore
    val authHeader = authOverride.getOrElse(getAuthHeader())
    request.removeHeaders("Authorization")
    request.setHeader("Authorization", authHeader)
    var executingRequest = true
    try {
      val response = client.execute(request)
      executingRequest = false
      val code = response.getStatusLine.getStatusCode
      val capacityLimitExceededResponse = code == 429 && capacityLimitExceeded(response)

      val successful = Set(200, 201, 202)(code)
      val retryable = if (code == 429) {
        !capacityLimitExceededResponse
      } else if (code == 401) {
        false
      } else if (extraCodesToRetry(code)) {
        true
      } else {
        !code.toString.startsWith("4")
      }

      if (code == 401 && !authRetryUsed) {
        response.close()
        request.releaseConnection()
        val refreshedAuthHeader = refreshAuthHeader(authHeader)
        sendWithFabricAuthRetries(
          client,
          requestData,
          retriesLeft,
          extraCodesToRetry,
          getAuthHeader,
          refreshAuthHeader,
          backoff429Ms,
          authRetryUsed = true,
          authOverride = Some(refreshedAuthHeader))
      } else if (successful || !retryable || retriesLeft.isEmpty) {
        if (capacityLimitExceededResponse) {
          logWarning(s"Capacity limit exceeded (non-retryable 429) on ${request.getURI}")
        }
        response -> request
      } else {
        val retryAfterMs = if (code == 429) {
          Option(response.getFirstHeader("Retry-After"))
            .flatMap(h => Try(h.getValue.toLong * 1000).toOption)
            .filter(_ >= 0)
            .map(math.min(_, MaxBackoffMs))
        } else {
          None
        }
        response.close()
        request.releaseConnection()
        if (code == 429) {
          val baseBackoff = retryAfterMs.getOrElse {
            val current = math.max(backoff429Ms, retriesLeft.head.toLong)
            math.min(current * 2, MaxBackoffMs)
          }
          val jitter = Random.nextInt(math.max((baseBackoff / 10).toInt, 1))
          Thread.sleep(math.min(baseBackoff + jitter, MaxBackoffMs))
          sendWithFabricAuthRetries(
            client,
            requestData,
            retriesLeft.tail,
            extraCodesToRetry,
            getAuthHeader,
            refreshAuthHeader,
            baseBackoff,
            authRetryUsed)
        } else {
          Thread.sleep(retriesLeft.head.toLong)
          sendWithFabricAuthRetries(
            client,
            requestData,
            retriesLeft.tail,
            extraCodesToRetry,
            getAuthHeader,
            refreshAuthHeader,
            authRetryUsed = authRetryUsed)
        }
      }
    } catch {
      case e: java.io.IOException if executingRequest =>
        request.releaseConnection()
        if (retriesLeft.isEmpty) {
          throw e
        }
        logError("Encountering a connection error", e)
        Thread.sleep(retriesLeft.head.toLong)
        sendWithFabricAuthRetries(
          client,
          requestData,
          retriesLeft.tail,
          extraCodesToRetry,
          getAuthHeader,
          refreshAuthHeader,
          backoff429Ms,
          authRetryUsed)
    }
  }
  //scalastyle:on magic.number
  //scalastyle:on method.length
  //scalastyle:on cyclomatic.complexity

  def advanced(retryTimes: Int*)(client: CloseableHttpClient,
                                 request: HTTPRequestData): HTTPResponseData = {
    try {
      SynapseMLLogging.logDebug(s"sending ${previewMessage(request.toHTTPCore)}")
      val start = System.currentTimeMillis()
      val usesTrustedFabricAuth = request.usesFabricAuth &&
        FabricClient.isOpenAIEndpoint(request.requestLine.uri)
      val (resp, req) = if (usesTrustedFabricAuth) {
        sendWithFabricAuthRetries(
          client,
          request,
          retryTimes.toArray,
          authOverride = request.authorizationHeader)
      } else {
        val httpRequest = request.toHTTPCore
        sendWithRetries(client, httpRequest, retryTimes.toArray) -> httpRequest
      }
      SynapseMLLogging.logMessage(
        s"finished sending to ${req.getURI} took (${System.currentTimeMillis() - start}ms)")
      val respData = convertAndClose(resp)
      req.releaseConnection()
      respData
    } catch {
      case e: Exception =>
        logError(s"Encountered Unknown exception while sending payload", e)
        null //scalastyle:ignore null
    }
  }

  def advancedUDF(retryTimes: Int*): UserDefinedFunction =
    UDFUtils.oldUdf(advanced(retryTimes: _*) _, StringType)

  /** Internal retry policy for the cancellable client; interruption and programming errors propagate. */
  private[ml] def bounded(retryTimes: Int*)(client: CloseableHttpClient,
                                          request: HTTPRequestData): HTTPResponseData =
    boundedWithFabricAuth(retryTimes,
      uri => FabricClient.isOpenAIEndpoint(uri),
      rejected => FabricClient.refreshCognitiveMWCTokenAuthHeader(rejected))(client, request)

  private[ml] def boundedWithFabricAuth(retryTimes: Seq[Int],
                                      isFabricEndpoint: String => Boolean,
                                      refreshFabricAuth: String => String)(
      client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData = {
    CancellableRequests.check()
    val httpRequest = request.toHTTPCore
    val refresh = if (request.usesFabricAuth && isFabricEndpoint(request.requestLine.uri)) {
      Some(refreshFabricAuth)
    } else None
    try {
      CancellableRequests.withRequest(httpRequest) {
        convertAndClose(sendWithRetryPolicy(client, httpRequest, retryTimes.toArray, Set.empty, 0,
          boundedPolicy = true, quiet = true, refreshAuth = refresh))
      }
    } catch {
      case _: java.io.IOException =>
        CancellableRequests.check()
        logWarning("HTTP connection failed after retry exhaustion")
        Option.empty[HTTPResponseData].orNull
    } finally {
      httpRequest.releaseConnection()
    }
  }

  def basic(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData = {
    val req = request.toHTTPCore
    val data = convertAndClose(client.execute(req))
    req.releaseConnection()
    data
  }

  def basicUDF: UserDefinedFunction = UDFUtils.oldUdf(basic _, StringType)
}

class AsyncHTTPClient(val handler: HandlingUtils.HandlerFunc,
                      override val concurrency: Int,
                      override val timeout: Duration,
                      val requestTimeout: Int)
                     (override implicit val ec: ExecutionContext)
  extends AsyncClient(concurrency, timeout)(ec) with HTTPClient {
  override def handle(client: CloseableHttpClient,
                      request: HTTPRequestData): HTTPResponseData = blocking {
    handler(client, request)
  }
}

class SingleThreadedHTTPClient(val handler: HandlingUtils.HandlerFunc, val requestTimeout: Int)
  extends HTTPClient with SingleThreadedClient {
  override def handle(client: CloseableHttpClient,
                      request: HTTPRequestData): HTTPResponseData = blocking {
    handler(client, request)
  }
}
