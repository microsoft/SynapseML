// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.fabric.FabricClient
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.commons.io.IOUtils
import org.apache.http.HttpEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, BasicHttpEntity, ByteArrayEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.spark.injections.UDFUtils
import org.apache.spark.internal.{Logging => SparkLogging}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets
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

  override def close(): Unit = {
    internalClient.close()
  }

  protected def sendRequestWithContext(request: RequestWithContext): ResponseWithContext = {
    request.request.map(req =>
      ResponseWithContext(Some(handle(internalClient, req)), request.context)
    ).getOrElse(ResponseWithContext(None, request.context))
  }

}

object HandlingUtils extends SparkLogging {
  private[ml] def convertAndClose(response: CloseableHttpResponse): HTTPResponseData = {
    val rData = new HTTPResponseData(response)
    response.close()
    rData
  }

  type HandlerFunc = (CloseableHttpClient, HTTPRequestData) => HTTPResponseData

  private def keepTrying(client: CloseableHttpClient,
                         request: HttpRequestBase,
                         retriesLeft: Array[Int],
                         e: Throwable,
                         extraCodesToRetry: Set[Int] = Set(),
                         backoff429Ms: Long = 0): CloseableHttpResponse = {
    if (retriesLeft.isEmpty) {
      throw e
    } else {
      Thread.sleep(retriesLeft.head.toLong)
      sendWithRetries(client, request, retriesLeft.tail, extraCodesToRetry, backoff429Ms)
    }
  }

  private val MaxBackoffMs: Long = 60000L // 1 minute cap for 429 backoff
  private val MaxResponseInspectionBytes = 1024 * 1024L

  private def copyResponseEntityMetadata(source: HttpEntity, target: AbstractHttpEntity): Unit = {
    Option(source.getContentEncoding).foreach(target.setContentEncoding)
    Option(source.getContentType).foreach(target.setContentType)
    target.setChunked(source.isChunked)
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
    Option(response.getEntity).flatMap { entity =>
      if (entity.getContentLength > MaxResponseInspectionBytes) {
        None
      } else {
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
            None
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
                                 ): CloseableHttpResponse = {
    try {
      val response = client.execute(request)
      val code = response.getStatusLine.getStatusCode
      //scalastyle:off magic.number
      val dontRetry = code match {
        case 200 => true
        case 201 => true
        case 202 => true
        case 429 =>
          // Inspect body to distinguish capacity errors from transient rate limits.
          if (capacityLimitExceeded(response)) {
            // Fabric capacity-exceeded 429s are NOT transient rate limits —
            // retrying will not help and causes hangs
            logWarning(s"Capacity limit exceeded (non-retryable 429) on ${request.getURI}")
            true
          } else {
            Option(response.getFirstHeader("Retry-After"))
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
          logWarning(s"got error  $code: ${response.getStatusLine.getReasonPhrase} on ${
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
        // Retry-After may be delta-seconds (e.g. "120") or an HTTP-date.
        // Parse as Long; if it's an HTTP-date or otherwise non-numeric, fall back to exponential backoff.
        // Cap server-provided values to MaxBackoffMs; Retry-After: 0 means "retry immediately" (RFC 7231).
        val retryAfterMs = if (code == 429) {
          Option(response.getFirstHeader("Retry-After"))
            .flatMap(h => Try(h.getValue.toLong * 1000).toOption)
            .filter(_ >= 0)
            .map(math.min(_, MaxBackoffMs))
        } else None
        response.close()
        if (code == 429) { // Do not count rate limiting in number of failures
          val baseBackoff = retryAfterMs.getOrElse {
            // No usable Retry-After header; use exponential backoff
            val current = math.max(backoff429Ms, retriesLeft.head.toLong)
            math.min(current * 2, MaxBackoffMs)
          }
          // Add jitter (up to 10% of base) to prevent thundering herd
          val jitter = Random.nextInt(math.max((baseBackoff / 10).toInt, 1))
          val sleepMs = math.min(baseBackoff + jitter, MaxBackoffMs)
          val source = if (retryAfterMs.isDefined) "Retry-After header" else "exponential backoff"
          logInfo(s"429 rate-limited on ${request.getURI}: " +
            s"sleeping ${sleepMs}ms ($source, jitter=${jitter}ms)")
          Thread.sleep(sleepMs)
          sendWithRetries(client, request, retriesLeft, extraCodesToRetry, baseBackoff)
        } else {
          Thread.sleep(retriesLeft.head.toLong)
          sendWithRetries(client, request, retriesLeft.tail, extraCodesToRetry)
        }
      }
    } catch {
      case e: java.io.IOException =>
        logError("Encountering a connection error", e)
        keepTrying(client, request, retriesLeft, e, extraCodesToRetry, backoff429Ms)
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
