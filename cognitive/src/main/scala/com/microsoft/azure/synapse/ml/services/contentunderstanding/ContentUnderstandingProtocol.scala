// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, RequestLineData}
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.{ByteArrayOutputStream, IOException, InputStream, InterruptedIOException}
import java.net.{URI, URISyntaxException}
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.Try

case class ContentUnderstandingResponse(operationLocation: Option[String],
                                        id: Option[String],
                                        status: String,
                                        httpStatus: Int,
                                        rawResponse: String,
                                        error: Option[String])

object ContentUnderstandingResponse extends SparkBindings[ContentUnderstandingResponse] {
  implicit val JsonFormat: RootJsonFormat[ContentUnderstandingResponse] =
    jsonFormat6(ContentUnderstandingResponse.apply)
}

/** The response retains the service error and operation handle, including provisioning timeouts. */
class ContentUnderstandingException(val response: ContentUnderstandingResponse)
  extends IllegalStateException(
    s"Content Understanding returned ${response.status} (HTTP ${response.httpStatus}): " +
      response.error.getOrElse("Polling budget exhausted; the operation may still be running."))

private[contentunderstanding] object ContentUnderstandingProtocol {
  val AnalyzersPath = "/contentunderstanding/analyzers"
  val ResultsPath = "/contentunderstanding/analyzerResults/"
  val DefaultApiVersion = "2025-11-01"
  val MillisecondsPerSecond = 1000
  val MaxRetryDelayMs = 60000L
  val TooManyRequests = 429
  private val BufferSize = 8192
  private val MaxPort = 65535
  private val HttpsPort = 443
  private val HttpPort = 80
  private val MaxAnalyzerIdLength = 64
  private val ManagementGetAttempts = 3
  private val StatusNames = Seq("NotStarted", "Running", "Succeeded", "Failed", "Canceled")
    .map(name => name.toLowerCase(java.util.Locale.ROOT) -> name).toMap
  private val TransientStatuses = Set(HttpStatus.SC_REQUEST_TIMEOUT, TooManyRequests,
    HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
    HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT)
  private val AdmissionRejections = Set(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN, TooManyRequests)
  private val UnavailableResults = Set(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_GONE)

  case class Settings(endpoint: String,
                      timeoutMs: Int,
                      maxPollAttempts: Int,
                      pollingDelay: Int,
                      maxResponseBytes: Int) {
    def requestConfig: RequestConfig = RequestConfig.custom()
      .setConnectTimeout(timeoutMs)
      .setConnectionRequestTimeout(timeoutMs)
      .setSocketTimeout(timeoutMs)
      .setRedirectsEnabled(false)
      .build()
  }

  private case class WireResponse(httpStatus: Int,
                                  body: String,
                                  location: Option[String],
                                  retryAfter: Option[String],
                                  diagnostic: Option[String] = None,
                                  transportFailure: Boolean = false) {
    def successful: Boolean = httpStatus >= HttpStatus.SC_OK && httpStatus < HttpStatus.SC_MULTIPLE_CHOICES
    def retryable: Boolean = transportFailure || (diagnostic.isEmpty && TransientStatuses(httpStatus))
    lazy val json: Option[JsObject] = asObject(body)
  }

  private class ResponseTooLarge extends RuntimeException

  def diagnostic(code: String, message: String): String =
    JsObject("code" -> JsString(code), "message" -> JsString(message)).compactPrint

  def parseUri(value: String, description: String): URI = {
    require(Option(value).exists(_.nonEmpty), s"$description must be nonempty.")
    try {
      new URI(value)
    } catch {
      case _: URISyntaxException => throw new IllegalArgumentException(s"$description must be a valid URI.")
    }
  }

  def validApiVersion(value: String): Boolean =
    Option(value).exists(_.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}(-preview)?"))

  private def safeSegment(value: String): Boolean =
    value.matches("[a-zA-Z0-9._-]+") && value != "." && value != ".."

  def validateAnalyzerId(value: String): Unit = {
    require(Option(value).exists(v => v.length <= MaxAnalyzerIdLength && safeSegment(v)),
      "analyzerId must contain 1-64 letters, digits, periods, underscores, or hyphens, and cannot be a dot segment.")
  }

  private def literalLoopback(host: String): Boolean = {
    val ipv6 = Set("[::1]", "::1", "[0:0:0:0:0:0:0:1]", "0:0:0:0:0:0:0:1")
    val parts = host.split("\\.", -1)
    ipv6(host) || (parts.length == 4 && parts.head == "127" &&
      parts.forall(p => p.matches("[0-9]{1,3}") && p.toInt <= 255))
  }

  private def validateAuthority(uri: URI): Unit = {
    require(uri.isAbsolute && !uri.isOpaque && Option(uri.getHost).exists(_.nonEmpty),
      "The endpoint must have an absolute HTTP(S) authority.")
    require(Option(uri.getRawUserInfo).isEmpty && Option(uri.getRawFragment).isEmpty,
      "Endpoint and operation URLs cannot contain user information or fragments.")
    require(uri.getPort >= -1 && uri.getPort <= MaxPort && uri.getPort != 0, "Invalid endpoint port.")
    val scheme = uri.getScheme.toLowerCase(java.util.Locale.ROOT)
    require(scheme == "https" || (scheme == "http" && literalLoopback(uri.getHost)),
      "Use HTTPS. HTTP is allowed only for literal loopback test endpoints.")
  }

  def endpointUrl(value: String): String = {
    val uri = parseUri(value, "endpoint")
    validateAuthority(uri)
    require(Option(uri.getRawQuery).isEmpty, "endpoint cannot contain a query.")
    val path = Option(uri.getRawPath).getOrElse("").reverse.dropWhile(_ == '/').reverse
    require(path.isEmpty || path == AnalyzersPath,
      "endpoint must be the resource root or its contentunderstanding/analyzers URL.")
    new URI(uri.getScheme, uri.getRawAuthority, AnalyzersPath, None.orNull, None.orNull).toString
  }

  def validateEndpoint(value: String): URI = {
    val uri = parseUri(value, "url")
    validateAuthority(uri)
    require(uri.getRawPath == AnalyzersPath && Option(uri.getRawQuery).isEmpty,
      "url must end with /contentunderstanding/analyzers and have no query. Use setEndpoint(resourceRoot).")
    uri
  }

  private def effectivePort(uri: URI): Int =
    if (uri.getPort >= 0) uri.getPort else if (uri.getScheme.equalsIgnoreCase("https")) HttpsPort else HttpPort

  private def sameOrigin(endpoint: URI, uri: URI): Boolean =
    endpoint.getScheme.equalsIgnoreCase(uri.getScheme) &&
      endpoint.getHost.equalsIgnoreCase(uri.getHost) && effectivePort(endpoint) == effectivePort(uri)

  def validateOperationLocation(endpoint: String,
                                location: String,
                                analyzer: Option[String] = None): URI = {
    val root = validateEndpoint(endpoint)
    val uri = parseUri(location, "operationLocation")
    validateAuthority(uri)
    require(sameOrigin(root, uri), "operationLocation must use the configured endpoint's scheme, host, and port.")
    val prefix = analyzer.map(id => s"$AnalyzersPath/$id/operations/").getOrElse(ResultsPath)
    val path = uri.getRawPath
    require(path.startsWith(prefix) && safeSegment(path.substring(prefix.length)),
      "operationLocation must identify one operation in the expected Content Understanding result path.")
    require(Option(uri.getRawQuery).exists(q =>
      q.startsWith("api-version=") && validApiVersion(q.stripPrefix("api-version="))),
      "operationLocation must have exactly one api-version query parameter.")
    uri
  }

  def canonicalJson(value: JsValue): String = value match {
    case JsObject(fields) =>
      fields.toSeq.sortBy(_._1).map { case (name, field) =>
        JsString(name).compactPrint + ":" + canonicalJson(field)
      }.mkString("{", ",", "}")
    case JsArray(values) => values.map(canonicalJson).mkString("[", ",", "]")
    case other => other.compactPrint
  }

  private def checkInterrupted(): Unit = {
    if (Thread.currentThread().isInterrupted) {
      throw new InterruptedException("Content Understanding request interrupted.")
    }
  }

  private def pause(delay: Long): Unit = {
    checkInterrupted()
    if (delay > 0) {
      try {
        Thread.sleep(delay)
      } catch {
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw error
      }
    }
  }

  private[contentunderstanding] def retryDelay(value: Option[String], fallback: Int): Long = {
    val seconds = value.flatMap(v => Try(v.trim.toLong).toOption).filter(_ >= 0)
      .map(s => math.min(s, MaxRetryDelayMs / MillisecondsPerSecond) * MillisecondsPerSecond)
    val date = value.flatMap(v => Try(
      ZonedDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant.toEpochMilli).toOption)
      .map(t => math.min(MaxRetryDelayMs, math.max(0L, t - System.currentTimeMillis())))
    seconds.orElse(date).getOrElse(fallback.toLong)
  }

  private def readBounded(input: InputStream, limit: Int, request: HttpRequestBase): String = {
    val output = new ByteArrayOutputStream(math.min(limit, BufferSize))
    val buffer = new Array[Byte](BufferSize)
    @tailrec
    def read(): Unit = {
      checkInterrupted()
      val remaining = limit.toLong - output.size() + 1
      val count = input.read(buffer, 0, math.min(buffer.length.toLong, remaining).toInt)
      if (count >= 0) {
        if (output.size().toLong + count > limit) {
          request.abort()
          throw new ResponseTooLarge
        }
        output.write(buffer, 0, count)
        read()
      }
    }
    try {
      read()
      new String(output.toByteArray, StandardCharsets.UTF_8)
    } catch {
      case error: InterruptedException =>
        request.abort()
        throw error
      case error: IOException =>
        request.abort()
        throw error
    } finally {
      input.close()
      output.close()
    }
  }

  private def exchange(client: CloseableHttpClient, settings: Settings, data: HTTPRequestData): WireResponse = {
    checkInterrupted()
    val request = data.toHTTPCore
    request.setConfig(settings.requestConfig)
    var code = 0
    var location = Option.empty[String]
    var retryAfter = Option.empty[String]
    try {
      val response = client.execute(request)
      try {
        code = response.getStatusLine.getStatusCode
        location = Option(response.getFirstHeader("Operation-Location")).map(_.getValue)
        retryAfter = Option(response.getFirstHeader("Retry-After")).map(_.getValue)
        val body = Option(response.getEntity).map { entity =>
          if (entity.getContentLength > settings.maxResponseBytes) {
            request.abort()
            throw new ResponseTooLarge
          }
          readBounded(entity.getContent, settings.maxResponseBytes, request)
        }.getOrElse("")
        WireResponse(code, body, location, retryAfter)
      } finally {
        response.close()
      }
    } catch {
      case _: ResponseTooLarge =>
        WireResponse(code, "", location, retryAfter, Some(diagnostic("ResponseTooLarge",
          "Response exceeded maxResponseBytes. Use explicit input ranges or increase the configured bound.")))
      case error: InterruptedIOException if Thread.currentThread().isInterrupted => throw error
      case _: IOException =>
        request.abort()
        val message = if (data.requestLine.method == "GET") {
          "HTTP polling transport failed. Retain the operation handle and resume polling."
        } else {
          "HTTP transport failed. A submitted request may have been accepted; it was not resubmitted."
        }
        WireResponse(code, "", location, retryAfter, Some(diagnostic("TransportError", message)),
          transportFailure = true)
    } finally {
      request.releaseConnection()
    }
  }

  private def withClient[T](settings: Settings)(body: CloseableHttpClient => T): T = {
    val client = HttpClients.custom().disableAutomaticRetries().disableRedirectHandling()
      .disableCookieManagement().setDefaultRequestConfig(settings.requestConfig).build()
    try {
      body(client)
    } finally {
      client.close()
    }
  }

  private def asObject(raw: String): Option[JsObject] = {
    try {
      raw.parseJson match {
        case value: JsObject => Some(value)
        case _ => None
      }
    } catch {
      case _: JsonParser.ParsingException => None
    }
  }

  private def stringField(value: JsObject, field: String): Option[String] =
    value.fields.get(field).collect { case JsString(text) => text }

  private def checkClientResponse(raw: WireResponse,
                                  response: ContentUnderstandingResponse,
                                  invalidStatus: Boolean,
                                  failOnClientError: Boolean): Unit = {
    if (failOnClientError && !raw.retryable && (!raw.successful || raw.diagnostic.isDefined || invalidStatus)) {
      throw new ContentUnderstandingException(response)
    }
  }

  private def decode(raw: WireResponse,
                     location: Option[String],
                     failOnClientError: Boolean = false): ContentUnderstandingResponse = {
    val json = raw.json
    val id = json.flatMap(stringField(_, "id"))
    val serviceError = json.flatMap(_.fields.get("error")).filterNot(_ == JsNull).map(_.compactPrint)
    val serviceStatus = json.flatMap(stringField(_, "status"))
      .flatMap(s => StatusNames.get(s.toLowerCase(java.util.Locale.ROOT)))
    val status = if (raw.transportFailure) {
      "Unknown"
    } else if (!raw.successful || raw.diagnostic.isDefined) {
      "Failed"
    } else {
      serviceStatus.getOrElse("Failed")
    }
    val missingStatusError = if (raw.successful && serviceStatus.isEmpty) {
      Some(diagnostic("InvalidResponse", "Expected a JSON operation object with a recognized status."))
    } else {
      None
    }
    val failedError = if (Set("Failed", "Canceled")(status)) {
      Some(diagnostic("OperationFailed", "The service operation did not succeed."))
    } else {
      None
    }
    val httpError = if (!raw.successful) {
      Some(diagnostic("HttpError", s"HTTP ${raw.httpStatus}; redirects are not followed."))
    } else {
      None
    }
    val response = ContentUnderstandingResponse(location, id, status, raw.httpStatus, raw.body,
      raw.diagnostic.orElse(serviceError).orElse(httpError).orElse(missingStatusError).orElse(failedError))
    checkClientResponse(raw, response, missingStatusError.isDefined, failOnClientError)
    response
  }

  private def ongoing(response: ContentUnderstandingResponse): Boolean =
    Set("Running", "NotStarted")(response.status)

  private def uncertainSubmission(raw: WireResponse, response: ContentUnderstandingResponse): Boolean = {
    val unreadable = raw.diagnostic.isDefined || raw.json.flatMap(stringField(_, "status"))
      .flatMap(status => StatusNames.get(status.toLowerCase(java.util.Locale.ROOT))).isEmpty
    raw.transportFailure || raw.httpStatus == HttpStatus.SC_REQUEST_TIMEOUT ||
      raw.httpStatus >= HttpStatus.SC_INTERNAL_SERVER_ERROR ||
      (raw.successful && unreadable) || (!raw.successful && response.operationLocation.isDefined)
  }

  private def decodeSubmission(raw: WireResponse, location: Option[String]): ContentUnderstandingResponse = {
    val response = decode(raw, location)
    if (response.operationLocation.isEmpty && AdmissionRejections(raw.httpStatus)) {
      response.copy(status = "Rejected")
    } else if (uncertainSubmission(raw, response)) {
      response.copy(status = "Unknown")
    } else {
      response
    }
  }

  private def safeSubmission(settings: Settings, raw: WireResponse): ContentUnderstandingResponse = {
    val location = raw.location.map { value =>
      try {
        Right(validateOperationLocation(settings.endpoint, value).toString)
      } catch {
        case _: IllegalArgumentException => Left(diagnostic("InvalidOperationLocation",
          "The service returned an unsafe or malformed Operation-Location; it was not requested."))
      }
    }
    val response = decodeSubmission(raw, location.flatMap(_.right.toOption))
    val locationError = location.flatMap(_.left.toOption)
    val missingLocation = raw.successful && (ongoing(response) || raw.httpStatus == HttpStatus.SC_ACCEPTED) &&
      response.operationLocation.isEmpty
    if (locationError.isDefined || missingLocation) {
      response.copy(status = if (raw.successful) "Unknown" else response.status,
        error = locationError.orElse(Some(diagnostic("MissingOperationLocation",
          "An accepted operation did not include Operation-Location and cannot be resumed."))))
    } else {
      response
    }
  }

  private def getRequest(source: HTTPRequestData, location: String): HTTPRequestData =
    source.copy(requestLine = RequestLineData("GET", location, None), entity = None,
      headers = source.headers.filterNot(h =>
        Set("content-length", "transfer-encoding", "host")(h.name.toLowerCase(java.util.Locale.ROOT))))

  private def decodePoll(raw: WireResponse,
                         location: String,
                         analyzer: Option[String],
                         failOnClientError: Boolean): ContentUnderstandingResponse = {
    val unavailable = analyzer.isEmpty && !raw.transportFailure && UnavailableResults(raw.httpStatus)
    val response = decode(raw, Some(location), failOnClientError && !unavailable)
    if (unavailable) response.copy(status = "ResultUnavailable") else response
  }

  private def pollWithClient(client: CloseableHttpClient,
                              settings: Settings,
                              source: HTTPRequestData,
                              location: String,
                              initial: Option[ContentUnderstandingResponse],
                              firstDelay: Long,
                              analyzer: Option[String] = None,
                              failOnClientError: Boolean = false): ContentUnderstandingResponse = {
    validateOperationLocation(settings.endpoint, location, analyzer)
    val request = getRequest(source, location)
    @tailrec
    def loop(remaining: Int, previous: Option[ContentUnderstandingResponse], delay: Long)
    : ContentUnderstandingResponse = {
      pause(delay)
      val raw = exchange(client, settings, request)
      val parsed = decodePoll(raw, location, analyzer, failOnClientError)
      val current = if (raw.retryable) {
        previous.map(_.copy(httpStatus = parsed.httpStatus, error = parsed.error))
          .getOrElse(parsed.copy(status = "Unknown"))
      } else {
        parsed
      }
      if (remaining == 1 || (!raw.retryable && !ongoing(current))) {
        if (failOnClientError && raw.retryable) {
          throw new ContentUnderstandingException(current)
        }
        current
      } else {
        loop(remaining - 1, Some(current), retryDelay(raw.retryAfter, settings.pollingDelay))
      }
    }
    loop(settings.maxPollAttempts, initial, firstDelay)
  }

  def execute(settings: Settings,
              request: HTTPRequestData,
              mode: String,
              failOnClientError: Boolean = false): ContentUnderstandingResponse = {
    withClient(settings) { client =>
      if (mode == "poll") {
        pollWithClient(client, settings, request, request.requestLine.uri, None, 0L,
          failOnClientError = failOnClientError)
      } else {
        val raw = exchange(client, settings, request)
        val submitted = safeSubmission(settings, raw)
        val resumable = ongoing(submitted) || (submitted.status == "Unknown" && submitted.operationLocation.isDefined)
        if (mode == "analyze" && resumable) {
          pollWithClient(client, settings, request, submitted.operationLocation.get, Some(submitted),
            retryDelay(raw.retryAfter, settings.pollingDelay), failOnClientError = failOnClientError)
        } else {
          submitted
        }
      }
    }
  }

  private def analyzerBody(raw: WireResponse): String = {
    val json = raw.json
    val hasError = json.exists(_.fields.get("error").exists(_ != JsNull))
    if (!raw.successful || raw.diagnostic.isDefined || json.isEmpty || hasError) {
      throw new ContentUnderstandingException(decode(raw, None))
    }
    raw.body
  }

  private def getAnalyzerWithClient(client: CloseableHttpClient,
                                    settings: Settings,
                                    request: HTTPRequestData): String = {
    @tailrec
    def get(remaining: Int): WireResponse = {
      val response = exchange(client, settings, request)
      if (response.retryable && remaining > 1) {
        pause(retryDelay(response.retryAfter, settings.pollingDelay))
        get(remaining - 1)
      } else {
        response
      }
    }
    analyzerBody(get(ManagementGetAttempts))
  }

  def getAnalyzer(settings: Settings, request: HTTPRequestData): String =
    withClient(settings)(getAnalyzerWithClient(_, settings, request))

  def createAnalyzer(settings: Settings,
                     request: HTTPRequestData,
                     getRequestData: HTTPRequestData,
                     analyzerId: String): String = {
    withClient(settings) { client =>
      val raw = exchange(client, settings, request)
      analyzerBody(raw)
      raw.location match {
        case Some(location) =>
          try {
            validateOperationLocation(settings.endpoint, location, Some(analyzerId))
          } catch {
            case _: IllegalArgumentException =>
              throw new ContentUnderstandingException(decode(raw, None).copy(status = "Failed",
                error = Some(diagnostic("InvalidOperationLocation",
                  "The analyzer creation operation URL was unsafe; it was not requested."))))
          }
          val completed = pollWithClient(client, settings, request, location, None,
            retryDelay(raw.retryAfter, settings.pollingDelay), Some(analyzerId))
          if (completed.status != "Succeeded" || completed.error.isDefined) {
            throw new ContentUnderstandingException(completed)
          }
          getAnalyzerWithClient(client, settings, getRequestData)
        case None =>
          val status = raw.json.flatMap(stringField(_, "status"))
          if (!status.exists(_.equalsIgnoreCase("ready"))) {
            throw new ContentUnderstandingException(decode(raw, None).copy(status = "Failed",
              error = Some(diagnostic("MissingOperationLocation",
                "Analyzer creation was not ready and supplied no management operation URL."))))
          }
          raw.body
      }
    }
  }
}
