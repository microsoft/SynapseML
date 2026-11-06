// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.azure.synapse.ml.io.http.{AsyncClient, HTTPRequestData, HTTPResponseData, HTTPSchema,
  RequestLineData}
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils.HandlerFunc
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpRequestBase}
import org.apache.http.conn.DnsResolver
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.SystemDefaultDnsResolver
import org.apache.http.protocol.BasicHttpContext
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.{ByteArrayOutputStream, IOException, InputStream, InterruptedIOException}
import java.net.{URI, URISyntaxException, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CancellationException, ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory,
  TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

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

private[contentunderstanding] object ContentUnderstandingProtocol {
  val AnalyzersPath = "/contentunderstanding/analyzers"
  val ResultsPath = "/contentunderstanding/analyzerResults/"
  val DefaultApiVersion = "2025-11-01"
  val MillisecondsPerSecond = 1000
  val TooManyRequests = 429
  private val BufferSize = 8192
  private val MaxPort = 65535
  private val HttpsPort = 443
  private val HttpPort = 80
  private val MaxAnalyzerIdLength = 64
  private val ManagementGetAttempts = 3
  private val StatusNames = Seq("NotStarted", "Running", "Succeeded", "Failed", "Canceled")
    .map(name => name.toLowerCase(java.util.Locale.ROOT) -> name).toMap
  private val CreationStatusNames =
    StatusNames ++ Map("creating" -> "Running", "ready" -> "Succeeded", "deleting" -> "Unknown")
  private val TransientStatuses = Set(HttpStatus.SC_REQUEST_TIMEOUT, TooManyRequests,
    HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
    HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT)
  private val AdmissionRejections = Set(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN, TooManyRequests)
  private val AcceptedSubmissionStatuses =
    Set(HttpStatus.SC_OK, HttpStatus.SC_CREATED, HttpStatus.SC_ACCEPTED)
  private val UnavailableResults = Set(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_GONE)
  private val MaxConcurrentDeadlineAborts = 4
  private[contentunderstanding] val RowAuthColumnMarker = "content-understanding-row-auth"
  private lazy val DeadlineExecutor = {
    val executor = new ScheduledThreadPoolExecutor(MaxConcurrentDeadlineAborts, new ThreadFactory {
      override def newThread(task: Runnable): Thread = {
        val thread = new Thread(task, "synapseml-cu-deadline")
        thread.setDaemon(true)
        thread
      }
    })
    executor.setRemoveOnCancelPolicy(true)
    executor
  }
  private[contentunderstanding] object ClientLifecycle {
    def closeQuietly(closeable: AutoCloseable, operation: String): Unit = {
      try {
        closeable.close()
      } catch {
        case NonFatal(error) => ContentUnderstandingLogging.cleanup(operation, error)
      }
    }

    def withClient[T](client: CloseableHttpClient)(body: => T): T = {
      try {
        body
      } finally {
        closeQuietly(client, "HTTP client")
      }
    }
  }

  case class Settings(endpoint: String,
                      timeoutMs: Int,
                      maxPollAttempts: Int,
                      pollingDelay: Int,
                      maxResponseBytes: Int,
                      maxOperationDuration: Int,
                      sharedAnalyzer: Boolean,
                      sharedModels: Boolean,
                      scalarAuthFingerprints: Set[String] = Set.empty,
                      allAuthScalar: Boolean = false) {
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
                                  transportFailure: Boolean = false,
                                  notSent: Boolean = false,
                                  expiredBeforeDispatch: Boolean = false) {
    def successful: Boolean = httpStatus >= HttpStatus.SC_OK && httpStatus < HttpStatus.SC_MULTIPLE_CHOICES
    def retryable: Boolean =
      !expiredBeforeDispatch && (transportFailure || (diagnostic.isEmpty && TransientStatuses(httpStatus)))
    lazy val json: Option[JsObject] = asObject(body)
  }

  private class ResponseTooLarge extends RuntimeException

  private[contentunderstanding] object DeadlineControl {
    case class Deadline(future: ScheduledFuture[_], fired: AtomicBoolean,
                        cancellation: Option[ScheduledFuture[_]] = None) {
      def hasExpired(budget: ContentUnderstandingBudget): Boolean = fired.get() || budget.remainingMs == 0
    }

    def schedule(request: HttpRequestBase, delayMs: Long): Deadline = {
      val fired = new AtomicBoolean(false)
      val future = DeadlineExecutor.schedule(new Runnable {
        override def run(): Unit = {
          fired.set(true)
          request.abort()
        }
      }, delayMs, TimeUnit.MILLISECONDS)
      val cancellation = AsyncClient.currentTaskContext.map { owner =>
        DeadlineExecutor.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = {
            if ((owner.isInterrupted() || owner.isCompleted()) && fired.compareAndSet(false, true)) {
              request.abort()
            }
          }
        }, 0L, ContentUnderstandingBudget.CancellationPollMs, TimeUnit.MILLISECONDS)
      }
      Deadline(future, fired, cancellation)
    }

    def release(request: HttpRequestBase, deadline: Deadline): Unit = {
      deadline.future.cancel(false)
      deadline.cancellation.foreach(_.cancel(false))
      // Requests are not reused; response/client cleanup owns a request already being aborted.
      if (!deadline.fired.get()) {
        request.releaseConnection()
      }
    }
  }

  private[contentunderstanding] def timeoutError: String =
    diagnostic("OperationTimedOut", "The operation budget was exhausted.")

  private def pollAttemptsError(management: Boolean): String = {
    val recovery = if (management) ContentUnderstandingTransport.ManagementRecovery
      else "Retain the handle and resume polling."
    diagnostic("PollAttemptsExhausted", s"maxPollAttempts was reached before the operation finished. $recovery")
  }

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

  private[contentunderstanding] def checkInterrupted(): Unit = {
    if (AsyncClient.isTaskCancelled) {
      throw new CancellationException("The owning Spark task has ended.")
    }
    if (Thread.currentThread().isInterrupted) {
      throw new InterruptedException("Content Understanding request interrupted.")
    }
  }

  private[contentunderstanding] def retryDelay(value: Option[String], fallback: Int): Long = {
    val deltaMillis = value.flatMap(ContentUnderstandingResponseParsing.retryAfterDeltaMillis)
    deltaMillis.orElse {
      value.flatMap(v => Try(
        ZonedDateTime.parse(v.trim, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant.toEpochMilli).toOption)
        .map(t => math.max(0L, t - System.currentTimeMillis()))
    }.getOrElse(fallback.toLong)
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
    // The enclosing HTTP response owns the stream; closing it here after abort can mask the original failure.
    try {
      read()
      new String(output.toByteArray, StandardCharsets.UTF_8)
    } catch {
      case error: InterruptedException =>
        request.abort()
        throw error
      case error: CancellationException =>
        request.abort()
        throw error
      case error: IOException =>
        request.abort()
        throw error
    }
  }

  private def readResponseBody(response: CloseableHttpResponse,
                               settings: Settings,
                               request: HttpRequestBase): String = {
    Option(response.getEntity).map { entity =>
      if (entity.getContentLength > settings.maxResponseBytes) {
        request.abort()
        throw new ResponseTooLarge
      }
      readBounded(entity.getContent, settings.maxResponseBytes, request)
    }.getOrElse("")
  }

  private def admissionRejected(method: String, status: Int, location: Option[String]): Boolean =
    method == "POST" && AdmissionRejections(status) && location.isEmpty

  private def exchange(client: CloseableHttpClient,
                       settings: Settings,
                       data: HTTPRequestData,
                       budget: ContentUnderstandingBudget,
                       managementGet: Boolean = false): WireResponse = {
    checkInterrupted()
    if (budget.remainingMs == 0) {
      WireResponse(0, "", None, None, Some(timeoutError),
        transportFailure = true, notSent = true, expiredBeforeDispatch = true)
    } else {
      exchangeBeforeDeadline(client, settings, data, budget, managementGet)
    }
  }

  private def exchangeBeforeDeadline(client: CloseableHttpClient,
                                     settings: Settings,
                                     data: HTTPRequestData,
                                     budget: ContentUnderstandingBudget,
                                     managementGet: Boolean): WireResponse = {
    val request = data.toHTTPCore
    val remainingTimeout = math.max(1L, math.min(settings.timeoutMs.toLong, budget.remainingMs)).toInt
    request.setConfig(RequestConfig.copy(settings.requestConfig)
      .setConnectTimeout(remainingTimeout)
      .setConnectionRequestTimeout(remainingTimeout)
      .setSocketTimeout(remainingTimeout).build())
    val deadline = DeadlineControl.schedule(request, budget.remainingMs)
    val context = new BasicHttpContext()
    var code = 0
    var location = Option.empty[String]
    var retryAfter = Option.empty[String]
    try {
      val response = client.execute(request, context)
      try {
        code = response.getStatusLine.getStatusCode
        location = Option(response.getFirstHeader("Operation-Location")).map(_.getValue)
        retryAfter = Option(response.getFirstHeader("Retry-After")).map(_.getValue)
        val body = readResponseBody(response, settings, request)
        WireResponse(code, body, location, retryAfter)
      } finally {
        ClientLifecycle.closeQuietly(response, "HTTP response")
      }
    } catch {
      case _: ResponseTooLarge =>
        WireResponse(code, "", location, retryAfter, Some(diagnostic("ResponseTooLarge",
          "Response exceeded maxResponseBytes. Use explicit input ranges or increase the configured bound.")))
      case error: UnknownHostException =>
        request.abort()
        checkInterrupted()
        val failure = BudgetedDnsResolver.diagnostic(error, deadline.hasExpired(budget))
        WireResponse(code, "", location, retryAfter, Some(failure), transportFailure = true, notSent = true,
          expiredBeforeDispatch = BudgetedDnsResolver.expiredBeforeDispatch(error))
      case error: InterruptedIOException if Thread.currentThread().isInterrupted => throw error
      case error: IOException =>
        request.abort()
        checkInterrupted()
        val notSent = code == 0 && !ContentUnderstandingTransport.hasStartedWriting(context)
        val expired = deadline.hasExpired(budget)
        val failure = if (admissionRejected(data.requestLine.method, code, location)) {
          diagnostic("ResponseBodyReadFailed",
            s"${error.getClass.getSimpleName}: HTTP $code rejected the submission, but its body could not be read.")
        } else {
          ContentUnderstandingTransport.failure(error, data.requestLine.method, notSent, expired, managementGet)
        }
        WireResponse(code, "", location, retryAfter, Some(failure), transportFailure = true, notSent = notSent,
          expiredBeforeDispatch = notSent && expired)
    } finally {
      DeadlineControl.release(request, deadline)
    }
  }

  private def withClient[T](settings: Settings,
                            dnsResolver: DnsResolver = SystemDefaultDnsResolver.INSTANCE,
                            budgetOverride: Option[ContentUnderstandingBudget] = None)
                           (body: (CloseableHttpClient, ContentUnderstandingBudget) => T): T = {
    val budget = budgetOverride.getOrElse(new ContentUnderstandingBudget(settings.maxOperationDuration))
    val client = HttpClients.custom().disableAutomaticRetries().disableRedirectHandling()
      .setDefaultConnectionConfig(ContentUnderstandingResponseParsing.HttpConnectionConfig)
      .setRequestExecutor(ContentUnderstandingTransport.RequestExecutor)
      .disableCookieManagement().setDefaultRequestConfig(settings.requestConfig)
      .setDnsResolver(new BudgetedDnsResolver(() => budget.remainingMs, dnsResolver)).build()
    ClientLifecycle.withClient(client) {
      body(client, budget)
    }
  }

  private def asObject(raw: String): Option[JsObject] = ContentUnderstandingResponseParsing.asObject(raw)

  private def stringField(value: JsObject, field: String): Option[String] =
    value.fields.get(field).collect { case JsString(text) => text }

  private def decode(raw: WireResponse,
                     location: Option[String],
                     statusNames: Map[String, String] = StatusNames): ContentUnderstandingResponse = {
    val json = raw.json
    val id = json.flatMap(stringField(_, "id"))
    val serviceError = json.flatMap(_.fields.get("error")).filterNot(_ == JsNull)
      .map(value => ContentUnderstandingResponseParsing.compact(value).getOrElse(
        diagnostic("InvalidResponse", "The service error could not be decoded.")))
    val serviceStatus = json.flatMap(stringField(_, "status"))
      .flatMap(s => statusNames.get(s.toLowerCase(java.util.Locale.ROOT)))
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
    ContentUnderstandingResponse(location, id, status, raw.httpStatus, raw.body,
      raw.diagnostic.orElse(serviceError).orElse(httpError).orElse(missingStatusError).orElse(failedError))
  }

  private def ongoing(response: ContentUnderstandingResponse): Boolean =
    Set("Running", "NotStarted")(response.status)

  class SharedFailure(response: ContentUnderstandingResponse, val category: String)
    extends ContentUnderstandingException(response)

  private[contentunderstanding] object FailureClassifier {
    def authHeaderFingerprint(name: String, value: String): String = {
      val normalizedName = name.toLowerCase(java.util.Locale.ROOT)
      val input = s"${normalizedName.length}:$normalizedName$value".getBytes(StandardCharsets.UTF_8)
      MessageDigest.getInstance("SHA-256").digest(input).map(byte => f"${byte & 0xff}%02x").mkString
    }

    def errorCodes(value: JsValue): Set[String] = ContentUnderstandingResponseParsing.errorCodes(value)

    private def scalarCredentialWasSent(settings: Settings, request: HTTPRequestData): Boolean =
      request.headers.exists(header =>
        settings.scalarAuthFingerprints(authHeaderFingerprint(header.name, header.value)))

    // The Fabric-named header is a wire-stripped provenance channel; this value denotes CU row credentials.
    private def rowCredentialWasSelected(request: HTTPRequestData): Boolean =
      request.headers.exists(header =>
        HTTPRequestData.isFabricAuthMarker(header.name) &&
          Option(header.value).exists(_.equalsIgnoreCase(RowAuthColumnMarker)))

    def isSharedAuthFailure(settings: Settings,
                            request: HTTPRequestData,
                            response: ContentUnderstandingResponse): Boolean =
      Set(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN)(response.httpStatus) &&
        !rowCredentialWasSelected(request) && (scalarCredentialWasSent(settings, request) || settings.allAuthScalar)
  }

  private def checkSharedFailure(settings: Settings,
                                 request: HTTPRequestData,
                                 response: ContentUnderstandingResponse,
                                 acceptedHandle: Boolean): Unit = {
    if (!acceptedHandle && response.status != "Unknown") {
      val codes = response.error.flatMap(asObject).map(FailureClassifier.errorCodes).getOrElse(Set.empty)
      val category = if (FailureClassifier.isSharedAuthFailure(settings, request, response)) Some("authentication")
        else if (settings.sharedAnalyzer &&
          (codes("AnalyzerNotFound") || response.httpStatus == HttpStatus.SC_NOT_FOUND))
          Some("analyzer")
        else if (settings.sharedModels && codes.exists(Set("DefaultsNotSet", "DeploymentNotFound",
          "ModelDeploymentNotFound"))) Some("deployment")
        else None
      category.foreach(value => throw new SharedFailure(response, value))
    }
  }

  private def uncertainSubmission(raw: WireResponse, response: ContentUnderstandingResponse): Boolean = {
    val unreadable = raw.diagnostic.isDefined || raw.json.flatMap(stringField(_, "status"))
      .flatMap(status => StatusNames.get(status.toLowerCase(java.util.Locale.ROOT))).isEmpty
    raw.transportFailure || raw.httpStatus == HttpStatus.SC_REQUEST_TIMEOUT ||
      raw.httpStatus >= HttpStatus.SC_INTERNAL_SERVER_ERROR ||
      (raw.httpStatus == HttpStatus.SC_NOT_FOUND && raw.diagnostic.isDefined) ||
      (raw.successful && unreadable) ||
      (!AcceptedSubmissionStatuses(raw.httpStatus) && raw.location.isDefined)
  }

  private def decodeSubmission(raw: WireResponse, location: Option[String]): ContentUnderstandingResponse = {
    val response = decode(raw, location)
    if (raw.notSent) {
      val error = if (raw.diagnostic.contains(timeoutError)) {
        Some(diagnostic("RequestNotSent", "The operation budget expired before the HTTP request was sent."))
      } else {
        response.error
      }
      response.copy(status = "Rejected", error = error)
    } else if (response.operationLocation.isEmpty && admissionRejected("POST", raw.httpStatus, raw.location)) {
      response.copy(status = "Rejected")
    } else if (uncertainSubmission(raw, response)) {
      response.copy(status = "Unknown")
    } else {
      response
    }
  }

  private def safeSubmission(settings: Settings, raw: WireResponse): ContentUnderstandingResponse = {
    val accepted = AcceptedSubmissionStatuses(raw.httpStatus)
    val validatedLocation = SubmissionLocation.validate(settings.endpoint, raw.location)
    val location = if (accepted) validatedLocation else None
    val response = decodeSubmission(raw, location.flatMap(_.right.toOption))
    val assessment = SubmissionLocation.assess(raw.httpStatus, raw.successful,
      raw.json.flatMap(stringField(_, "status")), accepted, response.operationLocation, validatedLocation)
    if (!raw.successful) {
      SubmissionLocation.withDiagnostic(response, validatedLocation.flatMap(_.left.toOption))
    } else if (assessment.error.isDefined || assessment.missing) {
      SubmissionLocation.withDiagnostic(response.copy(status = "Unknown"),
        assessment.error.orElse(Some(diagnostic("MissingOperationLocation",
          "A successful ongoing operation did not include Operation-Location and cannot be resumed."))))
    } else {
      response
    }
  }

  private def getRequest(source: HTTPRequestData, location: String): HTTPRequestData =
    source.copy(requestLine = RequestLineData("GET", location, None), entity = None,
      headers = source.headers.filterNot(h =>
        Set("content-length", "content-type", "transfer-encoding", "host")
          (h.name.toLowerCase(java.util.Locale.ROOT))))

  private def decodePoll(raw: WireResponse,
                         location: String,
                         analyzer: Option[String]): ContentUnderstandingResponse = {
    val unavailable = analyzer.isEmpty && !raw.transportFailure && UnavailableResults(raw.httpStatus)
    val response = decode(raw, Some(location))
    if (unavailable) response.copy(status = "ResultUnavailable")
    else if (analyzer.isDefined && UnavailableResults(raw.httpStatus) &&
      !raw.transportFailure && raw.diagnostic.isEmpty) {
      val error = JsObject("code" -> JsString("OperationLookupFailed"),
        "message" -> JsString(ContentUnderstandingTransport.ManagementRecovery),
        "innererror" -> response.error.map(value => asObject(value).getOrElse(JsString(value))).getOrElse(JsObject()))
      response.copy(error = ContentUnderstandingResponseParsing.compact(error).orElse(response.error))
    } else response
  }

  private def retainPollState(raw: WireResponse,
                              parsed: ContentUnderstandingResponse,
                              previous: Option[ContentUnderstandingResponse]): ContentUnderstandingResponse = {
    val missingStatus = raw.successful && raw.json.flatMap(stringField(_, "status"))
      .flatMap(status => StatusNames.get(status.toLowerCase(java.util.Locale.ROOT))).isEmpty
    val nonTerminalHttpError = raw.httpStatus >= HttpStatus.SC_MULTIPLE_CHOICES && parsed.status != "ResultUnavailable"
    if (raw.expiredBeforeDispatch && previous.isDefined) {
      val retained = previous.get
      retained.copy(error = retained.error.orElse(Some(timeoutError)))
    } else if (raw.retryable || raw.diagnostic.isDefined || missingStatus || nonTerminalHttpError) {
      previous.map(_.copy(httpStatus = parsed.httpStatus, error = parsed.error))
        .getOrElse(parsed.copy(status = "Unknown"))
    } else {
      parsed
    }
  }

  private[contentunderstanding] def pollWithClient(client: CloseableHttpClient,
                              settings: Settings,
                              source: HTTPRequestData,
                              location: String,
                              initial: Option[ContentUnderstandingResponse],
                              firstDelay: Long,
                              budget: ContentUnderstandingBudget,
                              analyzer: Option[String] = None): ContentUnderstandingResponse = {
    validateOperationLocation(settings.endpoint, location, analyzer)
    val request = getRequest(source, location)
    @tailrec
    def loop(remaining: Int, previous: Option[ContentUnderstandingResponse], delay: Long)
    : ContentUnderstandingResponse = {
      checkInterrupted()
      if (budget.remainingMs == 0 || delay >= budget.remainingMs) {
        val retained = previous.getOrElse(ContentUnderstandingResponse(Some(location), None, "Unknown", 0, "", None))
        retained.copy(error = retained.error.orElse(Some(timeoutError)))
      } else {
        budget.pause(delay)
        val raw = exchange(client, settings, request, budget, managementGet = analyzer.isDefined)
        val parsed = decodePoll(raw, location, analyzer)
        checkSharedFailure(settings, request, parsed, acceptedHandle = true)
        val current = retainPollState(raw, parsed, previous)
        if (remaining == 1 && ongoing(current) && current.error.isEmpty) {
          current.copy(error = Some(pollAttemptsError(analyzer.isDefined)))
        } else if (remaining == 1 || (!raw.retryable && !ongoing(parsed))) {
          current
        } else {
          loop(remaining - 1, Some(current), retryDelay(raw.retryAfter, settings.pollingDelay))
        }
      }
    }
    loop(settings.maxPollAttempts, initial, firstDelay)
  }

  def execute(settings: Settings,
              request: HTTPRequestData,
              mode: String,
              dnsResolver: DnsResolver = SystemDefaultDnsResolver.INSTANCE,
              budgetOverride: Option[ContentUnderstandingBudget] = None): ContentUnderstandingResponse = {
    withClient(settings, dnsResolver, budgetOverride) { (client, budget) =>
      if (mode == "poll") {
        pollWithClient(client, settings, request, request.requestLine.uri, None, 0L, budget)
      } else {
        val raw = exchange(client, settings, request, budget)
        val submitted = safeSubmission(settings, raw)
        checkSharedFailure(settings, request, submitted, acceptedHandle = submitted.operationLocation.isDefined)
        val resumable = ongoing(submitted) || (submitted.status == "Unknown" && submitted.operationLocation.isDefined)
        if (mode == "analyze" && resumable) {
          submitted.operationLocation match {
            case Some(location) =>
              pollWithClient(client, settings, request, location, Some(submitted),
                retryDelay(raw.retryAfter, settings.pollingDelay), budget)
            case None =>
              submitted.copy(status = "Unknown", error = Some(diagnostic("MissingOperationLocation",
                "The ongoing operation did not include Operation-Location and cannot be resumed.")))
          }
        } else {
          submitted
        }
      }
    }
  }

  /** Row handler for settings validated once on the driver.
    *
    * The inherited client has automatic retries, so each operation uses its own no-retry client while
    * still being scheduled by the shared HTTP pipeline. Service failures are returned as HTTP 200 so
    * SimpleHTTPTransformer keeps them in the output rather than discarding the response.
    */
  def rowHandler(settings: Settings, mode: String): HandlerFunc = {
    // The handler closure is deserialized per Spark task, not shared through an executor singleton.
    val stopped = new AtomicBoolean(false)
    val sharedError = diagnostic("SharedConfigurationFailure",
      "A shared authentication, analyzer, or deployment failure stopped new submissions in this partition.")
    (_, request) => {
      val response = if (stopped.get() && mode != "poll") {
        ContentUnderstandingResponse(None, None, "Rejected", 0, "", Some(sharedError))
      } else {
        try {
          execute(settings, request, mode)
        } catch {
          case error: ContentUnderstandingException =>
            if (stopped.compareAndSet(false, true)) ContentUnderstandingLogging.sharedStop(error)
            // Read failures have no complete body. Do not copy arbitrary service messages into this diagnostic.
            val cause = error.response.error.filter(_ => error.response.rawResponse.isEmpty).flatMap(asObject)
              .filter(value => stringField(value, "code").exists(
                Set("TransportError", "OperationTimedOut", "ResponseTooLarge", "ResponseBodyReadFailed")))
            val detail = cause.map(value =>
              JsObject(asObject(sharedError).get.fields + ("innererror" -> value)).compactPrint).getOrElse(sharedError)
            error.response.copy(error = Some(detail))
        }
      }
      HTTPSchema.stringToResponse(response.toJson.compactPrint, HttpStatus.SC_OK, "OK")
    }
  }

  private def analyzerBody(raw: WireResponse, requireReady: Boolean = false): String = {
    val json = raw.json
    val hasError = json.exists(_.fields.get("error").exists(_ != JsNull))
    if (!raw.successful || raw.diagnostic.isDefined || json.isEmpty || hasError) {
      throw new ContentUnderstandingException(decode(raw, None))
    }
    if (requireReady && !json.flatMap(stringField(_, "status")).exists(_.equalsIgnoreCase("ready"))) {
      throw new ContentUnderstandingException(decode(raw, None).copy(status = "Failed",
        error = Some(diagnostic("AnalyzerNotReady", "The retrieved analyzer is not ready for use."))))
    }
    raw.body
  }

  private def getAnalyzerResponse(client: CloseableHttpClient, settings: Settings,
                                  request: HTTPRequestData, budget: ContentUnderstandingBudget): WireResponse = {
    @tailrec
    def get(remaining: Int, previous: Option[WireResponse]): WireResponse = {
      val response = exchange(client, settings, request, budget, managementGet = true)
      if (response.expiredBeforeDispatch && previous.isDefined) {
        previous.get
      } else if (response.retryable && remaining > 1) {
        val delay = retryDelay(response.retryAfter, settings.pollingDelay)
        if (budget.remainingMs == 0 || delay >= budget.remainingMs) {
          response
        } else {
          budget.pause(delay)
          get(remaining - 1, Some(response))
        }
      } else {
        response
      }
    }
    get(ManagementGetAttempts, None)
  }

  def getAnalyzer(settings: Settings, request: HTTPRequestData,
                  budgetOverride: Option[ContentUnderstandingBudget] = None): String =
    withClient(settings, budgetOverride = budgetOverride) { (client, budget) =>
      analyzerBody(getAnalyzerResponse(client, settings, request, budget))
    }

  private def finishCreation(client: CloseableHttpClient, settings: Settings,
                              request: HTTPRequestData, budget: ContentUnderstandingBudget,
                              completed: ContentUnderstandingResponse): String = {
    val raw = getAnalyzerResponse(client, settings, request, budget)
    if (raw.expiredBeforeDispatch) {
      throw new ContentUnderstandingException(completed.copy(error = completed.error.orElse(Some(timeoutError))))
    }
    try {
      analyzerBody(raw, requireReady = true)
    } catch {
      case error: ContentUnderstandingException =>
        val retrieval = error.response
        throw new ContentUnderstandingException(completed.copy(status = "Failed", httpStatus = retrieval.httpStatus,
          rawResponse = retrieval.rawResponse, error = retrieval.error.orElse(Some(diagnostic("AnalyzerRetrievalFailed",
            "Analyzer creation completed, but retrieving its definition failed.")))))
    }
  }

  def createAnalyzer(settings: Settings,
                     request: HTTPRequestData,
                     getRequestData: HTTPRequestData,
                     analyzerId: String,
                     budgetOverride: Option[ContentUnderstandingBudget] = None): String = {
    withClient(settings, budgetOverride = budgetOverride) { (client, budget) =>
      val raw = exchange(client, settings, request, budget)
      if (raw.location.isDefined && !AcceptedSubmissionStatuses(raw.httpStatus)) {
        throw new ContentUnderstandingException(SubmissionLocation.rejectedCreation(
          decode(raw, None, CreationStatusNames), settings.endpoint, raw.location.get, analyzerId))
      }
      raw.location match {
        case Some(location) =>
          try {
            validateOperationLocation(settings.endpoint, location, Some(analyzerId))
          } catch {
            case _: IllegalArgumentException =>
              throw new ContentUnderstandingException(SubmissionLocation.rejectedCreation(
                decode(raw, None, CreationStatusNames), settings.endpoint, location, analyzerId))
          }
          val completed = pollWithClient(client, settings, request, location,
            Some(decode(raw, Some(location), CreationStatusNames)),
            retryDelay(raw.retryAfter, settings.pollingDelay), budget, Some(analyzerId))
          if (completed.status != "Succeeded" || completed.error.isDefined) {
            throw new ContentUnderstandingException(completed)
          }
          finishCreation(client, settings, getRequestData, budget, completed)
        case None =>
          analyzerBody(raw)
          val status = raw.json.flatMap(stringField(_, "status"))
          if (!status.exists(_.equalsIgnoreCase("ready"))) {
            val response = decode(raw, None, CreationStatusNames)
            throw new ContentUnderstandingException(response.copy(status = "Failed",
              error = response.error.orElse(Some(diagnostic("MissingOperationLocation",
                "Analyzer creation was not ready and supplied no management operation URL.")))))
          }
          raw.body
      }
    }
  }
}
