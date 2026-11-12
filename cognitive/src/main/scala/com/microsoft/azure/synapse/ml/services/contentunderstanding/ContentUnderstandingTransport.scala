// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.{HttpClientConnection, HttpRequest, HttpResponse, HttpStatus}
import org.apache.http.protocol.{HttpContext, HttpRequestExecutor}
import spray.json.JsObject

import java.io.IOException

private[contentunderstanding] object ContentUnderstandingTransport {
  private val RequestWriteStarted = "synapseml.cu.request-write-started"
  private val TooManyRequests = 429
  private val TransientStatuses = Set(HttpStatus.SC_REQUEST_TIMEOUT, TooManyRequests,
    HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
    HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT)
  private val AdmissionRejections = Set(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN, TooManyRequests)
  val ManagementRecovery = "Use getAnalyzer to inspect the analyzer; do not repeat creation."

  def admissionRejected(method: String, status: Int, location: Option[String]): Boolean =
    method == "POST" && AdmissionRejections(status) && location.isEmpty

  case class WireResponse(httpStatus: Int,
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
    def throttled(method: String): Boolean = httpStatus == TooManyRequests &&
      (method == "GET" || admissionRejected(method, httpStatus, location))
    lazy val json: Option[JsObject] = ContentUnderstandingResponseParsing.asObject(body)
  }

  val RequestExecutor: HttpRequestExecutor = new HttpRequestExecutor {
    override protected def doSendRequest(request: HttpRequest,
                                         connection: HttpClientConnection,
                                         context: HttpContext): HttpResponse = {
      // Mark before writing so partial writes remain ambiguous, not safely retryable.
      context.setAttribute(RequestWriteStarted, java.lang.Boolean.TRUE)
      super.doSendRequest(request, connection, context)
    }
  }

  def hasStartedWriting(context: HttpContext): Boolean =
    context.getAttribute(RequestWriteStarted) == java.lang.Boolean.TRUE

  def failure(error: IOException, method: String, notSent: Boolean, timedOut: Boolean,
              managementGet: Boolean = false): String = {
    if (notSent) {
      ContentUnderstandingProtocol.diagnostic("RequestNotSent",
        s"${error.getClass.getSimpleName}: The HTTP request was not sent.")
    } else if (timedOut) {
      ContentUnderstandingProtocol.timeoutError
    } else {
      val message = if (method == "GET" && managementGet) {
        s"HTTP management GET transport failed. $ManagementRecovery"
      } else if (method == "GET") {
        "HTTP polling transport failed. Retain the operation handle and resume polling."
      } else {
        "HTTP transport failed. A submitted request may have been accepted; it was not resubmitted."
      }
      ContentUnderstandingProtocol.diagnostic("TransportError", s"${error.getClass.getSimpleName}: $message")
    }
  }
}
