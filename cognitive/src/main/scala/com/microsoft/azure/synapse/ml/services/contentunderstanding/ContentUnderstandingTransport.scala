// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.{HttpClientConnection, HttpRequest, HttpResponse}
import org.apache.http.protocol.{HttpContext, HttpRequestExecutor}

import java.io.IOException

private[contentunderstanding] object ContentUnderstandingTransport {
  private val RequestWriteStarted = "synapseml.cu.request-write-started"
  val ManagementRecovery = "Use getAnalyzer to inspect the analyzer; do not repeat creation."

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
