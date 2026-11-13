// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import spray.json.JsString

/** The response retains the service error and operation handle, including provisioning timeouts. */
class ContentUnderstandingException(val response: ContentUnderstandingResponse)
  extends IllegalStateException(ContentUnderstandingException.message(response))

private[contentunderstanding] object ContentUnderstandingException {
  private val MaxDiagnosticCharacters = 4096
  private val DiagnosticCodes = Set(
    "AnalyzerNotReady", "AnalyzerRetrievalFailed", "DnsCapacityExceeded", "DnsResolutionFailed",
    "HttpError", "InvalidInput", "InvalidOperationLocation", "InvalidResponse", "MissingOperationLocation",
    "OperationFailed", "OperationLookupFailed", "OperationTimedOut", "PollAttemptsExhausted", "RequestNotSent",
    "ResponseBodyReadFailed", "ResponseTooLarge", "SharedConfigurationFailure", "TransportError",
    "UnacceptedOperationLocation")

  private def message(response: ContentUnderstandingResponse): String = {
    val code = response.error.filter(_.length <= MaxDiagnosticCharacters)
      .flatMap(ContentUnderstandingResponseParsing.asObject).flatMap(_.fields.get("code"))
      .collect { case JsString(value) if DiagnosticCodes(value) => value }
    val diagnostic = code.map(value => s" [$value]").getOrElse("")
    s"Content Understanding could not complete the call (status ${response.status}, HTTP ${response.httpStatus})" +
      s"$diagnostic. Inspect the exception response for the diagnostic and any retained operation handle. " +
      "Service details are not included in this message."
  }
}
