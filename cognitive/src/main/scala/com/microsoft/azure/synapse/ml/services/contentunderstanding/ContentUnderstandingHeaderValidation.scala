// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

private[contentunderstanding] object ContentUnderstandingHeaderValidation {
  private val ConnectorManagedHeaders = Set("content-length", "content-type", "host", "transfer-encoding")
  private val HeaderToken = "[!#$%&'*+.^_`|~0-9A-Za-z-]+"

  private def safeHeaderValue(value: String): Boolean =
    !value.exists(c => c > 255 || (Character.isISOControl(c) && c != '\t'))

  /** Rejects header injection and connector-managed headers without echoing the value. */
  def validateSetting(name: String, value: Any): Unit = value match {
    case text: String =>
      require(safeHeaderValue(text), s"$name cannot contain control characters or characters outside Latin-1.")
    case headers: Map[_, _] => headers.foreach { case (key, item) =>
      val header = key.toString
      require(header.matches(HeaderToken) && !ConnectorManagedHeaders(header.toLowerCase(java.util.Locale.ROOT)),
        s"$name contains an invalid or connector-managed header name.")
      require(safeHeaderValue(item.toString),
        s"$name header values cannot contain control characters or characters outside Latin-1.")
    }
    case _ =>
  }
}
