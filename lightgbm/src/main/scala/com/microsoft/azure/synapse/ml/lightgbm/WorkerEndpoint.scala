// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.net.{InetAddress, UnknownHostException}

private[lightgbm] final case class WorkerEndpoint(host: String, port: Int)

/** Parses the worker addresses exchanged by the LightGBM network handshake. */
private[lightgbm] object WorkerEndpoint {
  private val EndpointPreviewLimit = 200
  private val MinNetworkPort = 1

  /** Parse the first (main) address from a comma-delimited LightGBM machine list. */
  def parseFirst(nodes: String): WorkerEndpoint = {
    val nodeList = Option(nodes).getOrElse(invalid(nodes, "network node list is null"))
    val firstSeparator = nodeList.indexOf(',')
    parse(if (firstSeparator < 0) nodeList else nodeList.substring(0, firstSeparator))
  }

  /** Parse one endpoint without resolving or rewriting its host text. */
  def parse(endpoint: String): WorkerEndpoint = {
    val value = Option(endpoint).getOrElse(invalid(endpoint, "endpoint is null"))
    if (value.isEmpty) invalid(value, "endpoint is empty")

    val (host, portText, bracketed) =
      if (value.startsWith("[")) splitBracketed(value) else splitUnbracketed(value)
    validateHost(host, value, bracketed)
    WorkerEndpoint(host, parsePort(portText, value))
  }

  private def splitBracketed(endpoint: String): (String, String, Boolean) = {
    val closingBracket = endpoint.indexOf(']')
    if (closingBracket < 0) invalid(endpoint, "bracketed IPv6 host is missing its closing ']'")
    val suffix = endpoint.substring(closingBracket + 1)
    if (suffix.isEmpty) invalid(endpoint, "bracketed IPv6 host is missing its port")
    if (!suffix.startsWith(":")) {
      invalid(endpoint, "bracketed IPv6 host must be followed by a ':' port separator")
    }
    (endpoint.substring(1, closingBracket), suffix.substring(1), true)
  }

  private def splitUnbracketed(endpoint: String): (String, String, Boolean) = {
    if (endpoint.exists(character => character == '[' || character == ']')) {
      invalid(endpoint, "IPv6 brackets are unbalanced")
    }
    val portSeparator = endpoint.lastIndexOf(':')
    if (portSeparator < 0) invalid(endpoint, "missing ':' port separator")
    val host = endpoint.substring(0, portSeparator)
    if (host.contains(":") && isValidIpv6Literal(endpoint)) {
      invalid(endpoint, "bare IPv6 endpoint is ambiguous; use the unambiguous [IPv6]:port form")
    }
    (host, endpoint.substring(portSeparator + 1), false)
  }

  private def validateHost(host: String, endpoint: String, bracketed: Boolean): Unit = {
    if (host.isEmpty) invalid(endpoint, "host is empty")
    if (host.exists(isInvalidHostCharacter)) {
      invalid(endpoint, "host contains whitespace, control characters, or an endpoint delimiter")
    }

    val isIpv6Literal = host.contains(":")
    if (bracketed && !isIpv6Literal) {
      invalid(endpoint, "brackets are only valid around an IPv6 literal")
    }
    if (!isIpv6Literal && host.contains("%")) {
      invalid(endpoint, "a zone identifier is only valid on an IPv6 literal")
    }
    if (isIpv6Literal) validateIpv6Literal(host, endpoint)
  }

  private def isInvalidHostCharacter(character: Char): Boolean = {
    Character.isWhitespace(character) || Character.isISOControl(character) ||
      character == '[' || character == ']' || character == ','
  }

  private def validateIpv6Literal(host: String, endpoint: String): Unit = {
    val addressParts = host.split("%", -1)
    if (addressParts.length > 2) invalid(endpoint, "IPv6 zone identifier is malformed")
    if (addressParts.length == 2) validateZone(addressParts(1), endpoint)
    if (!canParseIpv6Address(addressParts(0))) invalid(endpoint, "host is not a valid IPv6 literal")
  }

  private def isValidIpv6Literal(host: String): Boolean = {
    val addressParts = host.split("%", -1)
    addressParts.length <= 2 && addressParts(0).contains(":") &&
      (addressParts.length == 1 || isValidZone(addressParts(1))) && canParseIpv6Address(addressParts(0))
  }

  private def canParseIpv6Address(address: String): Boolean = {
    try {
      // A colon-bearing literal is parsed locally by the JDK and never triggers a hostname lookup.
      InetAddress.getByName(address)
      true
    } catch {
      case _: UnknownHostException => false
    }
  }

  private def validateZone(zone: String, endpoint: String): Unit = {
    if (zone.isEmpty) invalid(endpoint, "IPv6 zone identifier is empty")
    if (!isValidZone(zone)) {
      invalid(endpoint, "IPv6 zone identifier contains whitespace, control characters, or a delimiter")
    }
  }

  private def isValidZone(zone: String): Boolean = zone.nonEmpty && !zone.exists(isInvalidZoneCharacter)

  private def isInvalidZoneCharacter(character: Char): Boolean = {
    Character.isWhitespace(character) || Character.isISOControl(character) ||
      character == ':' || character == '[' || character == ']' || character == ',' || character == '%'
  }

  private def parsePort(portText: String, endpoint: String): Int = {
    if (portText.isEmpty) invalid(endpoint, "port is empty")
    if (!portText.forall(character => character >= '0' && character <= '9')) {
      invalid(endpoint, "port is not a decimal integer")
    }
    val port = try {
      portText.toInt
    } catch {
      case _: NumberFormatException => invalid(endpoint, "port is too large")
    }
    if (port < MinNetworkPort || port > LightGBMConstants.MaxPort) {
      invalid(endpoint, s"port is outside the valid range $MinNetworkPort-${LightGBMConstants.MaxPort}")
    }
    port
  }

  private[lightgbm] def preview(endpoint: String): String = {
    val escaped = Option(endpoint).getOrElse("<null>").flatMap {
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case character if Character.isISOControl(character) => f"\\u${character.toInt}%04x"
      case character => character.toString
    }
    val preview = if (escaped.length <= EndpointPreviewLimit) escaped else escaped.take(EndpointPreviewLimit) + "..."
    s"'$preview'"
  }

  private def invalid(endpoint: String, reason: String): Nothing = {
    throw new IllegalArgumentException(
      s"Invalid LightGBM worker endpoint ${preview(endpoint)}: $reason. " +
        s"Expected hostname:port, IPv4:port, [IPv6]:port, or bare IPv6:port with a decimal port " +
        s"between $MinNetworkPort and ${LightGBMConstants.MaxPort}")
  }
}
