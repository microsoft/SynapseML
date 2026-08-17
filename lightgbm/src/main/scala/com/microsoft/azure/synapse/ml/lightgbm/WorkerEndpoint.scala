// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.net.{InetAddress, NetworkInterface, SocketException, UnknownHostException}

private[lightgbm] final case class WorkerEndpoint(host: String, port: Int) {
  /** Whether the host is an IPv6 literal, which has to be bracketed before a ':' port separator. */
  def isIpv6Literal: Boolean = WorkerEndpoint.isIpv6Host(host)

  /** The zone identifier of a scoped IPv6 literal, if it carries one. */
  def zoneId: Option[String] = {
    val separator = host.indexOf('%')
    if (separator < 0) None else Some(host.substring(separator + 1))
  }

  /** Whether the zone identifier is a numeric interface index, which is local to one machine. */
  def hasNumericZone: Boolean = zoneId.exists(zone => zone.nonEmpty && zone.forall(_.isDigit))

  /** The address without its zone identifier. */
  def address: String = {
    val separator = host.indexOf('%')
    if (separator < 0) host else host.substring(0, separator)
  }

  /** The unambiguous wire form of this endpoint. */
  def wireString: String = WorkerEndpoint.wireString(host, port)
}

/** Parses the worker addresses exchanged by the LightGBM network handshake. */
private[lightgbm] object WorkerEndpoint {
  private val EndpointPreviewLimit = 200
  private val MinNetworkPort = 1

  /** Whether a host is an IPv6 literal. Hostnames and IPv4 literals never contain a ':'. */
  def isIpv6Host(host: String): Boolean = Option(host).exists(_.contains(":"))

  /** Replace a numeric IPv6 scope with the interface name it stands for on this machine.
    *
    * A numeric scope is an interface index, which is only meaningful on the machine that produced
    * it, so it must never be published to peers. An interface name survives the trip whenever the
    * cluster names its interfaces consistently, which is the only case where a link-local address
    * can work at all. Anything else is returned unchanged.
    */
  def normalizeHost(host: String): String = {
    val endpoint = WorkerEndpoint(Option(host).getOrElse(""), 1)
    endpoint.zoneId.filter(zone => zone.nonEmpty && zone.forall(_.isDigit)).flatMap { zone =>
      try {
        Option(NetworkInterface.getByIndex(zone.toInt)).map(named => s"${endpoint.address}%${named.getName}")
      } catch {
        case _: SocketException => None
        case _: IllegalArgumentException => None
      }
    }.getOrElse(host)
  }

  /** Bracket an IPv6 literal so a ':' port separator stays unambiguous. Other hosts are unchanged. */
  def wireHost(host: String): String =
    if (isIpv6Host(host) && !host.startsWith("[")) s"[$host]" else host

  /** Render an endpoint in the wire form every LightGBM component parses.
    *
    * The result is parsed back before it is returned, so a host carrying a control character, a
    * delimiter, or an unbalanced bracket fails here instead of corrupting the line protocol or the
    * comma-delimited machine list it would have been written into.
    */
  def wireString(host: String, port: Int): String = {
    val endpoint = s"${wireHost(host)}:$port"
    parse(endpoint)
    endpoint
  }

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
