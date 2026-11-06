// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.config.{ConnectionConfig, MessageConstraints}
import spray.json.{JsArray, JsObject, JsString, JsValue, enrichString}

import scala.annotation.tailrec
import scala.util.control.NonFatal

private[contentunderstanding] object ContentUnderstandingResponseParsing {
  private val MaxDepth = 1000
  private val MaxNumberCharacters = 100
  private val MaxRetryAfterDigits = 19
  private val MillisecondsPerSecond = 1000
  private val MaxHeaderLineLength = 65536
  private val MaxHeaderCount = 200

  val HttpConnectionConfig: ConnectionConfig = ConnectionConfig.custom().setMessageConstraints(
    MessageConstraints.custom().setMaxLineLength(MaxHeaderLineLength).setMaxHeaderCount(MaxHeaderCount).build()).build()

  private def numericCharacter(value: Char): Boolean =
    (value >= '0' && value <= '9') || "+-.eE".contains(value)

  // spray-json 1.3.5 has no parser limits. Scan iteratively before entering its recursive parser.
  private class JsonBounds {
    private var depth = 0
    private var numberCharacters = 0
    private var quoted = false
    private var escaped = false

    private def stringCharacter(value: Char): Unit = {
      if (escaped) {
        escaped = false
      } else {
        value match {
          case '\\' => escaped = true
          case '"' => quoted = false
          case _ => ()
        }
      }
    }

    private def outsideCharacter(value: Char): Boolean = {
      if (numericCharacter(value)) {
        numberCharacters += 1
      } else {
        numberCharacters = 0
        value match {
          case '"' => quoted = true
          case '{' | '[' => depth += 1
          case '}' | ']' => depth -= 1
          case _ => ()
        }
      }
      depth >= 0 && depth <= MaxDepth && numberCharacters <= MaxNumberCharacters
    }

    def accept(value: Char): Boolean = {
      if (quoted) {
        stringCharacter(value)
        true
      } else {
        outsideCharacter(value)
      }
    }
  }

  private def safely[A](body: => A): Option[A] = {
    try {
      Some(body)
    } catch {
      case NonFatal(_) => None
      case _: StackOverflowError => None
    }
  }

  def asObject(raw: String): Option[JsObject] = {
    Option(raw).filter { text =>
      val bounds = new JsonBounds
      text.forall(bounds.accept)
    }.flatMap(text => safely(text.parseJson)).collect { case value: JsObject => value }
  }

  def compact(value: JsValue): Option[String] = safely(value.compactPrint)

  def retryAfterDeltaMillis(raw: String): Option[Long] = {
    val value = raw.trim
    if (value.nonEmpty && value.length <= MaxRetryAfterDigits && value.forall(c => c >= '0' && c <= '9')) {
      Some((BigInt(value) * MillisecondsPerSecond).min(BigInt(Long.MaxValue)).toLong)
    } else {
      None
    }
  }

  def errorCodes(value: JsValue): Set[String] = {
    @tailrec
    def collect(remaining: List[JsValue], codes: Set[String]): Set[String] = remaining match {
      case Nil => codes
      case JsObject(fields) :: rest =>
        val found = fields.get("code").collect { case JsString(code) => code }.toSet
        val nested = fields.get("innererror").toList ++ fields.get("details")
          .collect { case JsArray(details) => details.toList }.getOrElse(Nil)
        collect(nested ::: rest, codes ++ found)
      case _ :: rest => collect(rest, codes)
    }
    collect(List(value), Set.empty)
  }
}
