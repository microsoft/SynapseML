// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.config.{ConnectionConfig, MessageConstraints}
import org.apache.http.HttpStatus
import spray.json.{JsArray, JsObject, JsString, JsValue, enrichString}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
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

  def retryAfterMillis(value: Option[String]): Option[Long] =
    value.flatMap(retryAfterDeltaMillis).orElse {
      value.flatMap(v => safely(
        ZonedDateTime.parse(v.trim, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant.toEpochMilli))
        .map(t => math.max(0L, t - System.currentTimeMillis()))
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

private[contentunderstanding] object SubmissionLocation {
  case class Assessment(error: Option[String], missing: Boolean)

  def validate(endpoint: String, location: Option[String]): Option[Either[String, String]] =
    location.map { value =>
      try {
        Right(ContentUnderstandingProtocol.validateOperationLocation(endpoint, value).toString)
      } catch {
        case _: IllegalArgumentException => Left(ContentUnderstandingProtocol.diagnostic("InvalidOperationLocation",
          "The service returned an unsafe or malformed Operation-Location; it was not requested."))
      }
    }

  def assess(httpStatus: Int,
             successful: Boolean,
             rawStatus: Option[String],
             accepted: Boolean,
             operationLocation: Option[String],
             validated: Option[Either[String, String]]): Assessment = {
    val status = rawStatus.map(_.toLowerCase(java.util.Locale.ROOT))
    val terminal = status.exists(Set("succeeded", "failed", "canceled"))
    val requiresLocation = successful && !terminal &&
      (status.exists(Set("running", "notstarted")) || httpStatus == HttpStatus.SC_ACCEPTED)
    val unacceptedSuccess = successful && !accepted
    val error = if (requiresLocation || unacceptedSuccess) validated.flatMap(_.left.toOption) else None
    val unaccepted = validated.filter(_ => unacceptedSuccess).flatMap(_.right.toOption).map { _ =>
      ContentUnderstandingProtocol.diagnostic("UnacceptedOperationLocation",
        s"The service returned Operation-Location with HTTP $httpStatus; the handle was not followed.")
    }
    Assessment(error.orElse(unaccepted), requiresLocation && operationLocation.isEmpty)
  }

  def rejectedCreation(response: ContentUnderstandingResponse, endpoint: String,
                       location: String, analyzerId: String): ContentUnderstandingResponse = {
    val code = try {
      ContentUnderstandingProtocol.validateOperationLocation(endpoint, location, Some(analyzerId))
      "UnacceptedOperationLocation"
    } catch {
      case _: IllegalArgumentException => "InvalidOperationLocation"
    }
    val extra = ContentUnderstandingProtocol.diagnostic(code,
      s"The analyzer creation Operation-Location with HTTP ${response.httpStatus} was not followed. " +
        ContentUnderstandingTransport.ManagementRecovery)
    withDiagnostic(response.copy(status = "Unknown"), Some(extra))
  }

  def withDiagnostic(response: ContentUnderstandingResponse,
                     extra: Option[String]): ContentUnderstandingResponse = {
    extra.flatMap(ContentUnderstandingResponseParsing.asObject).map { diagnostic =>
      val error = response.error.flatMap(ContentUnderstandingResponseParsing.asObject).map { original =>
        val details = original.fields.get("details").map {
          case JsArray(values) => values
          case value => Vector(value)
        }.getOrElse(Vector.empty)
        JsObject(original.fields + ("details" -> JsArray(details :+ diagnostic)))
      }.getOrElse {
        JsObject(diagnostic.fields ++ response.error.map(value => "innererror" -> JsString(value)))
      }
      response.copy(error = ContentUnderstandingResponseParsing.compact(error).orElse(response.error))
    }.getOrElse(response)
  }
}
