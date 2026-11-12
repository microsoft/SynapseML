// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.{DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.Locale
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.ControlThrowable

private[contentunderstanding] object ContentUnderstandingThrottling {
  private val MinimumFallbackMs = 1000L
  private val MaximumFallbackMs = 60000L
  private val JitterDivisor = 10L
  private val LeapReferenceYear = 2000
  private val FutureYearWindow = 50
  private val YearsPerCentury = 100
  private val HttpYearDigits = 4
  private val Rfc1123Date = new DateTimeFormatterBuilder().parseCaseInsensitive()
    .appendPattern("[EEE, ]d MMM ").appendValue(ChronoField.YEAR, HttpYearDigits)
    .appendPattern(" HH:mm:ss ").appendOffset("+HHMM", "GMT")
    .toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
  private val AsctimeDate = new DateTimeFormatterBuilder().appendPattern("EEE MMM [ ]d HH:mm:ss ")
    .appendValue(ChronoField.YEAR, HttpYearDigits).toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
  private val Rfc850Date = new DateTimeFormatterBuilder().appendPattern("EEEE, dd-MMM-")
    .appendValueReduced(ChronoField.YEAR, 2, 2, LeapReferenceYear)
    .appendPattern(" HH:mm:ss 'GMT'").toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
    // Resolve the century before checking dates, without a conflicting weekday from another century.
    .withResolverFields(ChronoField.YEAR, ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_MONTH,
      ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE)

  private def httpDateMillis(raw: String, nowMillis: Long): Option[Long] = {
    val now = Instant.ofEpochMilli(nowMillis).atZone(ZoneOffset.UTC)
    val value = raw.trim
    // java.time cannot represent a leap second; advance from its preceding second after parsing.
    val normalized = value.replaceFirst("23:59:60(?= (?:GMT|[0-9]{4})$)", "23:59:59")
    val parsed = Try(ZonedDateTime.parse(normalized, Rfc1123Date))
      .orElse(Try(LocalDateTime.parse(normalized, AsctimeDate).atZone(ZoneOffset.UTC)))
      .orElse(Try {
        val parsed = LocalDateTime.parse(normalized, Rfc850Date).atZone(ZoneOffset.UTC)
        val boundary = now.plusYears(FutureYearWindow)
        val year = boundary.getYear - Math.floorMod(boundary.getYear - parsed.getYear, YearsPerCentury)
        // Compare in a leap year before resolving the century so February 29 is not silently clamped.
        val comparable = parsed.withYear(LeapReferenceYear).plusSeconds(if (normalized == value) 0 else 1)
        val resolvedYear = if (year == boundary.getYear &&
          comparable.isAfter(boundary.withYear(LeapReferenceYear))) year - YearsPerCentury else year
        LocalDate.of(resolvedYear, parsed.getMonthValue, parsed.getDayOfMonth)
          .atTime(parsed.toLocalTime).atZone(ZoneOffset.UTC)
      })
    parsed.map { date =>
      val instant = if (normalized == value) date.toInstant else date.toInstant.plusSeconds(1)
      if (!instant.isAfter(now.toInstant)) 0L
      else Try(Math.subtractExact(instant.toEpochMilli, nowMillis)).getOrElse(Long.MaxValue)
    }.toOption
  }

  /** Owns the partition's submission stop even with retries disabled, plus throttling diagnostics. */
  class Context extends Serializable {
    private val stopped = new AtomicBoolean(false)
    val reporter = new ContentUnderstandingLogging.ThrottleReporter
    def isStopped: Boolean = stopped.get()
    def stop(): Boolean = stopped.compareAndSet(false, true)
    def submissionStopped(method: String): Boolean = method == "POST" && isStopped
  }

  private object SubmissionStopped extends ControlThrowable

  class Backoff(pollingDelay: Int, clock: () => Long = () => System.currentTimeMillis()) {
    private val base = math.max(MinimumFallbackMs, pollingDelay.toLong)
    private val cap = math.max(MaximumFallbackMs, base)
    private var next = base

    // Normalize only opted-in retries, preserving the legacy parser's numeric length limit.
    private def normalizeDeltaSeconds(raw: String): String = {
      val value = raw.trim
      if (value.nonEmpty && value.forall(c => c >= '0' && c <= '9')) {
        val significant = value.dropWhile(_ == '0')
        val maximumSeconds = Long.MaxValue.toString
        if (significant.isEmpty) "0"
        else if (significant.length > maximumSeconds.length) maximumSeconds
        else significant
      } else {
        value
      }
    }

    def delay(retryAfter: Option[String]): Long = {
      val parsed = retryAfter.flatMap { raw =>
        ContentUnderstandingResponseParsing.retryAfterDeltaMillis(normalizeDeltaSeconds(raw))
          .orElse(httpDateMillis(raw, clock()))
      }
      parsed match {
        case Some(value) =>
          next = base
          value
        case None =>
          val current = next
          next = math.min(cap, current * 2)
          val lower = math.max(base, current - current / JitterDivisor)
          ThreadLocalRandom.current().nextLong(lower, current + 1)
      }
    }
  }

  def retry[T](enabled: Boolean, budget: ContentUnderstandingBudget,
               pollingDelay: Int, method: String, context: Context)(attempt: => T)
              (shouldRetry: T => Boolean, retryAfter: T => Option[String]): T = {
    if (!enabled) {
      attempt
    } else {
      val backoff = new Backoff(pollingDelay)
      @tailrec
      def loop(): T = {
        val mark = budget.mark()
        val response = attempt
        if (shouldRetry(response) && !context.submissionStopped(method)) {
          val waitMs = backoff.delay(retryAfter(response))
          context.reporter.retrying(method, waitMs)
          try {
            budget.pause(waitMs, () => {
              if (context.submissionStopped(method)) throw SubmissionStopped
              context.reporter.waiting(method, waitMs)
            })
          } catch {
            case SubmissionStopped => ()
          }
          budget.refundSince(mark)
          if (context.submissionStopped(method)) response else loop()
        } else {
          response
        }
      }
      loop()
    }
  }
}
