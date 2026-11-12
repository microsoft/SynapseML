// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class ContentUnderstandingThrottlingPolicySuite extends TestBase {
  import ContentUnderstandingStub.withReplies

  test("fallback backoff has a positive floor, grows with jitter, and respects larger configured delays") {
    val backoff = new ContentUnderstandingThrottling.Backoff(0)
    assert(backoff.delay(None) == 1000L)
    val second = backoff.delay(Some("invalid"))
    assert(second >= 1800L && second <= 2000L)
    val delays = (1 to 100).map(_ => backoff.delay(None))
    assert(delays.forall(delay => delay >= 1000L && delay <= 60000L))
    assert(delays.last >= 54000L)
    assert(delays.takeRight(50).distinct.size > 1)
    val intermediate = new ContentUnderstandingThrottling.Backoff(30000)
    assert(intermediate.delay(None) == 30000L)
    val capped = (1 to 100).map(_ => intermediate.delay(None))
    assert(capped.forall(delay => delay >= 54000L && delay <= 60000L))
    assert(capped.distinct.size > 1)
    Seq(60000, 120000).foreach { delay =>
      val configured = new ContentUnderstandingThrottling.Backoff(delay)
      assert((1 to 10).forall(_ => configured.delay(None) == delay.toLong))
    }
  }

  test("real pauses tick at entry and repeatedly while waiting") {
    val budget = new ContentUnderstandingBudget(1000)
    var ticks = 0
    budget.pause(0L, () => ticks += 1)
    assert(ticks == 1)
    ticks = 0
    budget.pause(300L, () => ticks += 1)
    assert(ticks >= 3)
  }

  test("real pauses propagate callback failures after sleeping") {
    val budget = new ContentUnderstandingBudget(1000)
    val failure = new IllegalStateException("stop waiting")
    var ticks = 0
    val thrown = intercept[IllegalStateException] {
      budget.pause(300L, () => {
        ticks += 1
        if (ticks == 3) throw failure
      })
    }
    assert(thrown eq failure)
    assert(ticks == 3)
  }

  test("valid Retry-After is uncapped and exact, including zero and HTTP dates") {
    val backoff = new ContentUnderstandingThrottling.Backoff(0)
    backoff.delay(None)
    assert(backoff.delay(Some("0")) == 0L)
    assert(backoff.delay(None) == 1000L)
    assert(backoff.delay(Some("120")) == 120000L)
    assert(backoff.delay(Some(Long.MaxValue.toString)) == Long.MaxValue)
    assert(backoff.delay(Some("Wed, 21 Oct 2015 07:28:00 GMT")) == 0L)
    val date = ZonedDateTime.now(java.time.ZoneOffset.UTC).plusMinutes(2).withNano(0)
    val before = System.currentTimeMillis()
    val delay = backoff.delay(Some(date.format(DateTimeFormatter.RFC_1123_DATE_TIME)))
    val after = System.currentTimeMillis()
    assert(delay >= math.max(0L, date.toInstant.toEpochMilli - after))
    assert(delay <= math.max(0L, date.toInstant.toEpochMilli - before))
  }

  test("opt-in Retry-After accepts padded and oversized delta-seconds without changing legacy parsing") {
    val backoff = new ContentUnderstandingThrottling.Backoff(0)
    Seq(("0" * 17 + "120") -> 120000L, ("9" * 20) -> Long.MaxValue,
      ("9" * 200) -> Long.MaxValue, ("0" * 200) -> 0L).foreach { case (header, expected) =>
      assert(backoff.delay(Some(header)) == expected)
      assert(backoff.delay(None) == 1000L)
      assert(ContentUnderstandingResponseParsing.retryAfterMillis(Some(header)).isEmpty)
    }
  }

  test("opt-in Retry-After accepts obsolete HTTP dates without changing legacy parsing") {
    val backoff = new ContentUnderstandingThrottling.Backoff(0)
    Seq("Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994").foreach { header =>
      assert(backoff.delay(Some(header)) == 0L)
      assert(backoff.delay(None) == 1000L)
      assert(ContentUnderstandingResponseParsing.retryAfterMillis(Some(header)).isEmpty)
    }
  }

  test("obsolete Retry-After dates use UTC, reject malformed dates, and apply the fifty-year boundary") {
    val now = ZonedDateTime.parse("2026-11-06T08:47:37Z")
    val backoff = new ContentUnderstandingThrottling.Backoff(0, () => now.toInstant.toEpochMilli)
    Seq("Friday, 06-Nov-26 08:49:37 GMT", "Fri Nov  6 08:49:37 2026").foreach { header =>
      assert(backoff.delay(Some(header)) == 120000L)
      assert(backoff.delay(None) == 1000L)
    }
    assert(backoff.delay(Some("Mon Nov 16 08:49:37 2026")) == 864120000L)
    val boundary = now.plusYears(50)
    val format = DateTimeFormatter.ofPattern("EEEE, dd-MMM-uu HH:mm:ss 'GMT'", java.util.Locale.US)
    assert(backoff.delay(Some(boundary.format(format))) ==
      boundary.toInstant.toEpochMilli - now.toInstant.toEpochMilli)
    assert(backoff.delay(Some(boundary.plusSeconds(1).format(format))) == 0L)
    val later = ZonedDateTime.parse("2090-11-06T08:47:37Z")
    val laterBackoff = new ContentUnderstandingThrottling.Backoff(0, () => later.toInstant.toEpochMilli)
    val nextCentury = later.plusYears(25)
    assert(laterBackoff.delay(Some(nextCentury.format(format))) ==
      nextCentury.toInstant.toEpochMilli - later.toInstant.toEpochMilli)
    val beforeNonLeapCentury = ZonedDateTime.parse("2050-01-01T00:00:00Z").toInstant.toEpochMilli
    assert(new ContentUnderstandingThrottling.Backoff(0, () => beforeNonLeapCentury)
      .delay(Some("Tuesday, 29-Feb-00 00:00:00 GMT")) == 0L)
    Seq("Friday, 30-Feb-26 08:49:37 GMT", "Fri Feb 30 08:49:37 2026",
      "Friday, 06-Nov-26 08:49:37 PST", "Fri Nov  6 08:49:37 2026 trailing").foreach { header =>
      val malformed = new ContentUnderstandingThrottling.Backoff(0, () => now.toInstant.toEpochMilli)
      assert(malformed.delay(Some(header)) == 1000L)
      val second = malformed.delay(Some(header))
      assert(second >= 1800L && second <= 2000L)
    }
  }

  test("opt-in HTTP dates accept leap seconds in each wire format") {
    val now = ZonedDateTime.parse("2016-12-31T23:59:58Z").toInstant.toEpochMilli
    val backoff = new ContentUnderstandingThrottling.Backoff(0, () => now)
    Seq("Sat, 31 Dec 2016 23:59:60 GMT", "Saturday, 31-Dec-16 23:59:60 GMT",
      "Sat Dec 31 23:59:60 2016").foreach { header =>
      assert(backoff.delay(Some(header)) == 2000L)
    }
  }

  test("opt-in HTTP dates reject signed or non-four-digit years without throwing or wrapping") {
    val now = ZonedDateTime.parse("2026-11-06T08:47:37Z").toInstant.toEpochMilli
    val asctime = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss uuuu", java.util.Locale.US)
    val malformed = Seq("01 Jan 999999999 00:00:00 GMT", "01 Jan 10000 00:00:00 GMT",
      "06 Nov 94 08:49:37 GMT") ++ Seq(300000000, -292275000, -1, 10000).map { year =>
      ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC).format(asctime)
    }
    malformed.foreach { header =>
      val backoff = new ContentUnderstandingThrottling.Backoff(0, () => now)
      assert(backoff.delay(Some(header)) == 1000L, header)
      val second = backoff.delay(Some(header))
      assert(second >= 1800L && second <= 2000L, header)
    }
  }

  test("opt-in HTTP dates safely measure valid dates against extreme clocks") {
    val header = Some("Fri, 31 Dec 9999 23:59:59 GMT")
    assert(new ContentUnderstandingThrottling.Backoff(0, () => Long.MinValue).delay(header) == Long.MaxValue)
    assert(new ContentUnderstandingThrottling.Backoff(0, () => Long.MaxValue).delay(header) == 0L)
  }

  test("throttle refunds preserve previously spent time and charge the final attempt") {
    Seq(false, true).foreach { enabled =>
      var now = 0L
      val waits = ArrayBuffer.empty[Long]
      val budget = new ContentUnderstandingBudget(1000, () => now) {
        override def pause(delay: Long, onTick: () => Unit): Unit = {
          onTick()
          waits += delay
          now += TimeUnit.MILLISECONDS.toNanos(delay)
        }
      }
      now += TimeUnit.MILLISECONDS.toNanos(300)
      var attempts = 0
      val response = ContentUnderstandingThrottling.retry(enabled, budget, 0, "GET",
        new ContentUnderstandingThrottling.Context) {
        attempts += 1
        now += TimeUnit.MILLISECONDS.toNanos(100)
        if (attempts == 1) 429 else 200
      } (_ == 429, _ => None)
      assert(budget.remainingMs == 600L)
      assert(attempts == (if (enabled) 2 else 1))
      assert(response == (if (enabled) 200 else 429))
      assert(waits.toSeq == (if (enabled) Seq(1000L) else Seq.empty))
    }
  }

  test("opt-in does not replay management creation or extend management polling") {
    val throttled = ContentUnderstandingStubReply(429, """{"error":{"code":"Throttled"}}""",
      Map("Retry-After" -> "0"))
    val ready = ContentUnderstandingStubReply(200, """{"analyzerId":"custom","status":"ready"}""")
    val operation = "/contentunderstanding/analyzers/custom/operations/create-1?api-version=2025-11-01"
    Seq(false, true).foreach { enabled =>
      withReplies(Seq(throttled, ready)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
          .setRetryThrottledRequests(enabled)
        assert(intercept[ContentUnderstandingException] {
          stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false)
        }.response.httpStatus == 429)
        assert(server.requests.map(_.method) == Seq("PUT"))
      }
      withReplies(Seq(ContentUnderstandingStubReply(202, "{}",
        Map("Operation-Location" -> ("$ROOT" + operation), "Retry-After" -> "0")),
        throttled, throttled, ContentUnderstandingStubReply(200, """{"status":"Succeeded"}"""), ready)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
          .setRetryThrottledRequests(enabled).setMaxPollAttempts(2).setPollingDelay(0)
        intercept[ContentUnderstandingException] {
          stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false)
        }
        assert(server.requests.map(_.method) == Seq("PUT", "GET", "GET"))
      }
      withReplies(Seq(throttled, throttled, throttled, ready)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
          .setRetryThrottledRequests(enabled).setPollingDelay(0)
        assert(intercept[ContentUnderstandingException](stage.getAnalyzer()).response.httpStatus == 429)
        assert(server.requests.map(_.method) == Seq("GET", "GET", "GET"))
      }
    }
  }

  test("partition diagnostics limit batches and keep reporting during a single long wait") {
    var now = 0L
    val reporter = new ContentUnderstandingLogging.ThrottleReporter(() => now)
    ContentUnderstandingFixtures.withLogs { events =>
      (1 to 1000).foreach(_ => reporter.retrying("POST", 120000L))
      assert(events().size == 1)
      now += TimeUnit.MILLISECONDS.toNanos(ContentUnderstandingLogging.ThrottleReportIntervalMs)
      reporter.waiting("POST", 120000L)
      assert(events().size == 2)
      assert(events().last.getMessage.getFormattedMessage.contains("retryCount=1000"))
      reporter.retrying("GET", 0L)
      assert(events().size == 2)
      now += TimeUnit.MILLISECONDS.toNanos(ContentUnderstandingLogging.ThrottleReportIntervalMs)
      reporter.waiting("GET", 0L)
      assert(events().size == 3)
      assert(events().last.getMessage.getFormattedMessage.contains("retryCount=1001"))
    }
  }

  test("a shared stop aborts the active retry tick and refunds only throttled work") {
    var now = 0L
    var attempts = 0
    var reachedAfterTick = false
    val context = new ContentUnderstandingThrottling.Context
    val budget = new ContentUnderstandingBudget(1000, () => now) {
      override def pause(delay: Long, onTick: () => Unit): Unit = {
        now += TimeUnit.MILLISECONDS.toNanos(50)
        context.stop()
        onTick()
        reachedAfterTick = true
      }
    }
    now += TimeUnit.MILLISECONDS.toNanos(300)
    val response = ContentUnderstandingThrottling.retry(true, budget, 0, "POST", context) {
      attempts += 1
      now += TimeUnit.MILLISECONDS.toNanos(100)
      429
    } (_ == 429, _ => Some("60"))
    assert(response == 429 && attempts == 1)
    assert(!reachedAfterTick)
    assert(budget.remainingMs == 700L)
  }

  test("retry ticks report during a single long wait without another rejected request") {
    var now = 0L
    var attempts = 0
    val context = new ContentUnderstandingThrottling.Context {
      override val reporter = new ContentUnderstandingLogging.ThrottleReporter(() => now)
    }
    val budget = new ContentUnderstandingBudget(1000, () => now) {
      override def pause(delay: Long, onTick: () => Unit): Unit = {
        onTick()
        now += TimeUnit.MILLISECONDS.toNanos(ContentUnderstandingLogging.ThrottleReportIntervalMs)
        onTick()
      }
    }
    ContentUnderstandingFixtures.withLogs { events =>
      val response = ContentUnderstandingThrottling.retry(true, budget, 0, "GET", context) {
        attempts += 1
        if (attempts == 1) 429 else 200
      } (_ == 429, _ => Some("120"))
      assert(response == 200 && attempts == 2)
      assert(events().size == 2)
      assert(events().forall(_.getMessage.getFormattedMessage.contains("retryCount=1 ")))
    }
  }
}
