---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 9
round: 3
timestamp: "2026-11-11T03:10:06Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e5075bafa44ac8f34c9c4864755b6c24417f2f9b1c02b45107c751126945d0de"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-9-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage.** I read every indexed current hunk range in files 1–31 from the frozen checkout. No range is left unread.
  - Main CU sources read in full: `ContentUnderstandingThrottling.scala` 1–91, `ContentUnderstandingBudget.scala` 1–56, `ContentUnderstandingLogging.scala` 1–69, `ContentUnderstandingTransport.scala` 1–75, `ContentUnderstandingResponseParsing.scala` 1–125 (plus hunks), and `ContentUnderstandingParams.scala` 1–185.
  - `ContentUnderstanding.scala`: 1–480.
  - `ContentUnderstandingProtocol.scala`: all hunks, plus 296–343, 417–551, and 575–830.
  - All Scala suite hunks (files 11–16 and 28–30), all indexed Python test ranges (files 8–10), `Utils.py` 95–175, and the Wrappable and PyCodegenSuite hunks.
  - The nbtest files (20–23), plus the non-diff file `FabricNotebookTests.scala` 151–162 and 224–250 (include allowlist).
  - The CU guide 1–700, `Autologging.md` 1–40, `sidebars.js` 25–50, and the Quickstart notebook 1–472.
- [x] **Retry gating.** In Protocol 317–336, `retryEnabled = analysis && settings.retryThrottledRequests`.
  - Transport 21–22 and 35–36: `throttled` requires a 429 that is either a GET or a POST with no Operation-Location. An empty Operation-Location header still blocks the retry.
  - Every management path passes `analysis = false`: Protocol 702–703, 752, and `pollWithClient` 610–611 with an analyzer. Management therefore never uses the new loop.
  - The `managementGet = !analysis && method == "GET"` derivation gives the same value as the old explicit arguments at every call site.
- [x] **Backoff boundaries** (Throttling 27–57).
  - `base = max(1000, pollingDelay)` (validator is `gtEq(0)`) and `cap = max(60000, base)`.
  - The `nextLong(lower, current + 1)` bounds always hold, since `base ≤ lower ≤ current`. `current * 2` cannot overflow, because `cap ≤ Int.MaxValue`.
  - The first fallback, and any fallback with base ≥ 60000, is deterministic. A valid header resets growth, and each exchange creates a new `Backoff`.
- [x] **Retry-After boundaries.**
  - Zero-padded seconds are normalized. Values with more than 19 significant digits saturate to `Long.MaxValue` instead of falling back (Throttling 33–44; ResponseParsing 90–97 uses a BigInt clamp).
  - Past RFC 1123 dates become 0 (ResponseParsing 98–103).
  - `pause(Long.MaxValue)` has no arithmetic overflow and stays cancellable.
- [x] **Stop, refund and cancellation** (Throttling 59–89).
  - The stop flag is checked before each wait. The `SubmissionStopped` ControlThrowable aborts the wait, and `refundSince(mark)` runs only on the throttled branch.
  - Budget 21–55 runs the tick at entry and on every 50 ms chunk. InterruptedException re-sets the interrupt flag and propagates.
  - Clients 50–93: `isTaskCancelled` includes task completion.
  - No hidden await timeout caps "unlimited" retries. HTTPTransformer 107–117 uses `Duration.Inf` because CU rejects `concurrentTimeout` (ContentUnderstanding 112–116).
  - The new `blocking {}` is harmless: it nests inside `AsyncHTTPClient.handle`'s `blocking` (HTTPClients 460–462), and on a single-threaded task thread it does nothing.
- [x] **Suppressed POST path.** Traced through Protocol 333–335, `decode` 417–450, `decodeSubmission` 512–528 and `checkSharedFailure` 485–500.
  - Result: `Rejected`, HTTP 429, and the raw body are kept. A read or ResponseTooLarge diagnostic is nested as `innererror`.
  - It does not re-trigger a SharedFailure. With retries off, `exchange` returns the raw response unchanged.
- [x] **Per-task state and logging.**
  - ContentUnderstanding 403–431 creates the handler per transform, and the closure is deserialized per Spark task, so each task gets its own `Context`.
  - Rows queued after a stop are short-circuited in Protocol 659–679.
  - The reporter (Logging 15–36) is synchronized and logs the first event, then at most once per minute, including during waits. Each log line holds only the method, retry count, wait and task IDs.
- [x] **Docs, notebook and nbtest.**
  - The notebook validates configuration and the model-deployment mapping before any call, and creates the analyzer eagerly.
  - Submit writes once with `errorifexists`, then reloads the snapshot.
  - Recovery checks handle origins before polling or writing, and needs no mappings or recreation. Display is SDK-free and uses `codePoint` substrings.
  - Unattended runs exclude the notebook on Databricks, Synapse (generated name) and Fabric (allowlist).
- [ ] **No tests executed.** This reviewer is read-only. The caller supplied the validation evidence (compile, style, 203 CU Scala tests, 3 Python regressions, Black). Live CU and packaged runs were not available.

## Issues

### Issue 1: Opted-in Retry-After handling misses the obsolete HTTP-date forms that RFC 9110 recipients must accept, so it falls back early
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingResponseParsing.scala; docs\Explore Algorithms\AI Services\Content Understanding.md
- **Line(s)**: ResponseParsing.scala 98–103; ContentUnderstandingThrottling.scala 46–56; Content Understanding.md 396–398 and 525–531
- **Description**: RFC 9110 §5.6.7 defines `HTTP-date = IMF-fixdate / obs-date`, where obs-date covers the rfc850-date and asctime-date forms. It says a recipient that parses timestamps "MUST accept all three HTTP-date formats."
  - `retryAfterMillis` only tries `DateTimeFormatter.RFC_1123_DATE_TIME`. Values like `Sunday, 06-Nov-94 08:49:37 GMT` or `Sun Nov  6 08:49:37 1994` therefore return `None`.
  - `Backoff.delay` then treats them as missing or invalid and uses the fallback.
  - The guide says "Valid `Retry-After` seconds or HTTP dates are honored exactly" and "Missing or malformed values use the adaptive fallback." Neither statement holds for these valid forms.
  - This is a defect in spec conformance and documentation accuracy, not a style preference. It is not a regression: the old `retryDelay` used the same formatter.
- **Risk**: If a gateway or proxy returns an obsolete-format date, opted-in retries fire after the fallback delay (1 s, growing toward 60 s) instead of at the time the server asked for. That is an early retry for a valid header, which goes against requirement 2 ("never early fallback").
  - The extra 429 traffic is bounded and cancellable. There is no duplicate-submission or data risk, because these POSTs were definitively rejected.
  - Real-world impact is low, since Azure CU sends delta-seconds.
- **Suggested Fix**: Either option resolves it.
  - **Smallest fix:** narrow the docs to "delta-seconds or IMF-fixdate (RFC 1123) dates; obsolete RFC 850/asctime dates use the fallback."
  - **Full fix:** on the opted-in path only, so the legacy and management paths stay unchanged, also parse RFC 850 dates (two-digit years per RFC 9110 §5.6.7) and asctime dates (`Locale.US`, GMT). Add ThrottlingPolicySuite cases for both forms: a future date gives the exact delay, a past date gives 0, and malformed values still fall back.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved in the working tree for stage-attempt 10.
- **What changed**: `ContentUnderstandingThrottling.Backoff` now parses RFC850 and asctime dates in UTC, in addition to RFC1123. RFC850 uses the exact 50-year future boundary, including the century/leap-day boundary. The opt-in parser also handles HTTP leap seconds. The shared legacy parser is unchanged; numeric normalization and adaptive fallback remain intact. The guide distinguishes opt-in HTTP-date support from unchanged legacy parsing.
- **Why**: Verified RFC 9110 section 5.6.7 requires all three formats and defines both the two-digit-year interpretation and leap seconds. Narrowing the documentation would weaken the human requirement to honor valid headers rather than fix it.
- **How verified**: The new obsolete-date regression first failed against the old implementation (`1000 != 0`). Aggregate compile, test-compile, production/test style and all 206 CU Scala tests then passed. Coverage includes future/past dates in both obsolete formats, malformed dates, fallback reset/growth, exact and just-over-50-year boundaries, cross-century/leap-day interpretation, leap seconds, and actual POST/GET protocol waits. The initial style run flagged unnamed date constants; these were named before the successful rerun. Logs: session `cu-loop-s10-negative.log` and `cu-loop-s10-scala-retry.log`. No live service or packaged execution.