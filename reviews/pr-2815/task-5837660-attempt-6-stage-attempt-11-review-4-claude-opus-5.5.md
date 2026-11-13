---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 11
round: 4
timestamp: "2026-11-11T05:16:15Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "459c76aca05b02616725823af856be92b7af839c997df1e406c5153a970d58f3"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-11-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage:** I read every indexed current range in files 1–31 with `view` from the frozen checkout and compared each removed baseline block with its replacement. No ranges are unread. I did not open `reviews\pr-2815` or any review run directory. Searches stayed inside the named source, test, docs and website paths, apart from `core\src\main\scala\...\io\http`, which I checked for `AsyncClient` task context and the `concurrency` default.
- [x] **Retry-After parsing** (`ContentUnderstandingThrottling.scala` 24–108):
  - IMF and asctime years are fixed-width, unsigned and four digits.
  - The RFC850 50-year boundary uses a strict `isAfter`.
  - An invalid Feb 29 falls back to backoff.
  - A leap second is normalized from `:60` to `:59`, then 1 s is added back.
  - Past dates give 0.
  - Overflow in `subtractExact` or `toEpochMilli` inside the `Try` saturates to `Long.MaxValue`.
  - Delta seconds: leading zeros are stripped, and more than 19 significant digits become `Long.MaxValue.toString`. That value is accepted by the unchanged `MaxRetryAfterDigits = 19` (`ResponseParsing:18`) and saturates through `BigInt(...).min(Long.MaxValue)` at 90–97. Large values never fall back early.
  - `retryAfterMillis` (99–104) is identical to the baseline `retryDelay`.
- [x] **Fallback backoff arithmetic:**
  - `base = max(1000, pollingDelay)` and `cap = max(60000, base)`. Because `current` is at most `Int.MaxValue`, `current * 2` and `current + 1` cannot overflow.
  - `lower = max(base, current - current/10)` is at most `current`, so `nextLong(lower, current + 1)` is always valid.
  - The first fallback, and every fallback when base ≥ 60000, is deterministic.
  - A valid header resets `next`, and each `retry` call creates a new `Backoff`.
  - The docs (Content Understanding.md 396–404, 502) and the `pollingDelay` description (Params 117–120) match the code.
- [x] **Retry loop, refund and stop** (Throttling 110–140, Budget 1–55, Protocol `exchange` 317–339):
  - The by-name `attempt` is re-evaluated on each loop.
  - `refundSince(mark)` covers disjoint, sequential intervals, so `started` never passes now. Only throttled attempts and their waits are refunded.
  - `SubmissionStopped` is caught only at the retry site. `InterruptedException` (with the interrupt flag restored), `CancellationException` from `checkInterrupted` (252–259) and other tick exceptions all propagate.
  - `Long.MaxValue` waits until cancelled, checking every ≤ 50 ms.
  - `retryEnabled = analysis && settings.retryThrottledRequests`. The relabel happens only when retry is on, the response is throttled and a POST is stopped. It keeps status and body and nests the read diagnostic as `innererror`.
  - A relabeled response has a diagnostic, so it is not `retryable` through the status path.
- [x] **No re-wrapping or lost diagnostics:** `checkSharedFailure` (485–500) classifies only 401/403 (`isSharedAuthFailure`, 478–482), 404/`AnalyzerNotFound`, and deployment codes. A relabeled 429 therefore never becomes a `SharedFailure`, so the `rowHandler` catch cannot replace its nested diagnostic.
- [x] **Throttle detection and call-site equivalence:**
  - `throttled` (Transport 34–35) is true only for a 429 on a GET, or on a POST with no `Operation-Location` header at all. A contradictory or invalid handle is never replayed.
  - Deriving `managementGet` as `!analysis && method == "GET"` matches the baseline's explicit flags at all four call sites (`pollWithClient` analysis and creation, `execute` POST, `getAnalyzerResponse`, `createAnalyzer` PUT). Management requests and non-429 responses are unchanged.
- [x] **Per-partition context and logging** (Protocol `rowHandler` 671–694, Logging 12–68):
  - The `Context` is serializable and captured in the task closure, so each task gets its own copy.
  - `context.stop()` keeps the baseline compare-and-set semantics.
  - `ThrottleReporter` is synchronized, logs on the first event and then at most once per 60 s, including during waits. Messages contain only the method, retry count, wait time and task IDs (from `AsyncClient.currentTaskContext`, which also covers Future threads). No URLs, bodies or secrets are logged.
- [x] **Scala test assertions traced against the code:** every assertion in ThrottlingSuite (1–373), ThrottlingPolicySuite (1–280) and OptionsSuite (1–71), plus the changed hunks in the Admission, Concurrency, Fuzzing, Recovery, Submission and ContentUnderstanding suites. Expected waits, request sequences, tick counts and diagnostics all agree.
- [x] **Requirements 3 and 4:**
  - `allowInputTruncation` omits unset or null values, sends `false`, is ignored in poll mode, and has no API-version allowlist (test_ContentUnderstanding.py 172–233).
  - The Utils.py ServiceParam overrides are removed, with no remaining `def isSet(` (PyCodegenSuite 188–194, ContentUnderstandingSuite 639–645).
  - Generic Python inspection excludes JVM-only ServiceParams and never calls `_java_service_value` (test_ContentUnderstanding.py 1179–1445; test_ServiceParamPythonBridge.py 123–246; test_OpenAIResponseSchema.py 87–94).
- [x] **Requirements 5 and 6:**
  - The notebook (1–472) validates configuration and origins before any service call or write, creates the analyzer eagerly, and sends `modelDeployments` only for analysis and only when non-empty. It saves with `errorifexists` and reloads before display; tests 674–945 cover this.
  - The notebook is excluded from unattended runs (SynapseTests 45–80, DatabricksUtilities 254–261).
  - The doc links resolve: `trailingSlash: true` (`website\docusaurus.config.js:17`) matches existing sibling links such as `Causal Inference\Overview.md:61`.
- [x] **Validation evidence:** the caller supplied it and I did not rerun anything — 208 CU Scala tests, 81 Python tests with 213 subtests, Black and whitespace checks, and the date-regression and notebook-overwrite mutation results. The earlier codegen, notebook and live-skip evidence was not rerun in this batch, and no live CU service was exercised.
- [x] **Optional observations, not defects:**
  - The IMF parser is lenient (case-insensitive, optional weekday, numeric offsets), like the legacy `RFC_1123` parser.
  - Leap-second normalization is case-sensitive on `GMT` and limited to `23:59:60`, which fits RFC 9110.
  - An empty or invalid `Operation-Location` conservatively blocks replay.
  - The asctime parser also accepts a single space before a one-digit day.