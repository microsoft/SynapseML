---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 10
round: 4
timestamp: "2026-11-11T04:33:10Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a8064c9d40a41c626505a2af54f4ccf0e9caa63140bfb5bba0f716b4ff58a47b"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-10-review-4-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage ledger.** I read every indexed current hunk range in files 1–31 from the frozen checkout and compared it with the removed baseline. That covers the `P\` Scala sources (1–7, 27), the Python tests (8–10), the Scala suites (11–16, 28–30), `Utils.py` (17), `Wrappable` (18), the `R\` codegen and nbtest files (19–23), the docs (24–25), `website\sidebars.js` (26), and all 472 notebook lines (31). I did not read anything under `reviews\pr-2815` or any other reviewer's output.
- [x] **Retry loop.** In `ContentUnderstandingThrottling.scala` lines 104–134:
  - `mark` is taken before each attempt, and `refundSince(mark)` runs after the wait (126).
  - The final attempt is charged.
  - A POST checks the stop flag before the wait and on every tick, using the `SubmissionStopped` `ControlThrowable`, which is caught by a stable-identifier match.
  - With retries disabled, the attempt runs exactly once.
- [x] **Retry scope and stop relabel.** In `ContentUnderstandingProtocol.scala` lines 317–338:
  - `retryEnabled = analysis && retryThrottledRequests`, so management GETs are excluded.
  - Only an opted-in, throttled POST that has been stopped is relabelled. The relabel nests the read error and keeps the status and body.
  - The retry-off path is unchanged.
  - Transport `throttled` and `admissionRejected` match the 429 rule: GET, or POST with no Operation-Location header at all. An empty header counts as present.
- [x] **Backoff arithmetic.** In Throttling lines 67–101:
  - base = max(1000, pollingDelay) and cap = max(60000, base).
  - The first delay is deterministic, and so is every delay when base ≥ 60000.
  - The jitter lower bound is never above `current`, so `nextLong` cannot throw.
  - The backoff resets per exchange and on any valid header.
  - It cannot overflow, because cap ≤ `Int.MaxValue`.
- [x] **Delta-seconds parsing.** `normalizeDeltaSeconds` works with `ResponseParsing.scala` lines 18 and 90–97:
  - `MaxRetryAfterDigits` = 19, which is the length of `Long.MaxValue`.
  - Large values saturate through BigInt.
  - Signs, decimals and non-ASCII digits fall back.
  - The legacy `retryAfterMillis` (99–103) is unchanged and still drives poll delays (Protocol 261–262, 617, 643, 707, 770).
- [x] **HTTP date parsing.** RFC850 uses `floorMod` for the 50-year boundary, with leap reference year 2000, Feb-29 handling, and leap-second normalization plus 1 second. I traced the tests in `ThrottlingPolicySuite` lines 60–132: 864120000 = 10 days + 120 s, and the second fallback falls in [1800, 2000].
- [x] **Budget.** In `Budget.scala`:
  - `pause` subtracts elapsed time rather than computing a deadline, so a `Long.MaxValue` wait cannot overflow.
  - `refundSince` never moves `started` past the current time.
  - Tick exceptions propagate.
  - An `InterruptedException` restores the interrupt flag.
- [x] **Throttle logging.** `Logging.scala` `ThrottleReporter` logs the first event, then at most once per 60 s or more, including during waits. It is synchronized and logs only task IDs, `retryCount` and `waitMs`.
- [x] **Exception propagation for Issue 1.** Nothing catches the exception on the way out:
  - Protocol 397–410: `withClient` uses only try/finally.
  - Protocol 661–680: `rowHandler` catches only `ContentUnderstandingException` (671).
  - Core `AsyncUtils.scala` 15–18: `bufferedAwait` rethrows the failure.
  - `HTTPClients.scala` 459–471: there is no catch.
- [x] **`allowInputTruncation`.** I traced `ContentUnderstanding.scala` 84–92 and 312–325 plus the Params file:
  - Unset or null values are omitted, and false is sent as "false".
  - It is sent only on POST, never in poll mode.
  - The column type is validated as BooleanType.
  - There is no API-version gating.
  - The retry trait is separate and defaults to false.
- [x] **Python.** The `Utils.py` overrides were removed, so PySpark's defaults apply. `clear` and `explainParam` behave as intended. `Wrappable` 468–475 no longer exempts `isSet`, and `PyCodegenSuite` 188–194 checks this. The assertions in the three Python test files were updated consistently.
- [x] **Notebook, docs and notebook-test exclusions.**
  - The notebook executes and saves once with `errorifexists` and reloads before display.
  - The analyzer is created eagerly.
  - Config and origins are validated before any recovery call or write.
  - `modelDeployments` is analysis-only.
  - Synapse and Databricks exclude the notebook, and the Fabric allowlist omits it.
  - The docs link and sidebar entry are correct, and the Autologging caveat is present.
- [ ] **No builds or tests were run** (read-only reviewer). The 206 CU Scala tests, the notebook and mutation tests, and the Black check are as reported by you and were not reproduced here. The live CU service and packaged artifacts were not exercised.
- [x] **Optional preferences, not counted as defects:**
  - RFC850 parsing ignores the weekday.
  - RFC1123 accepts non-GMT offsets and lenient field widths, the same as the legacy parser.
  - Asctime accepts a zero-padded or one-digit day.
  - Any 23:59:60 is accepted as a leap second.
  - `pause` runs two ticks on entry.
  - A throttled POST that the stop suppresses before its wait is charged to the budget.

## Issues

### Issue 1: An opt-in HTTP-date Retry-After with an out-of-range year throws or wraps instead of falling back
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottling.scala
- **Line(s)**: 23–24 and 37–53. The unguarded conversion is at 50–53 (`toEpochMilli - nowMillis` at 52). It is reached from 86–89 and 116.
- **Description**: Only the parsing (37–49) runs inside `Try`. The `parsed.map` conversion (50–53) does not. Two of the parse branches accept years that `java.time` can hold but epoch milliseconds cannot:
  1. **Asctime.** The `uuuu` pattern parses strictly as `appendValue(YEAR, 4, 19, EXCEEDS_PAD)`, which accepts signed years up to ±999,999,999. Example: an asctime date with the correct weekday and year `+300000000`.
  2. **RFC 1123.** `RFC_1123_DATE_TIME` uses `parseLenient()`. That makes its `appendValue(YEAR, 4)` read 1–9 unsigned digits, and the weekday is optional. Example: `01 Jan 999999999 00:00:00 GMT`.

  These inputs produce two failures:
  - **Exception.** For years of about ≥ 292,278,995 or ≤ −292,275,056, `Instant.toEpochMilli` throws `ArithmeticException`. Nothing on the path catches it: not `retry`, not `exchange` (Protocol 317–338), not `withClient` (397–410), not `rowHandler` (catches only `ContentUnderstandingException`, 671). Core `bufferedAwait` then rethrows it.
  - **Wrap.** For asctime dates less than `nowMillis` after `Instant.ofEpochMilli(Long.MinValue)` (about years −292,275,055 to −292,275,000), `toEpochMilli` succeeds but subtracting `nowMillis` underflows. `math.max(0L, …)` then returns a near-`Long.MaxValue` wait instead of 0 for a past date.

  The legacy `retryAfterMillis` (ResponseParsing 99–103) wraps `toEpochMilli` in `safely`, and the removed `retryDelay` used `Try`. This is therefore a regression, confined to the opt-in path. The default path, with retries off and poll delays going through the legacy parser, is unaffected.
- **Risk**:
  - It breaks requirement 2 ("missing/invalid → fallback; past → zero").
  - A 429 from a misbehaving service or proxy fails the Spark task. The task retry recomputes the partition and re-POSTs rows whose analyses were already accepted. That duplicates work and billing, contrary to "no uncertain replay".
  - A header that is always malformed fails the whole job after `spark.task.maxFailures`.
  - The far-past variant stalls a GET poll effectively forever. The shared stop does not cancel GET waits, so only an interrupt can end it.
  - Likelihood is low because it needs a non-RFC header and the feature is opt-in, hence Low.
- **Suggested Fix**: Make the conversion in `httpDateMillis` total. Leave the shared legacy parser unchanged.
  - Compute `val t = Try(instant.toEpochMilli).getOrElse(if (instant.isAfter(Instant.ofEpochMilli(nowMillis))) Long.MaxValue else Long.MinValue)`.
  - Return `if (t <= nowMillis) 0L else Try(Math.subtractExact(t, nowMillis)).getOrElse(Long.MaxValue)`. This saturates the same way as delta-seconds and never falls back early.
  - If years outside the spec should instead fall back, enforce RFC 9110's `4DIGIT` year for IMF-fixdate and asctime (for example, a `\d{4}` precheck), and still guard the conversion.
  - Add `ThrottlingPolicySuite` cases, each asserting that no exception is thrown and that the chosen value is returned:
    - `"01 Jan 999999999 00:00:00 GMT"`.
    - An asctime header formatted from `ZonedDateTime.of(300000000, …)`.
    - An asctime header for a date just after `Instant.ofEpochMilli(Long.MinValue)`, with an injected clock, expecting 0.
  - Optionally add an end-to-end `ThrottlingSuite` 429 case asserting that the row finishes without failing the task.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved locally; awaiting stage-attempt 11 independent review.
- **What changed**: Selected the RFC-grammar option: unsigned four-digit opt-in IMF/RFC1123 and asctime years. Conversion now stays in `Try`, compares past instants before subtraction, and saturates positive arithmetic overflow. The legacy parser remains unchanged.
- **Why**: Malformed dates fall back, while valid dates remain exact/uncapped without exceptions or arithmetic wrap.
- **How verified**: Pre-fix tests failed on `toEpochMilli` overflow and extreme-clock subtraction. The fixed seven-case malformed-year matrix, extreme-clock test, and submit/poll protocol fallback all passed in the 208-test CU run. Aggregate Scala compile/test-compile/style, fresh Python 81 tests plus 213 subtests, Black and whitespace passed. See session logs `cu-loop-s11-negative.log`, `cu-loop-s11-scala.log`, and `cu-loop-s11-python.log`.