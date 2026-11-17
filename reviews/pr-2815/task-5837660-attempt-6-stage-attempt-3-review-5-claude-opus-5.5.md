---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 3
round: 5
timestamp: "2026-11-10T21:04:15Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "b6f8a2c71b9517fefed50d0ccd5b0ef47981737203fd6020355776a454911016"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 42234 (char 42233)
Invalid CLI event on line 363: Expecting ',' delimiter: line 1 column 6934 (char 6933)

## Original Response

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-3-review-5-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the production changes:
  - `ContentUnderstandingThrottling.scala` lines 14–57. Each `retry` call creates a new `Backoff` and starts `loop(1L)`. It logs when the count is a power of two, then calls `pause` and then `refundSince`.
  - `ContentUnderstandingProtocol.scala`:
    - Lines 327–346: the retry rule is `429 && (GET || admissionRejected)`.
    - Lines 269–276: `checkInterrupted` checks `AsyncClient.isTaskCancelled` and then the thread's interrupt flag.
    - Line 612 opts in analysis polls only. Line 638 opts in the submit POST.
  - `ContentUnderstandingBudget.scala` lines 14–47.
  - `ContentUnderstandingParams.scala` lines 139–176.
  - `ContentUnderstanding.scala`: active params, Boolean type checking, and the `analyzeUrl` query.
- [x] `ContentUnderstandingThrottlingSuite.scala` lines 28–184 covers:
  - Retry-After longer than the budget, in submit/analyze and poll modes, with identical POST bodies.
  - Sparse, redacted logs.
  - No replay for a 429 with a valid or invalid Operation-Location, 500, 408, or a disconnect.
  - Retry of truncated or oversized 429 responses.
  - Unchanged normal poll-attempt and duration limits.
  - Interruption of the waiting thread.
- [x] `ContentUnderstandingThrottlingPolicySuite.scala` covers:
  - The fallback floor, growth, jitter and cap, including a configured 120000.
  - Exact Retry-After values: 0, 120, `Long.MaxValue`, and past and future dates, with reset.
  - Refunds with a fake clock (line 44).
  - Management PUT/GET calls are unaffected.
- [x] `ContentUnderstandingOptionsSuite.scala` covers `allowInputTruncation`:
  - Unset, true and false on the GA and preview API versions, plus clearing the value.
  - A column holding true, false or null.
  - Rejection of a StringType column.
  - Poll mode ignores a missing column.
- [x] Other Scala suites:
  - `ContentUnderstandingConcurrencySuite.scala` lines 192–220 checks that management behaves the same with retry false and true.
  - `ContentUnderstandingSuite.scala` lines 355–375 and `ContentUnderstandingConcurrencySuite.scala` lines 354–376 test only the default path. This is the basis for Issue 2.
  - `ContentUnderstandingRecoverySuite.scala` lines 769–781 uses reflection with the six-argument `exchange` (`false, false`).
  - `ContentUnderstandingSubmissionSuite.scala` lines 690–698 covers the delegated `retryDelay`.
  - `ContentUnderstandingFuzzingSuite.scala` lines 16–21 runs serialization fuzzing with both new params against a stub.
- [x] In Python `test_ContentUnderstanding.py`:
  - **MLflow autologging:** tests configured, copied and loaded stages for CU and AnalyzeDocument, using `concurrency == "1"` as a positive control.
  - **Guide examples (lines 467–533):** two POSTs, no requests when projecting saved Parquet, and an `AnalysisException` without a POST on rerun.
  - **Notebook (lines 534–760):** invalid run IDs and mappings make no calls; the full run passes; a mismatched origin is rejected with nothing written (lines 698–716); recovery works without the creation cell.
  - **Generic inspection:** lines 1003–1040 and 1232–1330.
- [x] Cancellation wiring: in `Clients.scala` lines 50–97, `isTaskCancelled` falls back to `TaskContext.get()`. `init_spark.py` line 11 uses `local[*]`. The cancellation test is at `test_ContentUnderstanding.py` lines 311–414. This is the basis for Issue 1.
- [x] Notebook: `resource_origin` and endpoint validation are at ipynb lines 80–100. The origin check raises "do not match" at line 344, and the `errorifexists` writes are at lines 290 and 361. This is the basis for Issue 5.
- [x] Also reviewed: the `DatabricksUtilitiesSuite.scala` exclusion test (lines 19–28), and the throttling and MLflow sections of `Content Understanding.md`, checked against what the tests do.
- [ ] Not checked: no test verifies the `scala.concurrent.blocking` pool notification. I did not raise this as an issue because `HTTPClients.scala` lines 460–471 already wrap CU handlers in `blocking`, so the inner call cannot be observed.
- [ ] Not checked: the validation results were reported by the driver. I did not run any build or test.

## Issues

### Issue 1: Throttle-wait cancellation through Spark's TaskContext has no test that can fail
- **Severity**: Medium
- **File**: `cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala`
- **Line(s)**:
  - Python: lines 311–414. The 429 stub with `Retry-After: 60` is at lines 330–333, the checks at 391–407, and the follow-up job at 413–414.
  - Scala: lines 145–184; `join()` has no timeout at line 175.
  - Production code: `ContentUnderstandingBudget.scala` lines 28–47.
- **Description**: The checks in the new "throttled" subtest cannot detect a broken cancellable wait:
  - The `collect()` failure containing "cancel" comes from `cancelJobGroup` on the driver, whatever the executor does.
  - The 1.25-second request-count window cannot see a GET that is not due for 60 seconds. In any case, `checkInterrupted()` runs before every send.
  - Under `local[*]`, the follow-up job runs on another core.

  Scala covers only `Thread.interrupt()` on a thread with no TaskContext. Suppose `pause` were simplified to a single `Thread.sleep(delay)`. Every test would still pass, even though `isTaskCancelled` would be ignored for the whole Retry-After.
- **Risk**: The requirement to support Spark cancellation for uncapped waits is unprotected. With `interruptOnCancel=false`, which is the usual setting, the task blocks in `Await(Duration.Inf)`. A regression would hold an executor slot for the full Retry-After, possibly hours, while the tests stay green.
- **Suggested Fix**: Use one of these checks:
  - **Python:** for `interrupt=False`, after cancelling, poll `statusTracker()` (`getJobIdsForGroup` → `stageIds` → `numActiveTasks`). Assert it reaches 0 within about 5 seconds while Retry-After is 60.
  - **Scala:** call `execute(..., budgetOverride = Some(recordingBudget))` inside a local Spark task started with `setJobGroup(..., interruptOnCancel = false)`. Cancel once `pause(60000)` starts. Use a static latch to assert a `CancellationException` within seconds.

  Separately, use `join(5000)` plus `!isAlive` at line 175 so a regression fails quickly.

### Issue 2: No opt-in tests prove that non-429 analysis responses keep their normal behaviour
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (and the test suites)
- **Line(s)**:
  - Protocol: lines 327–346.
  - Related tests: ThrottlingSuite lines 101–143, ContentUnderstandingSuite lines 355–375, ConcurrencySuite lines 354–376.
- **Description**: `admissionRejected` alone is also true for a POST that returns 401 or 403 without an operation handle. Every opt-in test uses only 429, 500, 408, a disconnect, or 200/202 responses. The tests that cover 401/403 admission and GET 503 never call `setRetryThrottledRequests(true)`.
- **Risk**: Plausible rewrites of the retry rule would pass every test:
  - `(GET && 429) || admissionRejected(...)` would retry 401/403 forever.
  - `GET && retryable` would exempt 5xx responses from the budget.

  Either breaks the requirements that "definitive 429 rejection" and "non-429 budgets unchanged" still hold.
- **Suggested Fix**:
  - Parameterize both existing suites with `retry <- Seq(false, true)`. Assert that 401 and 403 give the same outcome and the same requests under both settings.
  - Add an opt-in GET 503 case with `maxPollAttempts(1)`. Assert one GET and that the transient error is kept.

### Issue 3: The HTTP-date test does not check the past-date case or the first fallback value
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala`
- **Line(s)**: 61–79 (lines 73–75)
- **Description**:
  - When the header is a past date, the test checks nothing about timing.
  - When the header is missing or invalid, it only checks for at least 1000 ms of wall-clock time, and that includes Spark job overhead.

  So the test does not check that `_.retryAfter` from the response is passed to the backoff.
- **Risk**: A wiring regression would not be caught, such as passing `None` or reading the wrong header. Valid past-date headers would then not be honoured exactly.
- **Suggested Fix**: Drive `execute` with a recording `budgetOverride`, as in lines 151–157, and assert these exact waits:

  | Retry-After header | Expected wait |
  | --- | --- |
  | Past date | 0 |
  | Missing or invalid | 1000 (the first fallback has no jitter) |
  | `2` | 2000 |

### Issue 4: Nothing tests that backoff and log counts reset for each exchange
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala`
- **Line(s)**: 33–56 (line 39 creates `new Backoff`; line 55 calls `loop(1L)`)
- **Description**: Every integration test throttles only one exchange per operation. PolicySuite tests `Backoff` objects directly. No test throttles both the submit POST and a later GET when Retry-After is missing.
- **Risk**: Moving the backoff or the counter to per-operation or per-partition scope would grow delays up to 60 seconds and suppress logs without any test failing. That moves toward the global throttling the user prohibited.
- **Suggested Fix**: In analyze mode with a recording budget, script these responses in order:
  1. POST 429 with no header
  2. 202
  3. GET 429 with no header
  4. 200

  Assert that the throttle waits are `Seq(1000, 1000)` and that both log lines contain `retryCount=1`.

### Issue 5: The notebook's endpoint validation and default-port matching are untested
- **Severity**: Low
- **File**: `cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py`; `docs/Explore Algorithms/AI Services/Quickstart - Semantic Extraction with Content Understanding.ipynb`
- **Line(s)**: test lines 698–716; ipynb lines 80–100 and 344
- **Description**: The only test covers a mismatched non-default port. Two branches have no test:
  - The error at line 100, raised for user info in the URL, a non-HTTP(S) scheme, a missing host, or an invalid port.
  - The default-port matching at line 92, so that `https://host` and `https://host:443/...` are treated as the same origin.
- **Risk**: A regression could drop the rejection of user info in the endpoint. A regression in default-port matching could also block legitimate recovery as a false mismatch. The JVM's own validation limits the impact.
- **Suggested Fix**:
  - Run the configuration cell with each invalid endpoint and assert a `ValueError` and zero requests.
  - Add a recovery test whose saved handle includes `:443`, and assert it succeeds without a POST.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, resolved from the current task model catalog",
    "stageBehavior": "Six concurrent independent lenses; human requested review after the fixes",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head. Most volume is tests and documentation; production changes are localized to CU request options, throttling and time accounting, and shared Python parameter inspection.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, fallback-only backoff, elapsed-budget refunds, credential inspection and durable notebook recovery interact with existing state machines.",
  "rationale": "Six complementary lenses cover the localized production changes and cross-connector inspection implications. The user explicitly requested another review after fixing the recovered findings. Retain all earlier failed evidence; a transport failure cannot count as signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: when the fixes are in please start review after this. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 3,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The CLI JSON transport failure may recur. Offline evidence does not establish live CU compatibility or approval of matching packaged Python/JVM artifacts."
}

## Driver resolution update before stage attempt 4

Original failed transport remains failed; these are fixes to recovered findings.

- L5I1: cancellation checks both the driver worker and the owning Spark stages'
  active executor task counts, using the existing ten-second cancellation window
  rather than adding a timeout. Coverage includes interruptOnCancel=false with
  a 60-second throttling delay.
- L5I2: added opt-in/off comparisons for POST401/403 and polling GET503, checking
  statuses/errors and exact request counts with finite stub sequences.
- L5I3: recording budgets assert the exact waits forwarded through protocol
  execution for missing, invalid, past-date, and numeric Retry-After headers.
  Existing parser tests bound future-date calculation and uncapped values.
- L5I4: POST429 twice then GET429 verifies fallback growth within an exchange
  and reset for the later GET. Diagnostic counts are intentionally partition-wide
  under the new sparse-logging policy; a deterministic reporter test covers it.
- L5I5: actual notebook configuration tests reject userinfo, unsupported schemes,
  missing hosts and malformed/out-of-range ports, and test default-port/case
  origin equivalence without live service calls.

Throttling/policy tests passed 17/17. Broader CU coverage is 194 distinct cases
across runs, and compile/style/codegen passed. Python validation is still running
at this update; no new review dispatch will precede its completion.

Python subsequently passed: 96 tests and 248 subtests, seven live tests skipped,
including the strengthened cancellation and actual-cell configuration cases.
