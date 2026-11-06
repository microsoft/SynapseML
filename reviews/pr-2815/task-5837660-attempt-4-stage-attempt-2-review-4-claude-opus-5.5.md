---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 2
round: 4
timestamp: "2026-11-06T22:10:47Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9e4468393da7f731fe79a6b728173536674f5996f56167bc82fa470bcf58fcfe"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-2-review-4-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **DNS admission and permits** (`ContentUnderstandingDnsResolver.scala` L31-L94, L120-L158). I traced every permit path:
  - `acquire` registers the caller in the FIFO queue before entering `try`, then removes it and calls `notifyAll` in `finally`.
  - Only the caller at the head of the queue can call `tryAcquire`. Releases and the check-then-wait step share the `Waiters` lock, so no wakeup is lost.
  - Before submission, the `submitted` flag and `finally` return the permit after a cancellation, a deadline or a `RejectedExecutionException`. After submission, the worker's `finally` returns it.
  - Every timed wait or sleep uses `min(remaining, 50)` with `remaining > 0`, so `wait(0)` never causes an unbounded wait.
  - `waitRemaining(started, timeout)` gives a finite elapsed-time limit even with a constant budget provider.
  - Native DNS work keeps its permit until it finishes. Cancellation only stops the caller from waiting.
- [x] **Budget pause** (`ContentUnderstandingBudget.scala` L19-L40). The loop rechecks `checkInterrupted` at least every 50 ms. Millisecond truncation can add only about 1 ms. A `CancellationException` passes through unchanged, while an `InterruptedException` also restores the interrupt flag.
- [x] **Async task ownership** (`Clients.scala` L50-L94). The Spark task context is captured on the task thread and installed in a thread-local on each `Future` thread, which is restored or removed in `finally`.
  - Ended tasks fail with a `CancellationException` before the request body runs, so queued requests do not start.
  - An `InterruptedException` becomes a non-fatal `CancellationException` and the thread's interrupt flag is cleared, so the promise is always completed.
  - The new companion object adds no public members and leaves the `AsyncClient` class signature unchanged.
- [x] **Aborting blocked HTTP calls** (`ContentUnderstandingProtocol.scala` L133-L168, L270-L277, L299-L320, L380-L407). The cancellation poll aborts through the same `fired` flag as the deadline, and `release` cancels both scheduled tasks.
  - The `UnknownHostException` and `IOException` handlers call `checkInterrupted()` before classifying a deadline, so a cancellation-triggered abort is not reported as a timeout.
  - `readBounded` aborts on `CancellationException`.
  - Management calls on the driver have no task context, so no poller is scheduled.
- [x] **404/410 handling while polling** (`ContentUnderstandingProtocol.scala` L563-L593). For analysis polling, the `nonTerminalHttpError` change still gives the same results for readable, unreadable and oversized responses. For analyzer creation, 404/410 now keeps the previous Running state with an `OperationLookupFailed` diagnostic; without a previous state it returns `Unknown`. `OperationLookupFailed` was added to `DiagnosticCodes`. The documentation paragraph matches this behavior.
- [x] **Submission assessment** (`ContentUnderstandingProtocol.scala` L541-L556; `ContentUnderstandingResponseParsing.scala` L126-L159).
  - Non-2xx responses go through `retainFailure`, which keeps the service error as primary and appends the unsafe-location diagnostic. It handles a `details` value that is not an array, and wraps errors that are not objects in a string `innererror`.
  - For 203/206 responses, the new `unacceptedSuccess` flag yields an `UnacceptedOperationLocation` or `InvalidOperationLocation` diagnostic, and `uncertainSubmission` sets status `Unknown`. Untrusted handles are never followed.
- [x] **HTTP error-schema planning** (`SimpleHTTPTransformer.scala` L118-L139). Checked against runtime behavior:
  - Planning finds the existing error column by exact name, matching the runtime `df.columns.contains` check. Replacement uses Spark's name resolver, matching `withColumn`.
  - Nullability follows `coalesce`.
  - Wider-type operand order matches `findWiderCommonType`, since struct types are not partitioned as string types.
  - Only `SQLConf.get` is used, with no session or RDD. The `newCol` names cannot collide with the error column.
- [x] **Python changes** (`ContentUnderstandingParams.scala` L205-L232; `Utils.py` L170-L195, L76-L138; `Wrappable.scala` L505-L544).
  - `setParams` validates explicit None and scalar/column conflicts before any setter runs, then skips ordinary None values.
  - Loading refreshes every ordinary default from the JVM.
  - Scala `Seq` values become Python lists before the `Map` check, and raw bytes are returned before any JavaObject handling.
- [x] **Tests reviewed, not run.** Read `SimpleHTTPTransformerSuite` (`simpleTransformer` is a `def`, so no state is shared), the admission order test, the submission and creation tests, and the Python cancellation, `setParams` and load tests. Their assertions match the logic traced above. I did not execute anything.

## Issues

### Issue 1: Creation lookup 404/410 discards service errors that are not JSON objects
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 570-574 (with `retainPollState` 583-590)
- **Description**: The new `OperationLookupFailed` wrapper builds `innererror` as `response.error.flatMap(asObject).getOrElse(JsObject())`. Sometimes `decode` produces an error that `asObject` cannot parse:
  - a string or array service error, such as `{"error":"NotFound"}` or `{"error":[...]}`;
  - an object that exceeds the parser bounds.

  In those cases `innererror` silently becomes `{}`. `retainPollState` then keeps the previous `rawResponse` (the creation body), so the 404/410 body is not kept anywhere else. The new `SubmissionLocation.retainFailure` handles the same input shape correctly by wrapping it as `innererror: JsString(value)`.
- **Risk**: After a creation lookup failure, the service's only explanation can be lost. That makes the recommended `getAnalyzer` recovery harder to diagnose, and the creation path behaves differently from the submission path.
- **Suggested Fix**: Keep errors that are not objects as strings, for example `"innererror" -> response.error.map(e => asObject(e).getOrElse(JsString(e))).getOrElse(JsObject())`. Alternatively, reuse the `retainFailure` merge logic. Add a 404 case with `{"error":"NotFound"}` to the creation lookup test.

### Issue 2: Terminal 203/206 submissions now replace the service error with the handle diagnostic
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala; cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingResponseParsing.scala
- **Line(s)**: Protocol 547-552; ResponseParsing 136-141
- **Description**: `unacceptedSuccess` now applies to terminal 2xx statuses outside 200/201/202. `safeSubmission` then sets `error = raw.diagnostic.orElse(assessment.error)...`, which replaces the decoded service error. Before this change, terminal unaccepted responses kept the service error, or `OperationFailed` if there was none. For example, with HTTP 206, a body of `{"status":"Failed","error":{"code":"X",...}}` and an `Operation-Location` header, the error column now shows only `UnacceptedOperationLocation` or `InvalidOperationLocation`. The same change explicitly keeps the service error as primary for non-2xx responses. The raw body still survives in `rawResponse`, but the error column loses the failure reason.
- **Risk**: The error column is less useful for nonstandard terminal responses, and success and failure responses are handled inconsistently. Users who read only the error column lose the service's failure code.
- **Suggested Fix**: When `raw.successful && !accepted` and the response has an error, merge the handle diagnostic into it as `retainFailure` does: append to `details`, or keep a non-object error as `innererror`. Use the handle diagnostic alone only when there is no service error. Add a 203/206 case with an error body to the existing test.

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

## Driver resolution history: targeted iteration 3

The original response is preserved. The finalized receipt, actual model identity,
input hash and all 20 read-only calls passed the transport audit.

Both reported losses reproduced in `cu-focused-t3-red.log`: creation lookup
discarded a nonobject service detail, and nonstandard terminal submission
diagnostics replaced TerminalDetail. The creation matrix now includes objects
with and without code, strings, arrays, numbers and booleans. Terminal submission
cases include object, string and array service errors alongside both trusted and
untrusted discarded handles.

Creation wrapping now preserves nonobject serialized errors rather than replacing
them with an empty object. The existing diagnostic-merging helper is also reused
for successful terminal submissions: existing service errors remain available
alongside the handle warning, and a warning stands alone only when no original
error exists. State, handle trust and no-replay behavior remain unchanged.

Status: **implemented locally; green validation and fresh targeted review pending**.
Safe compaction remains in use, and the original verdict is not rewritten.
