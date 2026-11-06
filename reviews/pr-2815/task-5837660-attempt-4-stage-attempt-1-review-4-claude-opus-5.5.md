---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 4
timestamp: "2026-11-06T21:18:57Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "540c0e37b89d75e6d1a82934e7959671736c298fd7c5f61459f20f962e5d5b7c"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Spark task cancellation in the async client.** I traced `AsyncClient.withTaskContext` in `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala:50-74` through `AsyncUtils.bufferedAwait`. It restores or removes the thread-local owner in `finally`, refuses to start queued futures after the task ends, and converts `InterruptedException` into `CancellationException` so the promise completes. `Await` in `bufferedAwait` rethrows that failure.
- [x] **Abort and cancellation paths in `ContentUnderstandingProtocol.scala`.** The cancellation poller (lines 147-155) aborts the request and sets `fired`. In the exchange, both the `IOException` and `UnknownHostException` handlers (lines 382-399) call `checkInterrupted()` before they build a `TransportError` or timeout diagnostic. A cancelled task therefore raises `CancellationException` instead of being reported as an expired deadline. `release` (lines 159-166) cancels both scheduled futures. `setRemoveOnCancelPolicy(true)` at line 78 keeps cancelled periodic tasks out of the queue. `readBounded` aborts the request when it receives `CancellationException` (lines 313-315).
- [x] **DNS permits and time limits in `ContentUnderstandingDnsResolver.scala:31-104`.**
  - A permit is released exactly once on every path before submission: cancellation, deadline, rejected submission, or any other throw goes through `finally` when `submitted` is false. After submission, the worker's `finally` releases it.
  - Both wait loops compute the remaining time from `System.nanoTime` since they started, so a constant budget provider still ends.
  - `@tailrec` is valid in both `acquire` and `poll`: in `poll`, the recursive call is outside the `try`.
  - When a cancelled caller stops waiting, the native worker keeps running and still releases its permit when it finishes.
- [x] **Pause loop in `ContentUnderstandingBudget.scala:19-38`.** It checks for cancellation every 50 ms or less and keeps the total delay at `delay` plus or minus 1 ms. `CancellationException` is not caught by the `InterruptedException` handler, so it propagates.
- [x] **404/410 handling while polling (`decodePoll` and `retainPollState`, lines 562-596).** I traced each case:
  - **Readable analysis-result 404:** status becomes `ResultUnavailable`.
  - **Oversized or unreadable analysis-result 404:** the diagnostic is set or the call is retryable, so the prior state is kept, or `Unknown` if there was none.
  - **Analyzer-creation 404/410:** `nonTerminalHttpError` keeps the earlier creation state (`Running` from `creating`) with `OperationLookupFailed` and recovery text pointing to `getAnalyzer`. `createAnalyzer` then throws because the status is not `Succeeded`.
  - **Exception message:** `OperationLookupFailed` is on the exception-message allowlist (`ContentUnderstandingException.scala:14-19`), so the code appears in the message.

  This matches the edited documentation.
- [x] **Nonstandard terminal submissions.** I traced `SubmissionLocation.assess` (`ContentUnderstandingResponseParsing.scala:126-142`) through `safeSubmission` (`Protocol.scala:539-554`) for 203/206 with `Succeeded` or `Failed`, with both a valid and an invalid handle. All four cases give status `Unknown`, no `operationLocation`, and an `UnacceptedOperationLocation` or `InvalidOperationLocation` diagnostic. No GET is sent.
- [x] **Python bridge (`Utils.py:106-232`, CU `_transfer_params_from_java` in `ContentUnderstandingParams.scala:214-230`).**
  - The Scala `Seq` check runs before the `Map` check, and bytes are returned before any `_java2py` or pickle decoding.
  - JVM defaults are refreshed per parameter, and a Python default is removed when the JVM has none.
  - `_setDefault` skips type conversion for `JavaObject` values.
  - `_transfer_params_to_java` pushes ordinary defaults back to the JVM.
- [x] **Schema-only planning in `SimpleHTTPTransformer.scala:116-159`.** With an explicit transform-schema function, `Lambda.transformSchema` no longer calls `SparkContext.getOrCreate`. Nullability follows the semantics of `coalesce` and the nullable UDF. I also checked that the suite's `simpleTransformer` is a `def`, so `setErrorCol` in one test does not leak into others.
- [ ] Nothing was executed. Reviewer tools are read-only, and the driver's validation evidence was taken as supplied.

## Issues

### Issue 1: Unaccepted-handle diagnostic now replaces the service or HTTP error on non-2xx submissions
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingResponseParsing.scala (also ContentUnderstandingProtocol.scala `safeSubmission`)
- **Line(s)**: ResponseParsing 136-141; Protocol 545-551
- **Description**: `unaccepted` used to be produced only when `requiresLocation && !accepted`, which meant 2xx non-terminal responses. It is now produced for every non-accepted status that has a valid `Operation-Location`, including 3xx/4xx/5xx and 429/401/403. `safeSubmission` then sets `error = raw.diagnostic.orElse(assessment.error)`. For example, a 500 or 404 with a same-origin `Operation-Location` was already `Unknown` through `uncertainSubmission`, and its `error` held the service error (such as `InternalServerError` or `AnalyzerNotFound`) or the `HttpError` diagnostic. The `error` field now holds only `UnacceptedOperationLocation`.
- **Risk**: Users and pipelines that filter on the error code lose the service's failure classification for these responses. Those codes may indicate throttling, authentication problems or a missing analyzer. The original code is still available only by parsing `rawResponse`. The 203/206 terminal fix did not need this wider change.
- **Suggested Fix**: Produce the unaccepted diagnostic only when `successful && !accepted`. Alternatively, keep it for all statuses but nest the decoded service or HTTP error as `innererror`, as `decodePoll` does for `OperationLookupFailed`. Add a submission test for 500 with a same-origin `Operation-Location`.

### Issue 2: CU `setParams` no longer ignores explicit `None` for ordinary params and sends `null` to the JVM
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 205-211
- **Description**: The previous CU `setParams` skipped any `None` value. It now calls `_set_params_via_setters(kwargs)` with `skip_none=False`, and validation rejects `None` only for service params. For an ordinary param such as `pollingDelay=None`, `maxPollAttempts=None` or `outputCol=None`, the generated setter calls `self._set(name=None)`. That stores `None` in `_paramMap`. Then `ComplexParamsMixin._transfer_params_to_java` (`Utils.py:304-306`) calls `_make_java_param_pair(param, None)`, which sets the JVM param to `null`. For `Int` params, `null` unboxes to `0`. The constructor still uses `skip_none=True`, so `ContentUnderstanding(pollingDelay=None)` and `.setParams(pollingDelay=None)` now behave differently.
- **Risk**: A call that used to leave a setting unchanged now silently sets it to `0` or `null` at transform time. A zero polling delay can cause tight polling, and a `null` column name fails late, far from the call that caused it. The new tests cover only service params.
- **Suggested Fix**: In the CU `setParams`, either reject `None` for every param up front, before any setter runs, so the call stays all-or-nothing, or drop `None` for ordinary params as before while still raising for service params. Add one test for an ordinary param.

### Issue 3: Polling a fair DNS semaphore in 50 ms slices loses first-come-first-served ordering
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala
- **Line(s)**: 36-47 (with `Capacity = new Semaphore(MaxConcurrentLookups, true)` at line 133)
- **Description**: `Capacity` is a fair semaphore. The old code waited once with `tryAcquire(remaining)`, so a waiter kept its place in the queue. The new loop times out every ≤50 ms and calls `tryAcquire` again. Each timeout removes the waiter's queue node, and because of fairness (`hasQueuedPredecessors`), the retry joins the back of the queue. The longest waiters are pushed behind newer arrivals every cycle.
- **Risk**: When DNS workers are saturated, for example with 32 slow or blocked lookups, requests that arrived first and have the least budget left can keep losing permits to newer requests. They then fail with `DnsCapacityExceeded(deadlineExpired = true)`, while the old code would have given them the next free permit. Permit accounting is still correct; only the ordering and tail-latency behavior regressed.
- **Suggested Fix**: Keep the waiter's place while still checking for cancellation. One option is a FIFO queue of per-waiter promises that `release` completes; the waiter polls its own promise every 50 ms. Another is a single wait for the full timeout plus a cancellation hook that interrupts or signals the waiter. If the regression is accepted, document it beside `Capacity`.

### Issue 4: Explicit error-schema planning differs from runtime when `errorCol` matches an existing column only case-insensitively
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala
- **Line(s)**: 116-125 (runtime counterpart 150-158)
- **Description**: `errorSchema` matches the existing error field with case-sensitive `==`. At runtime, `df.withColumn(getErrorCol, …)` uses the session resolver, which is case-insensitive by default, and replaces the existing column in place under the new name. Take input `error` with `errorCol = "Error"`:
  - **Runtime output:** one column, `Error`, where `error` used to be.
  - **Planned schema:** `error` keeps its type and `Error` is appended.

  `transformSchema` previously ran the lambda on an empty DataFrame, so it always matched runtime. The new explicit plan breaks the "transformSchema matches runtime output" contract in this case.
- **Risk**: Downstream pipeline validation or schema-dependent planning sees a duplicate or extra column that will not exist when the transformer runs.
- **Suggested Fix**: In `errorSchema`, find the existing error field with the same case-insensitive resolver that `makePipeline` already uses. Replace that field in place, named `getErrorCol`, and use its nullability. Add one test with a differently-cased existing error column that asserts `transformSchema(frame.schema) == output.schema`.

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

## Driver resolution history: targeted iteration 2

The original review above is preserved. Its finalized receipt, actual model,
input hash and 28 scoped read-only calls passed the transport audit.

All four reported behaviors were reproduced before their corrections:

1. The unsuccessful-submission regression lost `ServiceDetail` when a location
   was returned. Handle diagnostics are now limited to successful response cases,
   preserving unsuccessful-response diagnostics and the existing Unknown state.
2. The ordinary-None regression showed mutation of the Python parameter map.
   CU setParams now first validates all service arguments, then delegates with
   ordinary None values skipped, preserving the previous no-op behavior and the
   atomic rejection of service None or conflicting bindings.
3. Under saturated DNS capacity, the newer waiter was admitted ahead of the older
   waiter. An explicit FIFO token remains queued across timed cancellation checks;
   it is removed on success, cancellation or expiration. Capacity release wakes
   waiters, and the existing bounded native worker pool remains unchanged.
   The regression also exercises cancellation of the oldest queued caller and
   admission of its successor.
4. The case-only schema regression showed a phantom planned column. Planning now
   uses Spark's resolver for replacement and its type coercion rules for exact-name
   preparation errors, including ANSI mode and incompatible/ambiguous inputs.

Red evidence: `cu-focused-t2-red-python.log`, `cu-focused-t2-red-core.log`, and
`cu-focused-t2-red-protocol-dns-r2.log`. The earlier protocol/DNS attempt was a
fixture failure (nested registration and cold-start timing), not defect evidence;
it remains retained alongside the corrected reproduction.

Status at this entry: **implemented locally; green validation and fresh targeted
review pending**. This does not claim measured production tail-latency improvement
or change the original ISSUES_FOUND verdict.

### Compatibility check during validation

The first green-validation attempt passed all four style scopes, main/test
compilation, both code-generation tasks and all 12 shared HTTP tests. CU passed
169 of 170 tests. Its existing throttled/unsafe-location regression still requires
an InvalidOperationLocation diagnostic; narrowing all such diagnostics to successful
responses removed that required information.

The existing assertion was retained. The correction now keeps an unsuccessful
service error as the primary diagnostic and appends the unsafe-location warning
to its details, preserving earlier details and innererror. Nonobject service errors
are retained in a diagnostic envelope. Safe compaction uses the existing bounded
helper. Additional public-path assertions verify both diagnostics and no GET.
The complete CU suites and Python regressions are being rerun before fresh review.
