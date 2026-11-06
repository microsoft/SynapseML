---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 4
round: 4
timestamp: "2026-11-06T22:58:04Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d48b30ee7d14ec955899cde5f1437356af1decf66d095973862ec92594c6afe5"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-4-review-4-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Checked the `rejectedCreation` path in `ContentUnderstandingResponseParsing.scala` (`SubmissionLocation.rejectedCreation` and `withDiagnostic`). It runs before any polling, decodes with `CreationStatusNames`, sets `Unknown`, adds `UnacceptedOperationLocation` or `InvalidOperationLocation` (validated against the intended analyzer ID) plus `ManagementRecovery`, keeps nested service details and the original body/status, and follows no handle. Compared it with the accepted-status branch in `createAnalyzer` in `ContentUnderstandingProtocol.scala` (`validateOperationLocation` catch, about lines 768-775).
- [x] Traced creation lookup 404/410 through `decodePoll`, `retainPollState` and the `pollWithClient` loop. The `OperationLookupFailed` wrapper needs `!transportFailure && diagnostic.isEmpty`. `nonTerminalHttpError` now keeps the previous `Running` state, and the loop stops because `parsed.status == "Failed"` is not ongoing. Oversized or truncated bodies keep their `ResponseTooLarge`/`TransportError` codes. For analysis polling, an oversized 404 still keeps the prior state because `raw.diagnostic.isDefined`, which matches the updated docs.
- [x] Checked the `safeSubmission` and `assess` rewrite. Unsuccessful responses keep the service error and only add `InvalidOperationLocation` for unsafe handles. A successful non-accepted response (203/206) gets `UnacceptedOperationLocation` or `InvalidOperationLocation` with status `Unknown`. A terminal 200 with an invalid handle is unchanged.
- [x] Checked `BudgetedDnsResolver.acquire`, `release` and `awaitResolution`. FIFO head-only `tryAcquire` happens inside the `Waiters` monitor. `wait(0)` is avoided because `remaining == 0` throws first. The waiter is removed and others are notified on every exit. The permit is released in the worker's `finally`, or by the caller when nothing was submitted. Waits use a fixed elapsed-time limit, so they still end with a constant budget provider. Native work still finishes in the bounded pool.
- [x] Checked `ContentUnderstandingBudget.pause` for chunked sleep and leftover-time arithmetic, the `DeadlineControl` periodic cancellation task (shared `fired` CAS, cancelled in `release`, `setRemoveOnCancelPolicy(true)`), and `checkInterrupted` placement in the `UnknownHostException`/`IOException` handlers and `readBounded`.
- [x] Checked `AsyncClient.withTaskContext` in `Clients.scala`. It captures the owner on the calling task thread, restores or removes the `ThreadLocal`, checks for a cancelled task before running the request body, and turns `InterruptedException` into `CancellationException`. Checked failure propagation through `AsyncUtils.bufferedAwait`.
- [x] Checked that `SimpleHTTPTransformer.errorSchema` matches the runtime `coalesce` and `withColumn` behaviour: replacement uses Spark's case resolver, coalescing only happens on an exact name, ANSI and non-ANSI type widening both apply, nullability is `existing.forall(_.nullable)`, the response column is nullable, and ambiguous names are rejected.
- [x] Checked the Python side (`Utils.py` Seq-before-Map conversion that leaves bytes alone, the generated `setParams` validate-then-set flow with `skip_none`, and `_transfer_params_from_java` default refresh or pop) against the new Python tests.
- [ ] I ran no tests. My tools are read-only, and validation evidence comes from the driver.

## Issues

### Issue 1: Accepted creation with an unsafe or wrong-analyzer handle still reports `Failed`, unlike the new `Unknown` result for rejected creation
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: about 766-776 (`createAnalyzer`, the `validateOperationLocation` catch right after the new `rejectedCreation` branch)
- **Description**: The new branch makes a 203/206/500 PUT with a discarded handle return `status = "Unknown"`, the `InvalidOperationLocation` or `UnacceptedOperationLocation` code, `ManagementRecovery` guidance and the original service error. A few lines later, an accepted 200/201/202 PUT (for example 201 with `{"status":"creating"}`) whose handle points to another origin or another analyzer is still turned into `status = "Failed"`. That branch also uses `decode(raw, None)` with the default `StatusNames` instead of `CreationStatusNames`, replaces the service error with a bare `InvalidOperationLocation` (or `raw.diagnostic`), and adds no `getAnalyzer` recovery guidance.
- **Risk**: The service has accepted the creation, so the analyzer may still be provisioning. Reporting `Failed` claims a remote failure that did not happen, which breaks the rule that discarded handles must leave the result uncertain and explain it. Users may also be prompted to repeat the PUT. The same invalid handle now gives a more cautious result on a rejected status than on an accepted one, which is the opposite of what the evidence supports. The existing test (`ContentUnderstandingSuite` "provisioning rejects analysis-result locations…") only checks that the code is present, not the status.
- **Suggested Fix**: Route this branch through the same helper, for example `SubmissionLocation.rejectedCreation(decode(raw, None, CreationStatusNames), settings.endpoint, location, analyzerId)`, or a variant that always uses `InvalidOperationLocation` here. That gives `Unknown`, keeps the original service diagnostics and adds `ManagementRecovery`. Extend the 201/202 test to assert `status == "Unknown"`, the recovery text, the original body/status, and PUT-only traffic.

### Issue 2: An interrupt check added after DNS capacity is acquired leaks a raw checked `InterruptedException` out of `DnsResolver.resolve`
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala
- **Line(s)**: 41-58 (the `ContentUnderstandingProtocol.checkInterrupted()` inside the post-`acquire` `try`)
- **Description**: Interrupts during `acquire` and `awaitResolution` are converted to `InterruptedIOException` through `interrupted(error)`. The new `checkInterrupted()` between acquiring the permit and submitting the lookup is caught only for `RejectedExecutionException`, so a thread interrupted in that window throws a raw `InterruptedException`. Java code cannot normally throw that checked exception from `DnsResolver.resolve(...) throws UnknownHostException`. HttpClient's `MainClientExec` only aborts the connection holder for `IOException`, `HttpException` and `RuntimeException`, so it skips that cleanup. `exchangeBeforeDeadline` has no handler for this exception either, so `request.abort()` is never called. On the driver path (`createAnalyzer`/`getAnalyzer`), the caller gets an exception type that this resolver does not otherwise produce.
- **Risk**: Exception types differ depending on where the interrupt lands, and HttpClient's abort path is bypassed. Leased-connection cleanup then depends only on the per-operation client being closed. Callers that expect `InterruptedIOException` (as the new admission test does for the other paths) may not handle this case.
- **Suggested Fix**: Add `case error: InterruptedException => throw interrupted(error)` to the post-acquire `try`, keeping the `finally` that releases the permit. A `CancellationException`, which is a `RuntimeException`, can keep propagating as it does now.

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

## Driver disposition after targeted attempt 4

The invocation passed the finalized receipt, actual input/runtime identity and
scoped-tool independence audit, with no violations or warnings. The original
ISSUES_FOUND verdict and both findings above remain intact.

Both findings were reproduced before correction in `cu-focused-t5-red.log`.
Accepted HTTP 200 creation with an unsafe handle reported Failed rather than
Unknown. An interrupt injected exactly at the post-admission DNS check produced
InterruptedException rather than InterruptedIOException.

Issue 1 is locally corrected by reusing the existing creation diagnostic helper
with CreationStatusNames in the accepted-status unsafe-handle catch. It preserves
service/read diagnostics and adds uncertainty and recovery guidance without a GET
or repeated PUT. The expanded public-path matrix covers accepted and nonaccepted
statuses, unsafe/wrong-analyzer handles, original bodies/statuses, primary service
errors and nested details. Existing successful provisioning, terminal operation
failure and response-read diagnostic tests remain unchanged and passing.

Issue 2 is locally corrected by adding the missing InterruptedException
normalization catch in the post-admission block. The permit-release finally and
worker ownership are unchanged. A deterministic test-thread interrupt injection
repeats past worker capacity, checks the IOException subtype, cause and interrupt
flag, confirms no lookup was submitted, and verifies a subsequent lookup succeeds.
It does not rely on a probabilistic timing race.

Affected main/test style and incremental compilation passed, followed by all 174
offline CU Scala tests and four affected Python public paths, including job
cancellation (`cu-focused-t5-green.log`). These narrow fixes follow the human's
request to protect adjacent behavior while continuing the iterative review loop.
Independent targeted re-review remains pending; no merge-readiness or external
validation claim is made. Nothing has been committed or pushed.
