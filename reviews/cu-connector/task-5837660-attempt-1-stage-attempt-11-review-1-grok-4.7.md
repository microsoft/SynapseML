---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 11
round: 1
timestamp: "2026-11-04T17:07:57Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "0f2aff510f04bbd34e3205c5257467ab57de76f818aa4ea8841718f2a9fcde4c"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: execute
attempt: 1
stage_attempt: 11
round: 1
timestamp: 2026-11-04T17:01:27Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: 2dc8d691af10046d826005022ba79295805216cbd99a60b92d85fea1da97e753
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-11-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed submission/poll classification in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (`exchange`, `decodeSubmission`, `safeSubmission`, `rowHandler`, `DeadlineControl`)
- [x] Reviewed DNS admission and deadline mapping in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala`
- [x] Reviewed row validation, column-auth marker, and shared-failure wiring in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`
- [x] Checked marker stripping in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPSchema.scala` and null-request skipping in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala`
- [x] Compared behavior with recovery/concurrency tests and the preserve-handles / unsent-DNS contract
- [ ] Tests were not executed in this review; supplied local validation was not re-run

## Issues

### Issue 1: Budget-expired DNS before send is classified as an ambiguous submission
- **Severity**: High
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala
- **Line(s)**: 56-78
- **Description**: `BudgetedDnsResolver` fails an over-budget lookup with `OperationDeadlineExceeded` before `delegate.resolve` returns, so no socket is opened. `BudgetedDnsResolver.diagnostic` then replaces that failure with `timeoutError` whenever the deadline fired or `remainingMs == 0`. `ContentUnderstandingProtocol.exchange` records that as a transport failure, and `decodeSubmission` only promotes `DnsResolutionFailed` / `DnsCapacityExceeded` to `Rejected`. `OperationTimedOut` falls through `uncertainSubmission` and becomes `Unknown`. The same collapse happens if a real `DnsResolutionFailed` coincides with budget expiry, because `diagnostic` keeps the pre-send message only when `!timedOut`.
- **Risk**: A POST/PUT that never left the executor is reported as an ambiguous acceptance (`Unknown` plus "retain any handle and resume polling"). Callers that correctly refuse to repeat `Unknown` work will drop a safe retry; callers that treat it like a sent operation have no handle to poll. This violates the requirement that unsent DNS/capacity failures stay distinguishable from possibly accepted work.
- **Suggested Fix**: For POST/PUT, preserve a pre-send code (`DnsResolutionFailed`, `DnsCapacityExceeded`, or a dedicated unsent-deadline code) when resolution throws before execute obtains a connection. Map only that case to `Rejected`. Keep `OperationTimedOut`/`Unknown` for GET polls and for aborts that may already have written the request.

### Issue 2: Four shared abort threads can miss operation deadlines
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 97-108, 168-200, 421-430
- **Description**: Deadline aborts run on a process-wide `ScheduledThreadPoolExecutor` of size 4. `DeadlineControl.release` waits on the request thread until the abort runnable finishes. `abort()` can block (the concurrency test models this). Four blocked aborts occupy every worker, so later deadlines stay queued past `maxOperationDuration`. A blocked `abort()` that needs the request thread, while that thread waits in `release`, can also deadlock that operation.
- **Risk**: In-flight analyze/poll calls continue after the configured budget, and a few stuck aborts disable deadline enforcement for every other CU task on the executor. Handles and remote work then outlive the bound the connector claims to enforce.
- **Suggested Fix**: Do not join the abort thread from the request thread. Use a larger dedicated pool, or schedule each abort so a blocked abort cannot consume the only threads that must fire other deadlines. Unblock the request by closing the client connection without waiting for `abort()` to return.

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

## Driver follow-up: attempt 11

The original findings and verdict above are preserved. Fixes are implemented; post-fix validation is pending.

- **Issue 1:** Submit/analyze stalled-DNS tests reproduced `Unknown` with zero connections; the existing poll path retained its handle. Added explicit `WireResponse.notSent` classification for pre-execute budget expiry and DNS exceptions. Submissions become `Rejected`, with `RequestNotSent` for an unsent deadline; poll outcomes retain their handle. Real DNS failure diagnostics are no longer discarded at deadline. Physical lookup permits are unchanged.
- **Issue 2:** Four blocking abort callbacks can occupy the shared abort workers; this supports a best-effort, not hard wall-clock, deadline contract. The claimed production deadlock was not reproduced: Java `FutureTask.cancel(false)` may succeed while the callable runs, so the former latch wait was not necessarily entered in the reported scenario. Removed the explicit join and avoid resetting requests already being aborted; response/client cleanup still owns resources. Connection-acquisition, connect and socket timeouts are capped by the remaining operation budget. Added a local HTTP regression with all four abort workers occupied, plus a no-reset-during-abort assertion. These are controlled regressions, not evidence of an observed production deadlock.
- **Human policy:** The user chose existing per-partition SynapseML concurrency and comparable HTTP/polling behavior, not an executor-wide admission cap or a new cancellation engine. Documentation and the parameter description now explicitly call the total budget best-effort. Bounded abort/DNS workers remain; no unbounded thread creation or early DNS permit release was introduced.
- **Evidence:** Red DNS results are in `cu-attempt11-scala-red.log`. The initial post-fix run stopped at the DNS test-helper style limit; that helper was split and `cu-attempt11-fixes-validation2` is running. Final validation remains pending; this does not change the original dirty verdict.

### Post-fix validation completed

- `cu-attempt11-fixes-validation2.log`: style and compilation passed, followed by 7/7 shared HTTP tests and 82/82 CU tests. Submit/analyze/poll stalled-DNS classifications passed. The occupied-abort-worker HTTP timeout test completed in 1.011 seconds with a one-second operation budget and a five-second ordinary HTTP timeout; no new operation cap was used. The no-reset-during-abort assertion also passed.
- Generated Python and local publication succeeded; fingerprint-verified offline Python passed 15/15. Full Black passed with 205 files unchanged.
- Issue 1 is locally fixed. Issue 2 is addressed under the user's best-effort deadline/concurrency policy, with its unreproduced production-deadlock claim explicitly qualified above. No hard wall-clock guarantee is claimed; cancellation/cleanup can still delay completion. Original verdict remains dirty pending the next independent attempt.