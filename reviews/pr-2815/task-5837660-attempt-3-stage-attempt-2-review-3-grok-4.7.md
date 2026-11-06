---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 3
timestamp: "2026-11-06T19:30:30Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "a239deea818e7689d128a6c71b171182fb496adfe9f680ba93ad1d6a43ebd788"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-3-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Poll, submission, and management failure retention in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala` (`checkInterrupted` at line 289, `retainPollState`, `exchangeBeforeDeadline`, `rowHandler`)
- [x] Executor scheduling versus task-thread interruption in `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala` (line 59) and `HTTPTransformer.scala` concurrency split
- [x] Timeout bound versus millisecond conversion in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala` (lines 110-111 and 137)
- [x] DNS permit release, deadline abort pool, shared-stop isolation, and admission/retry suites in the Content Understanding test sources (inspected, not executed)
- [ ] Live CU, packaged runtime, and Azure CI were not treated as evidence; none were supplied for this revision

## Issues

### Issue 1: Concurrency above 1 does not stop in-flight work when the Spark task is cancelled
- **Severity**: Medium
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala; cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: Clients.scala 54-62; ContentUnderstandingProtocol.scala 289-293
- **Description**: `checkInterrupted` only inspects the calling thread. For `concurrency == 1`, the handler runs on the Spark task thread, so a task interrupt is visible at the next poll boundary. For `concurrency > 1`, `AsyncClient` runs each request in `Future { ... }(ExecutionContext.global)` and the task thread only `Await`s. Spark cancellation interrupts the task thread, not those workers. `Await.result` does not cancel the futures. Workers also have no `TaskContext`, so they cannot see `TaskContext.isInterrupted`. Each orphaned call continues submitting or polling until its own `maxOperationDuration`, and a retry or speculative attempt can start another set while the old one is still running.
- **Risk**: After a job cancel, stage kill, or speculative duplicate, in-flight analyze/submit calls can still be accepted and polled for up to the operation budget (default 10 minutes, configurable to `Int.MaxValue`). That duplicates billable work, holds HTTP clients and DNS permits, and is not stopped by the existing interrupt checks.
- **Suggested Fix**: On the task thread, register a `TaskContext` completion listener that sets a per-partition `AtomicBoolean` and aborts the active `HttpRequestBase`. Pass that flag into `rowHandler` / `checkInterrupted`, and cancel the request futures when the await is interrupted. Keep concurrency 1 behavior unchanged.

### Issue 2: Validated timeout upper bound can wrap the millisecond socket timeout
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 110-111, 137
- **Description**: The guard allows `timeout <= Int.MaxValue / 1000` (about 2147483.647 seconds). `settings` then does `math.ceil(getTimeout * 1000).toInt`. At that boundary, double rounding can make the product slightly larger than `Int.MaxValue`. `ceil` then yields `2147483648`, and `toInt` wraps to `Int.MinValue`.
- **Risk**: `RequestConfig` rejects a negative connect/socket timeout, so a caller who set the documented maximum can fail the whole action before any row runs, or, if a negative value were accepted, disable the HTTP timeout and rely only on the best-effort deadline abort.
- **Suggested Fix**: Convert in `Long` space and clamp: `math.min(Int.MaxValue.toLong, math.ceil(getTimeout * 1000.0).toLong).toInt`. Reject the value in `validateConfiguration` if that result is not strictly positive.

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

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s3t2-grok-4.7-i1: Concurrent polling survives Spark cancellation

- Disposition: **confirmed**.
- Evidence: Loopback reproduction:0 late GETs at concurrency1,6 at concurrency2 after action cancellation and grace. Shared async implementation predates CU; queued POST claim not reproduced.
- Consolidated topic: `cancellation`.

### s3t2-grok-4.7-i2: Claimed Double-to-Int wrap

- Disposition: **false-positive**.
- Evidence: JDK11 probe: (int)2147483648.0 and computed maximum timeout both yield2147483647, not a negative value.
- Consolidated topic: `timeout-overflow`.
