---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 3
timestamp: "2026-11-06T19:22:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "89ff82d5e51dda348a83d6e6d9bbd96fc70b6e8e6f1799ca909191111a6eb56d"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Transport deadlines and aborts** (`ContentUnderstandingProtocol.scala` 351–429, `DeadlineControl` 177–199). I traced these failure cases:
  - The budget is already spent before dispatch.
  - The deadline aborts the request before `execute`. HttpClient's `RequestAbortedException` reaches the `IOException` branch with `notSent` and `expired` both true.
  - The request is aborted mid-read.
  - The `fired`/`releaseConnection` race.
  - `ResponseTooLarge` happens before or during the read.
  - The thread is interrupted. That case is rethrown.

  In each case the result is classified correctly and any handle that is available is kept.
- [x] **DNS budget** (`ContentUnderstandingDnsResolver.scala` 27–79). I checked:
  - The semaphore is released when the executor rejects the task, and released inside the task's `finally`.
  - A budget that expires after admission does not submit work. `ContentUnderstandingAdmissionSuite` 556–575 covers this.
  - Every resolver failure is an `UnknownHostException` subclass, so a failed lookup is classified `notSent` (`Rejected`) and is never replayed.
- [x] **Submission classification** (`decodeSubmission`/`safeSubmission`/`SubmissionLocation`, lines 43–75 and 533–566). I walked through:
  - 202 with an empty or oversized body.
  - 401 whose body can't be read.
  - 404 with and without a diagnostic.
  - An unsafe Operation-Location.
  - 200 Running with no location.

  Uncertain submissions become `Unknown`. They are polled only when a validated handle exists, and they are never resubmitted.
- [x] **Poll loop** (`pollWithClient`/`retainPollState`, lines 580–630). I checked:
  - It terminates for retryable, missing-status, non-terminal 4xx, `expiredBeforeDispatch` and attempt-exhaustion responses.
  - It keeps the last `Running` state on each of those paths.
  - Retry-After is capped at `Long.MaxValue` (`ContentUnderstandingResponseParsing.scala` 88–95).
  - A delay longer than the remaining budget returns immediately with `OperationTimedOut`.
- [x] **Shared-failure circuit** (`rowHandler` 668–694, `checkSharedFailure` 506–520, `rowUsesColumnAuth` `ContentUnderstanding.scala` 155–165):
  - The per-task `AtomicBoolean` is set by compare-and-set.
  - Poll mode never stops.
  - The provenance marker is removed from the wire in `HTTPSchema.scala` 209, and user headers can't spoof it (`CognitiveServiceBase.scala` 562–566).

  I compared this against the tests at `ContentUnderstandingSubmissionSuite` 309–345 and `ContentUnderstandingAdmissionSuite` 579–627.
- [x] **Row validation** (`ContentUnderstanding.scala` 267–292, 343–356, 380–388):
  - Every row validator throws `IllegalArgumentException`, which `rowError` turns into `InvalidInput`.
  - Invalid rows reach `inputFunc` as null and are skipped, so `prepareRequest` doesn't throw for them on the executor.
  - Ambiguous or case-variant columns are rejected using the resolver.
- [x] **Management calls** (`createAnalyzer`/`finishCreation`/`getAnalyzerResponse`, lines 712–800). I checked PUT transport failure, an unsafe creation handle, poll exhaustion, and a final GET that expires before dispatch. These match the documented behavior in `docs\Explore Algorithms\AI Services\Content Understanding.md` 482–492.
- [x] **Python lifecycle** (`ContentUnderstandingParams.scala` 158–264):
  - `setDocumentBytes` rejects `str`/`int` before touching the JVM.
  - `_transfer_params_to_java` rejects `None` without dropping pending values.
  - `_transfer_params_from_java` doesn't need the base complex-param branch, because CU has no non-service ComplexParam.
  - CU does not mix in `HasSetLocation` (`CognitiveServiceBase.scala` 688–700, 717–722), so a stale-URL `setLocation` path doesn't exist.
- [x] **Base-class change**: `ServiceAuthHeaders.addContentType` (`CognitiveServiceBase.scala` 358–361, 431) only removes duplicate Content-Type headers that differ in case. With exact-case keys, the previous code already overwrote the user's value, so existing behavior is preserved.
- [ ] I did not run any tests or live CU calls (read-only review). I relied on the supplied validation evidence for build and test status.

## Issues

### Issue 1: A scalar fallback-credential 401/403 also blocks rows that carry their own column credential
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala` and `ContentUnderstanding.scala`
- **Line(s)**: Protocol 499–504, 668–683; ContentUnderstanding.scala 155–165
- **Description**: Take this configuration: `setSubscriptionKeyCol("key")`, where some rows have a null key, plus a scalar `setAADToken(...)` as the fallback.
  - A row with a null key falls back to the scalar AAD token. It gets no row-auth marker, and its 401 matches `scalarAuthFingerprints`.
  - `isSharedAuthFailure` returns true, and `rowHandler` sets `stopped`.
  - From then on, every non-poll row in the partition is short-circuited at line 674 without being sent. That includes rows whose own column `subscriptionKey` is valid and has nothing to do with the failed scalar token.

  The `stopped` flag doesn't record which category tripped it, and it doesn't check whether the request carries the row-credential marker (`rowCredentialWasSelected`). No existing test covers a mix of null column credentials and a scalar fallback. `ContentUnderstandingSubmissionSuite` 309 only covers rows that all have non-null column tokens.
- **Risk**: Rows that would have succeeded are reported as `Rejected` with `SharedConfigurationFailure`. This causes extra retry work and confusing diagnostics. Nothing is lost or duplicated, because the rows are never sent and the documented retry query picks them up.
- **Suggested Fix**: Store the category that tripped the circuit, for example in an `AtomicReference[Option[String]]`. When the category is `"authentication"`, still dispatch rows whose `HTTPRequestData` carries `RowAuthColumnMarker`. Analyzer and deployment stops would keep their current behavior. Add a test with rows `[null-key → scalar AAD 401, valid column key → 200]`.

### Issue 2: A 404/410 on the analyzer-creation operation is reported as terminal `Failed`, not as an uncertain management state
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 573–579, 586, 622–623, 777–778
- **Description**: For management polls, `analyzer.isDefined`, so `decodePoll` doesn't map 404/410 to `ResultUnavailable` (line 576). But `retainPollState` still excludes 404/410 from `nonTerminalHttpError` (line 586) without checking whether this is a management poll. So a 404 or 410 on `/analyzers/{id}/operations/{op}` takes the `parsed` branch:
  - `decode` sets status `Failed` with an `HttpError` diagnostic.
  - The loop stops, and `createAnalyzer` throws with `status = "Failed"`.

  Any other non-transient 4xx on the same poll (400, 401, 403) instead keeps the prior `Running` state. In this case the PUT was accepted and the analyzer may already be ready (for example, the operation record expired), yet the error has no `ManagementRecovery` guidance ("Use getAnalyzer…; do not repeat creation"). No test covers a 404 on a creation poll.
- **Risk**: Callers may conclude creation failed and call `createAnalyzer(..., allowReplace=True)` again. That repeats billable or disruptive provisioning, which conflicts with the documented guidance not to repeat creation.
- **Suggested Fix**: Treat 404/410 as non-terminal for management polls by changing line 586 to `... && (analyzer.isDefined || !UnavailableResults(raw.httpStatus))`, threading `analyzer` into `retainPollState`. Alternatively, have `decodePoll` map a management 404/410 to `Unknown` with a diagnostic that includes `ContentUnderstandingTransport.ManagementRecovery`. Add a test where PUT returns 201 with a location and the poll returns 404. It should assert the status isn't `Failed`, the error contains `getAnalyzer`, and only one PUT was sent.

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

### s3t1-claude-opus-5.5-i1: Scalar fallback stops the whole partition

- Disposition: **intentional-documented-contract**.
- Evidence: CU guide explicitly states later rows with their own credentials are skipped after scalar fallback rejection; the circuit deliberately does not maintain per-credential state.
- Consolidated topic: `partition-stop`.

### s3t1-claude-opus-5.5-i2: Creation lookup404/410 misreports provisioning failure

- Disposition: **confirmed**.
- Evidence: Loopback accepted PUT then management GET404/410: Failed, retained handle, no getAnalyzer recovery guidance, exactly PUT+GET. cu-native-review-probes.log.
- Consolidated topic: `management-state`.
