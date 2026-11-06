---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 4
round: 1
timestamp: "2026-11-03T22:47:52Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "356ba092afc5918df3285e1694879178c69f0dd9eb0849f0e19496cb1d313633"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-4-review-1-grok-4.7.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala` for validation, `sharedAuth`, request construction, and the internal pipeline.
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` for exchange, client close, deadline abort, `errorCodes`, shared-failure handling, and poll GET reconstruction.
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` (`ServiceAuthHeaders.resolve` and `addHeaders`) for credential precedence versus `Content-Type`.
- [x] Compared shared-failure behavior with `docs/Explore Algorithms/AI Services/Content Understanding.md` and the Content Understanding Scala tests' error-payload coverage. Tests were not executed.
- [ ] Live Content Understanding tests were not treated as validation evidence; they are credential-gated and were not run.

## Issues

### Issue 1: Client close can replace a completed outcome
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 359-367
- **Description**: `withClient` calls `client.close()` in `finally` with no `NonFatal` guard. `closeQuietly` (lines 287-294) already documents that a close failure must not replace a known outcome, but that guard applies only to the response. A throwing client close replaces a successful return, a `ContentUnderstandingException`, or an interrupt.
- **Risk**: A completed analysis can fail the Spark task and be retried, resubmitting an accepted POST. A shared 401/403 or analyzer failure can also be masked, so the action fails as a generic close error instead of the intended shared failure.
- **Suggested Fix**: Close the client the same way as the response: catch `NonFatal` and discard it. Preserve interrupt status if the close path observes an interrupt.

### Issue 2: Column customHeaders disables shared auth fail-fast
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 66-67, 104-110
- **Description**: `sharedAuth` is `credentialParams.forall(isScalar)`, and `credentialParams` includes `customHeaders`. An unset param counts as scalar, but a column of non-auth correlation headers makes `sharedAuth` false even when `subscriptionKey`, `AADToken`, and `CustomAuthHeader` are scalar. `ServiceAuthHeaders.resolve` strips auth-named custom headers and will not let them override that scalar credential. The docs require HTTP 401/403 with scalar authentication to fail the action; only column-configured authentication should stay row-level.
- **Risk**: A wrong or revoked shared key becomes a per-row 401/403 instead of failing the action. Callers can keep submitting every row and may resubmit the partition after inspecting row errors.
- **Suggested Fix**: Treat authentication as shared when the resolved credential source is scalar. A column `customHeaders` should disable `sharedAuth` only when it actually supplies the credential, not when it carries only non-auth headers.

### Issue 3: Deadline abort can race a pooled connection
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 343-355
- **Description**: The deadline task calls `request.abort()`, then `finally` uses `deadline.cancel(false)` and `request.releaseConnection()`. `cancel(false)` does not stop an abort that has already started. That abort can run after the connection is returned to this client's pool and reused by the next poll. Separately, an abort `IOException` is `OperationTimedOut` only when `budget.remainingMs == 0`; otherwise it is a retryable `TransportError`.
- **Risk**: A late abort can fail a later poll on a reused connection. A deadline abort that arrives while remaining time is still positive is labeled `TransportError`/`Unknown` instead of `OperationTimedOut`. GET polls may retry; a POST is not replayed but is mislabeled.
- **Suggested Fix**: Do not release a connection that the deadline task has aborted, and classify an abort from that task as `OperationTimedOut` even if `remainingMs` has not yet reached zero. Ensure the abort cannot apply to a connection already returned to the pool.

### Issue 4: Shared error-code walk ignores details
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 418-435
- **Description**: `errorCodes` collects `code` and recurses only through `innererror`. Azure error objects can also carry recognized codes in `details`. `checkSharedFailure` therefore misses `AnalyzerNotFound`, `DefaultsNotSet`, `DeploymentNotFound`, or `ModelDeploymentNotFound` when those codes appear only in `details`. Tests cover `code` and `innererror` only.
- **Risk**: A shared analyzer or deployment misconfiguration can remain a row-level failure instead of failing the action, so the job continues to submit rows that cannot succeed.
- **Suggested Fix**: Walk `details` arrays as well as `innererror`, and add a shared-failure test whose recognized code is present only under `details`.

### Issue 5: Bodyless GET requests send Content-Type
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 480-483
- **Description**: Poll and management GETs are built with `addHeaders`, whose default `addContentType` is true (`CognitiveServiceBase.scala` lines 499 and 538-540), so `Content-Type: application/json` is added. `getRequest` copies those headers and removes only `content-length`, `transfer-encoding`, and `host`. The initial poll GET in `ContentUnderstanding.scala` (line 277) has the same header.
- **Risk**: A bodyless GET with `Content-Type` is not a valid request shape. A proxy or service can reject the poll or analyzer GET, turning a resumable operation into a transport or HTTP failure.
- **Suggested Fix**: Pass `addContentType = false` for bodyless GETs, and strip `content-type` in `getRequest`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Client and response close failures are ignored after preserving the request outcome; deadline abort completion is coordinated before connection release.
- **Why**: Prevents cleanup failures and late aborts from replacing completed outcomes or affecting a later request.
- **How verified**: Added close/deadline regressions in the recovery suite; all 66 Content Understanding Scala tests passed.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`

### Issue 2
- **Status**: Fixed
- **What changed**: Shared-auth classification now distinguishes scalar explicit credentials from credentials supplied by a custom-header column.
- **Why**: Non-auth custom-header columns no longer suppress fail-fast behavior for a shared key, while row-provided credentials remain row-specific.
- **How verified**: Added shared-auth and row-credential regressions; all 66 Content Understanding Scala tests passed.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`, `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`

### Issue 3
- **Status**: Fixed
- **What changed**: Deadline aborts are awaited before releasing the connection, and timer-fired state takes precedence when classifying timeout I/O failures.
- **Why**: Eliminates abort/reuse races and reliably reports deadline exhaustion as a timeout.
- **How verified**: Added deadline recovery coverage; all 66 Content Understanding Scala tests passed.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`

### Issue 4
- **Status**: Fixed
- **What changed**: Error-code extraction now recursively visits `details` arrays as well as `innererror`.
- **Why**: Recognized analyzer and deployment errors nested in service details retain shared-failure behavior.
- **How verified**: Added a shared-error regression with `AnalyzerNotFound` only under `details`; all 66 Content Understanding Scala tests passed.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`

### Issue 5
- **Status**: Fixed
- **What changed**: Bodyless polling and analyzer GETs omit `Content-Type`, and reconstructed GET requests strip it.
- **Why**: Avoids sending a JSON entity declaration on requests without a body.
- **How verified**: Added assertions for poll and management GET headers; all 66 Content Understanding Scala tests passed.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`, `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`