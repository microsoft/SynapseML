---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 22
round: 1
timestamp: "2026-11-05T14:40:36Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "60b95415ea4fec24937fe057120c939ed7e9c4ef30b7256d884e239af4d373eb"
---

---
producer: review-code
schema_version: 1
tier: max
contract: direct
execution_backend: run-review-round
attempt: 1
stage_attempt: 22
round: 1
timestamp: null
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: 89c7ee46006dcdf0bf8f804eed43db4f55cc34dcb738731dee17aa7e36a4a237
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-22-review-1-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `ContentUnderstandingProtocol.scala` createAnalyzer (lines 730–770) and pollWithClient expiry fallback (lines 588–620); analyze passes `Some(submitted)`, create passes `None`.
- [x] Checked CognitiveServiceBase.scala marker stripping and auth header resolution against shared-stop / column-auth precedence. Marker cannot be spoofed by user headers.
- [x] Checked DNS permit lifetime, 401/403/429 body-read classification, operation-location validation, and no automatic POST/PUT replay against the acceptance contract.
- [x] Python `createAnalyzer` JSON encoding and scalar/column param inspection inspected in `ContentUnderstandingParams.scala`. No additional defect confirmed.
- [ ] Wall-clock timestamp not available to this read-only reviewer; not invented. Tests were not executed.

## Issues

### Issue 1: createAnalyzer drops accepted PUT state when the first poll wait exceeds the budget
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 751–753, 598–601
- **Description**: After an accepted PUT, `createAnalyzer` calls `pollWithClient(..., initial = None, firstDelay = retryDelay(raw.retryAfter, ...))`. If that delay is longer than the remaining budget, `pollWithClient` synthesizes `Unknown` / HTTP 0 / empty body and only keeps the location, adding `OperationTimedOut`. Analyze mode passes `Some(submitted)`, so the same expiry retains the submission status and body. The accepted PUT response is known remote state and is discarded here.
- **Risk**: Callers see a transport-like failure for an accepted create. A retry can repeat the PUT. A later non-accepted response drops the operation location, so the in-progress create may no longer be recoverable from the exception.
- **Suggested Fix**: Pass the decoded accepted PUT response as `pollWithClient`'s initial state, matching analyze. Pre-dispatch expiry should retain HTTP 200/201/202, raw body, and handle, and add `OperationTimedOut` only when that response has no error.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver resolution - attempt 23
- **Status**: Fixed locally and validated; independent re-review pending. Original finding/verdict retained.
- **Cause**: The original create caller passed `initial=None`; deadline handling added during this work depended on an initial response. The omission is present in retained snapshots from attempt10 onward, not newly introduced in attempts21-22. It was an incomplete integration of earlier deadline work.
- **Verified impact**: Local public-JVM reproduction for accepted HTTP200/201/202 returned Unknown/HTTP0/empty raw response after a long first Retry-After. The handle was retained, with one PUT and no GET. Automatic PUT replay and immediate handle loss were not reproduced; a caller-initiated second creation is a separate action.
- **Change**: After management handle validation, seed `pollWithClient` with the decoded accepted PUT. Existing polling retention then preserves body/status/id/handle and diagnostic precedence on pre-dispatch expiry without an alternate creation loop.
- **Regression evidence**: HTTP200/201/202, clean/service-error/malformed bodies, deadline before/during first wait, and occupied-DNS-capacity expiry. Controls retain later HTTP/transport error precedence and existing successful/invalid-body/unsafe-handle creation behavior. Four creation regressions failed before the fix; fixed full CU run134/134 and Python28/28 passed. Full validation and scrutiny evidence is retained in the GPT attempt22 driver section.