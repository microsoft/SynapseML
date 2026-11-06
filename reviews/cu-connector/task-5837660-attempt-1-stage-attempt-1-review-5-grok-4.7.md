---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 5
timestamp: "2026-11-05T20:39:26Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "f00e495535d0d912bcd110b54217439b5aefa3f369f6a07f62bc401378bcf321"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-5-grok-4.7.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared retained status vocabulary and poll continuation in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (`StatusNames`, `ongoing`, `SubmissionLocation.assess`, `retryDelay`) with fixtures in the six CU Scala suites.
- [x] Confirmed HTTP 203/206 Running/NotStarted coverage only rejects unaccepted handles in `ContentUnderstandingAdmissionSuite.scala`, and that `Canceled` does not appear in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding`.
- [x] Confirmed Retry-After coverage is numeric/past-date/malformed (`ContentUnderstandingSuite.scala`, `ContentUnderstandingRecoverySuite.scala`, `ContentUnderstandingConcurrencySuite.scala`) and does not exercise a future RFC 1123 date.
- [x] Spot-checked shared-stop, empty-body 401/403/429, DNS deadline vs DNS failure, creation status mapping, and explicit analyzerId persistence; those contract paths have direct assertions and are not re-filed.

## Issues

### Issue 1: Canceled is special-cased and untested
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 93-94
- **Description**: `StatusNames` treats `Canceled` as a known operation status, and submission assessment treats it as terminal so a missing or unsafe `Operation-Location` must not become `MissingOperationLocation` / `InvalidOperationLocation`. No CU suite fixture returns `Canceled` on POST or GET. A regression that drops it from the map would mark a canceled operation `Unknown` or keep polling, and current tests would still pass.
- **Risk**: Callers cannot rely on a canceled analysis stopping with the service status and handle. Resume selection that treats only `NotStarted`/`Running`/`Unknown` as pollable would be wrong if `Canceled` is misclassified as ongoing.
- **Suggested Fix**: Add submit and poll cases with `{"status":"Canceled"}`: accepted terminal submit must not GET; a poll must stop, keep the handle, and not replace the status with `Unknown` or `PollAttemptsExhausted`.

### Issue 2: Accepted NotStarted is never polled
- **Severity**: Medium
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingAdmissionSuite.scala
- **Line(s)**: 148-176
- **Description**: The only `NotStarted` assertions are HTTP 203/206, which must not follow `Operation-Location`. `ongoing` also treats accepted `NotStarted` like `Running`, but every successful analyze/poll fixture uses `Running` or `Succeeded`. Removing `NotStarted` from `StatusNames` or `ongoing` would not fail those 203/206 checks.
- **Risk**: A 202 `NotStarted` submission could be left `Unknown` or not resumed, so analyze mode would not poll a valid handle.
- **Suggested Fix**: Add a 202 `NotStarted` analyze case that polls to `Succeeded` without a second POST, and a poll-mode case that retains `NotStarted` plus `PollAttemptsExhausted` when the attempt budget ends on a clean body.

### Issue 3: Future HTTP-date Retry-After is untested
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 304-308
- **Description**: `retryDelay` prefers parsed RFC 1123 dates and no longer caps them. Tests cover `0`, a 1970 date, malformed text, `120`, a 19-digit number, and a 20-digit fallback. None use a future HTTP date longer than the operation budget.
- **Risk**: Re-capping or ignoring the date branch would poll early or drop a handle, and numeric tests would still pass.
- **Suggested Fix**: Submit or poll with `Retry-After` set to a far-future RFC 1123 date and a short `maxOperationDuration`; assert one POST or GET, retained handle/body, and `OperationTimedOut` rather than an early follow-up GET.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed.
- **What changed**: SubmissionSuite covers Canceled analyze/submit with absent, valid and unsafe supplied handles, plus a terminal poll.
- **Why**: Assert Canceled/raw body, the existing OperationFailed diagnostic instead of location errors, appropriate retained handle and exactly one request.
- **How verified**: All seven new suite tests passed in `cu-stage5-coverage-validation-retry2.log`; runtime cancellation semantics unchanged.

### Issue 2
- **Status**: Addressed.
- **What changed**: Added accepted NotStarted submit, analyze-to-Succeeded, and poll-attempt-exhaustion cases.
- **Why**: Assert exact NotStarted state/raw response when retained, PollAttemptsExhausted on a clean final poll, safe handle retention and POST/GET counts without resubmission.
- **How verified**: Passing SubmissionSuite test across all three modes.

### Issue 3
- **Status**: Addressed (duplicate of Opus issue 3).
- **What changed**: Added direct uncapped future-date parsing and public analyze/poll budget cases.
- **Why**: A far-future valid RFC-1123 date must preserve state and expire before any follow-up request rather than use fallback or a short cap.
- **How verified**: Passing SubmissionSuite test asserts one POST/GET, retained HTTP/body/handle and OperationTimedOut without a long wait.