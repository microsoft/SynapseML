---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 6
round: 1
timestamp: "2026-11-04T04:51:13Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "775ecb0b1c3d81dd0277a0c388bd90d56867c5b520db47aa4f5a3b87edf4e761"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 6
round: 1
timestamp: 2026-11-04T12:00:00Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: a6880db5fda399b888cc8b028781490bf67ae7ee159eb68d8b6d1ea592285a0a
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-6-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`: `decode` (430–460), `checkSharedFailure` (498–507), `safeSubmission` (531–552), `AcceptedSubmissionStatuses` (65–66), and `createAnalyzer` (648–656, 688–722).
- [x] Checked the status and shared-failure contract in `docs/Explore Algorithms/AI Services/Content Understanding.md` (172–173, 188–197).
- [x] Checked `ContentUnderstandingSuite.scala` (230–260, 650–668). Unsafe-location tests cover 202 `Running` and 429, not terminal success or `AnalyzerNotFound`. Provisioning success uses a `ready` GET body, but only the no-location path requires it.
- [x] Traced auth and request assembly in `ContentUnderstanding.scala` and `CognitiveServiceBase.scala`. Shared 401/403 uses HTTP status, so that path is not the masked one. Operation-location checks stay fail-closed; this review did not treat caller validation as proof.
- [ ] No commands or tests were executed. This review is source inspection only.

## Issues

### Issue 1: Malformed Operation-Location overwrites a terminal submission outcome
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 531–552 (rewrite at 546–549); classification at 498–507
- **Description**: `safeSubmission` validates `Operation-Location` even when it will not follow it. If validation fails, it always rewrites `status` to `Unknown` and replaces `error` with `InvalidOperationLocation`. That runs after `decode` has already read the trusted POST body. HTTP 200/201 are accepted statuses, so a readable `Succeeded`, `Failed`, or `Canceled` body is discarded. The same replacement removes service error JSON before `checkSharedFailure`. Shared 401/403 still throws because it uses `httpStatus`. `AnalyzerNotFound`, `DefaultsNotSet`, `DeploymentNotFound`, and `ModelDeploymentNotFound` are read only from `response.error`, so a malformed location header hides them. Existing tests lock `Unknown` for 202 `Running` and 429 with an invalid location. They do not cover a terminal body or a shared analyzer/deployment code plus an invalid location.
- **Risk**: Callers who treat only `Succeeded` as success can miss a completed, billable operation and resubmit it. A shared analyzer or deployment failure can stay a row result instead of failing the action before a handle is accepted, which contradicts the documented contract. `rawResponse` still has the body, but status and error no longer match it.
- **Suggested Fix**: Do not request an unsafe location. If the POST body already has a recognized terminal status, keep that status. Preserve service error JSON so shared-failure classification still sees analyzer and deployment codes. Force `Unknown` and `InvalidOperationLocation` only when the body is ongoing, missing, or unrecognized, including the existing 429 case. Add tests for HTTP 200 `Succeeded` and HTTP 404 `AnalyzerNotFound`, each with a malformed `Operation-Location`.

### Issue 2: Polled analyzer creation does not require a ready resource
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 648–656 and 701–711, compared with 715–722
- **Description**: When `createAnalyzer` receives an accepted operation location, it polls until `Succeeded` and then returns the follow-up GET body from `analyzerBody`. That helper rejects non-success, diagnostics, unparseable JSON, and an `error` field, but it does not require `status` `ready`. The no-location path does require a case-insensitive `ready` status before returning the body. The success test uses a GET body that is already `ready`, so this gap is untested.
- **Risk**: After a poll reports success, a 200 GET whose analyzer status is still `creating` or otherwise not `ready` is returned as a completed definition. The two success paths disagree, and a caller can treat a non-ready analyzer as provisioned.
- **Suggested Fix**: After the successful poll, require the retrieval body to have status `ready`, matching the no-location path. If it does not, throw `ContentUnderstandingException` with the retrieval response instead of returning it. Cover a non-ready GET after a `Succeeded` poll.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: Terminal submission status and service error data are preserved when an unused operation URL is malformed; malformed locations still prevent unsafe polling.
- **Why**: A response that will not be polled must not overwrite an authoritative terminal service result.
- **How verified**: Added a terminal-service-failure regression with a malformed location; all 75 tests in the four offline CU Scala suites passed.

### Issue 2
- **Status**: Resolved
- **What changed**: Analyzer retrieval after successful provisioning now requires status `ready`, matching the no-operation-location path.
- **Why**: A successful poll alone does not prove the retrieved analyzer resource is ready.
- **How verified**: Added a non-ready retrieval regression; it raises `AnalyzerNotReady`, and the recovery suite passed all 29 tests.