---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 23
round: 1
timestamp: "2026-11-05T15:47:48Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "27641902d2304b6495527ab2310f3a938adbe5ab817b9dbbc7a1814d1db97080"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: direct
attempt: 1
stage_attempt: 23
round: 1
timestamp: 2026-03-22T12:00:00Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: 67f7c6c9ca722c7ba6e465327ac25b1ad2e2880635209e6d85ed7e6b25608271
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-23-review-1-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Shared-stop contract traced in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (`checkSharedFailure` ~497–509, submission call ~633, poll call ~608) against auth/analyzer/model rules and `acceptedHandle`.
- [x] Submission location attachment traced in `safeSubmission` / `AcceptedSubmissionStatuses` / `uncertainSubmission` (~512–520): a validated `Operation-Location` on HTTP 200/201/202 is kept even when body status is terminal `Failed`.
- [x] Row-auth marker and scalar-fingerprint precedence checked in `ContentUnderstanding.scala` (`rowUsesColumnAuth`, `settings.scalarAuthFingerprints`, `allAuthScalar`) and `ServiceAuthHeaders` marker strip before the wire send.
- [x] Poll/creation retention, DNS capacity (no worker replacement), `saveDefaultParamsAsExplicit = false`, and Python scalar/column `isSet` paths inspected against the acceptance rules; those paths match the spec.
- [ ] Tests were not executed; prompt validation counts were not treated as independent evidence.

## Issues

### Issue 1: Terminal shared analyzer/model failure with Operation-Location does not stop the partition
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 497–509, 633
- **Description**: `execute` calls `checkSharedFailure(..., acceptedHandle = submitted.operationLocation.isDefined)`. `safeSubmission` attaches a validated `Operation-Location` for every accepted submission status, including HTTP 200/201/202 whose body status is terminal `Failed` with `AnalyzerNotFound` or a shared model code. `checkSharedFailure` then skips the throw because `acceptedHandle` is true. The row is not resumable (`ongoing` is false and status is not `Unknown`), so the handle is only stored. Later rows in the partition are still submitted. Polling correctly passes `acceptedHandle = true` after a real acceptance; this hole is only the pre-poll submission check.
- **Risk**: A definitive shared analyzer or deployment failure that also carries a valid operation URL does not stop new submissions. The partition keeps issuing billable analyze calls after the configuration is already known to be unusable. Spec requires that definitive pre-acceptance shared failures stop new submissions while still retaining any handle on the triggering row.
- **Suggested Fix**: Suppress the stop only for a truly accepted in-flight handle, matching the resumable condition already used below the call: `ongoing(submitted) || (submitted.status == "Unknown" && submitted.operationLocation.isDefined)`. A terminal `Failed` response with shared analyzer/model codes should still throw `ContentUnderstandingException` so `rowHandler` sets the partition stop, while the response (including `operationLocation`) remains the row result.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver disposition after collection
- **Status**: Invalid under the agreed acceptance contract; original reviewer verdict preserved.
- **Why**: Acceptance and resumability are distinct. HTTP 200/201/202 with a validated operation handle is accepted even when its returned operation status is already terminal. The specified partition stop covers definitive pre-acceptance failures, not terminal results of accepted operations. Replacing `acceptedHandle` with the resumable predicate would intentionally expand the stop policy.
- **Evidence**: `safeSubmission` attaches only a validated accepted-response handle; `checkSharedFailure` excludes accepted responses; `pollWithClient` also treats subsequent operation failures as accepted. The documented shared-stop policy and saved-row recovery selection use these separate concepts.
- **What changed**: No classifier or shared-stop change. The next review context will explicitly distinguish acceptance from resumability to remove this ambiguity.