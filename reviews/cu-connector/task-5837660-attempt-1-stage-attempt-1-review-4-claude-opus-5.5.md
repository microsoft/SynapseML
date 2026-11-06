---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 4
timestamp: "2026-11-05T20:04:04Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "055daf94718a0d7f7833fd10b8d195cdafcf87d2f57213625822eea9fab287e0"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Checked the submission path in `ContentUnderstandingProtocol.scala`. `SubmissionLocation.assess` is at lines 63-75 and `safeSubmission` is at lines 543-557. With 200/201/202, validated handles are followed. Other statuses that include a location become Unknown and the handle is not followed. A 401/403/429 response without a location stays Rejected. A contradictory or unsafe handle becomes Unknown with `InvalidOperationLocation`.
- [x] Checked `decode` at lines 436-470 in `ContentUnderstandingProtocol.scala`. When the status is missing or not recognized, the result gets `InvalidResponse` and is never assumed to be Running. Diagnostics are chosen in this order: transport, service error, HTTP, missing status, then failed. `CreationStatusNames` maps creating to Running, ready to Succeeded and deleting to Unknown, and analysis submissions and polls still use `StatusNames`.
- [x] Checked how poll state is kept and how deadlines are handled, by reading the code in `ContentUnderstandingProtocol.scala`, `ContentUnderstandingTransport.scala`, `ContentUnderstandingDnsResolver.scala` and `ContentUnderstandingBudget.scala`. When time runs out before a request is sent, the previous status, body and error are kept, and `OperationTimedOut` is added only when there was no earlier error. A real DNS failure is not counted as running out of time. Management retrieval allows 3 GET attempts. If time runs out before the first GET, a Succeeded creation stays completed and gets `OperationTimedOut`.
- [x] Checked settings and credential handling in `ContentUnderstanding.scala` lines 120-165: `settings`, `isScalar`/`isColumn`, `rowUsesColumnAuth`, and the `allAuthScalar` check over the five auth parameters. Shared-stop eligibility is turned off in poll mode. A credential from a column stays row-level even when its value matches a scalar fallback.
- [x] Checked input validation in `ContentUnderstanding.scala` lines 166-345: `validateParamSchema`, `validateInputSchema`, `validateRow`, `prepareRequest` and `handlingFunc`. A column must match exactly by name and must be unambiguous under the resolver. Only the parameters used in the current mode are validated (`activeParams` at lines 83-91, the `getVectorParamMap` override at lines 93-96). The inherited handler deliberately throws, so it cannot run outside the snapshot-bound handler.
- [x] Checked the `CognitiveServiceBase.scala` changes: `addContentType` removes any existing Content-Type header regardless of case before adding its own. Also checked the new `getCustomHeaders`/`setCustomHeadersCol`/`getCustomHeadersCol` accessors and the base helpers `getVectorParamMap` (line 67), `getValueOpt` (line 102) and `getValueAnyOpt` (line 113).
- [x] Checked that the Python test fixtures (`test_ContentUnderstanding.py`, `analyzer()` at line 141) match the Scala requirements for inputs and output columns.
- [ ] I did not run any tests or builds; this review was read-only by policy. The validation logs were supplied as evidence, and I did not reproduce them.

## Issues

### Issue 1: Misleading MissingOperationLocation diagnostic when a successful but non-accepted response includes a valid Operation-Location
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 63-75 (`SubmissionLocation.assess`), 543-557 (`safeSubmission`)
- **Description**: Take a successful response that is not 200/201/202, such as HTTP 203 or 204. Suppose it has a Running or NotStarted status and a valid, same-origin `Operation-Location` header. `accepted` is false, so `location` is set to `None` and `response.operationLocation` is empty. In `assess`, `requiresLocation` is true because the response is successful, not terminal and Running. `validated` is `Right(...)`, so `error` is `None`. `missing` is `requiresLocation && operationLocation.isEmpty`, which is true. `safeSubmission` therefore reports `MissingOperationLocation` with the message "A successful ongoing operation did not include Operation-Location". That statement is false: the header was there, and the connector chose not to follow it because the HTTP status is not an accepted submission status. The status (Unknown) and the decision not to follow the handle are both correct. Only the diagnostic code and message are wrong.
- **Risk**: Someone troubleshooting will conclude the service omitted the handle when it actually returned one with an unexpected status. They may investigate the wrong cause, or assume the operation cannot be found when a handle exists.
- **Suggested Fix**: In `safeSubmission`, or by adding a third result to `Assessment`, handle the case of a non-accepted status with a present, valid location separately. Give it its own diagnostic, for example `UnacceptedOperationLocation` with "The service returned Operation-Location with HTTP <status>; the handle was not followed." Keep Unknown and keep the handle unfollowed. Add a test that sends a 203 with a Running status and a valid handle, and asserts the diagnostic, that no GET is sent, and that no handle is invented.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed and locally verified; independent second-attempt review complete.
- **What changed**: `SubmissionLocation.assess` supplies `UnacceptedOperationLocation` with the actual HTTP status for an otherwise valid ongoing response whose handle is excluded by the accepted-status policy. It does not change accepted HTTP statuses, expose/follow the handle, or replay submission.
- **Why**: A supplied but unaccepted handle is not a missing header. Unsafe-location and response-read diagnostic precedence remain unchanged; missing headers and terminal responses retain their previous handling.
- **How verified**: `cu-stage4-fix-red.log` reproduced the diagnostic assertion failure through the public transformer before the production edit. The test checks HTTP 203/206, Running/NotStarted, analyze/submit, original status/body/id, absent exposed handle, and POST-only traffic. It was moved from RecoverySuite to the existing AdmissionSuite after the first validation run correctly rejected RecoverySuite's resulting 811 lines. No style suppression or additional suite was introduced. `cu-stage4-fix-green-retry.log` passes cognitive main/test style and compilation plus 144/144 tests across all six CU suites. The new matrix passes all eight combinations. Whitespace checks passed.

## Driver: High stage 4, attempt 1 collection

- Human-selected High/direct policy; GPT and Opus high reasoning, Grok low. Exact catalog/runtime reviewers: `gpt-6.1-sol`, `claude-opus-5.5`, `grok-4.7`. Primary: `gpt-6-astra`.
- Broad 36-path change with substantial concurrency, acceptance-state, deadline and parameter-lifecycle complexity. The human efficiency override takes precedence over the prior Max tier. No new bounds or permission changes.
- All three artifacts persisted and input hashes matched. Actual tool arguments were audited: source-only read tools, no previous/sibling feedback access. GPT and Grok each had one recoverable missing/malformed-path lookup; neither was a transport failure.
- GPT clean (209.115s), Grok clean (368.438s), Opus one Low diagnostic finding (167.636s). The frozen source matched snapshot SHA256 `40c8e6b62ff00388c564ee462be9974de3014193af6537585747ed8bf987333c` before the correction.
- Stage 4 is not yet cleared. After validation, all three reviewers must independently review attempt 2. High advances after its second completed attempt with any unresolved findings retained; a failed invocation does not satisfy this gate.
- Stage 2 remains advanced-with-unresolved. Architecture item 3 (handler documentation) is addressed; the other eight recommendations remain unimplemented, including prioritized 1/7 and explicitly deferred 2/5/8. This diagnostic correction does not authorize those architecture edits.
- PR 2698 remains open against master at the original head. No commit, push, live CU call, ADO edit, target integration, or CI claim.

### Attempt 2 pre-dispatch decision

Continue the same High/direct human-selected reasoning and three exact live-catalog models above. Stage attempt 2 uses the independently verified 36-path snapshot `cu-stage4-attempt2.patch`, 450942 bytes, SHA256 `aa19a32443ec23ba79de2e41e0378041c73b0c824afe067c6d35a7fe1eb7df7b`. Source remains frozen during collection. Reviewers receive scope, contract, and qualified validation evidence, not prior/sibling reports or driver findings. Current Scala validation covers the correction; prior 29/29 Python tests and matching published-JAR hashes belong to snapshot25, not a newly published correction. Python/code-generation interfaces were not changed by this fix.

### Collection update

Attempt 2 is complete: GPT and Opus clean; Grok reported a separate resource-only creation-status recommendation. This Low diagnostic fix remains resolved. Stage 4 advances with that separate unresolved recommendation under the High two-attempt rule; see the attempt-2 Grok artifact for the complete disposition.