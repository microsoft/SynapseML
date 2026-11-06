---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 3
timestamp: "2026-11-06T19:27:42Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "90c91ab386ef25ea246019da947546ba777b31c63dfd751f76fbaf48ae5f8e24"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-3-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied diff, including the frozen Python setter correction and associated regressions.
- [x] Traced submission, polling, management, and diagnostic transitions through `ContentUnderstandingProtocol.scala` and the admission/recovery suites.
- [x] Inspected operation deadlines, DNS admission/capacity release, interruption, cleanup, and asynchronous scheduling against `ContentUnderstandingDnsResolver.scala`, `HTTPClients.scala`, `Clients.scala`, and `AsyncUtils.scala`.
- [x] Checked scalar/column validation, binary limits, authentication provenance, shared-stop concurrency, and row preservation against implementation and focused tests.
- [x] Inspected Python/JVM parameter transfer, generated accessors, copy/save/load, serializer compatibility, documentation, and the dedicated CI leg.
- [ ] No tests or commands were executed. Supplied validation evidence was considered; live-service, packaged-runtime, and Azure CI validation remain outstanding.

## Issues

### Issue 1: Nonstandard terminal submissions can return Unknown without any diagnostic
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 63–71, 524–545
- **Description**: Return HTTP 203 or 206 with `{"id":"op-1","status":"Succeeded"}` and a valid `Operation-Location`. `uncertainSubmission` changes the outcome to `Unknown` because the HTTP status is outside the accepted set. However, `SubmissionLocation.assess` suppresses the unaccepted-handle diagnostic for terminal statuses. The resulting response has no handle and `error = None`; consequently, `errorCol` is also null. The existing nonstandard-status regression covers only `Running` and `NotStarted`.
- **Risk**: An explicitly uncertain submission bypasses the connector’s diagnostic/error-column path, leaving callers without the reason that completion and its handle were rejected.
- **Suggested Fix**: When an unaccepted handle causes an otherwise clean response to become `Unknown`, supply `UnacceptedOperationLocation` without replacing an existing primary diagnostic. Extend the public transformer regression with `Succeeded` and verify a populated error column and no follow-up GET.

### Issue 2: Analysis-only unavailable-result handling incorrectly discards management operation state
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 572–595
- **Description**: `decodePoll` correctly limits `ResultUnavailable` handling to analysis operations, but `retainPollState` excludes HTTP 404/410 from state retention regardless of operation type. Reproduce with a creation PUT returning `creating` and a valid management handle, followed by a management poll returning HTTP 404 or 410 with `{"error":{"code":"OperationNotFound"}}`. The exception replaces the known `Running` state and accepted resource body with `Failed` and the GET error body, although the GET did not establish that provisioning failed.
- **Risk**: Management-operation lookup failure is misreported as terminal provisioning failure, losing the last known remote state needed to interpret and recover the creation attempt.
- **Suggested Fix**: Exempt only responses actually classified as analysis `ResultUnavailable` from nonterminal HTTP-error retention. Preserve management state and its handle while attaching the newer HTTP status and diagnostic. Add creation-path regressions for both 404 and 410.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Uncertain terminal submissions can lack a diagnostic.
- **How verified**: Static control-flow inspection; regression not executed.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: Analysis-specific 404/410 semantics leak into management polling.
- **How verified**: Static state-transition inspection; regression not executed.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s3t2-gpt-6.1-sol-i1: Nonstandard terminal POST loses uncertainty diagnostic

- Disposition: **confirmed**.
- Evidence: Loopback203/206 with Succeeded body and valid handle: Unknown, no handle, no response.error/errorCol, exactly one POST. cu-native-review-probes.log.
- Consolidated topic: `uncertain-diagnostic`.

### s3t2-gpt-6.1-sol-i2: Creation lookup404/410 misreports provisioning failure

- Disposition: **confirmed**.
- Evidence: Loopback accepted PUT then management GET404/410: Failed, retained handle, no getAnalyzer recovery guidance, exactly PUT+GET. cu-native-review-probes.log.
- Consolidated topic: `management-state`.
