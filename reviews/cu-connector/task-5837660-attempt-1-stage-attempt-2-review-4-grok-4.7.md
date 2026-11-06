---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 4
timestamp: "2026-11-05T20:34:08Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "5ce17078682f112646a64a724076717446d8fa9d3154ca10ee1a591fb308f8db"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-4-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read creation status map and URL-less/accepted PUT decoding in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (CreationStatusNames, `decode`, `createAnalyzer`).
- [x] Checked submission/poll acceptance, handle assessment, and prior-state retention against the retained contract (`SubmissionLocation.assess`, `safeSubmission`, `retainPollState`, `pollWithClient`).
- [x] Checked DNS deadline versus genuine DNS/capacity failures in `ContentUnderstandingDnsResolver.scala` and the `UnknownHostException` branch of `exchangeBeforeDeadline`.
- [x] Checked shared-auth provenance (`rowUsesColumnAuth`, `FailureClassifier`, `ServiceAuthHeaders.resolve`) and exact-versus-resolver column checks in `ContentUnderstanding.scala`.
- [x] Checked Retry-After digit saturation and iterative JSON bounds in `ContentUnderstandingResponseParsing.scala`. No off-by-one against the 19-digit and depth-1000 limits.

## Issues

### Issue 1: Creation decoding accepts analysis statuses
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 93-95, 768-784
- **Description**: `CreationStatusNames` is `StatusNames` plus the resource aliases. An initial PUT is therefore treated as a known operation when its status is `notstarted`, `running`, `succeeded`, or `canceled`. Those are not creation statuses. `creating` is the only creating state that should map to `Running`. With a handle, that body is seeded as `Running`/`Succeeded`/`Canceled` and can be retained across a pre-poll deadline. Without a handle, `decode` finds a known status and no error, so the URL-less path synthesizes `MissingOperationLocation` instead of keeping `InvalidResponse`.
- **Risk**: A non-ready or unexpected resource body is resumable or classified as a missing handle rather than an invalid creation response. Callers can persist the wrong diagnostic and retry or poll on a status the creation contract does not define.
- **Suggested Fix**: Use a creation-only map: `creating` to `Running`, `ready` to `Succeeded`, `deleting` to `Unknown`, and `failed` to `Failed`. Leave `notstarted`, `running`, `succeeded`, and `canceled` unrecognized so `decode` keeps `InvalidResponse`. Do not union `StatusNames` into creation decoding. Keep operation vocabulary on analysis submissions and operation polls.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Unresolved compatibility recommendation; not adopted in this cycle.
- **What changed**: No source change. Preserve the original Medium finding and the existing additive creation decoder.
- **Why**: The supplied contract explicitly says creation decoding "additionally maps" resource states. Existing operation-shaped creation responses remain supported; changing to a resource-only map would narrow that behavior. This is distinct from expanding operation-poll vocabulary, which the current code does not do. No resource-only restriction was authorized.
- **How verified**: `ContentUnderstandingProtocol.scala:94-97` confirms the union. `ContentUnderstandingAdmissionSuite.scala:167-195` explicitly exercises operation-shaped Running creation replies alongside creating/ready/deleting resources, preserving prior status, body, id and diagnostic when no poll can be sent. Unknown/missing/malformed cases retain InvalidResponse. In `pollWithClient`, permission to poll follows the validated accepted management handle, not the initial status label; removing aliases would not prevent that polling. The URL-less path always throws for non-ready responses. Thus the diagnostic difference exists, but a new acceptance or replay defect was not established. No live CU behavior was inferred.

## Driver: High stage 4 final outcome

- **Advanced-with-unresolved**, not clean: two valid High attempts are complete; no third detailed-correctness attempt.
- Attempt 2: GPT clean (138.732s), Opus clean (268.659s), Grok one reported Medium compatibility finding (487.913s). All reports persisted, input hashes matched, and actual tool arguments were source-only read tools with no prior/sibling feedback access or failed calls.
- Attempt 1's Low misleading-handle diagnostic is fixed with red/green evidence and 144 passing CU tests; neither clean reviewer reported a remaining defect in the corrected source.
- Retain this one unresolved finding in addition to the eight architecture recommendations from stage 2. Handler documentation remains the only implemented architecture item; priorities 1/7 and explicit deferrals 2/5/8 remain unchanged.
- Continue to stage 5, testing and coverage, under the same human-selected High/direct policy: current-catalog `gpt-6.1-sol/high`, `grok-4.7/low`, `claude-opus-5.5/high`; primary `gpt-6-astra`. Stage 5 starts at attempt 1 and gets the same immutable source snapshot, not this feedback.
- Broad 36-path change and substantial state/concurrency/lifecycle complexity still justify the review breadth. The human override controls tier/reasoning; no new bounds, permissions, commits, pushes, external writes, or live tests.