---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 3
round: 6
timestamp: "2026-11-05T23:10:59Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "e9c34fdf950aea0594da9111d42ac54ca5b9913b18725cf2cc72fa681da5b072"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-3-review-6-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared management vs analysis recovery wording in `ContentUnderstandingTransport.scala` (lines 27-42) and `ContentUnderstandingProtocol.scala` (lines 196-201, 409-416, 602-630).
- [x] Confirmed `createAnalyzer` polls management handles through `pollWithClient` without `analyzerRetrieval`, so those GETs use the public analysis message.
- [x] Checked `docs/Explore Algorithms/AI Services/Content Understanding.md` and the submission-suite retrieval test: final analyzer GET advice is correct; creation-operation poll advice is not covered.
- [ ] Tests were not executed in this read-only pass.

## Issues

### Issue 1: Creation-operation diagnostics prescribe analysis poll recovery
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 199-201, 409-416, 619-623
- **Description**: `pollAttemptsError` always says to retain the handle and resume polling. `pollWithClient` uses that string for both analysis and `createAnalyzer` management polls, and its `exchange` call leaves `analyzerRetrieval` false. `ContentUnderstandingTransport.failure` then tells every non-retrieval GET to "resume polling." A creation operation URL is not a valid public analysis poll handle.
- **Risk**: Exhausted creation polls and transport failures during creation polling tell users to recover through analysis poll mode, which rejects management URLs. That can lead to a repeated PUT or a failed poll instead of keeping the management response.
- **Suggested Fix**: When `pollWithClient`'s `analyzer` is defined, use a management-specific diagnostic. Say to keep the management response, not to pass that URL to analysis poll mode, and not to repeat the PUT. Pass that context into `ContentUnderstandingTransport.failure` so creation-operation GETs do not use the analysis polling sentence. Leave analysis poll wording unchanged.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Verified by driver; unresolved. Duplicate of GPT Issue1 in this pass.
- **What changed**: No source changes during this review.
- **Why**: Creation-operation handles cannot be resumed through public analysis poll mode; exhaustion and transport advice should distinguish management recovery.
- **How verified**: Static inspection of creation's Some(analyzerId) poll context, unconditional pollAttemptsError, default exchange context and GET transport wording. Original Medium severity and ISSUES_FOUND retained. No new runtime reproduction.
- **Driver record**: Effective decision, validation, frozen snapshot, audit and aggregate disposition are in the same-pass GPT artifact's Third-pass driver disposition.

### Subsequent authorized remediation
- The human authorized fixing and rerunning until clean. Management context now covers creation polling, and exhaustion/transport advice uses getAnalyzer without repeating creation. Analysis recovery remains unchanged.
- Local validation passed165/165 CU tests, main/test style/compile, codegen, formatting and whitespace. Snapshot6 and red/green evidence are recorded in the same-pass GPT artifact. Pending independent rerun; original Medium/ISSUES_FOUND preserved.