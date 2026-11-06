---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 6
timestamp: "2026-11-05T21:12:00Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "a7e8c998f6404c66f40f394fdb85db9983583c7ab13fcb1592c795522c996809"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-6-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Public docs in `docs/Explore Algorithms/AI Services/Content Understanding.md` and `docs/Use with MLFlow/Autologging.md` match the retained contract (no journal, no auto POST replay, credential redaction limits, dataset resolver vs schema-only active session, creation status vocabulary, DNS/deadline diagnostics).
- [x] Observability path checked in `ContentUnderstanding.scala` (`FeatureNames.AiServices.ContentUnderstanding`), `ContentUnderstandingOutputParser`, and `SynapseMLLogging.scala` (`logClass` emits certified events; `logTransform` does not).
- [x] Provenance header is stripped before the wire in `HTTPSchema.scala` (`isFabricAuthMarker` / `FabricAuthMarkerHeader`); CU tests assert it is not sent.
- [x] Naming and defaults checked against `ContentUnderstandingParams.scala`, `HTTPTransformer.scala` `ConcurrencyParams`, and `ContentUnderstandingProtocol.scala` (`DefaultApiVersion`, `AcceptedSubmissionStatuses`, `CreationStatusNames`).
- [ ] Live CU, external CI, and a new combined Scala run were not claimed or re-executed in this read-only pass.

## Issues

### Issue 1: Internal output parser emits public Content Understanding usage events
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 476-478
- **Description**: `ContentUnderstandingOutputParser` calls `logClass(FeatureNames.AiServices.ContentUnderstanding)` and is constructed from `getInternalOutputParser` on every `transform` / internal pipeline build. `logClass` always forwards the feature name to certified-event logging. The public stage already logs that feature at user construction, so each action adds another constructor event for an internal parser the caller did not instantiate.
- **Risk**: Usage and billing-adjacent telemetry over-count Content Understanding. Driver logs also gain an extra constructor record per action, which makes real stage construction harder to distinguish.
- **Suggested Fix**: Do not call `logClass` on the internal parser, or log a non-user feature name. Keep `logClass` only on `ContentUnderstanding`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed; duplicate of Opus issue 2.
- **What changed**: Internal output-parser constructor uses FeatureNames.Core; public CU feature logging remains unchanged.
- **Why**: Prevent internal parser construction from emitting extra public-CU certified feature events. No billing calculation or exact adoption multiplier was inferred.
- **How verified**: A local logging-hook regression failed before the change and passed after it while validating parsed output. `cu-stage6-polish-green.log` passes 12 focused tests, including parser persistence, with style and compilation passing.