---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 2
timestamp: "2026-11-06T19:11:26Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "6e93d62ed3787720ecac9413e2fa2340a76cd2f23d1834faf8501735db5ed3e5"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-2-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied diff against `AGENTS.md`, the review skill, master branch guidance, and runtime manifests.
- [x] Traced the native transformer through shared HTTP pipeline, authentication, operation-state, deadline, DNS, and management abstractions.
- [x] Inspected shared ServiceParam/code-generation changes, raw-byte handling, pending-value preservation, scalar inspection, and persistence dependencies.
- [x] Compared Scala persistence and generated Python lifecycle behavior with `test_ContentUnderstanding.py`, `test_ServiceParamPythonBridge.py`, and serializer regression coverage.
- [x] Reviewed documentation, sidebar registration, and the authorized dedicated CI matrix leg. Considered supplied validation evidence without executing commands.
- [ ] Live CU, packaged-runtime, and Azure CI validation remain outstanding as stated in the supplied provenance.

## Issues

### Issue 1: Python loading restores only two persisted non-service defaults
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala`
- **Line(s)**: 220–229
- **Description**: `_transfer_params_from_java` imports explicit non-service parameters but refreshes defaults only for `outputCol` and `errorCol`. CU now saves defaults separately from explicit settings (`ContentUnderstanding.scala:64`), so other persisted defaults are ignored by Python. For example, save `ContentUnderstanding()._setDefault(maxOperationDuration=1000)` and reload it: the JVM retains the saved 1,000-ms default, but Python reports its constructor default of 600,000 ms. The next parameter transfer writes that constructor default back to the JVM (`Utils.py:300–306`).
- **Risk**: Loading, wrapping, executing, or resaving a stage can silently replace persisted polling, duration, or size-limit defaults. This also undermines saved defaults when constructor defaults change between releases. Existing Python default-lifecycle tests cover output/error columns and ServiceParams, not these ordinary parameters.
- **Suggested Fix**: Restore every available non-ServiceParam JVM default into Python’s `_defaultParamMap`, while retaining explicit values separately and leaving ServiceParams JVM-owned. Add a focused round-trip regression with a customized duration or response-size default, checking inspection and subsequent transfer/resave.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Persisted defaults must remain authoritative across the Python/JVM lifecycle.
- **How verified**: Static source and test inspection; no tests executed by this reviewer.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s2t2-gpt-6.1-sol-i1: Python load replaces ordinary persisted defaults

- Disposition: **confirmed**.
- Evidence: Local save/load reproduction: saved JVM default1000, loaded Python default600000, then transfer overwrites JVM to600000. cu-native-review-probes.log.
- Consolidated topic: `persisted-defaults`.
