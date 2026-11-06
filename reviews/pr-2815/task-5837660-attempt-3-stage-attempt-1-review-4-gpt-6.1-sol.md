---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 4
timestamp: "2026-11-06T19:41:12Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "904c18a528081d2f7f00d3be6f0963ddd2777f61bc7d7e191ddf48198c5d2d4a"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-4-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied diff, including the frozen source/test correction, rather than HEAD alone.
- [x] Traced submission, polling, management, status retention, retry counts, deadlines, response limits, and DNS admission through `ContentUnderstandingProtocol.scala` and associated suites.
- [x] Checked row validation, scalar/column types, authentication precedence, snapshot isolation, schema preservation, and HTTP pipeline integration against their core dependencies.
- [x] Examined Python parameter inspection, generated accessors, byte/map conversion, copy/save/load, explicit/default persistence, and regression tests in `Utils.py`, `Wrappable.scala`, `ComplexParamsSerializer.scala`, and the Python suites.
- [x] Compared documentation and CI additions with implementation behavior and inspected supplied validation provenance. No commands or tests were executed by this reviewer.
- [ ] Live CU, packaged-runtime, and Azure CI validation remain outstanding external gates, as stated in the supplied evidence.

## Issues

### Issue 1: Pending service parameters with JVM defaults break parameter explanations
- **Severity**: Medium
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 244–246, interacting with 158–168
- **Description**: A pending, non-sensitive ServiceParam falls through to PySpark’s `explainParam`, but the overridden `hasDefault` can return `True` for a JVM-only default. PySpark’s explanation implementation then indexes `_defaultParamMap[param]`, where generated ServiceParam defaults are deliberately absent (`Wrappable.scala:186–189`). For example, `stage = ContentUnderstanding(); stage.set(stage.apiVersion, "2025-11-01"); stage.explainParam(stage.apiVersion)` raises `KeyError` before transfer. `explainParams()` fails likewise. The same interaction affects existing wrappers such as `AnalyzeDocument`. The new pending-default test checks `hasDefault` but does not exercise explanations.
- **Risk**: Valid pending scalar configuration makes public parameter inspection fail, contradicting the supported pending-value precedence contract and regressing existing service wrappers.
- **Suggested Fix**: Render pending ServiceParam explanations without delegating to PySpark’s local-map-only implementation. Read any displayed default from the appropriate JVM/Python source, preserve pending-value precedence and credential redaction, and add focused regressions for `explainParam` and `explainParams` before transfer on CU and an existing wrapper.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: JVM-backed default visibility is incompatible with the inherited explanation path’s direct Python default-map lookup.
- **How verified**: Static source and test inspection; no execution performed.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s4t1-gpt-6.1-sol-i1: Claimed pending-value explanation KeyError

- Disposition: **false-positive**.
- Evidence: Initial diagnostic did not raise KeyError. Installed PySpark3.5 Params.explainParam:301-317 guards map indexing with membership, not hasDefault. Narrow positive control passed for explainParam/explainParams with JVM-only default and pending scalar; cu-native-review-explanation-probe.log.
- Consolidated topic: `pending-explanation`.
