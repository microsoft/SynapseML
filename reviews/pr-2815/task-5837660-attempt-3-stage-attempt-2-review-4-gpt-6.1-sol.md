---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 4
timestamp: "2026-11-06T19:50:55Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "7add06a167f634923877f9d45a3c53dde12698461b22042b4abdbffd508d3165"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-4-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied 37-path diff, including the current source/test correction.
- [x] Traced submission, polling, management, retained-handle, retry, and deadline transitions in `ContentUnderstandingProtocol.scala` against the supplied Scala suites.
- [x] Checked row validation, scalar/column types, authentication provenance, snapshot execution, and schema preservation in `ContentUnderstanding.scala` and its HTTP dependencies.
- [x] Reviewed DNS admission, response bounds, header validation, interruption, and cleanup paths.
- [x] Traced generated Python setters/getters, pending-value precedence, scalar inspection, copy, and persistence through `Wrappable.scala`, `ContentUnderstandingParams.scala`, and `Utils.py`.
- [x] Reviewed shared serializer/schema changes, documentation, and the dedicated CI matrix addition.
- [ ] No commands or tests executed; supplied validation evidence was considered without treating it as independent execution. Live-service and Azure CI validation remain outstanding.

## Issues

### Issue 1: Pending service values with JVM defaults break parameter explanations
- **Severity**: Medium
- **File**: `core\src\main\python\synapse\ml\core\schema\Utils.py`
- **Line(s)**: 158–168, 244–246
- **Description**: Noncredential service parameters with pending Python values fall through to PySpark’s `explainParam`. The new `hasDefault` can return `True` for a JVM-only scalar default, but PySpark’s implementation then indexes `_defaultParamMap[param]`. Generated wrappers deliberately omit service defaults from that dictionary (`Wrappable.scala:186–189`), causing `KeyError`. For example:
  ```python
  stage = ContentUnderstanding()
  stage.set(stage.apiVersion, "2026-06-01-preview")
  stage.explainParam(stage.apiVersion)
  ```
  `stage.explainParams()` also fails in this state. The new pending-default test checks `hasDefault` but does not exercise explanations.
- **Risk**: Valid pending settings break public parameter inspection across CU and existing service wrappers, undermining the promised pending-value lifecycle.
- **Suggested Fix**: Format pending service-parameter explanations explicitly using the Python pending value and JVM scalar default, without materializing defaults into Python parameter maps. Preserve credential redaction. Add regression cases for `explainParam` and `explainParams` before transfer on CU and an existing wrapper.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: JVM-aware default inspection is incompatible with the inherited explanation method’s local-map lookup.
- **How verified**: Static data-flow review; regression execution remains pending.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s4t2-gpt-6.1-sol-i1: Claimed pending-value explanation KeyError

- Disposition: **false-positive**.
- Evidence: Initial diagnostic did not raise KeyError. Installed PySpark3.5 Params.explainParam:301-317 guards map indexing with membership, not hasDefault. Narrow positive control passed for explainParam/explainParams with JVM-only default and pending scalar; cu-native-review-explanation-probe.log.
- Consolidated topic: `pending-explanation`.
