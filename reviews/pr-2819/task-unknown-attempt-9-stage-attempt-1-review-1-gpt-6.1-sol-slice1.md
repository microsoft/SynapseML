---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 9
stage_attempt: 1
round: 1
timestamp: "2026-11-09T03:16:46Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "d319b4fe1283c3f3e83ad684ce7dd697bfab3834d8e2ded95557aa86dee59b87"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2819/task-unknown-attempt-9-stage-attempt-1-review-1-gpt-6.1-sol-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 607 lines of the assigned frozen diff in the five required ranges.
- [x] Traced declaration extraction, effective binding selection, and normalization in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala` through the alias checks and stub rendering in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`.
- [x] Inspected continued declarations, decorator bindings, overrides, and Unicode collision coverage in `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala`.
- [x] Checked the baseline in `build.sbt`, `environment.yml`, and `.github/skills/synapseml-local-setup/SKILL.md`: Python 3.11.8 and JDK 11 expose the normalization mismatch below.
- [ ] No commands or tests executed under the read-only contract. Caller-reported compilation, 93 passing tests with zero skips, and scalastyle results were not independently verified.

## Issues

### Issue 1: Host-JDK normalization misses valid Python identifier collisions
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 18
- **Description**: `java.text.Normalizer` uses the host JDK's Unicode data, which does not match the supported Python interpreter. Python 3.11 recognizes U+A7F2 (MODIFIER LETTER CAPITAL C, added in Unicode 14) and NFKC-normalizes it to ASCII `C`; JDK 11 leaves it unchanged. Consequently, a hand-written method named `getModel` + U+A7F2 + `ol` binds as `getModelCol` in Python but passes `validateServiceParamAliases()`. A real Param named `model` + U+A7F2 + `ol` similarly escapes the collision check against the generated `modelCol` argument.
- **Risk**: Accepted declarations can silently replace generated service-column accessors, or produce duplicate normalized constructor arguments and make the generated wrapper fail Python compilation. The new helper therefore does not fully enforce Python identifier identity on the documented toolchain.
- **Suggested Fix**: Use normalization compatible with the supported Python Unicode tables rather than relying solely on the host JDK. Add focused accessor and Param-alias regressions using U+A7F2, validated with JDK 11 and Python 3.11 without changing dependency or runtime pins.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Identifier comparisons must match Python normalization on the supported baseline.
- **How verified**: Static source and toolchain-contract review only; no fix or regression tests executed.