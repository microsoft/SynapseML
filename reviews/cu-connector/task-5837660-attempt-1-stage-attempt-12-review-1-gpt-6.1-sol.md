---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 12
round: 1
timestamp: "2026-11-04T19:20:13Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "7919dbb18a501a5815cf131ba0b7b5bbe0684a42adc521a08182ddc499e66809"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot
attempt: 1
stage_attempt: 12
round: 1
timestamp: "2026-11-04T19:10:11.580Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: "8f0df9d0d747feca015c8c175835328ebb4ac31fd4db4f5857105cbf77099f3f"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-12-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected CU request preparation, submission classification, polling, shared-failure handling, and management in `ContentUnderstanding.scala` and `ContentUnderstandingProtocol.scala`.
- [x] Inspected DNS capacity ownership, deadline handling, header validation, and supporting shared HTTP source.
- [x] Traced shared Python parameter transfer through `core\src\main\python\synapse\ml\core\schema\Utils.py` and generated setters in `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`.
- [x] Inspected CU Scala/Python regression coverage, including `test_ContentUnderstanding.py:255-337`, plus documentation and the approved `pipeline.yaml` suite selection.
- [ ] No commands or tests executed under this read-only assignment. Supplied validation results were considered caller-provided evidence, not independently reproduced.

## Issues

### Issue 1: Parameter transfer removes values required by standard Python Params APIs
- **Severity**: Medium
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 124-128
- **Description**: Transfer now calls the Python generated setter, which removes the parameter from `_paramMap` (`Wrappable.scala:205-206`). Consequently, `s = AnalyzeDocument().setParams(subscriptionKey="synthetic-key")` supports `s.getOrDefault(s.subscriptionKey)` before transfer, but after `s._transfer_params_to_java()` that inherited accessor raises `KeyError`: service parameters have no Python default, and their cached value has been deleted. `extractParamMap()` likewise drops the configured value. The new JVM-aware `isSet` and `explainParam` do not repair these other standard APIs. Previously, transfer called the JVM setter directly and preserved the pending Python entry.
- **Risk**: Normal transform/save/copy operations change observable parameter state across existing service wrappers. Parameter inspection fails, and exporting configuration through `extractParamMap()` silently omits configured service values.
- **Suggested Fix**: Keep scalar Python state synchronized with JVM state, or implement coherent JVM-aware generic parameter access and extraction while preserving column bindings. Extend the existing non-CU regression test to check `getOrDefault` and `extractParamMap` before and after transfer.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Shared parameter transfer introduces a regression in standard Python Params access.
- **How verified**: Static source trace against the supplied before/after diff; runtime regression verification remains pending.

## Driver triage and reproduction

- The original report/verdict is preserved. `cu-attempt12-python-red.log` reproduced `KeyError` from `getOrDefault(subscriptionKey)` after transfer, after the before-transfer assertions passed.
- Implemented JVM-aware scalar `getOrDefault` and `extractParamMap` in shared `ComplexParamsMixin`, without creating stale Python caches. Pending Python values and explicit extra-map overrides keep precedence. Column bindings remain JVM-backed and do not become scalar placeholders or fallback defaults during map projection/copy.
- Shared load handling leaves ServiceParams on the JVM. Raw byte scalar access bypasses PySpark's pickle-oriented `_java2py` conversion. Post-fix generated-wrapper validation is pending.

### Driver decision and collection evidence

- Human-selected **Max**, stage 1 attempt 12; broad change (28 diff paths, 283477 bytes), high interaction complexity across transport, distributed concurrency, recovery, serialization and Python/JVM state.
- Primary: `gpt-6-astra`. Current live-catalog reviewers: `gpt-6.1-sol/max`, `grok-4.7/low` (human override), `claude-opus-5.5/max`. Runtime receipts matched all three selections. All returned persisted `issues_found`.
- Human unavailable-Grok exception remains conditional on GPT and Opus clearing; available Grok findings count. Stage 1 does not advance dirty. No limits or permission controls were bypassed.
- Source fingerprints matched the frozen snapshot after all results were collected. Transport audit: GPT 55, Grok 53, Opus 152 calls; only view/rg/grep/glob, zero feedback-path queries. No source was changed during review.
- Five consolidated topics: generic scalar APIs; raw-byte model loading; credential descriptions; ambiguous shared-failure classification; pre-send connection classification. Scala reproduction is running after correcting one test line-length violation; production Scala remains unchanged for that run.

### Validation progress

- Scala red run reproduced the TLS pre-send classification and ambiguous shared-response defects reported by the other reviewers. Related protocol fixes are now implemented.
- Full Black passed with 205 files unchanged. The first combined post-fix validation stopped at the new exchange method's complexity; diagnostic construction was extracted without changing behavior or suppressing the lint rule.
- Active run: `cu-attempt12-fixes-validation2`, covering cognitive main/test style, compilation, affected suites, wrapper regeneration and local publication. The Python fix is not yet declared green; the next offline run must use the regenerated shared mixin and fingerprint-verified JARs.

### Completed local verification

- `cu-attempt12-fixes-validation4.log`: cognitive main/test style, compile/test-compile and all 85 CU tests passed, followed by core/cognitive codegen and local publication.
- `cu-attempt12-python-fixed.log`: all 16 offline tests passed. Generic scalar access and map extraction now preserve values after transfer; explicit extra-map overrides win. Loaded column bindings survive copy/transfer/resave, remain absent from scalar map projection, and cannot fall back to scalar defaults through `getOrDefault`.
- Generated `Utils.py` matched canonical source byte-for-byte. Loaded JARs matched rebuilt artifacts: core `df7f14cd24fb7c5bf630a502d57c3d477b5a9ba1f81f708b8881e59c95f034d1`, cognitive `78c3fb9730a142d34b5f0541036ff3b11833381cd51d9c2ca243137f78fadca8`.
- Full Black passed before the final test assertions; the only subsequently changed Python file also passed Black. No generated source was manually edited.
- The transport helper was extracted into private `ContentUnderstandingTransport.scala` to satisfy complexity/method-count limits, then its singleton naming was corrected. No lint rules were disabled. This finding is locally addressed; the original dirty verdict is preserved pending the next independent attempt.