---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 6
timestamp: "2026-10-21T21:47:59Z"
outcome: "clean"
reviewer_model: "gpt-6.1-sol"
input_sha256: "8e865a0eb677e54af7e2674fdd106844c8337df884f8b32790bbdb7fc16907be"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-3-stage-attempt-1-review-6-gpt-6.1-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`: scalar and row-bound cache validation permits empty maps, restricts supported fields and values, and preserves omission for unset options and null column rows. The added validation introduces no network calls, content insertion, or retained cache state.
- [x] Traced Chat Completions, Responses, and Foundry parameter lists and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` delegation. Cache options reach the selected service without replacing caller-selected deployments. Inspected `OpenAIPromptCacheSuite.scala` and the public Foundry transform test in `OpenAIPromptMultimodalRequestSuite.scala`.
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` and its suite. Only `ModelNotFound` gains conditional tenant/capacity guidance; the exception class and other status handling remain unchanged.
- [x] Reviewed `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` and the other three changed notebooks. Documentation distinguishes implicit and explicit caching, describes minimum TTL rather than maximum retention, and avoids automatic-breakpoint or guaranteed-savings claims. Current examples select Luna while embedding selections remain unchanged.
- [x] Traced generated service-parameter accessors in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` and inspected `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` for dictionary validation, column bindings, copying, and persistence coverage.
- [x] Confirmed the three changed favorite-color fixtures request full sentences while their greater-than-ten-character assertions remain intact. Inspected `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` to verify deliberate older-model compatibility cases remain present.
- [x] Assessed the supplied validation evidence, including the reported 55 passing live-suite tests and one unchanged intentional ignore. No commands or tests were executed by this reviewer.
- [ ] Final CI qualification remains deferred: fresh published-head CI and current-head automated review are required. Supplied bridge validation used Python 3.12.3, not the target Python 3.11.8. This CLEAN verdict applies only to the frozen pre-commit review.

## Driver decision and aggregate evidence

This section is driver-authored; the independent reviewer response above is unchanged.

- **Decision:** Medium by explicit human selection. The 29-path patch has modest validation branching and cross-language serialization/persistence interactions. The human selected exact `gpt-6.1-sol` at `max`, superseding the complementary-family default; the live catalog supports that selection. Primary-runtime identity was unavailable and is not invented.
- **Execution:** Six sequential independent script-owned Execute invocations, using fresh external transports and the same full frozen diff. Trusted repository scheduling supersedes the installed concurrent default. All six returned persisted `clean` evidence with actual runtime model `gpt-6.1-sol`, exit zero, and no unresolved findings.
- **Source identity:** Manifest `d915a3f9e98cc7d59f760f8d38e1c5981e1958474aad1870b4e909a13ee7fc48`; complete diff `0ac860af199ca2f91812fe8e81601bfa4ab523e2cebbe38247b9825087eb53bd`; target `48b77657f615e25d443d39177e108eae467e3949`. The full patch was independently applied to a disposable index and its complete path/mode/blob manifest verified.
- **Transport:** Every input embedded that exact diff, stayed below the 1 MiB prompt budget, and used only permitted read tools. No previous/sibling feedback was consumed. Lens 2's two conservatively flagged root-path globs were constrained to policy filenames, returned no files, and were independently cleared by inspecting their complete tool results. Original audit flags were retained.
- **CI correction:** The predecessor's live favorite-color requests validly returned `Red.` while inherited assertions required more than ten characters. Both chat and Responses reproduced this with normal completion and no reasoning-token exhaustion. Three fixtures now request full sentences; assertions remain unchanged. All 55 affected live Scala tests and test style passed, with one unchanged intentional custom-endpoint ignore.
- **Retained evidence:** The predecessor's exact Azure merge published 43 CPU and 4 GPU Databricks E2E passes, with zero E2E skips. Its OpenAI publication was 263 passed, 3 failed and 2 intentional custom-endpoint ignores; earlier automatic repetitions also exposed the Responses fixture. Repeated executions are not summed as unique coverage.
- **Runtime:** All five production files remain byte-identical to r3. The existing two bridge tests/nine subtests and 39 synthetic managed-Spark integration checks remain runtime evidence, not substitutes for fresh PR-head CI. Local Python remains 3.12.3 rather than target 3.11.8.
- **Bounds:** Three pre-commit passes consumed out of six; 13 actual reviewer invocations total. Zero final CI-qualified passes consumed out of two. The first dirty response's accepted privacy exclusion remains documented in attempt 2, round 6; its original private transport is retained and must not be published. No budgets were reset.
- **Gate:** This pass uses the trusted local-gate exception for a validated correction to red predecessor CI. Fresh required CI and automated review are mandatory after publication. The separate final CI-qualified pass remains pending; this is not a merge-readiness declaration.