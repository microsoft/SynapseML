---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 3
timestamp: "2026-10-21T21:33:36Z"
outcome: "clean"
reviewer_model: "gpt-6.1-sol"
input_sha256: "324515944fec4717cee29a60f6180f60a6bce7fadce5ef46ff1bc3903f652f55"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-3-stage-attempt-1-review-3-gpt-6.1-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Inspected `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAI.scala:316-344,457-466`: empty maps are accepted; unsupported keys, values and null scalar maps are rejected; unset options and null column rows omit the field.
- [x] Traced Chat Completions, Responses and Foundry parameter lists and delegation through `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIPrompt.scala:668-705`. Inspected the six-stage boundary-case matrix in `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIPromptCacheSuite.scala`.
- [x] Inspected `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIRequestBody.scala`: cache options remain part of each request payload, outside the existing response-format serialization cache. No new shared mutable request state or automatic content breakpoints were introduced.
- [x] Checked generic parameter serialization and generated-wrapper conversion against `cognitive\src\test\python\synapsemltest\services\test_ServiceParamPythonBridge.py:149-198`, including invalid-update preservation and scalar/column bindings through copying, saving and loading.
- [x] Compared `core\src\main\scala\com\microsoft\azure\synapse\ml\fabric\OpenAIFabricSetting.scala:21-67` with the supplied original. Only `ModelNotFound` advice changes; other status gates, exception classes and the existing API-failure boundary remain unchanged.
- [x] Reviewed the model-selection hunks and all three corrected favorite-color fixtures. Full-sentence requests now match the unchanged greater-than-ten-character assertions. `docs\Explore Algorithms\OpenAI\OpenAI.ipynb:305-338` distinguishes optional cache modes, minimum TTL, service defaults and cost uncertainty without promising savings.
- [x] Assessed the supplied validation as driver-reported evidence, not reviewer-executed tests. It reports 55 corrected live-suite tests passing with one intentional ignore, plus two real-Spark bridge tests with nine subtests. Local Python 3.12.3 does **not** establish coverage on the target Python 3.11.8.
- [ ] Fresh published-head CI and current-head automated review remain pending. This CLEAN verdict covers the frozen pre-commit patch only; it is not final CI-qualified approval.