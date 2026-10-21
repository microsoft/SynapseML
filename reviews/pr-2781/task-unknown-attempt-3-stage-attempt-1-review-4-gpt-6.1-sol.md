---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 4
timestamp: "2026-10-21T21:39:14Z"
outcome: "clean"
reviewer_model: "gpt-6.1-sol"
input_sha256: "4b577fa1ed7f3168e6537e1639caf4cc48ca5e0764aef2eb969aea708b2803e8"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-3-stage-attempt-1-review-4-gpt-6.1-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Checked scalar and row-bound validation in [OpenAI.scala](cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala#L316), tracing `getValueOpt` in [CognitiveServiceBase.scala](cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala#L102). Empty maps are accepted; unsupported keys, invalid values and null scalar maps are rejected; null column rows omit the field.
- [x] Traced `prompt_cache_options` through the Chat Completions, Responses and Foundry parameter lists and [OpenAIPrompt service delegation](cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala#L669). Token remapping, model selection and Responses nesting preserve the new top-level object without introducing defaults or content breakpoints.
- [x] Inspected the existing `ServiceParam` JSON encoding and generated Python conversion paths alongside [test_ServiceParamPythonBridge.py](cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py#L149). The added tests cover constructor dictionaries, `setParams`, invalid-update preservation, copying, and scalar/column persistence. [OpenAIPromptCacheSuite.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala) covers payload shapes and malformed values across all six stage configurations.
- [x] Compared the extracted status branches in [OpenAIFabricSetting.scala](core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala#L36) with the supplied original. Only `ModelNotFound` advice changes; other statuses, exception classes and deployment values retain their behavior. Inspected the corresponding status tests.
- [x] Checked the Luna substitutions and retained compatibility boundaries: [OpenAIAPIKey.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala#L10) selects the live test deployment; [OpenAICoreOfflineSuite.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala#L119) retains deliberate old-model cases; embedding selections and production legacy handling remain unchanged.
- [x] Verified the full-sentence favorite-color prompts and unchanged greater-than-ten-character assertions in [OpenAIChatCompletionSuite.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala), [OpenAIResponsesSuite.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala), and [OpenAIDefaultsSuite.scala](cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala).
- [x] Reviewed the four notebook changes, including [OpenAI.ipynb](docs/Explore%20Algorithms/OpenAI/OpenAI.ipynb). The caching documentation distinguishes optional modes and minimum TTL, explains explicit mode without breakpoints, and makes no guaranteed retention or savings claim.
- [x] Considered the driver-supplied compilation, offline, bridge, synthetic-integration and corrected live-suite results as supplied evidence—not independently executed tests or current-candidate CI.
- [ ] Fresh published-candidate CI and current-head automated review remain deferred until publication, as permitted for this pre-commit pass. The supplied local bridge run used Python 3.12.3 rather than target Python 3.11.8. No commands or tests were executed in this read-only review.