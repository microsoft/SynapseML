---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 5
timestamp: "2026-10-21T21:43:26Z"
outcome: "clean"
reviewer_model: "gpt-6.1-sol"
input_sha256: "d44ee68e8ff1dcca46d5154ce2f76ddb0ea54b9340a9537c6d0e4968f3ce59be"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-3-stage-attempt-1-review-5-gpt-6.1-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Reviewed `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` against the production changes. Its six stage variants cover unset and empty options, supported values, column precedence, null-row omission, malformed scalar and row values, and rejection through direct parameter assignment. Exact payload comparisons protect unrelated request fields.
- [x] Inspected the public-transform coverage in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala:172-202`. The Foundry-backed test captures actual loopback HTTP bodies, checks scalar and column options and the selected model, asserts no row errors and exactly two requests, and uses server cleanup protected by `finally`.
- [x] Reviewed `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py`. The new tests exercise generated constructors, `setParams`, setters, getters, validation failures, copying, and save/load for Chat Completions, Responses, and OpenAIPrompt with both scalar and column bindings. Temporary storage and the deserialization configuration are restored.
- [x] Compared `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` with `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala`. Coverage checks the ModelNotFound exception class and conditional guidance, Allowed, the other failure statuses, unknown statuses, and non-string results. The public method invokes the extracted status check outside its existing lookup catch.
- [x] Inspected the full-sentence fixtures and retained greater-than-ten-character assertions in OpenAIChatCompletionSuite, OpenAIResponsesSuite, and OpenAIDefaultsSuite. Independently confirmed that deliberate older-model sampling coverage remains in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala:119-154`.
- [x] Considered the driver-supplied validation as pre-commit evidence only: affected offline tests, two real-Spark bridge tests with nine subtests, synthetic integration checks, and 55 passing tests after the fixture correction with one unchanged intentional ignore. No commands or tests were executed by this reviewer.
- [ ] Fresh current-head CI and validation on the pinned Python 3.11.8 remain outstanding; the supplied local bridge validation used Python 3.12.3. The predecessor’s failed CI does not establish a defect in this correction, and this CLEAN verdict is not final CI qualification.