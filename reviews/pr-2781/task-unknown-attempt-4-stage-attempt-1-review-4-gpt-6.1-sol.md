---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 4
timestamp: "2026-10-23T09:19:14Z"
outcome: "clean"
reviewer_model: "gpt-6.1-sol"
input_sha256: "97c5e6f18fb53a5ee15facbf9417df89bb95a97b64989c7fa0cf91af02b4b8f9"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-4-stage-attempt-1-review-4-gpt-6.1-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Reviewed all 23 supplied path changes against frozen base `71346cf992d489007d133a1a9f83c325e7694373`, including Scala/Python fixtures and four current notebooks.
- [x] Traced `core\src\main\scala\com\microsoft\azure\synapse\ml\fabric\OpenAIFabricSetting.scala`: helper extraction preserves status conversion, exception types, rejection branches and availability enforcement. Only `ModelNotFound` receives conditional guidance.
- [x] Inspected `core\src\test\scala\com\microsoft\azure\synapse\ml\fabric\OpenAIFabricSettingSuite.scala`: assertions cover the retained failure, conditional advice, allowed/denied statuses, unknown status and non-string rejection.
- [x] Checked model selection and expected URL/payload consistency throughout `cognitive\src\test`; removed deployment aliases have no remaining references. Full-sentence prompts retain the existing greater-than-ten-character assertions, and Responses helper settings remain fixture-local.
- [x] Traced relevant production request construction and notebook configuration. Caller-selected deployments, embedding selections, deliberate offline compatibility cases, legacy handling and existing caching remain unchanged; no generic model default or deferred cache API is introduced.
- [x] Considered driver-supplied validation; no tests were executed during this review. Exact-environment limitations remain: Python 3.12.3 versus target 3.11.8, and bundled Scala 2.12.18 versus target 2.12.17.
- [ ] Latest-target integration, fresh narrowed-head remote CI and the final CI-qualified review remain pending; this verdict covers the frozen pre-commit source patch only.