---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 5
timestamp: "2026-10-23T12:40:52Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "54e29b2f5095f6377785f034fb21c2a67be05e6987f130c64775dd373865f177"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2781/task-unknown-attempt-5-stage-attempt-1-review-5-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared cache-option wiring and serialization with `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala:23-120`.
- [x] Inspected real Spark HTTP coverage for scalar/column options, provider HTTP 400 responses, and marked/unmarked content in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala:143-210`.
- [x] Inspected Python copy/persistence coverage in `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py:23-66` and Fabric status coverage in `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala`.
- [x] Reviewed the supplied validation summary and notebook request/usage examples.
- [ ] Independent runtime reproduction was not performed: this pass is read-only. Supplied evidence retains the Python 3.12.3 versus target 3.11.8 and bundled Scala 2.12.18 versus build 2.12.17 limitations; new-head remote CI remains pending.

## Issues

### Issue 1: String-only column fixtures miss mixed-type cache-option failures
- **Severity**: Medium
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala
- **Line(s)**: 23-25, 67-75
- **Description**: Column tests exclusively use `MapType(StringType, StringType)`. They never exercise an object combining strings and booleans, although the scalar tests explicitly support `{"ttl":"12h","prewarm":true}`. Preserving those types in one Spark object requires a struct. That column currently fails: `CognitiveServiceBase.scala:102-106` retrieves its `Row` unchanged, `OpenAI.scala:444-447` forwards it unchanged, and `UntypedArrayParam.scala:23-43` rejects `Row` during JSON serialization. A real DataFrame using `struct(lit("12h").alias("ttl"), lit(true).alias("prewarm"))` therefore cannot forward the equivalent scalar options.
- **Risk**: Per-row mixed-type options fail locally instead of reaching authoritative provider validation. String-map fixtures conceal this scalar/column inconsistency.
- **Suggested Fix**: Normalize struct-backed option values using their Spark schema before JSON encoding, without adding cache-policy validation. Add public-transform HTTP regressions for both APIs and OpenAIPrompt that assert string and boolean JSON types together.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending; no edits made.
- **Why**: Column bindings must preserve mixed JSON primitive types.
- **How verified**: Static source trace; runtime reproduction and regression coverage remain pending.

## Driver resolution addendum

The original findings and verdict above are preserved. The driver reproduced the
reported failure through both direct transformers and both `OpenAIPrompt` API
routes using the reviewed installed binaries: all four failed locally on the
struct value before making an HTTP request.

The follow-up normalizes Spark rows, nested maps, and sequences only within
`prompt_cache_options`, preserving primitive JSON types without adding cache
policy validation. Focused Scala regressions cover nested values and real
transformer requests. The notebook explains using a struct for mixed types.
Fresh compiled/generated/installed artifacts passed 69 Scala tests, 25 Python
tests plus 12 subtests, the five executable tutorial cells, and eight mixed-type
HTTP requests across all four public routes. String lifetimes and true/false
Boolean values reached the wire unchanged.

Issue 1 is **addressed locally, pending publication**. Its source/test/documentation
fix invalidates this entire pass. Round 6 was not dispatched, and this pass is
not presented as a clean precommit gate. A complete new six-lens pass is required
on source manifest
`0294032458aa55909c1208076e07a5594c374ba905d66e0efdbb2cde41920dd6`.

### Driver decision and provenance

| Field | Effective decision |
| --- | --- |
| Tier/source | Medium, explicitly selected by the human |
| Change size | 29 full-patch Scala/Python/notebook paths; eight cache-amendment paths |
| Complexity | Small additive serialization changes with cross-language, nested-schema, persistence, and provider-error interactions |
| Rationale | Full-patch independent lenses despite a lean production change; preserve the supplied review and safety bounds |
| Primary model | Runtime identity unavailable; not inferred |
| Reviewers | Exact `gpt-6.1-sol`, `max` reasoning, eligible in the live catalog |
| Model override | Human exact-model selection overrides the installed complementary-family default |
| Scheduling override | Human and trusted repository sequential policy overrides installed Medium concurrency |
| Input manifest | `1e8c6a599c8a661be003bd0e4fa0e942ceff7a612b903385ec04ca813046243f` |
| Input diff | `e33a45203603052a0ea25401359b44b3ed823c8c47e01058f0c86b5cfdbfd3a7` |
| Stage outcomes | Rounds 1–4 clean; round 5 one reproduced finding; round 6 not run |
| Transport | Script-owned Execute; exact full diff embedded; identity, read-only tool scope, privacy, and frozen source audited for all five invocations |
| Bounds | Precommit 5/6 consumed; final 0/2; unlimited overall fix cycles does not reset those caps |

This is precommit evidence only. New-head CI, automated feedback, and a distinct
final CI-qualified pass remain required. No package release or merge is implied.