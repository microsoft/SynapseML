---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 27
stage_attempt: 1
round: 1
timestamp: "2026-11-10T09:50:32Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e7dc22c45f1fe81ee167caa687e724e17abd8f2b73c2a7b7912c9c2f71c3a930"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-27-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage limit, disclosed.** All 48 manifest paths were visible in the supplied diff when this review began. Context compaction later removed the raw diff text. After that, every production path was rechecked against the working-tree source (target plus the staged diff), and selected tests and the notebook were re-read. Before/after attribution relies on the earlier diff read. This verdict is not a clean signoff.
- [x] **Breakpoint validation.** Breakpoints are checked in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala:42-77`. Non-finite numbers fail with `<location>.prompt_cache_breakpoint`. Non-object markers and unsupported values are rejected. `AnyJsonFormat.write` throws `IllegalArgumentException` for unsupported values and non-string keys (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:17-46`).
- [x] **Per-row errors with no HTTP request.** Chat (`OpenAIChatCompletion.scala:561-570`) and Responses (`OpenAIResponses.scala:533-542`, `356-400`, `218-219`) catch these errors. They set a per-row `errorCol`, null the messages column, and `shouldSkip` prevents the HTTP call.
- [x] **Nested nulls are consistent.** Chat (`OpenAIChatCompletion.scala:231-286`) and Responses (`OpenAIResponses.scala:230-285`) both omit null struct and map members and keep null array elements.
- [x] **Luna default.**
  - Chat sets it only when `internalServiceType == "openai"` (`OpenAIChatCompletion.scala:124-127`). Foundry overrides that with a constant def (`AIFoundryChatCompletion.scala:56`), so Foundry gets no Luna default.
  - Responses and Prompt set it at `OpenAIResponses.scala:145-149` and `OpenAIPrompt.scala:272-285`.
  - Global settings take precedence through `transferGlobalParamsToParamMap`. This is exercised by `OpenAIRuntimeDefaultsSuite.scala:47-80`.
- [x] **Prompt behavior.**
  - Legacy Azure Chat with only `model` set is rejected (`OpenAIPrompt.scala:678-708`).
  - Schema-only resolution runs on a copy (`OpenAIPrompt.scala:761-765`).
  - No-op `store=false` is skipped for Chat and Foundry (`OpenAIPrompt.scala:674-675`).
  - Responses defaults to `store=false`, including for null column values (`OpenAIResponses.scala:170-178`).
  - `reasoningEffort` and `reasoning.effort` must agree (`OpenAIResponses.scala:203-216`).
- [x] **Dismissed concerns.**
  - The suspected null-pointer in `getFilterReason` cannot occur: its only caller passes a non-null row (`OpenAIPrompt.scala:310-323`).
  - The schema extensions add no duplicate field names (`OpenAISchemas.scala:83-150` compared with the case classes).
  - `HasOpenAITextParams` mixing in `ComplexParamsWritable` causes no `DefaultParamsWritable` conflict in `cognitive/src/main/scala`.
- [x] **Persistence and exact decimals.** Opted-in params are filtered at `ComplexParamsSerializer.scala:69-77`. The reader parses metadata with exact decimals (`ComplexParamsSerializer.scala:172-184`). An explicit handler stays guarded on reload (`OpenAIPromptCacheSuite.scala:313-335`).
- [x] **Approved changes are not reported as defects.** The Chat rejection of legacy controls (`OpenAIChatCompletion.scala:141-150`) and Responses sampling forwarding are prominently disclosed at `docs/Explore Algorithms/OpenAI/OpenAI.ipynb:1100` and `:1102`.
- [x] **Live tests.** They are opt-in behind `SYNAPSEML_LIVE_CACHE_TESTS`, set credentials only after save and load, and the evidence they record contains no keys (`cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py:26-56`, `:177-185`).
- [ ] **Tests were not executed.** This lens is read-only.

## Issues

### Issue 1: Opted-in defaults are still saved in `defaultParamMap` and restored on load
- **Severity**: Low
- **File**: core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala
- **Line(s)**: 69-77, 130-137, 178-184
- **Description**: `paramsToPersist` removes an unset `deploymentName` from `paramMap`. However, `getMetadataToSave` still writes `instance.defaultParamMap` without that filter, so `{"deploymentName":{"left":"gpt-6-luna"}}` is saved. On load, Spark's `getAndSetParams` calls `setDefault` with the saved value. The default is therefore pinned by the artifact, not recreated by the constructor as the spec describes. The test at `OpenAIPromptCacheSuite.scala:309` only checks that `isSet` matches, so it cannot detect this.
- **Risk**: No visible effect today, because the saved and constructor values are the same. If a later release changes the default, stages saved by this version would keep sending `gpt-6-luna`.
- **Suggested Fix**: Exclude `paramsWithImplicitDefaults` from `jsonDefaultParams` as well, and add a metadata assertion. Alternatively, document that Spark-pinned defaults are the intended contract.

### Issue 2: Some Responses content-part encoding errors omit the content location
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala
- **Line(s)**: 272-285, 303-331
- **Description**: Chat wraps encoding failures as `messages[i].content[j] is invalid: ...` (`OpenAIChatCompletion.scala:427-449`). Responses does not. For example, a breakpoint struct containing `MapType(IntegerType, …)`, or a struct whose length does not match its schema, produces only `Content part map keys must have string type`. The row still fails per row and sends no request. Only the message is less specific, and this may predate the PR.
- **Risk**: Responses diagnostics are weaker than Chat's for invalid breakpoint objects, which conflicts with the intent to behave consistently across both APIs.
- **Suggested Fix**: Wrap each part's `encodeStructuredValue` call in Responses with the same location prefix that Chat uses.

### Issue 3: The notebook attributes a client-side Foundry `store` rejection to the endpoint
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 1114
- **Description**: The notebook says "Foundry model-inference endpoints reject true and non-null storage-column values". In fact, SynapseML rejects any resolved `store` value locally (`OpenAIChatCompletion.scala:142-143`), and this fails the Spark task. That includes a scalar `false` set directly on `AIFoundryChatCompletion`.
- **Risk**: This contradicts the requirement that documentation distinguish user configuration from provider support. Users of the stage directly may expect `setStore(False)` to work, or expect a per-row provider error instead of a task failure.
- **Suggested Fix**: State that SynapseML rejects any configured `store` value for Foundry model inference during request construction, which fails the task. Note that Prompt omits only the no-op scalar `false`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Trusted repository policy requires six independent lenses; parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Explicit human selection one_pass_60 authorizes exactly one new complete pass27, 60 minutes per lens, with 26 consumed and no waiver or automatic renewal."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 277051 bytes.",
  "cyclomaticComplexity": "Low added branching, moderate interaction risk across shared service parameters, request encoding, schema validation, generated wrappers and constructor-recreated defaults.",
  "rationale": "Review the complete cache/default feature, including persistence repairs, JSON-compatible breakpoint validation, consistent nested-null encoding, public-transform regressions and accepted compatibility disclosures.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available current Anthropic Opus frontier model and supports max reasoning, complementary to this OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 37948d7d03ef0fa7f1aa24c54c2dd4d8a29c8dc0 (unchanged from trusted 6c2699defcfb24d5bff5ba55f2252ba2d8cbea24)",
  "mode": "sequential",
  "reviewAttempt": 27,
  "stageAttempt": 1,
  "manifestSha256": "539872b55d94d9f1a105d54afdc956d8ad084dd9d310854783bccd68ccd55cf0",
  "diffSha256": "34915f4069de67038d9bc0d725f2b0af47c796e201fe58c0b66d083334231d6a",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver resolution addendum

The original review above is preserved unchanged. Its `issues_found` verdict
and incomplete-coverage disclosure remain in force; this was not a completed
six-lens pass. Lenses 2-6 were not started. A separately authorized complete
split pass must assess the repaired patch before publication.

### Issue 1: fixed

`ComplexParamsSerializer` now uses the same opt-in filter for `paramMap`,
`defaultParamMap`, and complex-parameter locations. It omits a
constructor-recreated parameter only when unset; explicit values (including
the constructor's current value) and ordinary defaults retain their existing
persistence behavior. Newly saved unset deployment defaults follow the
loading version's constructor; older artifacts with saved defaults retain
their existing reader semantics.

The new test, **Unset constructor-recreated defaults are omitted from both
metadata maps**, failed before the production repair because the saved
default remained present. It now covers unset, explicit-default and custom
values through metadata inspection, load and resave. The final focused run
passed 18 core tests and 61 cognitive tests, including portable-model,
ordinary-default, custom-handler and Java-deserialization-guard regressions.
Both core style checks passed.

### Issue 2: pre-existing behavior; no unrelated encoder change

The cited Responses `encodeStructuredValue` and `encodedMessageContent`
implementations are byte-identical to target
`37948d7d03ef0fa7f1aa24c54c2dd4d8a29c8dc0`. The generic message for malformed
structured content therefore predates this PR. Existing per-row failure and
no-HTTP behavior remain intact. This is not treated as a demonstrated
regression, and the repair does not broaden into an unrelated encoder refactor.

### Issue 3: fixed

The notebook now states that **SynapseML** locally rejects any resolved
Foundry model-inference `store` value during request construction, failing
the Spark task. That includes scalar `false` set directly on
`AIFoundryChatCompletion`; Prompt omits only no-op scalar false. The clarification
distinguishes client validation from provider responses. Only Markdown cells
changed, with list-of-string sources and executable cells preserved.
