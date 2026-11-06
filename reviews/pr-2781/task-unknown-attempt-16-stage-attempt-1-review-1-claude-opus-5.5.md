---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 1
timestamp: "2026-11-06T04:39:24Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2e9319abe5a41015c7078e310e3a4a635dc01d344e7069e134bb6356f2646c20"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-16-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Shared writer** (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, lines 21 and 114-128)
  - `paramsWithImplicitDefaults` is empty by default.
  - The filter `!implicitDefaults.contains(p.param) || instance.isSet(p.param)` drops only unset opted-in params. Explicit values, including ones equal to the fallback, stay in `paramMap`.
  - `defaultParamMap` is still fully written, and both maps parse numbers with `useBigDecimalForDouble = true`.
- [x] **Shared reader** (same file, lines 164-176)
  - Only the `params`, `defaultParams` and `metadata` JSON on Spark's parsed `Metadata` are replaced with a precise parse.
  - Class, uid, the `getAndSetParams` default/explicit logic and complex-param loading are unchanged. No reader guard is relaxed.
  - The precise parse changes only `-0.0` → `0.0` for Double params. This is negligible and not flagged.
- [x] **AnyJsonFormat** (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`, lines 23-62)
  - Write: null → `JsNull`; Java and Scala BigDecimal → exact `JsNumber`; non-string map keys throw.
  - Read: numbers that a Double cannot represent exactly stay BigDecimal.
- [x] **Cache options and text params** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`)
  - Lines 176-177: only `HasOpenAITextParams` opts in, and only `deploymentName`.
  - Lines 328-353 and 466-472: Row/Map/Seq normalization applies only to `promptCacheOptions`, and null members are kept.
  - All four `sharedTextParams` lists include the option: `OpenAI.scala`, `OpenAIChatCompletion.scala`:88, `OpenAIResponses.scala`:113 and `AIFoundryChatCompletion.scala`:38.
  - Chat and Responses `getOptionalParams` both call `super`.
  - A null options column resolves to `None`, so the whole field is omitted.
- [x] **Fallback scope**
  - Chat (`OpenAIChatCompletion.scala`:116-119) applies the default only when `internalServiceType == "openai"`. `AIFoundryChatCompletion` overrides it with the constant `"foundry"` (a def, so it is safe during construction).
  - Responses default is at line 142; Prompt default is at line 278.
  - `OpenAIEmbedding` uses `HasOpenAIEmbeddingParams`, so it gets no default.
  - `GlobalParams.scala`:43-52 transfers only params that are `!isSet`, so the default never blocks a global deployment.
- [x] **Prompt routing** (`OpenAIPrompt.scala`)
  - Lines 667-668: `hasAIFoundryModel` excludes v1 URLs.
  - Lines 687-694: the alias applies only when the deployment is unset and the stage is not on the Foundry path. Legacy Chat with only a model now fails at the `require`.
  - `transform` transfers globals first (line 531). The write path (lines 64-81) delegates to the shared writer, so the filter applies.
- [x] **Prompt `transformSchema`** (lines 750-754)
  - Works on `copy(ParamMap.empty)`.
  - The custom `copy` (lines 49-62) keeps `postProcessingExplicitlySet`, and an empty `extra` triggers no validation.
  - The original stage is never mutated.
- [x] **Breakpoint validation**
  - `HasMessagesInput.validatePromptCacheBreakpoint` (`OpenAI.scala`:39-46) is called before type dispatch in Chat (`OpenAIChatCompletion.scala`:385) and Responses (`OpenAIResponses.scala`:472). Allowed-field sets are at Chat 348/356 and Responses 421/429/447.
  - Legacy map-backed Chat rejects only non-null breakpoints, because `mapBackedContentParts` removes nulls first (`OpenAIChatCompletion.scala`:287-315). Otherwise the collapse and image conversion are unchanged.
- [x] **Python wrappers**: in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`:176-198, ServiceParams get no Python default, so Python transfer cannot turn the fallback into a set value. The existing `responseFormat: ServiceParam[Map[String, Any]]` is a precedent for this option type.
- [x] **Tests reviewed**
  - `OpenAIRuntimeDefaultsSuite.scala` covers:
    - the fallback;
    - scalar, column, global and null-column deployments;
    - a distinct alias on `*.openai.azure.com/openai/v1/`;
    - Foundry isolation and the legacy Chat guard;
    - schema-only invariance and fit/save credential non-materialization.
  - `test_ServiceParamPythonBridge.py` limits the unsafe-deserialization opt-in to Chat/Responses and restores it in `finally`.
- [x] **Considered, not flagged**: a Row without a schema inside scalar options would NPE in `normalizeCacheOptionValue`. That input is unsupported, and the spec explicitly adds no row-error guarantee for unsupported types.
- [ ] **Test and build execution**: not performed (read-only review). The supplied validation results are taken as evidence, not re-run.
- [ ] **Live hosted-model behavior and remote CI**: not verifiable offline. `OpenAIAPIKey.scala`:14 and the notebook (line 66) depend on a `gpt-6-luna` deployment existing in the shared resource.

## Issues

### Issue 1: Prompt model-alias tests use the fallback value, so they cannot detect a lost alias
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala
- **Line(s)**:
  - Lines 783-796 (`setModel` at 786, assertion at 794).
  - Same pattern in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala`, lines 579-600 (`setModel` at 583, assertion at 600).
- **Description**:
  - The diff changes the Foundry-host v1 Prompt test model from `gpt-5.1` to `gpt-6-luna`. That is now the same as the unset-deployment fallback (`OpenAIDefaults.scala`:12).
  - If the alias block (`OpenAIPrompt.scala`:687-694) stopped running for `services.ai.azure.com/openai/v1` (for example, if `!hasAIFoundryModel` became `!isAIFoundryEndpoint`), the fallback would still send `model: gpt-6-luna`, and the test would still pass.
  - `OpenAIRuntimeDefaultsSuite` checks a distinct alias only on `example.openai.azure.com/openai/v1/`. Its Foundry-host cases use `AIFoundryChatCompletion` or a non-v1 Prompt URL.
  - The Responses payload test in `OpenAIPromptSuite` also uses the fallback value, so the non-v1 Responses alias branch has no distinct-value check either.
- **Risk**: A future change that silently sends these Prompt requests to `gpt-6-luna` instead of the caller's model would pass the unit tests, causing wrong-model results or unexpected cost. The current code is correct.
- **Suggested Fix**: Use a value different from the fallback (for example, `custom-model`) in both tests and assert that it appears in `model`. Optionally, assert that the Foundry-host v1 request URL ends in `/openai/v1/chat/completions`, as the test name implies.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "One additional complete six-lens precommit pass explicitly approved under cap sixteen, with fifteen already consumed. Each invocation has a 40-minute bound. No budget reset or new CI permissions."
  },
  "changeSize": "Thirty-five source/test/notebook paths in the complete staged PR diff, 135361 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
  "cyclomaticComplexity": "Low new branching, with moderate interaction risk across shared service parameters, three text stages, request serialization, generated Python wrappers, global overrides, and parameter persistence.",
  "rationale": "Review the complete cache feature and unset-deployment fallback, limited to OpenAI text stages. Only the text deployment fallback opts into implicit saved defaults; all other defaults retain legacy serialization. Include exact decimal JSON number serialization and metadata roundtrips, non-mutating global-aware Prompt schema resolution, the explicit legacy Chat model-only error, and consistent breakpoint object-shape validation. Neither old-head CI nor an earlier review qualifies this patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available Anthropic Opus frontier model in the live catalog and supports maximum reasoning, complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193",
  "mode": "sequential",
  "reviewAttempt": 16,
  "stageAttempt": 1,
  "manifestSha256": "a85b4aabd0b5807f049737fdcc7ad670533e11d5f875f56e789ffecd1f91bcd9",
  "diffSha256": "13922a0bea06cb73e3ced227321b50b197f6b4ea133aa28bc269c36cc22730bb",
  "dispatchStatus": "starting_lens1_on_new_locally_validated_freeze",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
