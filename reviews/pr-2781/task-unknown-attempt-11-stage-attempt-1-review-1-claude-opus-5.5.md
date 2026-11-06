---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 1
timestamp: "2026-11-05T21:31:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d98fc69e6eea1c2f869765c7fa587ab459dc41ae7e7428c0f14a545260c0ef84"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-11-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Prompt model alias and guard.** Reviewed in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`:
  - guard and `require` at 687-694;
  - `transform` transfers global params first (530-531);
  - `transformSchema` does not transfer them (750-753);
  - endpoint helpers at 663-668.
  
  `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` fills only params that are not set (41-52) and does not override any getter.
- [x] **Default deployment.** Each text stage gets its fallback through `setDefault`, not through a global value:
  - `OpenAIChatCompletion.scala` 116-119;
  - `OpenAIResponses.scala` 140-144;
  - `OpenAIPrompt.scala` 278;
  - the constant in `OpenAIDefaults.scala` 12.
  
  Explicit scalar, column and global deployments take precedence. A null column binding does not fall back to Luna (`OpenAIRuntimeDefaultsSuite.scala` 44-107). Embeddings have no default.
- [x] **Foundry route.** Luna does not leak into Foundry requests. `withV1DeploymentModel` (`OpenAI.scala` 548-554) applies only to v1 URLs, Foundry uses `models/chat/completions`, and the Foundry payload keeps `model` (`OpenAIRuntimeDefaultsSuite.scala` 109-118).
- [x] **Serializer.** In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` 112-118:
  - only unset ServiceParams are left out of `paramMap`;
  - `defaultParamMap` is still written in full;
  - the reader still uses `getAndSetParams` (165), which restores defaults as defaults.
  
  `Serializer.LegacyObjectDeserializationConfig` is unchanged, so no reader guard was relaxed.
- [x] **Cache options plumbing.**
  - `promptCacheOptions` is in `sharedTextParams` for the base stage, Chat, Responses and AIFoundry.
  - Chat and Responses `getOptionalParams` overrides call `super`, so the cache-option normalization applies.
  - `AnyJsonFormat` write/read (`core/.../param/UntypedArrayParam.scala` 23-60) round-trips Int, Long, Double, Boolean, String, Seq and Map.
  - The Python test asserts that booleans stay booleans (`test_ServiceParamPythonBridge.py` 100-126).
- [x] **Breakpoint validators.** `prompt_cache_breakpoint` is now allowed on Chat text and image_url parts and on Responses input_text, input_image and input_file parts. The struct encoders drop null fields (`OpenAIChatCompletion.scala` 222-228, 247).
- [x] **Fabric.** `OpenAIServicesBase.getInternalTransformer` (`OpenAI.scala` 580-585) now checks tenant status for `gpt-6-luna` when no deployment is set. The `core/.../fabric/OpenAIFabricSetting.scala` message refactor and its two-argument overload are correct.
- [x] **Observation, not counted:** the Responses `dropSamplingForGpt5` check (`OpenAIResponses.scala` 207-213) matches only `gpt-5`. If the new default model rejects sampling parameters, the provider would return errors. This is an existing heuristic and the model's capabilities could not be verified.
- [ ] **Not done:** no tests were executed (read-only lens). The hosted `gpt-6-luna` deployment and the provider's handling of `prompt_cache_options` and null values were not verified.

## Issues

### Issue 1: `OpenAIPrompt.transformSchema` now throws when a model alias's endpoint, deployment or API type comes from global params
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 687-694 (new `require`), 750-753 (`transformSchema` calls `getOpenAIChatService`); compare 530-531
- **Description**: The new `require` checks `isSet(deploymentName)`, `isOpenAIV1Endpoint`/`hasAIFoundryModel` (both read `get(url).orElse(getDefault(url))`) and `getApiType` using only the stage's own params. `transform` copies global params onto the stage first, but `transformSchema` does not.

  Consider a stage with an explicit `setModel(...)` whose URL comes from `OpenAIDefaults.setURL` (a v1 URL or a Foundry URL), or whose deployment or `apiType="responses"` comes from `OpenAIDefaults`. At schema time it looks like a legacy Chat model-only configuration and fails with "model is not a deployment alias for legacy Chat endpoints". `transform` succeeds for the same stage once the globals are copied over. Before this change the alias condition was simply false in that state, so schema resolution succeeded.
- **Risk**: This is a demonstrated regression. JVM code paths that call `transformSchema` before running a stage now fail for configurations that worked before and that `transform` still supports:
  - Scala/Java `PipelineModel.transform` and `Pipeline.fit`;
  - tuning wrappers;
  - direct `transformSchema` calls.
  
  PySpark pipelines skip this path, so the Python validation would not catch it. The error is also misleading: it tells users who already set a global deployment to call `setDeploymentName`.
- **Suggested Fix**: Choose one of these:
  - Evaluate the guard against effective values, using `get(p).orElse(GlobalParams.getParam(p)).orElse(getDefault(p))` for url, deploymentName, model and apiType.
  - Resolve the schema from a copy that has the globals transferred.
  - Enforce the `require` only on the `transform`/`prepareEntity` path.
  
  Add Scala regression tests for three cases: a global v1 URL, a global Foundry URL, and a global deployment with a legacy URL. Each case should use an explicit `setModel` and exercise `transformSchema` and `Pipeline().setStages(Array(prompt)).fit(df).transform(df)`.

### Issue 2: Column-bound `promptCacheOptions` structs and maps send null fields as JSON `null`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**: 333-340 (`normalizeCacheOptionValue`); `getOptionalParams` ~453-459
- **Description**: `normalizeCacheOptionValue` keeps null values from struct and map entries, and `AnyJsonFormat` writes them as `JsNull`. A struct options column whose row lacks `ttl` therefore sends `{"mode":"explicit","ttl":null}`. Three things conflict with this:
  - The scalar setters never send a key for an absent option.
  - The content-part encoders added alongside this feature drop null fields (`OpenAIChatCompletion.scala` 222-228, 247).
  - The notebook (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb` 957-959) tells users to omit `ttl` to get the service default and to use structs for mixed types. Following both instructions together produces explicit nulls.
- **Risk**: If the provider treats an explicit `null` differently from an omitted field, or rejects it, row-level options from heterogeneous or inferred structs fail or diverge from the scalar path.
- **Suggested Fix**: Drop null entries while normalizing Row and Map values. Add a test where one row of a struct column has a null field.

### Issue 3: Map-backed Chat content handles `prompt_cache_breakpoint` inconsistently and silently drops it for text-only content
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala
- **Line(s)**: 271-284 (`collapseContentPartsToText`), 307-333 (`encodedMapBackedContent`)
- **Description**: For `ArrayType(MapType(StringType, ...))` content, the same breakpoint field gets three different outcomes:
  - **Text-only content:** the parts collapse into joined text and the breakpoint is silently discarded.
  - **Image parts:** an image part carrying a breakpoint is rejected by `requireOnlyFields(part, Set("type","image_url","detail"))`.
  - **Text parts mixed with images:** the breakpoint is forwarded using the map's value type, not the documented `{"mode":"explicit"}` object.
- **Risk**: In explicit cache mode, a silently dropped breakpoint means no cache reads or writes, with no error raised. This is an edge case rather than a regression, because the documentation directs users to nested struct schemas.
- **Suggested Fix**: Reject breakpoints in map-backed content with an error that points to the struct schema, or forward them consistently. Add a test.

### Issue 4: The Python test enables unsafe Java deserialization for `OpenAIPrompt` loads, beyond the stated Chat/Responses scope
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py
- **Line(s)**: 28-51
- **Description**: The test sets `spark.synapseml.legacy.allowUnsafeJavaDeserialization=true` around every load in the loop, including `OpenAIPrompt`. The same file loads `OpenAIPrompt` without this setting (~83, ~139), so Prompt does not need it. The stated scope covers only the Chat/Responses fixtures.
- **Risk**: Direct risk is minimal, because the setting is restored in `finally` and the fixtures are freshly written. However, it could hide a future regression in which Prompt artifacts start requiring unsafe deserialization.
- **Suggested Fix**: Enable the setting only for Chat and Responses, and keep Prompt loads on the default secure reader.

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

### Issue 4
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
    "budget": "Two additional precommit passes explicitly approved. Ten of twelve consumed before this dispatch; no budget reset."
  },
  "changeSize": "Thirty-four source/test/notebook paths in the complete staged PR diff, 116090 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
  "cyclomaticComplexity": "Low new branching, with moderate interaction risk across shared service parameters, three text stages, request serialization, generated Python wrappers, global overrides, and parameter persistence.",
  "rationale": "Review the complete cache feature and unset-deployment fallback. Include the serializer correction that preserves implicit service defaults without changing ordinary default serialization, and the explicit failure for unsupported legacy Chat model-only selection. Neither old-head CI nor an earlier review qualifies this patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available Anthropic Opus frontier model in the live catalog and supports maximum reasoning, complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193",
  "mode": "sequential",
  "reviewAttempt": 11,
  "stageAttempt": 1,
  "manifestSha256": "de2d855ab7069f0e0bff6871c2bb6ee553870db021c6d988fd9cd7b0e87056d0",
  "diffSha256": "6fc8cd55d27c1641ec3200bdfc6138403853618831e7a39f7380d381e8b0ab2d",
  "dispatchStatus": "not_started_local_gates_passed",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
