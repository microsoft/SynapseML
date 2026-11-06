---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 1
timestamp: "2026-11-05T22:04:50Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ace00cc9de0189acd26467e33f177dcd7fca6b97695cf51d720d77dbc3bc500d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-12-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Global settings still win over the Luna fallback.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` (43-52) copies a global value only into params that are not explicitly set (`!isSet(p)`). The new `setDefault(deploymentName -> Left(gpt-6-luna))` therefore does not hide a global deployment. The test at `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` (63-77) checks this.
- [x] **The default is limited to the three intended stages.** It is set in `OpenAIChatCompletion.scala` (116-119), `OpenAIResponses.scala` (140-144) and `OpenAIPrompt.scala` (278); embeddings are untouched. `AIFoundryChatCompletion` inherits the default, but its URL path (`models/chat/completions`) and required `model` param are unchanged. On v1 URLs, `withV1DeploymentModel` adds `model` only when it is absent.
- [x] **The model-alias guard behaves as specified.** In `OpenAIPrompt.scala` (687-693), aliasing happens only when `model` is defined, `deploymentName` is not explicitly set, and the route is not Foundry. Legacy Chat model-only setups fail through `require`. Scalar, column and global deployments skip the alias and are preserved.
- [x] **The serializer change is narrow, and no reader security check was relaxed.** In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, the writer leaves a ServiceParam out of `paramMap` only when it is not explicitly set; on load, defaults are restored through `getAndSetParams`/`setDefault`. The `spark.synapseml.legacy.allowUnsafeJavaDeserialization` setting at `core/src/main/scala/org/apache/spark/ml/Serializer.scala:32` is unchanged by the diff.
- [x] **The cache options keep their JSON types.**
  - `normalizeCacheOptionValue` in `OpenAI.scala` (316-340) converts Rows, Maps and Seqs into plain nested values, and `getOptionalParams` (452-458) applies it.
  - `AnyJsonFormat` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`, 23-58) writes null as JsNull and reads JsNull back as null. Explicit null members therefore survive both the request and save/load. `OpenAIRequestBody.encode` uses the same format.
  - A column whose value is entirely null makes `getValueOpt` return None, so the whole field is omitted.
- [x] **Python values reach the JVM intact, and the notebook's calls exist.** `core/src/main/python/synapse/ml/core/schema/Utils.py` and `ServiceParam.toMap` deep-convert dicts and lists and keep `None` as null. `cognitive/src/main/python/synapse/ml/services/openai/OpenAIDefaults.py` defines `set_verbosity` and `set_reasoning_effort` (114-129), which the notebook calls.
- [x] **Legacy Chat map-content rejection matches the spec.** In `OpenAIChatCompletion.scala` (307-316), the error is raised inside the existing validation step, so it lands in `errorCol` with no HTTP call. `mapBackedContentParts` drops null values first, so the original text collapse still applies when no breakpoint is supplied.
- [x] **Prompt forwards the options.** `OpenAIPrompt` mixes in `HasOpenAITextParamsExtended` (line 36), and lines 684-685 copy params onto the inner completion stage.
- [ ] **Not done: running tests.** This was a read-only review. The supplied local and isolated validation results are taken as reported and were not re-run.
- [ ] **Not verifiable here: provider-side behaviour.** This covers whether the provider accepts option values, including the notebook's `max` reasoning effort; the Scala doc at `OpenAI.scala:421` allows "any user string". It also covers how the provider handles string-typed breakpoints.
- [ ] **Not raised as issues (existing limits or unproven):**
  - `AnyJsonFormat` cannot write Decimal, Timestamp or binary values, or maps with non-string keys. This applies to all optional params already and fails as a row error.
  - `OpenAIRuntimeDefaultsSuite` (lines 48 and 58) asserts that no global deployment is set rather than clearing it. That only causes failures if another suite leaks global state, which I did not demonstrate.

## Issues

### Issue 1: `OpenAIPrompt.transformSchema` now writes global settings, including the API key, into the stage, so they get saved
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 751 (new `transferGlobalParamsToParamMap()` call in `transformSchema`). The test that asserts this behaviour is `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` 158-162.
- **Description**: `transformSchema` now calls `transferGlobalParamsToParamMap()` on `this`. That call explicitly sets every unset param that has a global value, including:
  - `subscriptionKey`, which is linked to the global `OpenAISubscriptionKey` in `CognitiveServiceBase.scala` 133-139;
  - the URL, deployment, model and API type.

  Spark's JVM `Pipeline.fit` calls `transformSchema` on each stage. For transformer stages after the last estimator, it then puts those same instances into the `PipelineModel`. `ComplexParamsWriter` saves explicitly set ServiceParams in plain text as `{"left": value}`, and nothing redacts them.
  - **Before this PR:** only `transform` changed the stage this way, and that part is unchanged. The base `CognitiveServicesBase.transformSchema` still does not.
  - **After this PR:** a schema check alone is enough. That includes `Pipeline.fit`, `PipelineModel.transformSchema`, or a direct `transformSchema` call.
- **Risk**:
  - **Leaked key:** someone who sets the key globally, as the notebook shows, then runs `Pipeline.fit` and saves the model, writes the key into the saved metadata. Before this change, that path did not save it.
  - **Stale settings:** values are fixed at schema/fit time, so the fitted stage ignores later `OpenAIDefaults` changes. Before, they were fixed only at the first `transform`.
  - The new test locks the side effect in (`schemaStage.getUrl == url`).
- **Suggested Fix**:
  - Resolve global settings on a copy instead of `this`, for example `val resolved = copy(ParamMap.empty)`, then `resolved.transferGlobalParamsToParamMap()`, then compute the schema and run the guard on `resolved`. This keeps the same helper and precedence the spec requires.
  - Update the test to check the schema result and the guard error instead of the stage's params.
  - Add regression checks that, after `transformSchema` or JVM `Pipeline.fit` with a global key, `subscriptionKey` is not explicitly set and a saved PipelineModel does not contain the key.

### Issue 2: Responses map-based content now sends a `prompt_cache_breakpoint` as a JSON string
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala
- **Line(s)**: 421, 429, 447 (field lists in `validateInputTextPart`, `validateInputImagePart` and `validateInputFilePart`); 259-270 (`encodeMap`)
- **Description**: All three Responses checks now accept `prompt_cache_breakpoint` without looking at the value's type. Content stored as a map rather than a nested struct (string keys, string values) goes through `encodeMap`, and `encodeStructuredValue` leaves strings unchanged. A breakpoint such as `"{\"mode\":...}"` is therefore sent to the provider as a JSON string.
  - Before this PR, the same input was rejected locally as an unsupported field.
  - Chat rejects the same layout with a "requires a nested struct content schema" error (`OpenAIChatCompletion.scala` 307-316).
  - The spec says breakpoints require nested struct content schemas and should not be sent as strings.
- **Risk**: Chat and Responses now handle the same unsupported input differently. Responses spends an HTTP call and returns an unclear provider 400 (or whatever the provider does with a string) instead of the local error with guidance that Chat gives.
- **Suggested Fix**: In the Responses map-content path, reject a non-null `prompt_cache_breakpoint` with the same nested-struct error, routed through `errorCol` before any HTTP call. Alternatively, require the encoded breakpoint to be an object. Add a Responses map-content regression test that checks the error appears in `errorCol` and no HTTP request is made.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "Last of the two additional precommit passes explicitly approved. Eleven of twelve consumed before this dispatch; no budget reset."
  },
  "changeSize": "Thirty-four source/test/notebook paths in the complete staged PR diff, 120849 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
  "cyclomaticComplexity": "Low new branching, with moderate interaction risk across shared service parameters, three text stages, request serialization, generated Python wrappers, global overrides, and parameter persistence.",
  "rationale": "Review the complete cache feature and unset-deployment fallback. Include implicit service-default persistence without changing ordinary default serialization, global-aware Prompt schema resolution, the explicit legacy Chat model-only error, and consistent legacy map-content breakpoint rejection. Neither old-head CI nor an earlier review qualifies this patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available Anthropic Opus frontier model in the live catalog and supports maximum reasoning, complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193",
  "mode": "sequential",
  "reviewAttempt": 12,
  "stageAttempt": 1,
  "manifestSha256": "1437cd76e82a6cce6b43214d2aed4f960c2f7a87eb773ce661c943e24c3e38df",
  "diffSha256": "e6cfb58954a34966a3698e90b6d05b8967e7a8af9d65c57e606fef8996a5b1a6",
  "dispatchStatus": "not_started_local_gates_passed",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
