---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 1
timestamp: "2026-11-06T03:21:14Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ba35ed93fd2c9bb43416dd368a5d08c2301b73a7c53a05dbb2391e8c6dbc8ad3"
---

I found one Medium issue: the serializer change affects persistence for non-OpenAI services. The rest of the patch matched the spec in source review.

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-15-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I reviewed the supplied diff and surrounding product source using only view, grep and glob. I ran no builds, tests or commands, and I did not re-check the supplied local validation evidence.
- [x] **Fallback scope.** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala:114-119` sets the default only when `internalServiceType == "openai"`.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala:56` overrides that check with a `def` returning `"foundry"`, which is safe during superclass construction.
  - Foundry's `prepareUrlRoot` (`AIFoundryChatCompletion.scala:62-64`) and its `model`-bearing payload never read `deploymentName`.
  - The other defaults are at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala:142` and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala:278`. `OpenAIEmbedding` gets no default.
- [x] **Request routes.**
  - Legacy Chat builds its URL with `getValue(row, deploymentName)` (`OpenAIChatCompletion.scala:125-131`).
  - v1 Chat adds `model` through `withV1DeploymentModel` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala:~559-565`).
  - Responses `mergeModel` (`OpenAIResponses.scala:~174-182`) sends the fallback on both legacy and v1 URLs.
- [x] **Precedence.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala:43-52` fills only params that are not set. Explicit scalar or column deployments and `OpenAIDefaults.setDeploymentName` therefore override the fallback.
- [x] **Prompt alias and guard** (`OpenAIPrompt.scala:671-697`).
  - The condition is `!isSet(deploymentName) && !hasAIFoundryModel`.
  - The `require` fires only for legacy Chat with a model and no deployment. That setup already failed before this PR because the `openai/deployments/{d}` URL had no deployment. No previously working path is newly rejected.
  - On the Foundry route, Prompt's default is copied onto `AIFoundryChatCompletion` (`:683-685`). That value is never used: Foundry's URL and payload ignore it, and the Fabric default-endpoint check cannot reach a Foundry host.
- [x] **Prompt `transformSchema`** (`OpenAIPrompt.scala:750-754`) resolves globals on `copy(ParamMap.empty)`.
  - The copy keeps the same uid, so global registrations (`GlobalParams.scala:13,36-38`) still resolve.
  - `copy` (`:49-62`) preserves `postProcessingExplicitlySet`, the class's only mutable field (`:114`).
  - `transform` (`:530-531`) is unchanged.
- [x] **Cache options.**
  - `OpenAI.scala:~326-351,~464-470` normalizes only `promptCacheOptions`.
  - Chat (`OpenAIChatCompletion.scala:133-136`) and Responses (`OpenAIResponses.scala:~165-172`) both call `super.getOptionalParams`.
  - All four `sharedTextParams` lists include the new param.
  - A null whole column is dropped by `getValueOpt`, and null members serialize as `JsNull`.
- [x] **JSON values.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:23-63` writes Short, Byte, Float, Double and Scala/Java BigDecimal as JSON numbers. It rejects non-string keys. On read, decimals that a Double cannot represent stay `BigDecimal`.
- [x] **Breakpoint validation.**
  - The shared `validatePromptCacheBreakpoint` (`OpenAI.scala:39-46`) runs before type dispatch in Chat `validateContentPart` and Responses `validateContentPart` (`OpenAIResponses.scala:~465-482`).
  - Chat's map-content rejection (`OpenAIChatCompletion.scala:307-315`) is reached from `validateMessages` (`:~481-505`) inside the validation UDF that runs before any HTTP call. Null map entries are filtered out first (`:294-301`).
  - Responses map content rejects string markers through the same validator.
- [x] **Serializer.**
  - Writer filter and BigDecimal parsing: `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:112-124`.
  - The reader (`:159-172`) swaps only the parsed param fields. Class instantiation and `getAndSetParams` are unchanged.
  - I traced Issue 1 through `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:300-308,764-767` and the `apiVersion` defaults in:
    - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/language/AnalyzeText.scala:172`
    - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/language/AnalyzeTextLongRunningOperations.scala:96`
    - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/form/FormRecognizerV3.scala:43`
- [x] **Notebook.** `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` (~308, ~957-959) explains:
  - default precedence;
  - the legacy Chat model-only error;
  - null members versus absent keys and whole-null columns.
- [ ] Not verifiable from source: whether `gpt-6-luna` accepts sampling params. `dropSamplingForGpt5` (`OpenAIResponses.scala:~208-214`) matches only `gpt-5`. Not raised as an issue because the provider owns this behavior.
- [ ] Not executed: Python generated-wrapper runtime. I read `core/src/main/python/synapse/ml/core/schema/Utils.py` and the codegen source instead.

## Issues

### Issue 1: Saved non-OpenAI models can pick up the OpenAI global apiVersion after load
- **Severity**: Medium
- **File**: core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala
- **Line(s)**: 112-116. Related code:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:300-308,764-767`
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala:43-52`
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/language/AnalyzeText.scala:172`
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/language/AnalyzeTextLongRunningOperations.scala:96`
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/form/FormRecognizerV3.scala:43`
- **Description**: The new writer filter leaves unset `ServiceParam` defaults out of `paramMap` for every `ComplexParamsWritable` stage, not just the OpenAI text stages that need it. Shown by code trace, not executed:
  1. `HasAPIVersion.apiVersion` is a `ServiceParam`. It is registered under the OpenAI-specific key `OpenAIApiVersion`. AnalyzeText, AnalyzeTextLongRunningOperations and the FormRecognizerV3 analyzer mix it in and set their own defaults.
  2. Before this PR, `extractParamMap()` wrote those defaults into `paramMap`. After load, `apiVersion` was explicitly set, so `transferGlobalParamsToParamMap()` (called first in `CognitiveServicesBaseNoHandler.transform`) skipped it.
  3. Now those defaults go only to `defaultParamMap`. After load, `isSet(apiVersion)` is false. If the session has called `OpenAIDefaults.setApiVersion(...)`, a reloaded Language or Form model sends the OpenAI api-version to its own service.
  - The root cause predates this PR: an OpenAI global key sits on a shared trait, and freshly built instances already have the problem. The regression is that newly saved models no longer keep their saved `apiVersion` regardless of session globals.
- **Risk**: Reloaded non-OpenAI pipelines saved with this version fail on every row (unsupported api-version). If the service happens to accept the version string, they run against a different API contract instead. This happens whenever the same session sets the OpenAI global api version. It contradicts the PR's stated intent that the change is limited to OpenAI text stages and that ordinary saved behavior is preserved. The stated coverage is OpenAI-only.
- **Suggested Fix**: Limit the omission to the params that need it. Options:
  - Add a core-level marker, such as an overridable set of "implicit service defaults" on the instance. OpenAIChatCompletion, OpenAIResponses and OpenAIPrompt would include `deploymentName`. All other `ServiceParam` defaults would keep the old explicit serialization.
  - Move `GlobalParams.registerParam(apiVersion, OpenAIApiVersion)` from `HasAPIVersion` into `HasOpenAISharedParams`. This also closes the existing leak for fresh instances, but is broader.

  In either case, add a save/load regression test: AnalyzeText (or FormRecognizerV3) with `OpenAIDefaults.setApiVersion` set must keep its saved `apiVersion`. If the global-wide behavior is intended, document the persistence change instead.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "The user explicitly authorized additional review time after a timed-out pass. One additional complete six-lens pass is authorized, cap fifteen with fourteen already consumed; each invocation has a 40-minute bound. No budget reset or new CI permissions."
  },
  "changeSize": "Thirty-five source/test/notebook paths in the complete staged PR diff, 132829 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
  "cyclomaticComplexity": "Low new branching, with moderate interaction risk across shared service parameters, three text stages, request serialization, generated Python wrappers, global overrides, and parameter persistence.",
  "rationale": "Review the complete cache feature and unset-deployment fallback, limited to OpenAI text stages. Include implicit service-default persistence without changing ordinary default serialization, exact decimal JSON number serialization and metadata roundtrips, non-mutating global-aware Prompt schema resolution, the explicit legacy Chat model-only error, and consistent breakpoint object-shape validation. Neither old-head CI nor an earlier review qualifies this patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available Anthropic Opus frontier model in the live catalog and supports maximum reasoning, complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193",
  "mode": "sequential",
  "reviewAttempt": 15,
  "stageAttempt": 1,
  "manifestSha256": "4e8bb6eca8197adb4cbcd8f4bf4f5f74831fceeb8c3536452a16a8d9942917b1",
  "diffSha256": "395f1b31ebe567be5d9ead1e7df02b99ad40e263058d7ecc8b9d8d6f0c062a53",
  "dispatchStatus": "not_started_exact_source_unchanged_local_evidence_reused",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
