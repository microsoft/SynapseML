---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 1
timestamp: "2026-11-06T12:11:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "bc07b1b48d6bff5713cbf23704b693b2e26eab50b8557e6ee777f2947b17f532"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-20-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Saving keeps the fallback implicit.**
  - In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `paramsWithImplicitDefaults` (L20-21) is empty by default. The writer filter (L113-118) drops an opted-in param from `paramMap` only when it isn't set, and defaults are still written to `defaultParamMap`.
  - Only `HasOpenAITextParams` opts in (`cognitive/.../openai/OpenAI.scala` L176-177). Its only users are Chat, Responses, Prompt and Foundry, and all of them already used the complex writer. Spark compares params by parent uid and name, so `Set.contains` works.
  - Other services and ordinary defaults are still saved as explicit values.
- [x] **Exact decimals survive save and load, and no reader guard is relaxed.**
  - Numbers are parsed with `useBigDecimalForDouble=true` when written (L122-127). The reader replaces only `params`, `defaultParams` and `metadata` with a precise parse (L163-172). Class, uid and Spark's set-param path are unchanged.
  - In `core/.../param/UntypedArrayParam.scala`, Scala and Java BigDecimal are written as exact `JsNumber` (L31-32). Reads convert to Double only when `isDecimalDouble` holds (L49-56), so 2^63 and 2^64 stay BigDecimal.
  - The narrower read changes only number literals that are exact binary doubles but not in shortest decimal form. Earlier writers never produced those, and `ValueIndexer.scala` L163-178 already handles BigDecimal levels, so existing `UntypedArrayParam` users don't regress.
- [x] **The Luna default applies only when no deployment is set.**
  - Chat's constructor guard (`OpenAIChatCompletion.scala` L117-119) skips the default for Foundry, which overrides `internalServiceType` with "foundry" (`AIFoundryChatCompletion.scala` L56). Responses (L140-144) and Prompt (L269-282) also set the default.
  - Globals fill only unset params (`GlobalParams.scala` L43-52). Column bindings replace the default, so a null deployment value doesn't silently fall back to Luna.
- [x] **Prompt model aliases and the legacy Chat guard behave as specified.**
  - In `OpenAIPrompt.scala` L683-695, a v1 endpoint or Responses gets the model as its deployment. Otherwise, legacy Chat requires a custom URL root, and the guard uses the same check as URL construction (`CognitiveServiceBase.scala` L483).
  - `transform` resolves the service on the driver (L537), so the error is raised before any Spark job starts.
  - On the Foundry route, the Luna deployment copied from Prompt is never used. The URL is `models/chat/completions` (`AIFoundryChatCompletion.scala` L62-64) and the payload carries its own `model`.
- [x] **Prompt schema checks don't change the original stage.**
  - `transformSchema` (`OpenAIPrompt.scala` L753-757) resolves globals on a `copy(ParamMap.empty)`. The copy keeps the post-processing flag (L49-62).
  - `OpenAIRuntimeDefaultsSuite.scala` covers this (L243, L258, L275): no credential is materialised, changed globals are picked up, and Pipeline.fit/save doesn't persist globals.
- [x] **Prompt-cache options are forwarded unchanged, with JSON types preserved.**
  - `normalizeCacheOptionValue` (`OpenAI.scala` ~L346-353) converts Spark struct rows, maps and arrays and keeps nested nulls, which become JSON null.
  - `getValueOpt` (`CognitiveServiceBase.scala` L102-107) omits the whole field when the options column value is null.
  - Foundry's parameter list includes the options (L26-44).
  - The notebook (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb` L957-960) explains null members versus absent keys using a map-typed column.
- [x] **Breakpoint validation is shared and reaches the error column before any HTTP call.**
  - `validatePromptCacheBreakpoint` (`OpenAI.scala` ~L39-46) is called by Chat (L385) and Responses (L472).
  - Struct and map encoders drop null entries first (Chat L232-252; Responses L230-270).
  - Legacy map-based Chat content rejects any non-null breakpoint (L307-315), while content without breakpoints keeps its existing text and image handling.
  - Both stages catch `IllegalArgumentException` into the error column (Chat L543-552; Responses L537-546).
- [x] **Python and test scoping are correct.**
  - Python `Decimal` arrives as `java.math.BigDecimal` and stays that way (`JsonEncodableParam.scala` L59-81).
  - `setResponseSchema` checks the schema before changing any params (`HasOpenAIResponseSchema.scala` L31-35).
  - The unsafe-deserialization setting is enabled only around Chat/Responses fixture loads and restored in `finally` (`test_ServiceParamPythonBridge.py` L41-56). Prompt loads through the normal secure reader.
- [x] **Considered and dismissed as not regressions:**
  - **Fabric schema check with only a global deployment:** a direct Chat `transformSchema` on the default Fabric endpoint now checks Luna's status, because `getDeploymentName` doesn't read globals. It already failed before this PR, and `transform` checks the global deployment.
  - **Duplicate struct field names in options:** they collapse into one key, the same as the existing `encodeStruct`.
  - **Python getters for decimal options:** they return floats; this getter code (`Utils.py` L130-137) predates the PR.
  - **Fabric status refactor:** `assertModelStatus` matches statuses exactly as before; only guidance text was added.
- [ ] **Tests not run by this reviewer:** the review used read-only tools. The supplied compile, style, Scala, Python and loopback results are claims I reviewed but didn't reproduce, and no hosted-model run was checked.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "One complete six-lens precommit pass explicitly approved under cap twenty, with nineteen already consumed. Each invocation has a 40-minute bound. No budget reset or new CI permissions."
  },
  "changeSize": "Thirty-nine source/test/notebook paths in the complete staged PR diff, 146580 bytes. Production branching is small; regression fixtures and examples account for most of the breadth.",
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
  "reviewAttempt": 20,
  "stageAttempt": 1,
  "manifestSha256": "f880b4febff5c0040c405e730a9f874643e405db1d04cddda7d3cbc75d6f46b3",
  "diffSha256": "650dd8df5f9335e723470722cca8ca40d83e02ae8e167d7888614babcba6942d",
  "dispatchStatus": "starting_lens1_on_unchanged_locally_validated_freeze",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
