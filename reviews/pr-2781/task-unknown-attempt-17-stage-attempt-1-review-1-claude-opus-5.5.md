---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 1
timestamp: "2026-11-06T09:01:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "5594de4919321263cb7b1475ff67baa6bca0f51d17db800a4769cde28182d2db"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-17-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Writer and reader persistence.** File: `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`.
  - `paramsWithImplicitDefaults` is empty by default (line 21).
  - The writer leaves an opted-in parameter out of `paramMap` only when `!isSet` (lines 114–121). `defaultParamMap` is still written in full, and both maps are parsed with `useBigDecimalForDouble = true` (lines 124–129).
  - The reader keeps Spark's `parseMetadata` result and only swaps in precisely parsed `params`, `defaultParams` and `metadata` (lines 166–172) before `getAndSetParams`.
  - Class and uid handling are unchanged. No reader security guard is relaxed.
  - Re-rendering a `JDecimal` keeps a decimal point or exponent, so Spark's typed `jsonDecode` (`DoubleParam`, `DoubleArrayParam`) still gets doubles back.
- [x] **Opt-in scope.** Only `HasOpenAITextParams` overrides the opt-in set (`cognitive/.../openai/OpenAI.scala:176-177`).
  - Only four classes mix it in: Chat, Responses, Prompt and AIFoundry.
  - `apiVersion` and output-column defaults are still written to `paramMap`, so they keep legacy explicit serialization.
- [x] **JSON values.** `AnyJsonFormat` in `core/.../param/UntypedArrayParam.scala`:
  - Scala and Java `BigDecimal` are written as exact `JsNumber` (lines 31–32). Null is written as `JsNull`, and `JsNull` is read back as null.
  - Numbers read back as `Int`, then `Long`, then `Double` only when the value round-trips as a double; otherwise they stay `BigDecimal` (lines 46–55). Values that came from doubles still load as `Double`.
- [x] **Fallback placement.** The fallback is set only on the three text stages.
  - Chat sets it under `internalServiceType == "openai"` (`OpenAIChatCompletion.scala:116-119`). That is a constant `def`, so it is safe to call during construction, and `AIFoundryChatCompletion.scala:56` returns `"foundry"`.
  - Responses sets it at `OpenAIResponses.scala:142` and Prompt at `OpenAIPrompt.scala:278`.
  - Embedding has no default (`OpenAIEmbedding.scala:82` checks `isSet`).
  - Foundry builds its URL from `models/chat/completions` and takes the payload model from `model`, so an inherited deployment value is ignored.
- [x] **Prompt alias guard** (`OpenAIPrompt.scala:667-694`).
  - The guard runs only when `model` is set, the deployment is not set, and the route is not Foundry.
  - `require` rejects legacy Azure Chat. v1 and Responses routes apply the alias. Global and column deployments become `isSet` after transfer, so they win over the alias.
  - Before this PR, legacy Chat with only a model had no deployment and failed, so the new guard does not break a working setup.
- [x] **Schema-only path** (`OpenAIPrompt.scala:750-756`). `transformSchema` resolves global parameters on `copy(ParamMap.empty)`.
  - `copy` (lines 49–62) keeps `postProcessingExplicitlySet`.
  - The copy has the same uid, so uid-derived column defaults match and the global parameter registry does not grow.
  - The original stage is never mutated. `transform` (lines 530–533) behaves as it did before.
- [x] **Cache options** (`OpenAI.scala:328-353, 466-472`).
  - The parameter is in all three `sharedTextParams` overrides, and Chat and Responses `getOptionalParams` delegate to `super` for it.
  - Row and Map values are converted recursively and null members are kept. A null whole column omits the field through `getValueOpt`.
- [x] **Breakpoint validation.**
  - The shared validator is at `OpenAI.scala:39-46`.
  - Chat calls it at `OpenAIChatCompletion.scala:385`, and Responses at `OpenAIResponses.scala:472`.
  - The allowed-field sets include the breakpoint field (Chat lines 348 and 356; Responses lines 421, 429 and 447).
  - Struct encoding drops null fields and turns nested rows into maps, so valid object markers pass and null markers never trigger a false rejection.
  - Map-backed Chat content with a breakpoint is rejected (lines 307–315). Without a breakpoint, text collapsing and image conversion work as before.
- [x] **Python bridge.**
  - `Wrappable.scala:177-196` generates no Python-side defaults for ServiceParams.
  - `Utils.py:172-194` sends a ServiceParam to Java only when it was set in Python. The Luna value therefore stays a JVM default, so the Prompt alias works from Python and copy keeps it as a default.
- [x] **Tests.** I read the relevant cases; I did not run them.
  - `OpenAIRuntimeDefaultsSuite.scala` covers four routes, explicit, column and global deployments, aliases, Foundry and Embedding, the legacy guard, global schema and pipeline handling, credential non-materialization, and fit, save and load. Each global change is reset in `finally`.
  - `test_ServiceParamPythonBridge.py:23-75` sets the unsafe-deserialization flag only for Chat and Responses and restores or unsets it in `finally`. Prompt loads with the normal secure reader.
- [x] **Docs and security.**
  - The cache section of `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` matches the code on breakpoint shapes, null versus absent keys, and `ttl` semantics.
  - No secrets were added: the notebook uses `find_secret` and the tests use synthetic keys. New error messages report locations only, not content.
- [ ] Tests were not executed; this is a read-only lens.
- [ ] I could not verify whether the pinned OpenAI SDK accepts the notebook's `prompt_cache_options=` keyword argument. This only matters if that cell is run.
- [ ] The supplied diff was cut off after the `OpenAIResponses` hunk. For the remaining paths I reviewed the checked-out post-change source, but I could not check it against the frozen diff hash.
- [ ] Remote CI, coverage and live hosted-model results are still pending, as the plan states.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "One additional complete six-lens precommit pass explicitly approved under cap seventeen, with sixteen already consumed. Each invocation has a 40-minute bound. No budget reset or new CI permissions."
  },
  "changeSize": "Thirty-five source/test/notebook paths in the complete staged PR diff, 137310 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
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
  "reviewAttempt": 17,
  "stageAttempt": 1,
  "manifestSha256": "b9ee51510ac4419c5c29862b0335c4a87d6e2a29b96c5204a4a2f558f80e2399",
  "diffSha256": "847285ed81d4ba8972190b83abde12828668df5c6c22f4141c56b244712cabde",
  "dispatchStatus": "starting_lens1_on_test_corrected_locally_validated_freeze",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
