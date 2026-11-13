---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-11-06T09:56:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ac0a97b3293502088a790222adeef150ff2e835c34b47af51ab2ebe711af8c3c"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-18-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence writer and reader.** In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`:
  - The writer (lines 114-129) drops an opted-in parameter only when `!isSet`.
  - Every other parameter in `extractParamMap()` and all of `defaultParamMap` is still written.
  - Both are encoded with `useBigDecimalForDouble = true`.
  - The reader (lines 164-176) keeps Spark's `parseMetadata` and `getAndSetParams`, and only swaps in a precise parse of `paramMap`, `defaultParamMap` and `metadata`. No reader guard was relaxed.
- [x] **Where the default applies.**
  - `OpenAIChatCompletion.scala` lines 116-119 set the default only when `internalServiceType == "openai"`. `AIFoundryChatCompletion.scala` line 56 overrides that `def` to `"foundry"`, so Foundry gets no default and its URL (lines 62-64) never uses the deployment.
  - `transferGlobalParamsToParamMap` only fills unset parameters, so a global deployment still takes precedence over the fallback.
- [x] **Cache options.** In `OpenAI.scala`, `promptCacheOptions`, `normalizeCacheOptionValue` and `getOptionalParams` cover Chat, Responses and Foundry.
  - A whole-column null is omitted because `getValueOpt` returns None.
  - Null members are written as JSON null by `AnyJsonFormat` (`UntypedArrayParam.scala` line 24).
- [x] **Breakpoint validation.** Chat drops null map entries before rejecting a breakpoint in map-backed content (`OpenAIChatCompletion.scala` lines 297-315).
  - Struct-backed parts go through `validatePromptCacheBreakpoint`.
  - Validation runs in a UDF that writes to errorCol and nulls the messages, so `shouldSkip` prevents the HTTP call (lines 162-207 and 543-552).
- [x] **Prompt guard and schema.** The guard is at `OpenAIPrompt.scala` lines 687-694. `transformSchema` at lines 750-754 works on a `copy(ParamMap.empty)`, so global values are never written onto the original stage.
- [x] **BigDecimal consumers.** I checked who relies on the new BigDecimal handling in `UntypedArrayParam.scala` (write at lines 31-32, read at line 55):
  - `HasOpenAIResponseSchema.scala` line 34 validates schemas through `anyFormat.write`.
  - `VerifyUntypedArrayParam.scala` lines 149-158 and `OpenAIResponseSchemaSuite.scala` lines 132-154 still expect BigDecimal to be rejected.
- [x] **Python bridge test.** `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` turns on the unsafe-deserialization flag only around Chat/Responses fixture loads and restores it in `finally`. Prompt uses the normal secure reader.
- [x] **Considered but not reported:**
  - The gpt-5-only sampling-parameter drop in Responses predates this PR, and the notebook already tells users to leave sampling parameters unset.
  - The notebook's `max` reasoning-effort value is allowed because the parameter accepts any string.
- [ ] **No tests run.** This lens was read-only. I relied on the supplied validation evidence, which does not name `VerifyUntypedArrayParam` or `OpenAIResponseSchemaSuite`. Remote CI is pending.

## Issues

### Issue 1: Two existing tests still expect BigDecimal to be rejected and will fail
- **Severity**: High
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`; `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala`
- **Line(s)**: `UntypedArrayParam.scala` 31-32; `VerifyUntypedArrayParam.scala` 149-158; `OpenAIResponseSchemaSuite.scala` 148-153
- **Description**: `AnyJsonFormat.write` now writes Scala and Java BigDecimal as an exact `JsNumber`. Two existing tests still expect the old behavior:
  - **"AnyJsonFormat throws on unsupported type"** expects `BigDecimal("0.12345678901234567890123456789").toJson` to throw `IllegalArgumentException`. The new `case v: BigDecimal => JsNumber(v)` cannot throw, so this test fails.
  - **"schema-only setters reject invalid input without changing a configured response format"** expects `setResponseSchema(Map("maximum" -> BigDecimal(...)))` to throw. `setResponseSchema` validates through `AnyJsonFormat.anyFormat.write` (`HasOpenAIResponseSchema.scala` line 34), so the call now succeeds. It also changes `responseFormat`, which breaks the check at line 153 as well.
- **Risk**: The core and cognitive unit-test jobs will fail every time, blocking the full build. The public `setResponseSchema` behavior also changes: it now accepts Scala/Java BigDecimal and Python `Decimal`. No test or documentation covers that change.
- **Suggested Fix**:
  1. Keep BigInt and custom objects in the "unsupported" lists.
  2. Turn the BigDecimal and `java.math.BigDecimal` cases into positive checks that the exact JSON number is written.
  3. In the schema-setter test, check that the `maximum` value keeps its exact number.
  4. Run:
     - `sbt "core/testOnly com.microsoft.azure.synapse.ml.param.VerifyUntypedArrayParam"`
     - `sbt "cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIResponseSchemaSuite"`

### Issue 2: The legacy-Chat model guard breaks Prompts that use `customUrlRoot`
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: 687-694 (the guard now also runs during schema validation via lines 750-754)
- **Description**: Take a Prompt that uses chat_completions on a non-v1 URL, has `customUrlRoot` set and no deployment, and has `model` defined. The model may be set explicitly or come from a global `OpenAIDefaults.setModel`.
  - Before this PR, the alias branch was skipped without error, and requests went out. `CognitiveServiceBase.prepareUrl` (lines 483-484) returns the custom root without evaluating `prepareUrlRoot`. `withV1DeploymentModel` (`OpenAI.scala` lines 561-567) only reads the deployment on v1 URLs.
  - Now the `require` throws during `transformSchema`, `Pipeline.fit` and `transform`, even though no deployment value would be used.
- **Risk**: A narrow regression. Pipelines that send requests through a custom URL (for example, a gateway) and also have a model configured, often through a session-wide global meant for Foundry stages, now fail.
- **Suggested Fix**: Skip the guard when `get(customUrlRoot).nonEmpty`. Alternatively, document the behavior change. Either way, add a regression test for `customUrlRoot` plus model on chat_completions.

### Issue 3: Some saved decimal values change after save/load
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`
- **Line(s)**: 55 (with the Double write at line 30)
- **Description**: After the precise metadata parse, `promptCacheOptions` values are decoded through `AnyJsonFormat.read`. That method turns any number matching `isExactDouble || isBinaryDouble` into a Double. Spray then writes a Double using its shortest decimal form (`Double.toString`), which is not the exact value for integers beyond Long range.
  - Example: `setPromptCacheOptions(Map("n" -> BigDecimal("9223372036854775808")))` (2^63) sends the exact number before saving.
  - After `save`/`load`, the payload carries `9.223372036854776E+18`, which is 9223372036854776000.
  - 2^64 behaves the same way.
  - The new test (`OpenAIPromptCacheSuite.scala` lines 118-132) uses a value that is not an exact double, so it does not cover this case.
- **Risk**: The spec promises exact BigDecimal values through save/load/resave. For this narrow set of values, the number silently changes after reload. The practical effect on cache options is small.
- **Suggested Fix**: Convert to Double only when `num.isDecimalDouble`, i.e. `BigDecimal.decimal(num.toDouble) == num`, and keep the BigDecimal otherwise. Add save/load tests for 2^63 and 2^64 with both Scala and Java BigDecimal.

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
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "One additional complete six-lens precommit pass explicitly approved under cap eighteen, with seventeen already consumed. Each invocation has a 40-minute bound. No budget reset or new CI permissions."
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
  "reviewAttempt": 18,
  "stageAttempt": 1,
  "manifestSha256": "b9ee51510ac4419c5c29862b0335c4a87d6e2a29b96c5204a4a2f558f80e2399",
  "diffSha256": "847285ed81d4ba8972190b83abde12828668df5c6c22f4141c56b244712cabde",
  "dispatchStatus": "starting_lens1_on_unchanged_locally_validated_freeze",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
