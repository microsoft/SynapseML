---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 23
stage_attempt: 1
round: 1
timestamp: "2026-11-09T23:56:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0d4902ce7689aa766eda3630f8b6df7051562893c46adbab8995998264d66741"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-23-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence opt-in** (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`):
  - `paramsWithImplicitDefaults` is empty by default (L21).
  - `paramsToPersist` (L65-74) leaves out opted-in params only when they are unset. Both the complex-param locations (L59-66) and the regular metadata (L122-150) use it.
  - Explicitly set values are still saved, including a handler equal to the default.
  - The reader (L165-185) keeps precise parsing, Spark metadata validation and the explicit/default distinction.
  - Defaults that don't opt in keep the legacy `defaultParamMap` behavior.
- [x] **Composition and handler guard:**
  - `HasOpenAITextParams` adds `deploymentName` to the parent set (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` L177). `OpenAIServicesBase` adds `handler` (L567).
  - Resulting sets: Chat and Responses get {deploymentName, handler}. Prompt gets {deploymentName}. Embeddings get {handler}.
  - The constructor still builds the default handler (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala` L37).
  - Tests in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` (~L295-333) show two things. A safe reload leaves the handler unset. An explicit handler equal to the default still raises `SecurityException` when legacy deserialization is off.
- [x] **gpt-6-luna default scope:**
  - The constant is defined once as `private[openai]` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` L12.
  - It is applied only in three places:
    - `OpenAIChatCompletion.scala` L124-127, guarded by `internalServiceType == "openai"`. That is a `def`, so AI Foundry gets no default.
    - `OpenAIResponses.scala` L145-149.
    - `OpenAIPrompt.scala` L272-286.
  - There is no routing by modality, Embeddings are unchanged, and global values fill only unset params.
- [x] **Prompt schema-only check:**
  - `OpenAIPrompt.scala` L761-765 resolves globals on `copy(ParamMap.empty)`, so the original stage is not changed.
  - `getOpenAIChatService` (L678-707) copies params except local params and a no-op store (L674-675).
  - Model aliases work for v1 and Responses. Legacy Chat with only a model set is rejected unless `customUrlRoot` is set (L700-704).
- [x] **Store, seed and reasoning:**
  - Responses `getOptionalParams` (`OpenAIResponses.scala` L162-170) starts from `store -> false`.
  - A null storage-column value becomes `None` in `getValueOpt` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` L102-107), so store stays false.
  - Responses rejects inherited Chat controls, including a global seed. None of those controls has a default.
  - `mergeReasoning` (L191-203) requires `reasoning.effort` and `reasoningEffort` to agree. A null nested effort defers to `reasoningEffort` (`OpenAIPromptCacheSuite.scala` L352-356).
  - Foundry rejects `store` (`OpenAIChatCompletion.scala` L142-143).
- [x] **Finite-number checks:**
  - `normalizeCacheOptionValue` (`OpenAI.scala` L352-368) rejects Double and Float NaN and ±Inf with a message naming the parameter.
  - It runs in scalar `isValid` (`OpenAI.scala` L336-339; `HasOpenAIModelParams.scala` L74-77) and in column encoding (`OpenAI.scala` L484; `HasOpenAIModelParams.scala` L148-154).
  - Tests: `OpenAIPromptCacheSuite.scala` L426-477.
- [x] **Exact decimals:**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` writes Scala and Java BigDecimal exactly (L31-32).
  - When reading (L48-57), it converts only to Int, Long or an exactly representable Double. Other values stay BigDecimal.
  - Other unsupported types throw at `throwFailure` (L45).
  - Tests: `OpenAIPromptCacheSuite.scala` ~L502-536.
- [x] **Breakpoint shape:**
  - `validatePromptCacheBreakpoint` (`OpenAI.scala` L39-46) is called from Chat (`OpenAIChatCompletion.scala` L399) and Responses (`OpenAIResponses.scala` L472).
  - Chat rejects map-based content parts that carry a breakpoint (~L321-330).
  - Tests: ~L479-500.
- [x] **Usage and response schema:**
  - The `ResponsesDetailed` mapping is `private[openai]` (`UsageUtils.scala` L57). The public mappings are unchanged.
  - The `OpenAISchemas.scala` additions (L98-143) are new nullable fields only.
- [x] **Live tests:** `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` runs only with `SYNAPSEML_LIVE_CACHE_TESTS=1`. It sets the key after save/load (~L178-179) and logs no secrets.
- [x] **Coverage estimate:** Grepping the new feature symbols found 36 changed paths under `cognitive/src`, `core/src` and `docs/`. I read every product file among them.
- [ ] **No commands or tests run:** this was a read-only review, so the plan's local validation claims are not re-verified here.
- [ ] **Full diff not re-listed:** after context compaction, my copy of the supplied diff ends after the first `OpenAI.scala` hunk.
  - I could not re-list all 48 paths, compare base and head hunks, or confirm that the working tree is exactly `b51b396`.
  - The review relies on the patched source in the working tree. Some changed test files and notebooks were not re-read individually, for example the Langchain, PDF and Forms quickstarts.
- [ ] **Live, CI and hosted qualification:** not assessed. These need credentials and are outside this review's scope.

## Issues

### Issue 1: NaN and Infinity inside `prompt_cache_breakpoint` objects are sent as JSON `null` without an error
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**:
  - cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala L39-46
  - cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala L268-283 (values passed through unchanged at L281)
  - cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala L272-285 (L282)
  - core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala L29-30
- **Description**:
  - What the code shows:
    - `validatePromptCacheBreakpoint` only checks that the marker is an object.
    - The content-part encoders pass individual values through unchanged.
    - `AnyJsonFormat` writes Double and Float values with spray-json `toJson`. Spray-json turns NaN and ±Infinity into `JsNull`.
  - Effect: a breakpoint field such as `ttl` that comes from a DoubleType or FloatType struct field, or from a `MapType(StringType, DoubleType)` value, is sent as `{"ttl": null}` with no error.
  - The PR rejects these same values in `promptCacheOptions` and `reasoning` (`normalizeCacheOptionValue`, OpenAI.scala L352-368). No test covers breakpoints; OpenAIPromptCacheSuite.scala L426-477 covers only options and reasoning.
  - Other content-part fields already turned NaN into null before this PR, so that behavior is not raised here.
- **Risk**:
  - This goes against the spec: reject unsupported non-JSON values, and do not silently coerce values.
  - The spec also treats an explicit null member as different from a missing key. Sending null therefore changes the request's meaning (for example, a null TTL) instead of failing, which makes the cause hard to find.
  - In practice this should be rare.
- **Suggested Fix**:
  - `normalizeCacheOptionValue` lives in `HasOpenAITextParams`, which `HasMessagesInput` can't reach. Move the finite-number check into a shared helper that both can use.
  - Apply it recursively to the breakpoint value inside `validatePromptCacheBreakpoint`, naming `prompt_cache_breakpoint` in the error.
  - Add Chat and Responses tests for Double and Float NaN and ±Inf in breakpoints built from structs and from maps.

### Issue 2: Chat now fails outright on `echo`, `cacheLevel` and `bestOf`, which it previously forwarded; this change isn't in the stated intended behavior
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala
- **Line(s)**:
  - cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala:
    - L141-150 (rejection at L145)
    - `sharedTextParams` L78-97 (echo L85, cacheLevel L87, bestOf L91)
    - `prepareEntity` L152-157
  - cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala L595-614 (request body built at L608 with no catch)
- **Description**:
  - What the code shows:
    - These params are still in Chat's `sharedTextParams`. Before this PR they were forwarded as `echo`, `cache_level` and `best_of`.
    - Chat `getOptionalParams` (outside Foundry) now throws `IllegalArgumentException` whenever any of them has a value.
    - The exception is thrown inside `prepareEntity` → `inputFunc`, which has no per-row catch. So it fails the Spark task instead of writing to `errorCol`.
    - Prompt in chat mode gets the same behavior through its copied params.
  - The change is intentional: OpenAIPromptCacheSuite.scala L343 tests it, and `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` L1110 documents it.
  - However, the intended-behavior spec lists rejections only for Responses, and it requires copy/save/load/resave behavior to be preserved.
- **Risk**:
  - Shown by the code: saved Chat or Prompt pipelines that set these params still load, but `transform` now fails the whole job instead of reporting errors per row.
  - Possible but unverified: OpenAI-compatible endpoints that accept or ignore these fields would stop working. Azure Chat may already have rejected them per row.
- **Suggested Fix**:
  - Confirm the intent. If kept, record it as a breaking change in the PR description and release notes, next to the documented rejection of the global seed in Responses.
  - Alternatively, apply the rejection only to Azure Chat endpoints, or report it as a per-row validation error so legacy and mixed pipelines fail per row, not as a whole job.

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
    "tier": "Trusted repository policy requires six independent lenses; parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Human explicitly renewed one full precommit pass, cap 23 with 22 consumed; 40-minute per-lens bound retained. No budget reset."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 264231 bytes.",
  "cyclomaticComplexity": "Low added branching, moderate interaction risk across shared service parameters, request encoding, schema validation, generated wrappers and constructor-recreated defaults.",
  "rationale": "Review the complete cache/default feature, including the pending persistence and request-validation repairs. Six independent complementary-model lenses cover the cross-cutting risk.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available current Anthropic Opus frontier model and supports max reasoning, complementary to this OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 6c2699defcfb24d5bff5ba55f2252ba2d8cbea24",
  "mode": "sequential",
  "reviewAttempt": 23,
  "stageAttempt": 1,
  "manifestSha256": "5d3d1bc2c1d9e85460ca82b1ad4d32861541eea2c3dc63ecd764fc376013799a",
  "diffSha256": "0ca40e07bc845ab9601ecfa86fd5ed9c8071c20bcd3bdca5583e93be13d4ed23",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver triage (2026-11-10)

The original review above is preserved. Request construction was exercised
against the exact rebuilt JARs used for this patch's local validation, with
unsafe Java deserialization disabled and no provider requests.

- **Issue 1: confirmed.** All 24 combinations of Chat or Responses, Double or
  Float, struct-backed or map-backed breakpoint objects, and NaN or either
  infinity silently produced `{"ttl": null}`. Object-shape validation alone
  does not prevent non-JSON numeric values inside breakpoint objects.
  Recommended repair is recursive finite-number validation scoped to
  `prompt_cache_breakpoint`, retaining valid finite values and avoiding
  unrelated changes to shared JSON serialization.
- **Issue 2: confirmed compatibility behavior, intent decision pending.**
  Chat and Prompt Chat reject each of `echo`, `cacheLevel` and `bestOf`
  during request construction, before HTTP execution. Existing tests and the
  notebook explicitly describe fail-fast rejection, so this is not an
  accidental divergence from the implemented behavior. It is nevertheless
  a compatibility change from forwarding these fields, and the PR description
  only describes unsupported-parameter rejection generically. Retaining
  this behavior requires an explicit compatibility decision and prominent
  disclosure; restoring forwarding is the alternative.

The transport audit confirmed that the complete frozen diff was supplied,
the runtime reviewer was the requested model, and no earlier review feedback
was accessed. The reviewer nevertheless reported losing part of the diff
after context compaction. This result is therefore not complete full-patch
signoff, independently of the findings.

Pass 23 stopped after its first lens. All 23 authorized pre-commit attempts
are consumed. No source repair, later lens, commit, push or Azure trigger was
performed after this review. A repair and fresh complete review require an
explicit decision and renewed budget; earlier passing validation does not
clear the new finding.

## Driver resolution (2026-11-10)

The author authorized the repair, explicitly chose to retain the Chat
fail-fast compatibility change, and renewed one complete pre-commit pass.
The cap is now 24 with 23 attempts consumed; the separate final-review
allowance is unchanged. The original review and triage remain intact.

- **Issue 1: resolved.** `OpenAIJsonUtils.normalizeValue` shares the existing
  recursive finite-number checks between request options and breakpoint
  validation. Errors identify the content location and
  `prompt_cache_breakpoint`. Both protected normalization signatures remain
  available, and the shared JSON formatter is unchanged.
  Four named regressions cover both low-level and Prompt routes: 72
  non-finite cases spanning Double/Float, NaN/both infinities and
  struct/map/nested-array markers are rejected. Another 36 cases preserve
  finite maximum floating-point values and exact Decimal(38,18) values.
- **Issue 2: explicitly accepted compatibility change.** The author selected
  fail-fast rejection rather than legacy forwarding. The notebook now has a
  prominent breaking-change note covering saved stages, custom endpoints,
  Spark-task failure rather than `errorCol`, and removal of the unsupported
  settings. The PR description must carry the same prominent disclosure
  when the reviewed change is published; that publication step is pending.

The two non-finite regression tests failed before the production repair,
while the two finite-value tests passed. After repair, all-module production
and test compilation and Scala style checks passed. All 120 selected Scala
tests passed, followed by 28 Python bridge tests and 28 subtests against
freshly rebuilt isolated packages and JARs, plus seven local HTTP request
checks. No live retention or new hosted qualification is claimed.

Pass 23 remains an incomplete review with findings, not clean signoff. A
fresh full pass must assess the final frozen diff. Its context will include
the complete path manifest and explicit bounded-read/coverage instructions;
loss of necessary diff context remains a blocker.
