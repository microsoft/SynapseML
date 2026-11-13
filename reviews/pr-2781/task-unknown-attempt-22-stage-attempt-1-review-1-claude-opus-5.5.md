---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 22
stage_attempt: 1
round: 1
timestamp: "2026-11-09T22:30:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e21ac5f4b548bf93eb0eddd6112d5ff67fbb104819ce145fb4623fb179590acb"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-22-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence opt-in matches the spec.**
  - In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `paramsWithImplicitDefaults` is `Set.empty` by default (line 21).
  - `paramsToPersist` (lines 69–76) filters both the complex-param locations (line 60) and the regular `paramMap` metadata (line 128).
  - Lines 133, 136 and 175 parse with `useBigDecimalForDouble = true`.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` adds `deploymentName` to the set through `super` (line 177), and `OpenAIServicesBase` adds `handler` (line 564).
  - Explicit handlers are still written, so the existing Java-deserialization guard still applies to them.
- [x] **Defaults never override explicit or global selection.**
  - `getValueOpt` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` (lines 102–107) reads the set value, then the default.
  - Global transfer fills only unset params, so the `gpt-6-luna` defaults never block a global value. The defaults are at `OpenAIPrompt.scala` line 281 and `OpenAIResponses.scala` lines 143–147; Chat sets one only when `internalServiceType == "openai"`, which excludes Foundry.
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` covers scalar, column, global, alias, Foundry and embedding cases.
- [x] **Schema-only validation does not mutate the stage.** `OpenAIPrompt.scala` lines 761–762 resolve globals on `copy(ParamMap.empty)`, leaving the original stage unchanged.
- [x] **Store and unsupported-control handling.**
  - `skipNoOpStore` (`OpenAIPrompt.scala` lines 674–675, used at line 692) drops only a no-op scalar false, and only for Chat or a Foundry model.
  - Responses keeps `store -> Left(false)`.
  - Responses rejects inherited Chat controls, including `seed` (`OpenAIResponses.scala` lines 169–176).
- [x] **Cache options and breakpoints.**
  - Non-object `prompt_cache_breakpoint` markers are rejected (`OpenAI.scala` lines 39–46).
  - Non-finite numbers are rejected at set time (lines 329–342) and during column encoding (lines 352–365).
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` lines 408–431 cover Double and Float NaN and ±Inf, for both a scalar and a nested column.
- [x] **BigDecimal exactness.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` writes Scala and Java BigDecimal exactly (lines 31–32). On read (lines 50–55), it converts to Double only when `isDecimalDouble`.
- [x] **Usage accounting.** `ChatCompletionsDetailed` and `ResponsesDetailed` only add fields; the public ChatCompletions and Responses mappings are unchanged. Foundry inherits the Chat response schema, so the detailed fields resolve.
- [x] **Candidates checked and dropped:**
  - The `getFilterReason` call is guarded (`OpenAIPrompt.scala` lines 310–319).
  - The schema extensions add no duplicate fields.
  - Chat's rejection of echo, cacheLevel and bestOf, and Responses' rejection of a global seed, are intentional and documented.
  - A failed column-encoding check aborts the task through `inputFunc` (`CognitiveServiceBase.scala` lines 595–613). The exception path existed before this PR, and the spec requires rejecting bad values during column encoding. The notebook's error-column statement (line 859) refers only to provider errors.
- [ ] Tests, builds and commands were not run (read-only review). The validation results in the prompt were not reproduced.
- [ ] I did not confirm by command that the working tree matches published head `b51b396704a8c145267775e7857fbb6ffa569375`; findings cite the source on disk. Coverage focused on the Scala production paths and their tests. Generated Python wrapper output and every notebook cell were not re-read in full.

## Issues

### Issue 1: Prompt hides the response ID for null `store` rows that Responses may still retain
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 207–210 and 373–393. Related code: `OpenAIResponses.scala` lines 107–124 and 143–147, and `CognitiveServiceBase.scala` lines 102–107. Related test: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` lines 137–156, especially 142 and 150–153.
- **Description**:
  - With `setStoreCol(...)`, the column binding replaces the stage's `store -> Left(false)` default.
  - For a row whose store value is null, `getValueOpt` returns `None`, so the Responses request omits `store`.
  - Prompt then builds the response ID with `F.when(responseCol.isNotNull && F.col(storeFlagCol), ...)`. A null flag makes the condition null, so `responseIdCol` is null.
  - The PR's own test asserts both results for `api = "responses"`: the request's `store` is `None` for the "unset" row, and that row's ID is null.
  - The public OpenAI Responses API documents `store` as defaulting to true. This is an external fact; the Azure behavior was not verified here. So an omitted `store` likely means the provider keeps the response while Prompt treats the row as unstored and withholds its ID.
  - This contradicts the notebook's claim that "Prompt exposes the ID in `responseIdCol` for stored rows" (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb` line 1106), and the intent that Responses retains store=false as its default.
  - Chat is not affected, because Chat Completions defaults `store` to false.
- **Risk**: Responses may be kept for rows the user believes are not stored. The user also loses the ID needed to retrieve or delete them, which matters for privacy and retention compliance.
- **Suggested Fix**: Pick one consistent rule for null Responses rows. The preferred option is to send `store: false` when the column value is null, so the request and the ID gating agree. If null is meant to defer to the provider, make Prompt treat a null flag as "possibly stored" for Responses (for example, coalesce the flag to true) and expose the ID. Then document the rule and update the test at lines 137–156 to assert it.

### Issue 2: `reasoning` accepts non-finite numbers that become `null` after save and load
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala
- **Line(s)**: 70–79 (no `isValid`) and 144–150 (normalization at request time). Related code: `OpenAI.scala` lines 352–358 (hard-coded error message) and `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` lines 29–30.
- **Description**:
  - `promptCacheOptions` checks for finite numbers at set time (`OpenAI.scala` lines 336–339). The new `ServiceParam[Map[String, Any]]` named `reasoning` has no such check, so `setReasoning(Map("x" -> Double.NaN))` succeeds.
  - Before a save, the request fails in `getOptionalParams` through `normalizeCacheOptionValue`, with the misleading message "promptCacheOptions numbers must be finite."
  - On save, `AnyJsonFormat` writes Double and Float with `toJson`. spray-json turns NaN and Infinity into `JsNull`. The reloaded stage therefore holds `null` and sends it without any error.
  - The regression test at `OpenAIPromptCacheSuite.scala` lines 408–431 covers only `promptCacheOptions`.
- **Risk**: A value changes silently across save and load, and the error names the wrong parameter. Likelihood is low, because reasoning values are normally strings.
- **Suggested Fix**: Apply the same finite-number `isValid` to `reasoning` and any other `Map[String, Any]` model param. Include the param name in the error message. Add scalar and column-path tests.

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
    "budget": "Human explicitly renewed one full precommit pass, cap 22 with 21 consumed; 40-minute per-lens bound retained. No budget reset."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 260583 bytes.",
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
  "reviewAttempt": 22,
  "stageAttempt": 1,
  "manifestSha256": "62f8d4304c91bd630d6cb7382b365c0b79cd0b8777e7c62c00acd28136f0c64b",
  "diffSha256": "c49ae3a75a53141cab98ac01be8d97cac0a6cf5ba564da55cd68b74c588d8982",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver triage (2026-11-09)

The original review above is preserved. Both findings were reproduced against
the exact rebuilt JARs used for the local validation, without provider requests:

- **Issue 1: confirmed request/ID inconsistency.** Both low-level Responses
  and Prompt Responses omit `store` for a null column value. The existing
  multimodal test also explicitly expects a null response ID for that row.
  The official [Responses create reference](https://developers.openai.com/api/reference/resources/responses/methods/create)
  documents `store` as defaulting to true when omitted. This establishes a
  retention risk; actual Azure-side retention was not tested. Recommended
  behavior is to send `store=false` for null Responses column values, retaining
  the library's no-storage default and existing response-ID gating.
- **Issue 2: confirmed persistence inconsistency.** NaN and both infinities
  are accepted inside nested `reasoning` maps. Request construction before
  save fails with the cache-options error message. Saving and reloading with
  unsafe Java deserialization disabled converts each value to null and allows
  request construction. Recommended repair is set-time validation for reasoning
  and parameter-specific diagnostics during scalar and column normalization,
  without altering the unrelated shared JSON formatter.

The API references were checked directly. The Chat create reference still
lists a false default; a migration-guide statement about new-account storage
differs. No inference about a particular account's retention policy is made.

Pass 22 stopped after its first lens. The 22 authorized pre-commit attempts
are consumed; no subsequent lens, source repair, commit, push or Azure trigger
has been performed following these findings. A new complete review after any
repair requires explicit renewal, not a reset of previous attempts.

## Driver resolution (2026-11-09)

The original review and triage above remain unchanged. The author authorized
both repairs and one fresh complete pre-commit pass, with 22 attempts already
consumed and a new cap of 23. The separate final-review cap is unchanged.

- **Issue 1: resolved.** `OpenAIResponses.getOptionalParams` starts with
  `store=false` and then applies resolved row parameters. Explicit true and
  false still win; null storage-column values retain the no-storage default.
  This applies to both low-level Responses and Prompt Responses. The parameter
  documentation and notebook explain the null-column behavior. Regression
  coverage checks both routes, all three flag values, output-column collisions
  and unchanged null response-ID gating. Chat null-column behavior is unchanged.
- **Issue 2: resolved.** The `reasoning` parameter now validates scalar JSON
  values before persistence. Shared request normalization reports the actual
  parameter name and rejects nested non-finite numbers on column paths.
  Tests cover Double and Float NaN and both infinities on both routes.
  Finite maximum floating-point values, exact BigDecimal values beyond the
  Double range and explicit nulls retain copy/save/load coverage. The unrelated
  shared JSON formatter was not changed.

The new null-storage and non-finite-reasoning regressions failed before the
repair. After the repair, all-module production/test compilation and Scala
style checks passed, followed by 4 core persistence tests and 112 cognitive
cache, runtime-default, v1 and multimodal request tests, with no failures or
skips. Freshly rebuilt isolated Python wrappers and JARs passed 28 bridge tests
and 28 subtests, plus seven local HTTP request checks. Unsafe Java
deserialization remains disabled in the deployment roundtrip cases.

These are local results for the pending patch, not current-head CI or hosted
retention qualification. Earlier live and benchmark evidence remains
historical. Pass 22 is not resumed or relabeled clean; the repaired patch
requires the newly authorized complete pass 23.
