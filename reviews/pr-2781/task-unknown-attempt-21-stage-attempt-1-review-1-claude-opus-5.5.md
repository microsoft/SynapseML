---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 1
timestamp: "2026-11-09T20:50:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "eab760f40e7e05bad5ad71bb4c472ed513c369f2446d6586e985a856f94ccd71"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-21-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence writer.** In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`:
  - `paramsWithImplicitDefaults` is an empty set by default.
  - `paramsToPersist` drops an opted-in parameter only when it is unset. This applies to both the complex-param locations and the metadata writer.
  - The precise BigDecimal parse still goes through Spark metadata validation and `getAndSetParams`.
  - Legacy non-complex defaults are still written.
- [x] **Parameter composition.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` lines 176–177, `HasOpenAITextParams` adds `deploymentName` to the superclass set. `OpenAIServicesBase` adds `handler`. Linearization for Chat, Responses and Foundry was checked.
- [x] **Guard-on tests.** `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` lines 233–278 cover safe reload, resave, and `!isSet(handler)`. They also check that explicit or old-style handlers still raise `SecurityException`. The guard stays enabled.
- [x] **BigDecimal handling.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` lines 16–62:
  - It writes Scala and Java `BigDecimal` exactly.
  - It reads integers in this order: Int, then Long, then `isDecimalDouble`, then BigDecimal.
  - Tests: `OpenAIPromptCacheSuite.scala` lines 394–425 and `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` lines 75–89.
- [x] **Default deployment and model selection.**
  - The default deployment is limited to `internalServiceType == "openai"` (`OpenAIChatCompletion.scala` lines 124–127).
  - The Prompt default is at `OpenAIPrompt.scala` line 283.
  - Legacy Chat model-only selection is rejected unless `customUrlRoot` is set (`OpenAIPrompt.scala` lines 695–705), as the spec requires.
- [x] **`store` forwarding.** Traced through `OpenAIPrompt.scala` (lines 206–209, 283, 294–296, 674–693) and `OpenAIChatCompletion.scala` (lines 110–116, 141–149).
- [x] **Global `seed` path.** Traced through:
  - `OpenAIDefaults.scala` lines 63–73
  - `OpenAIResponses.scala` lines 169–177 and 202–213
  - `OpenAIPrompt.scala` lines 534–535
  - The `inputFunc` UDF in `CognitiveServiceBase.scala`
- [x] **Foundry usage and schema.** `AIFoundryChatCompletion` uses the Chat schema (`OpenAISchemas.scala` lines 109–117), so the `ChatCompletionsDetailed` mapping is safe. `ResponsesDetailed` remains package-private (`UsageUtils.scala` lines 49–60).
- [x] **Breakpoint validation.** `validatePromptCacheBreakpoint` in `OpenAI.scala` rejects non-object markers. `normalizeCacheOptionValue` (`OpenAI.scala` lines 347–354) keeps JSON types without coercing them.
- [x] **Python bridge and notebook.** Checked the unsafe-flag usage in `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` lines 218–265, and `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` lines 308–334, 467 and about 1101–1110.
- [ ] **No tests or commands were run.** This is a read-only review limited to view, grep and glob.
- [ ] **Diff truncated.** My copy of the supplied diff was cut off after `OpenAI.scala`. I reviewed the remaining paths against the checked-out source and could not confirm it byte-for-byte against the diff SHA-256.

## Issues

### Issue 1: OpenAIPrompt now sends `"store": false` on every default Chat Completions request
- **Severity**: Medium
- **File**:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`
- **Line(s)**:
  - `OpenAIPrompt.scala`: 283, 294–296, 674–675, 690–693
  - `OpenAIChatCompletion.scala`: 110–116, 148
- **Description**: This is a demonstrated code path.
  - OpenAIPrompt has the default `store -> Left(false)`, and `"store"` is not in `localParamNames`.
  - `getOpenAIChatService` copies `extractParamMap()`, defaults included, onto the child stage. It skips `store` only for the Foundry no-op (`skipNoOpFoundryStore`).
  - The PR adds a `store` ServiceParam to `OpenAIChatCompletion`, so `completion.hasParam("store")` is now true.
  - `getOptionalParams` then forwards `"store" -> false` whenever the value is defined.
  - Before this PR, Chat had no `store` parameter, so the default never reached the request.
  - The standalone `OpenAIChatCompletion` has no `store` default and leaves the field out. The two entry points now send different requests for the same configuration.
  - Existing tests cover only explicit `true`, column bindings and Foundry absence (`OpenAIPromptCacheSuite.scala` lines 74–108; `OpenAIPromptMultimodalRequestSuite.scala` lines 118–154). No test checks the default Prompt Chat request body.
- **Risk**: The request-body change for the most common Prompt path is demonstrated. Rejection by a provider is plausible but not demonstrated. Users who pin older Azure `apiVersion` values, or who route through strict OpenAI-compatible `customUrlRoot` gateways, may get "unrecognized argument" 400 errors on requests that worked before.
- **Suggested Fix**:
  - Extend the no-op skip to the Chat route whenever `store` resolves to scalar `false`, i.e. `!mayStoreResponses`.
  - Base the check on the value rather than on `isSet`, because the legacy writer persists defaults as set.
  - Add a test that the default Prompt Chat request body omits `store`, while explicit `true` and column bindings are still forwarded.

### Issue 2: A global `OpenAIDefaults.setSeed` now fails every Responses-backed job
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala`
- **Line(s)**:
  - `OpenAIResponses.scala`: 169–171 and 202–213
  - Related:
    - `OpenAIDefaults.scala` lines 63–73
    - The seed `GlobalKey` registration in `OpenAI.scala`, near line 412
    - `transferGlobalParamsToParamMap` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`
    - `OpenAIPrompt.scala` line 535
- **Description**: This is a demonstrated code path.
  - `getOptionalParams` now calls `rejectUnsupportedParams(..., seed, ...)`.
  - `seed` is a globally registered parameter, and `transferGlobalParamsToParamMap` copies unset globals onto the stage. A session-wide `OpenAIDefaults.setSeed(42)` therefore makes `seed` defined on every `OpenAIResponses` stage, and on Prompt stages using `apiType="responses"`.
  - Every row then throws `IllegalArgumentException` inside the request-building UDF. The Spark job fails instead of reporting a per-row error.
  - Before this PR, Responses silently ignored `seed`.
  - A related case behaves the same way. In `mergeReasoning`, a global `reasoningEffort` that conflicts with an explicitly set `reasoning.effort` raises an error instead of letting the explicit value win.
  - The notebook documents the rejection (around line 1110). Tests cover only an explicit seed (`OpenAIPromptCacheSuite.scala` lines 110–117).
- **Risk**: Workloads that set the global seed for Chat reproducibility and also run Responses stages fail hard after the upgrade. The same applies when combined with `OpenAIDefaults.setApiType("responses")`.
- **Suggested Fix**:
  - Reject only values set explicitly on the stage. Either record which parameters were explicitly set before the global transfer, or skip transferring globals for parameters a stage does not support. Let explicit `reasoning.effort` take precedence over a global `reasoningEffort`.
  - If failing is the intended contract, add a test for the global case and state the global behavior in the `OpenAIDefaults.setSeed` documentation.

### Issue 3: The Python reload test still enables unsafe Java deserialization for Chat and Responses
- **Severity**: Low
- **File**: `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py`
- **Line(s)**: 218–265
- **Description**:
  - `test_text_deployment_defaults_and_bindings_survive_copy_and_load` still turns on the unsafe-deserialization flag. Its comment says that "Only Chat/Responses need this".
  - After the persistence fix, stages using the default handler no longer write the handler UDF, so the flag should not be needed for default, scalar or column bindings.
  - The Scala guard-on tests exist, but this Python path cannot catch a regression in safe reload. The stale comment also contradicts the "guard remains enabled" contract.
- **Risk**: A Python-side regression in guard-on reload would go unnoticed. The test also suggests to readers that the unsafe flag is still required.
- **Suggested Fix**: Remove the flag for the default and binding cases and check that reload succeeds with the guard on. Keep the flag only in a deliberate explicit-handler case.

### Issue 4: The notebook still says BigDecimal schema values are rejected, which contradicts the new support
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 467
- **Description**: This contradiction is demonstrated.
  - The notebook says "Scala `BigInt` and `BigDecimal` values are not supported by the existing parameter serializer and are rejected".
  - The PR adds `BigDecimal` and `java.math.BigDecimal` writing (`UntypedArrayParam.scala` lines 32–33), documents exact support (`HasOpenAIResponseSchema.scala` lines 26 and 54), and tests `decimal.Decimal` (`test_OpenAIResponseSchema.py` lines 75–89).
  - Only `BigInt` is still rejected.
- **Risk**: The user documentation contradicts the API docstrings and the actual behavior, so users may avoid an exact-precision feature that works.
- **Suggested Fix**: Change the sentence to say that Scala/Java `BigDecimal` and Python `decimal.Decimal` are kept as exact JSON numbers, while `BigInt` is still unsupported.

### Issue 5: NaN and Infinity in `promptCacheOptions` silently become JSON `null`
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` (with `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`)
- **Line(s)**: `OpenAI.scala` 347–354; `UntypedArrayParam.scala` 30
- **Description**:
  - The serializer behavior is pre-existing; the PR makes it newly reachable.
  - `normalizeCacheOptionValue` passes `Double` values through unchanged. `AnyJsonFormat` writes them with `v.toJson`, and spray-json turns NaN and ±Infinity into `JsNull`.
  - A NaN in a scalar map or a struct-column field therefore becomes an explicit `null` member.
  - The spec says to reject non-JSON values rather than silently discarding them, and that an explicit `null` differs from an absent key.
- **Risk**: This is an edge case, and the impact on the provider is speculative. A NaN option value could change cache semantics silently instead of failing clearly.
- **Suggested Fix**: In `normalizeCacheOptionValue`, or in the breakpoint and option validation, reject non-finite `Double` and `Float` values with an `IllegalArgumentException` that names the field. Add one negative test.

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

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Trusted repository policy requires six independent lenses; moderate parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Human explicitly renewed one full precommit pass, cap 21 with 20 consumed; 40-minute per-lens bound retained. No budget reset."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 254493 bytes, with substantial test/example coverage.",
  "cyclomaticComplexity": "Low added branching, moderate interaction risk across shared service parameters, request encoding, schema validation, generated wrappers and constructor-recreated defaults.",
  "rationale": "Review the full cache/default feature, not only the six pending files. New persistence logic is narrowly opt-in and does not change the inference hot path. Six independent complementary-model lenses cover the cross-cutting risk without introducing a broader multi-model workflow.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available current Anthropic Opus frontier model and supports max reasoning, complementary to this OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 6c2699defcfb24d5bff5ba55f2252ba2d8cbea24",
  "mode": "sequential",
  "reviewAttempt": 21,
  "stageAttempt": 1,
  "manifestSha256": "eabd3e91760143dbe04b2c772a7166e59213e46f19047259012982402d55253b",
  "diffSha256": "6c9c3eecb96402650abf4196bb59720cc021927321eb9cf9cbc361c902a6dc94",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver triage (2026-11-09)

The original review above is preserved. No production fix or new review pass has
been performed in response to these findings yet.

- **Issue 1 — confirmed request-shape change; compatibility hardening warranted.**
  A local JVM probe against the rebuilt artifacts confirmed that default Prompt
  Chat requests contain `"store": false`, while low-level Chat omits the field.
  No HTTP request was made, so rejection by a particular provider is not claimed.
  Omit the no-op scalar false value on the Prompt Chat route, preserving actual
  storage requests, column bindings and the explicit Responses no-storage default.
- **Issue 2 — confirmed behavior, but the proposed silent-ignore change is not
  accepted.** The local JVM probe confirmed that a global seed reaches Responses
  and triggers the documented unsupported-parameter rejection. The feature's
  existing contract is explicit rejection rather than pretending unsupported
  controls took effect. Add coverage and clarify the global-seed documentation.
  Different reasoning-control parameters retain their explicit conflict check;
  no new cross-parameter precedence rule is inferred.
- **Issue 3 — accepted.** The Python bridge test still enables legacy Java
  deserialization and contains an obsolete explanation. Replace that override
  with guard-on coverage, restoring the caller's prior Spark configuration.
- **Issue 4 — accepted.** The notebook contradicts the implemented and tested
  exact-decimal support. Correct the documentation while retaining the BigInt
  limitation.
- **Issue 5 — accepted.** The local JVM probe confirmed that the shared JSON
  formatter maps NaN and both infinities to JSON null. Cache-option normalization
  currently passes these values unchanged. Reject non-finite numeric cache-option
  values explicitly with focused scalar and column regression coverage, without
  changing unrelated shared-serializer behavior.

The saved reviewer input and its actual user-message transport both contained
the complete 48-path diff (254,493 bytes). The review nevertheless reports
incomplete diff visibility and experienced context compaction; this result is
not counted as a clean or complete pre-commit qualification.

Pass 21 stopped after this lens. Any source fix invalidates its frozen input
and requires a newly authorized complete pass; the 21-pass allowance is not
silently renewed. Nothing has been committed or pushed, and the newly authorized
Azure CI trigger remains unused.

## Resolution after local validation (2026-11-09)

The original review and triage remain unchanged above. The following changes
were made after this pass stopped:

- Issue 1: Prompt now omits scalar `store=false` for Chat as well as Foundry,
  including copy/save/load, without changing Responses' no-storage default or
  explicit storage/column behavior. The new Chat regression failed before the
  fix and passes afterward.
- Issue 2: The unsupported global-seed rejection remains intentional. A new
  regression proves that Chat retains the global seed and Responses rejects
  it. The API documentation and notebook explain resetting the global seed
  and using per-stage Chat seeds in mixed-API sessions.
- Issue 3: All three Python text-stage deployment roundtrips now load with
  unsafe Java deserialization explicitly disabled and restore the caller's
  configuration in `finally`.
- Issue 4: The notebook now documents exact Scala/Java BigDecimal and Python
  decimal.Decimal support, retaining the unsupported BigInt limitation.
- Issue 5: Cache-option validation rejects non-finite Double/Float values at
  scalar assignment, before persistence, and recursively during column
  encoding. The new regression covers NaN and both infinities on all six
  request routes. Finite maximum floating-point values and a decimal beyond
  the Double range remain supported. The shared JSON formatter is unchanged.

All-module production/test compilation and Scala style checks passed.
Targeted Scala validation passed 82 tests. Freshly rebuilt isolated Python
packages and JARs passed 28 bridge tests, 28 subtests, and seven local HTTP
request checks. Python formatting and notebook source-array checks passed.
Earlier live-provider and benchmark evidence was not rerun or attributed to
these newly rebuilt artifacts.

The user authorized one fresh complete pre-commit pass, increasing the cap
from 21 to 22 without resetting consumed attempts. This resolution does not
retroactively make pass 21 clean. Commit, push, current-head CI and final
CI-qualified review are still pending at the time of this entry.
