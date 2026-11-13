---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 26
stage_attempt: 1
round: 1
timestamp: "2026-11-10T06:10:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4a6a92af26cae3d9c2cf7ff423b64ea6db0505b47c94dfe2b92bd3d4393127f6"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-26-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence opt-in.**
  - In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `paramsToPersist` (lines 59–77) keeps a parameter unless it is opted in and also unset (`!implicitDefaults.contains(p.param) || instance.isSet(p.param)`). Both the complex-param locations (line 60) and the metadata `paramMap` (line 128) use it.
  - `defaultParamMap` is still written (lines 130–143). The reader parses decimals exactly (lines 165–182).
  - The opt-in set is composed through `super`: `HasOpenAITextParams` adds `deploymentName` and `OpenAIServicesBase` adds `handler` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`).
  - Explicit handlers stay persisted and guarded. `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` lines 21–71 expect `SecurityException`.
- [x] **Default deployment.**
  - The Chat constructor guard (`OpenAIChatCompletion.scala` lines 125–128) skips `gpt-6-luna` for Foundry because `AIFoundryChatCompletion.internalServiceType` is a `def` returning `"foundry"`.
  - Global parameters transfer only for unset parameters, so a global deployment overrides the default.
  - `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` lines 218–264 check that the default stays unset and that an explicit value equal to the default is persisted.
- [x] **Breakpoint validation.**
  - `OpenAI.scala` lines 42–58 rejects non-object markers and non-finite numbers, and wraps serializer failures (including non-string keys) with `<location>.prompt_cache_breakpoint`. `OpenAIJsonUtils` (lines 61–75) does the recursive normalization.
  - Validation runs on encoded messages: Chat `validateContentPart` (lines 392–408) and Responses (lines 464–481).
  - Invalid rows get a null `messages` value and are skipped. `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.scala` lines 138–145 keep the existing `errorCol`, so no HTTP request is sent.
  - Regression tests are at `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` lines 431–578.
- [x] **Cache options, reasoning and exact numbers.**
  - Scalar `isValid` hooks: `OpenAI.scala` lines 358–385 and `HasOpenAIModelParams.scala` line 75.
  - Column encoding passes parameter-specific names (`OpenAI.scala` lines 497–503; `HasOpenAIModelParams.scala` lines 147–153).
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`:
    - Lines 23–45 write Scala and Java `BigDecimal` exactly and reject non-string keys and unsupported values.
    - Lines 47–58 convert a number back to `Double` only when that is lossless (`isDecimalDouble`).
- [x] **Request controls.**
  - Chat `getOptionalParams` (`OpenAIChatCompletion.scala` lines 141–150):
    - Foundry rejects `store`.
    - Other endpoints reject `echo`, `cacheLevel` and `bestOf` (approved fail-fast).
    - `reasoning`, `include` and `truncation` are always rejected.
  - Responses (`OpenAIResponses.scala` lines 168–214):
    - It rejects inherited Chat controls, including `seed`.
    - It builds the request from `Map("store" -> false) ++ super`, so a null store column keeps `false`.
    - It requires the two reasoning-effort settings to agree.
    - It forwards `temperature` and `top_p` without the gpt-5 name filter (approved).
- [x] **Prompt.** In `OpenAIPrompt.scala`:
  - Lines 196–211 skip the no-op `store` setting.
  - Lines 684–708 keep model aliases and reject model-only selection on legacy Chat.
  - Lines 769–773 resolve globals in `transformSchema` on a copy.
  - Lines 352–390 choose the usage normalizer, including `ResponsesDetailed`.
  - Usage schemas in `OpenAISchemas.scala` lines 80–135 match `UsageUtils.scala`.
- [x] **Notebook disclosures** in `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`:
  - Line 308: default precedence and no modality routing.
  - Line 806: save before attaching credentials.
  - Line 976: explicit null versus absent members.
  - Line 1098: Chat compatibility break.
  - Line 1100: Responses sampling change.
  - Line 1116: Responses rejections, including a global seed.
- [x] **Security.**
  - The live test `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` runs only when `SYNAPSEML_LIVE_CACHE_TESTS=1` and sets credentials after save/load.
  - Searching the changed Python tests and notebooks found no hard-coded keys or printed secrets.
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` lines 12–14 read keys from the environment or `Secrets`.
- [ ] **Tests not executed by this reviewer.** Pass status comes only from the supplied validation evidence. Several Scala/Python suites were assessed by targeted search, not a full read.
- [ ] **Pre-existing versus new lines.** The read-only tools cannot diff against the merge base. `ignore("Custom EndPoint")` in `OpenAIChatCompletionSuite.scala:567` and `OpenAIPromptSuite.scala:510` look pre-existing and are not counted as findings.

## Issues

### Issue 1: Notebook `reasoning_effort` values contradict the parameter documentation
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`
- **Line(s)**: notebook 313; `OpenAI.scala` 466
- **Description**:
  - The new reasoning-model cell lists `none`, `low`, `medium`, `high`, `xhigh`, or `max` as if that were the complete set.
  - The `reasoningEffort` parameter documentation, which becomes the generated Python docstring, lists `'none','minimal','low','medium','high','xhigh'` "or any user string".
  - The two disagree on `minimal` and `max`. I could not confirm whether the Scala string predates this PR.
- **Risk**: Documentation only; there is no runtime effect. Users may choose an unsupported value and get per-row provider errors. A fixed list also blurs the line between user configuration and provider support, which the spec asks documentation to keep clear.
- **Suggested Fix**: Make the two lists match, or describe the notebook values as examples whose support depends on the deployment.

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
    "tier": "Trusted repository policy requires six independent lenses; parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Human requested the concrete repair and readiness directly after the explicit budget stop; bounded to one fresh pass26 with 25 consumed, 40 minutes per lens. Strict gates retained; no open-ended renewal."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 273091 bytes.",
  "cyclomaticComplexity": "Low added branching, moderate interaction risk across shared service parameters, request encoding, schema validation, generated wrappers and constructor-recreated defaults.",
  "rationale": "Review the complete cache/default feature, including the pending persistence repairs, JSON-compatible breakpoint validation, public-transform regressions and accepted compatibility disclosures.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available current Anthropic Opus frontier model and supports max reasoning, complementary to this OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 6c2699defcfb24d5bff5ba55f2252ba2d8cbea24",
  "mode": "sequential",
  "reviewAttempt": 26,
  "stageAttempt": 1,
  "manifestSha256": "d93ac5b26fe98fcb16a1496b03b702949fe3eeb616d80f3e05918b38a21b028b",
  "diffSha256": "fcb62692f8d74f0f27fb6c7bc4e890a19ed286140b6afd55b7a3c3119488cfee",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver Reconciliation Addendum - 2026-11-10

The original review and verdict above are preserved. The notebook's reasoning
guidance now explicitly says that effort strings are forwarded without a client
allowlist and that support is deployment-specific. The examples include
`minimal`; no example value is presented as universally supported. This resolves
the documentation clarity concern without narrowing accepted values.

This is not a clean qualification of attempt 26. The source patch changed and a
new complete pre-commit pass is required.
