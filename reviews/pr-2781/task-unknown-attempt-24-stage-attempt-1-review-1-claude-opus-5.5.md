---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 24
stage_attempt: 1
round: 1
timestamp: "2026-11-10T03:24:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "038d660fafa885d524341b2bed9bbdc7a4e37cf78f2cbf7e122273dfa7977d3c"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-24-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage.** I assessed all 48 manifest paths against the supplied diff before context compaction. I then re-checked the production code in the working tree with targeted reads. For test and notebook paths I used targeted reads and searches instead of re-reading every hunk line by line. Nothing was executed: no tests, builds or commands.
- [x] **Persistence opt-in.**
  - `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:69-77`: `paramsToPersist` keeps a param unless it has opted in and is unset. The same filter applies to the complex-param locations (`:59-67`) and the regular `paramMap` (`:129-130`).
  - `:131-132`: complex params are kept out of `defaultParamMap`. The non-complex `deploymentName` default is still written there. On load it is restored with `setDefault`, so `isSet` stays false and global deployment settings still apply.
  - The handler default comes from `setDefault` in `core/.../io/http/HTTPTransformer.scala:37`. `OpenAIServicesBase` opts that handler into the mechanism at `cognitive/.../openai/OpenAI.scala:572-574`.
  - Legacy behavior is covered by `core/src/test/.../serialize/ValidateComplexParamSerializer.scala:218-231` (explicit defaults stay persisted) and `core/src/test/.../stages/VerifyUDFTransformerPersistence.scala:21,58` (the deserialization guard stays on).
- [x] **Exact JSON numbers.**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:24-62` writes Scala and Java `BigDecimal` values exactly. On read it tries Int, then Long, then Double only when the value round-trips exactly, and otherwise keeps `BigDecimal`. Unsupported values throw "Cannot serialize".
  - Covered by `core/src/test/.../param/VerifyUntypedArrayParam.scala:37-46,115-139`.
- [x] **Non-finite numbers.**
  - `cognitive/.../openai/OpenAI.scala:51-65`: `normalizeValue` rejects Double and Float NaN and infinities, and recurses into Rows, Maps and Seqs.
  - The scalar `isValid` checks for `promptCacheOptions` (`OpenAI.scala:348-361`) and `reasoning` (`HasOpenAIModelParams.scala:69-80`) both use it. Column bindings are normalized in `getOptionalParams` (`OpenAI.scala:491`).
  - Tests: `OpenAIPromptCacheSuite.scala:431-482`.
- [x] **Breakpoints.**
  - `OpenAI.scala:40-48` validates breakpoints and puts the content location plus `prompt_cache_breakpoint` in the error.
  - Chat (`OpenAIChatCompletion.scala:392-408,424-447`) and Responses (`OpenAIResponses.scala:464-482`) call it.
  - Exact finite values are tested at `OpenAIPromptCacheSuite.scala:514-537`.
- [x] **Store, controls and aliases.**
  - Prompt Chat and Foundry skip a scalar `store=false` (`OpenAIPrompt.scala:674-675`).
  - Responses defaults `store` to false and rejects unsupported controls (`OpenAIResponses.scala:145-149,170-178`). Reasoning aliases must agree (`OpenAIResponses.scala:203-216`).
  - Chat rejects echo, bestOf and cacheLevel (`OpenAIChatCompletion.scala:141-150`), disclosed in `docs/Explore Algorithms/OpenAI/OpenAI.ipynb:1098`.
  - Prompt rejects model-only selection on legacy Azure Chat endpoints (`OpenAIPrompt.scala:693-704`).
- [x] **Usage accounting.** Prompt uses the Detailed mappings (`OpenAIPrompt.scala:355-357`; `UsageUtils.scala:49-60`). `ResponsesDetailed` is `private[openai]` and its nested paths are resolved explicitly, so the public `normalize` still treats keys as literal field names.
- [x] **Live tests.** They only run with `SYNAPSEML_LIVE_CACHE_TESTS=1` (`cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py:26-27`). The credential is set only after save/load (`:177-179`), and the evidence records contain no keys (`:46-56`).
- [x] **Ruled out:**
  - A null-row failure in `getFilterReason`: the only caller checks for null first.
  - Field collisions from the schema extensions.
  - Defaults on rejected params.
  - `transformSchema` changing the original stage: it works on a copy.
- [ ] Runtime validation was not performed because this review was read-only. The removed gpt-5 behavior was not viewed directly, since no git history was available; it is inferred from `OpenAI.ipynb:1114` ("no longer silently removed") and the tests.

## Issues

### Issue 1: Notebook says non-finite breakpoint errors fail the Spark task, but `transform` returns them as per-row `errorCol` entries
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb (code: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala, OpenAIResponses.scala)
- **Line(s)**: notebook 802; OpenAIChatCompletion.scala 176-221, 392-399, 495-521, 557-566; OpenAIResponses.scala 356-372, 464-472, 520-542; OpenAIPrompt.scala 367-368
- **Description**:
  - The notebook says these errors "fail the Spark task; they are not provider errors returned in `errorCol`."
  - That is not what the public `transform` path does in Chat or Responses. `structuredMessageValidationError` runs `validateMessages`, which reaches `validateContentPart`, then `validatePromptCacheBreakpoint`, then the `require` in `normalizeValue`.
  - The resulting `IllegalArgumentException` is caught. The message goes to `errorCol`, and the messages value is set to null so no HTTP request is sent. `OpenAIResponsesMultimodalSuite.scala:300-348` shows the same contract for other validation errors.
  - Prompt goes through `service.transform`, so it behaves the same way. The same applies to the error for a breakpoint that is not an object.
  - Only the direct request-encoding hook throws. That hook is what `OpenAIPromptCacheSuite.scala:495-511` (`payload`) tests.
- **Risk**: Users expecting a task failure get null outputs with an error message in `errorCol` instead. The documented behavior does not match the code and has no test.
- **Suggested Fix**: Change the sentence to say these are local per-row validation errors reported in `errorCol`, and no request is sent. Add Chat and Responses `transform` tests that check the message (location, `prompt_cache_breakpoint`, "finite") and that the handler is never called.

### Issue 2: Removing the gpt-5 temperature/top_p filter is a compatibility change outside the approved list
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala; docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: OpenAICoreOfflineSuite.scala 119-139; OpenAI.ipynb 1114
- **Description**:
  - Explicit or global `temperature` and `top_p` are now always sent. Previously they were silently dropped when the deployment name contained "gpt-5". No gpt-5 handling remains in `cognitive/src/main`.
  - The spec approves only the echo/bestOf/cacheLevel change, which also requires prominent PR-description disclosure. This change is mentioned in the notebook but not labeled as a behavior change.
- **Risk**: gpt-5 pipelines using `OpenAIDefaults.setTemperature` or `setTopP`, or the scalar settings, may start getting per-row provider errors. This depends on the provider and was not demonstrated against a live endpoint.
- **Suggested Fix**: Get explicit approval for the change. Then disclose it prominently in the PR description next to the echo/bestOf/cacheLevel change, and label the notebook paragraph as a behavior change. Otherwise, restore the filter.

### Issue 3: Non-JSON scalar values in promptCacheOptions/reasoning are accepted at set time and fail later with a generic error
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala; HasOpenAIModelParams.scala; core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala
- **Line(s)**: OpenAI.scala 51-65, 355-357; HasOpenAIModelParams.scala 69-80; UntypedArrayParam.scala 17-45
- **Description**:
  - `isValid` only checks that numbers are finite. Dates, timestamps, byte arrays, `BigInt`/`BigInteger` and maps with non-string keys pass the setter and `copy`.
  - They fail later, at `save` or at request construction, with "Cannot serialize … of type …". That message does not name the parameter.
  - Nothing is silently coerced, and this is not a regression. However, `BigInteger` is valid JSON and is not listed as unsupported in the parameter documentation.
- **Risk**: Failures surface late, either at save or as a mid-job task failure, and the diagnostics are weaker than the parameter-specific errors for non-finite numbers.
- **Suggested Fix**: In the scalar `isValid`, also try encoding the normalized value with `AnyJsonFormat.write` and wrap any error with the parameter name. Optionally, write `BigInt`/`BigInteger` as an exact JSON number.

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
    "tier": "Trusted repository policy requires six independent lenses; parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Human explicitly renewed one full precommit pass, cap 24 with 23 consumed; 40-minute per-lens bound retained. No budget reset."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 268303 bytes.",
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
  "reviewAttempt": 24,
  "stageAttempt": 1,
  "manifestSha256": "5f3c7bb4c291a1c5684f6c8a79a71564da7bede5f032d766f5a6d0183cd5d4fa",
  "diffSha256": "c68b4bf403c01ea11f1bd55dcbb55b5b6b7c3a9be1a063c28447f9ed4aa232b4",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver triage after round 1

The original findings and resolution placeholders above are preserved. This pass
stopped after round 1; the other five lenses have not run. The reviewer reports
assessing all 48 manifest paths before compaction. The transport audit confirms
the exact frozen diff, the requested runtime model, read-only tools, and no
access to earlier review feedback. This is not a clean precommit signoff.

### Issue 1: Confirmed documentation and regression-coverage defect

Four public `transform(...).collect()` probes used the exact JARs from the
current local validation: Chat and Responses, each with a non-finite numeric
breakpoint and a non-object breakpoint. All four Spark actions completed, with
null output and a local per-row `errorCol.response` naming
`messages[0].content[0].prompt_cache_breakpoint`. The original messages were
preserved and a counting local HTTP server observed zero requests.

The notebook's Spark-task-failure statement is therefore incorrect. The proposed
repair is to document local per-row validation errors and add public-transform
regressions with a zero-request assertion. No product source or notebook was
changed during triage. The direct encoding-hook tests do not establish the public
transform error contract. No public Prompt breakpoint claim is made by this probe:
the attempted direct-message Prompt setup lacked its required prompt template,
and ordinary Prompt templates do not insert explicit content breakpoints.

### Issue 2: Existing intentional behavior; explicit compatibility decision pending

The base-to-patch diff confirms removal of `dropSamplingForGpt5` from Responses.
This is not a newly introduced change in the current numeric-validation repair:
the published PR description already explains that Responses no longer silently
drops `temperature` / `top_p` based on a model-name heuristic, and
`OpenAICoreOfflineSuite` explicitly tests forwarding them for `gpt-5-mini`.
Nevertheless, affected deployments can now reject settings they previously
ignored. The driver will obtain an explicit decision before publication and
prominently label the compatibility effect if forwarding is retained. The
separate Chat echo/bestOf/cacheLevel approval does not by itself approve this
sampling behavior change.

### Issue 3: Non-blocking diagnostics enhancement, not silent serialization

`promptCacheOptions` documents that dates, timestamps, binary values, and
non-string object keys are unsupported. The scalar validator promises early
rejection of non-finite numbers, not acceptance of arbitrary runtime objects or
set-time validation of every unsupported type. `AnyJsonFormat.write` explicitly
rejects unsupported values and non-string map keys instead of converting or
transmitting them. BigInt/BigInteger support and parameter-named early failures
would be useful additional behavior, but are not required to fix the verified
numeric-corruption defect. No acceptance expansion or serialization-policy
change is planned here. The weaker diagnostics remain visible as an enhancement
request rather than being reported as fixed.

### Gate status

Precommit review budget is exhausted at 24/24. No further reviewer has been
launched, no new commit has been published, and the one authorized guarded CI
trigger remains unused. A repair and a fresh complete precommit pass require
explicit renewed authority; the distinct final CI-qualified review remains
pending.

## Resolution after authorized follow-up

The author authorized correcting the documentation, adding public-transform
regressions, retaining Responses sampling forwarding with a prominent
compatibility disclosure, and one fresh complete precommit pass. This does not
change the verdict or retroactively complete the remaining lenses of pass 24.

- **Issue 1 resolved:** `OpenAI.ipynb` now describes local per-row breakpoint
  errors and null output values, with no request sent for invalid rows.
  `OpenAIPromptCacheSuite` adds one public-transform test per API, covering twelve
  Double/Float NaN and infinity cases in nested breakpoint objects. Both tests
  assert the content-path diagnostic, null output and zero handler calls.
  Existing scalar-breakpoint tests already cover non-object markers through the
  public transform path.
- **Issue 2 resolved by explicit compatibility acceptance:** the author selected
  forwarding, not restoration of the gpt-5 name filter. The notebook now
  prominently labels the Responses sampling behavior change beside the Chat
  compatibility warning, including possible provider errors and clearing
  incompatible stage/global settings. Matching PR-description disclosure is a
  publication prerequisite.
- **Issue 3 deferred as a non-blocking diagnostics enhancement:** unsupported
  objects still fail explicitly during serialization. This follow-up does not
  add BigInt/BigInteger support or promise setter-time rejection for every
  unsupported runtime type.

Cognitive test compilation and test Scala style passed. All 49 tests in
`OpenAIPromptCacheSuite` and `OpenAICoreOfflineSuite` passed with no failures,
cancellations, ignored or pending tests. Production sources remain byte-identical
to the previously validated patch; the prior 120 Scala tests, 28 Python tests
plus 28 subtests, and seven local HTTP request checks are retained evidence, not
claimed as new runs. Notebook cell sources remain arrays of strings.
