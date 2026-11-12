---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 26
stage_attempt: 1
round: 3
timestamp: "2026-11-10T06:52:21Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "757389482cab1562c2192c3ca896dbb70d778675e2dc119baa9a69cd7c4fbf15"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-26-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage.** All 48 paths in the frozen manifest were assessed against the supplied diff for this lens:
  - 10 cognitive main Scala files
  - 3 core main Scala files
  - 17 cognitive Scala test files
  - 4 core Scala test files
  - 9 Python test files
  - 5 notebooks
- [x] **Context disclosure.** The context was compacted after the first full-diff pass. I re-checked every finding and every non-issue below by reading the current source in bounded ranges. After compaction I could no longer tell from the diff whether the Chat struct encoder is new or older code. Issue 2 therefore relies on which inputs can now reach that code, which the current source shows directly.
- [x] **Executor failure path.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala`, `inputFunc` (lines 595-613) calls `prepareEntity` without a try/catch. An exception from `getOptionalParams` therefore fails the whole Spark task. Requests run with `setConcurrency(getConcurrency)` (line 754). This is the basis for Issue 1.
- [x] **Per-row breakpoint validation.**
  - `validatePromptCacheBreakpoint` and `OpenAIJsonUtils.normalizeValue` throw `IllegalArgumentException` that names the location (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` lines 42-76).
  - Chat `transform` (`OpenAIChatCompletion.scala` lines 176-221) and `structuredMessageValidationError` (line 557) catch it, write `errorCol` and null the messages. `shouldSkip` (line 161) then sends no HTTP request. Responses uses the same flow (`transform` line 356, validation error line 533, `shouldSkip` line 218).
  - Timestamp, date and binary leaves pass through `normalizeValue` unchanged. They then fail in `anyFormat.write` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`), so they are handled per row.
- [x] **Default deployment and globals.**
  - `transferGlobalParamsToParamMap` only fills parameters where `!isSet` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` line 46). The new deployment default (`OpenAIResponses.scala` lines 145-149, plus the matching Chat default) therefore does not block a global deployment setting.
  - Foundry overrides `internalServiceType`, so it gets no deployment default.
- [x] **Persistence.**
  - `paramsWithImplicitDefaults` is empty by default (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` line 21).
  - `paramsToPersist` (lines 69-78) drops opted-in parameters only when they are unset. It feeds both the complex-param locations (line 60) and the metadata (lines 128-136).
  - The precise metadata parse is at line 175. `deploymentName` and `handler` opt in at `OpenAI.scala` lines 206 and 584.
  - An explicit handler, even one equal to the default, is still persisted.
- [x] **Responses `store` and rejected parameters.**
  - `store` is in the Responses `sharedTextParams` override (`OpenAIResponses.scala` lines 107-126).
  - `Map("store" -> false) ++ super...` (line 173) keeps `false` for unset values and null column values.
  - `rejectUnsupportedParams` (`HasOpenAIModelParams.scala` lines 156-159) checks `getValueOpt`, so null column values are not rejected.
  - None of the rejected parameters has a default, so stages with only defaults pass.
- [x] **Null safety.** `addRAIErrors` (`OpenAIPrompt.scala` line 310 onward) wraps the output row in `Option` before calling `isContentFiltered` or `getFilterReason`. The null path in Responses `getFilterReason` is therefore unreachable.
- [x] **Concurrency.**
  - `GlobalParams` accessors are `synchronized` (lines 20-36).
  - Each stage gets its own request-body cache (`@transient lazy`), and the PR adds no new shared mutable state.
  - Prompt `transformSchema` (`OpenAIPrompt.scala` line 761) resolves globals on a copy, so the original stage is not changed.
- [x] **Schema and usage.** `OpenAIResponseSchemas.extend` (`OpenAISchemas.scala`) adds fields at the end, so positional row converters stay compatible. Every key used by the ChatCompletionsDetailed and ResponsesDetailed mappings in `UsageUtils.scala` exists in the extended schemas.
- [x] **Live-test gating.** `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` runs only when `SYNAPSEML_LIVE_CACHE_TESTS == "1"` (lines 26-27). It raises `SkipTest` when credentials are missing (lines 34-36). Foundry and retention tests have their own gates (lines 318-319 and 370-371).
- [x] **Considered but not reported.**
  - A `Row` without a schema inside a scalar `promptCacheOptions` or `reasoning` value makes `normalizeValue` (`OpenAI.scala` line 70) throw a NullPointerException. The contract explicitly excludes earlier parameter-named errors for other unsupported objects, and rows that come from a DataFrame carry a schema.
  - Approved or already documented: the Chat `echo`/`bestOf`/`cacheLevel` fail-fast, the Responses global-seed rejection and Responses `temperature`/`top_p` forwarding.
  - Older behavior: NaN in a response schema becomes null in `AnyJsonFormat`.
- [x] No commands or tests were run for this review. The validation results in the prompt were not reproduced.

## Issues

### Issue 1: A bad value in a sparse column can fail the Spark task after earlier rows were already sent
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala`. Also affected: `OpenAIResponses.scala`, `OpenAIChatCompletion.scala`, `OpenAI.scala` and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala`.
- **Line(s)**:
  - `HasOpenAIModelParams.scala` 147-195
  - `OpenAIResponses.scala` 170-178 and 203-214
  - `OpenAIChatCompletion.scala` 141-150
  - `OpenAI.scala` 62-76 and 495-501
  - `CognitiveServiceBase.scala` 595-613
- **Description**: These checks run once per row inside `prepareEntity` → `getOptionalParams`, on the executors:
  - rejection of unsupported parameters (`rejectUnsupportedParams`)
  - the `stop`/`stopSequences` conflict (`resolveStopSequences`)
  - log-prob conflicts (`resolveLogProbParams`)
  - the reasoning-effort conflict (`mergeReasoning`)
  - the finite-number check on column values of `promptCacheOptions` and `reasoning`

  `inputFunc` does not catch exceptions, so any failed check fails the whole task. If the bad value comes from a column, rows that are null or valid go through. The task then fails at the first bad row, after the requests for earlier rows were already sent; with concurrency above 1, several may be in flight. Examples:
  - `setSeedCol`, `setStopCol` or `setLogitBiasCol` on Responses
  - a reasoning column whose effort disagrees with `reasoningEffort` on some rows
  - an include column containing `message.output_text.logprobs` while `logProbsEnabled=false`
  - a NaN in a `promptCacheOptions` column

  The same problem inside a `prompt_cache_breakpoint` instead becomes a per-row error with no request sent. Scalar and global values fail on the first row that is processed, before any request, so the approved Chat fail-fast decision is not in question. This pattern already existed (the `resolveMaxTokens` conflict and Responses `mergeModel` on v1); the PR applies it to many new parameters and to data-dependent number checks.
- **Risk**: Spark retries the task (`spark.task.maxFailures` defaults to 4 on clusters) and sends the earlier rows again. That means duplicate billed requests, and duplicate stored responses when `store=true`, before the job fails and loses all partial results. Where the failure happens depends on how the data is partitioned.
- **Suggested Fix**: Keep fail-fast, but make it deterministic and happen before anything is sent:
  - In Chat and Responses `transform` (Prompt goes through its child stage), reject column bindings of parameters that are never supported. A seed column on Responses is unsupported whatever its values are. Validate scalar and global conflicts on the driver.
  - For checks that depend on the data (finite numbers in columns, reasoning-effort, stop and log-prob conflicts), either move them into the existing per-row validation UDF so they produce `errorCol` with no request, or document that they fail the task.
  - Add a regression test with a sparse column (null in early rows, a bad value later) that asserts the handler is never called.

### Issue 2: Chat rejects valid nested nulls in breakpoint objects that Responses accepts
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`
- **Line(s)**: 245-281 (`encodeStructuredArray`, `encodeStructuredMap`, `encodeStructuredValue`) and 424-447 (`encodedContentPart`). Compare `OpenAIResponses.scala` 272-285, which checks for null at 273-274.
- **Description**: Chat's `encodeStructuredValue` has no null check at the top. Consider a breakpoint that contains an array of structs, arrays or maps with a null element, or a string-keyed map of structs with a null value. Chat then throws "Expected struct content part but found null" (or "Expected array content…" / "Expected map content part…"). The user sees `messages[i].content[j] is invalid: Expected struct content part but found null`. For the same input, Responses forwards the array element as JSON `null` and drops the map entry. Both stages forward nulls inside primitive arrays.

  The Chat allow-lists accept only `type`, `text`, `image_url` and `prompt_cache_breakpoint` (lines 361-389), and `image_url` holds only strings. Nested arrays and maps of complex types can therefore reach this encoder only through the new breakpoint support.
- **Risk**: A breakpoint that is valid JSON fails on Chat (as a per-row error with no request) but works on Responses, and the error wrongly describes it as a content-part type problem. Nothing is corrupted or sent by mistake, but users see false rejections and confusing errors when the same DataFrame is used with both APIs.
- **Suggested Fix**: Add the same null check to Chat (`if (value == null) null else …`), and keep the top-level null check in `encodedContentPart`. Add a Chat/Responses parity test using a breakpoint with an array of structs that contains a null element.

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

## Driver Resolution Addendum - 2026-11-10

The original review and verdict above are preserved.

- **Issue 2: fixed locally, not published.** Added the same leading null guard to
  Chat's `encodeStructuredValue` that Responses already uses. Nested null array
  elements remain JSON null; null struct fields and map entries are omitted.
  The top-level content-part validation remains unchanged.
- **Regression evidence:** the new Chat regression failed before the fix while
  its Responses counterpart passed. After the fix, all 70 tests in
  `OpenAIPromptCacheSuite`, `OpenAIChatCompletionMultimodalSuite` and
  `OpenAIResponsesMultimodalSuite` passed. The new parameterized cases cover
  four nested-null shapes through both direct and Prompt routes on both APIs
  (16 request-payload cases). Eight public-transform loopback HTTP checks also
  passed using the rebuilt JAR: four Chat and four Responses cases, with no row
  errors. Main and test Scala style checks reported zero errors and warnings.
- **Documentation:** the OpenAI notebook now describes the shared null-handling
  convention.
- **Issue 1 remains open.** This targeted repair does not change option-column
  fail-fast behavior or resolve the other review findings.
- **Review status:** attempt 26 is not a clean qualification. Its fourth lens
  timed out, and this source fix invalidates its frozen patch. The user
  authorized only this repair and targeted local checks, then a stop. No new
  review, commit, push or CI run was performed.

## Subsequent Driver Reconciliation - 2026-11-10

The user subsequently authorized publication work and one new bounded complete
review pass. This does not turn attempt 26 into a clean result.

Issue 1's documentation alternative is now implemented: the notebook's local
option-validation paragraph explicitly distinguishes task-level option errors
from per-row breakpoint errors, warns that earlier valid requests may already
have been sent, and describes duplicate requests or stored responses on retries.
It advises validating or filtering per-row options before transformation. The
existing scalar-setter and column-encoding error behavior is unchanged; no
guarantee of exactly-once HTTP execution or driver-wide prevalidation is made.
The source-derived retry risk is acknowledged, not dismissed.
