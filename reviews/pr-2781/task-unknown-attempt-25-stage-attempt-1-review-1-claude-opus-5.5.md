---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 25
stage_attempt: 1
round: 1
timestamp: "2026-11-10T05:08:19Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "915b904a8edfd9c8531bc6d56921ecca96623ee59f64453de833e021837d2a41"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-25-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage.** I read the full supplied diff and checked all 48 manifest paths for this lens. Later checks used the post-change working-tree versions of the same paths, which contain the staged changes.
- [x] **Per-row breakpoint validation.**
  - In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`, the transform at 176-221 sets messages to null for invalid rows, so `shouldSkip` skips the HTTP call. `encodedContentPart` (424-447) runs `validateContentPart` (392-408), which calls `validatePromptCacheBreakpoint` at 399. `structuredMessageValidationError` is at 557-566.
  - `OpenAIResponses.scala` follows the same pattern at 472 and 533-542.
  - `OpenAI.scala:40-66` recursively rejects non-finite Double and Float values and puts `<location>.prompt_cache_breakpoint` in the error.
- [x] **Null versus absent, and map keys.**
  - `encodeStruct` drops null struct fields: `OpenAIChatCompletion.scala:232-243` and `OpenAIResponses.scala:230-247`.
  - The map encoders filter out null entries and reject non-string keys within the row: Chat 255-266 and 279-280, Responses 259-270 and 280-281.
- [x] **Default deployment and precedence.**
  - The Chat default applies only when `internalServiceType == "openai"` (`OpenAIChatCompletion.scala:124-127`). Responses defaults are at `OpenAIResponses.scala:145-149` and Prompt defaults at `OpenAIPrompt.scala:272-285`.
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala:43-49` fills only unset params, so an explicit scalar, column or global deployment wins.
  - Foundry's `prepareUrlRoot` override makes the default deployment copied from Prompt harmless.
- [x] **Persistence.**
  - In `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, opted-in params are left out of both paramMap and complexParamLocs only when unset (21, 69-77). `defaultParamMap` is still written (122-151), so loading restores the default without marking it set. The precise parse is at 165-187.
  - `OpenAI.scala:194-196` composes `deploymentName` into the opt-in set, and 572-622 opts in the built-in handler.
  - Explicit handlers count as set, so they are still persisted and still pass through the existing deserialization guard.
- [x] **AnyJsonFormat.** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:23-57`:
  - writes Scala and Java BigDecimal exactly and keeps field order with ListMap;
  - throws on non-string keys and unsupported types;
  - keeps BigDecimal on read for values that are not exact decimal doubles.
  - The `isValid` hooks for `promptCacheOptions` and `reasoning` use parameter-specific names (`OpenAI.scala` ~345-375, `HasOpenAIModelParams.scala` ~70-80).
- [x] **Request controls.**
  - Chat `getOptionalParams` (`OpenAIChatCompletion.scala:141-150`) rejects echo, cacheLevel, bestOf and Responses-only controls, and rejects store for Foundry.
  - Responses (`OpenAIResponses.scala:170-212`) rejects inherited Chat controls including seed, defaults store to false, requires the reasoning aliases to agree, and forwards temperature/top_p.
  - `getValueOpt` includes defaults (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:102-107`).
  - These are spec-approved behaviors and are not flagged.
- [x] **Prompt.** In `OpenAIPrompt.scala`:
  - `mayStoreResponses` is at 207-210.
  - At ~661-690: no-op store skipping; true and column bindings kept; alias and `customUrlRoot` handling; legacy model-only Chat rejected.
  - Schema-only validation runs on a copy (~751-755).
- [x] **Usage and schemas.**
  - `OpenAISchemas.scala:26-145` has no duplicate fields.
  - The detailed-usage keys added in `UsageUtils.scala` are additions only.
  - `getFilterReason` is only reached for non-null filtered outputs (`OpenAIPrompt.scala` ~310-325, `OpenAIResponses.scala` ~603-613).
- [x] **Docs and live tests.**
  - `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` prominently discloses both the Chat legacy-control rejection and the Responses sampling change, and documents the global-seed rejection.
  - `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` is opt-in through `SYNAPSEML_LIVE_CACHE_TESTS=1`, and it saves before setting credentials.
- [x] **Test-gap search.** A grep of `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai` for Timestamp, BinaryType, `Array[Byte]`, DateType and "Cannot serialize" found no breakpoint coverage for unsupported leaf types. The only match was an unrelated attachment helper at `OpenAIPromptHttpTestBase.scala:82`.
- [ ] **Not a clean signoff.** Context compaction removed the verbatim diff text after the full read. Later checks used the working-tree versions of the changed files, and comparisons against removed lines rely on that first read.
- [ ] **Nothing was executed.** I used read-only tools only and did not run any commands, builds or tests. The validation claims in the prompt were not reproduced.
- [ ] **Hosted and live provider behavior: not applicable** to this precommit lens.

## Issues

### Issue 1: Non-JSON leaf values in a nested `prompt_cache_breakpoint` skip per-row validation and fail the Spark task
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**: 40-48 and 51-66 (the fall-through is at 64). Related:
  - `OpenAIChatCompletion.scala`: 281, 399, 557-566, 572-580
  - `OpenAIResponses.scala`: 282, 472, 533-542, 544-562
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`: 17-20, 45
- **Description**: A breakpoint struct can contain TimestampType, DateType or BinaryType fields. Spark returns these as `java.sql.Timestamp`, `java.sql.Date`, `Array[Byte]`, or `java.time.*` objects when the Java 8 datetime API is on.
  1. Both encoders pass these values through unchanged (`case _ => value` at Chat 281 and Responses 282).
  2. `validatePromptCacheBreakpoint` accepts the marker because it is a Map. `normalizeValue` checks only Double and Float values and returns everything else as-is.
  3. `structuredMessageValidationError` therefore reports no error.
  4. During request construction, `requestBody.encode` calls `AnyJsonFormat.write`, which hits `throwFailure` at line 45.
  5. That throw happens in the request-preparation UDF, outside the per-row try/catch, so the Spark task fails.

  The error message ("Cannot serialize … of type …") names neither the content location nor `prompt_cache_breakpoint`. Non-string map keys and NaN/Infinity are already caught per row.
- **Risk**: One bad row aborts the whole job instead of producing an `errorCol` entry with null output. This conflicts with the per-row message-validation contract that the PR applies to non-finite numbers and non-object markers, and the error is hard to trace. No request is sent and no data leaks. This is a gap in the new feature, not a regression.
- **Suggested Fix**: In `validatePromptCacheBreakpoint`, after `normalizeValue`, choose one of these:
  - call `AnyJsonFormat.anyFormat.write(normalized)` inside a try/catch and rethrow `IllegalArgumentException` with `$location.prompt_cache_breakpoint` in the message; or
  - allow only JSON-compatible leaf types: String, Boolean, integral types, finite Float/Double, BigDecimal, Seq, String-keyed Map and Row.

  Add Chat and Responses public-transform tests with a timestamp or binary breakpoint field. They should assert a per-row `errorCol`, null output and zero HTTP calls.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Trusted repository policy requires six independent lenses; parameter/persistence interactions justify Medium.",
    "model": "Current task-tool catalog and installed review-code complementary frontier-family rule.",
    "schedule": "Trusted repository gauntlet requires sequential lenses unless parallel execution is requested.",
    "budget": "Human explicitly renewed one full precommit pass, cap 25 with 24 consumed; 40-minute per-lens bound retained. No budget reset."
  },
  "changeSize": "48 source/test/notebook paths in the complete staged PR diff; 270392 bytes.",
  "cyclomaticComplexity": "Low added branching, moderate interaction risk across shared service parameters, request encoding, schema validation, generated wrappers and constructor-recreated defaults.",
  "rationale": "Review the complete cache/default feature, including the pending persistence repairs, public-transform regressions and explicitly approved compatibility disclosures.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available current Anthropic Opus frontier model and supports max reasoning, complementary to this OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 6c2699defcfb24d5bff5ba55f2252ba2d8cbea24",
  "mode": "sequential",
  "reviewAttempt": 25,
  "stageAttempt": 1,
  "manifestSha256": "ceb2b3156fdfa81a036bbe4dfed0ac06a66908406fc8b35be912424beb9596e8",
  "diffSha256": "f19b3943c3d3327e14406473550f12f023a45dfeebfa3ba27f49638ea974dffe",
  "dispatchStatus": "prepared_not_started",
  "finalReview": "A separate final CI-qualified pass remains required; zero of two final passes consumed."
}

## Driver triage and stop

The original finding and response above are preserved. Round 1 was finalized
with `issues_found`; rounds 2-6 were not started. The transport audit verified
the exact frozen diff, the requested runtime model, read-only tools, and no
access to earlier review feedback. The reviewer's explicit context-compaction
caveat is retained; this result is not a clean signoff.

### Issue 1 confirmed through the public transform path

Six probes used the exact JARs from the current production-source validation:
Chat and Responses, each with a timestamp, date, and binary value inside a
nested breakpoint object. Every `transform(...).collect()` action failed with
`Cannot serialize ... of type ...`, without the breakpoint's content-location
diagnostic. A counting local HTTP server observed zero requests.

The existing per-row validation accepts the object and its unsupported leaf,
then request serialization throws outside the message-validation catch. This
is a confirmed robustness gap in the new breakpoint feature. It is distinct
from the deferred scalar-setter diagnostics enhancement: an invalid message
row aborts the Spark action rather than producing a row-level error.

The targeted repair would validate the normalized breakpoint with the existing
JSON serializer inside the message-validation boundary, preserving supported
JSON values and adding the content location to unsupported-value diagnostics.
Public Chat and Responses regressions should require row errors, null output
and zero HTTP calls for unsupported timestamp/date/binary leaves. No product
source, tests, or notebook was changed during this triage.

### Budget and publication status

The author explicitly retained the strict review gates. The authorized
precommit budget is exhausted at 25/25, so the driver has stopped rather than
launching another attempt. The earlier 49 targeted tests remain passing but
do not cover this newly reproduced case. No new commit, push or CI trigger has
occurred. The separate final CI-qualified review and current-head CI remain
pending; the one authorized guarded CI trigger is still unused.

## Resolution after authorized repair

The author requested fixing this confirmed failure and getting the PR ready.
The driver retained the strict gates and bounded the continuation to one fresh
complete precommit attempt.

`HasMessagesInput.validatePromptCacheBreakpoint` now checks the normalized
marker with the existing JSON serializer inside the message-validation
boundary. Unsupported leaves raise `IllegalArgumentException` with the full
content location and a JSON-compatibility diagnostic, which the existing Chat
and Responses transforms convert into per-row errors. The pre-existing
non-finite check stays outside the new catch, retaining its diagnostic.
Supported JSON types and the shared serializer are unchanged.

`OpenAIPromptCacheSuite` adds public-transform regressions for timestamp, date,
and binary leaves inside nested breakpoint objects, using both Java datetime
modes on an isolated Spark session. The two tests failed before the repair with
the reported serialization error, then passed all twelve cases after it. They
require content-location errors, null output and zero handler calls. The
messages-column documentation and notebook now explicitly identify these
unsupported types.

Cognitive production/test compilation and Scala style passed. All 76 tests
across the cache, Chat multimodal, Responses multimodal and core-offline suites
passed with no failures, cancellations, ignored or pending tests. Rebuilt JARs
and freshly installed isolated Python wrappers passed 28 bridge tests, 28
subtests and seven local HTTP request checks. Existing finite-number precision,
non-finite per-row errors and non-object marker regressions remain green.

Issue 1 is resolved by this repair. Pass 25 remains an issues-found historical
result; the fix still requires the newly authorized complete review and
current-head CI before readiness.
