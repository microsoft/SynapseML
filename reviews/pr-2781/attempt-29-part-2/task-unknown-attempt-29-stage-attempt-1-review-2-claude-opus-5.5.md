---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 2
timestamp: "2026-11-10T13:13:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "cd3bd19e70519c2d97ba873f442acdd635cb57c132e8ea9a8c4c5cfcc5381b27"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-2/task-unknown-attempt-29-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Style limits:** `scalastyle-test-config.xml` sets `maxFileLength` to 800 (line 4), `maxLineLength` to 120 (line 7) and enables `NullChecker` (line 66).
  - Changed suite lengths: `OpenAIV1EndpointSuite` 797, `OpenAIPromptMultimodalRequestSuite` 782, `OpenAIPromptCacheSuite` 775, `OpenAIPromptSuite` 738, `OpenAIPromptHttpTestBase` 130.
  - New `null` literals carry `// scalastyle:ignore null`.
- [x] **Coverage searches:**
  - Searching `cognitive/src/test` (Scala and Python) for `setEcho|setBestOf|setCacheLevel` found only `OpenAIPromptCacheSuite.scala:348`.
  - `intercept[SparkException]|assertThrows[SparkException]` has no match in any OpenAI suite.
  - `setStore(false)` has no direct `AIFoundryChatCompletion` case. Its matches are Prompt routes only: cache suite lines 88 and 118, `OpenAIPromptSuite.scala:667` and `OpenAIRuntimeDefaultsSuite.scala:133`.
- [x] **Failure paths in production, which explain Issue 1's split:**
  - Option rejections come from `rejectUnsupportedParams` at `OpenAIChatCompletion.scala` lines 141–150.
  - `CognitiveServiceBase.scala` lines 597–599 call `prepareEntity` with no catch, so those rejections fail the task.
  - Breakpoint validation is a separate message-only pre-pass at `OpenAIChatCompletion.scala` lines 188–197 and 561–569.
  - The catch at `OpenAI.scala` lines 49–52 only rewraps the error.
  - So production matches the contract, and Issue 1 is a gap in test coverage, not a production regression.
- [x] **Name-based gating removed:** searching `cognitive/src/main/scala/.../services` for `gpt-5|gpt5|isReasoningModel` finds nothing. The renamed tests in `OpenAIChatCompletionSuite` and `OpenAIPromptResponsesSuite` do not rely on name filters.
- [x] **No stale symbols:** `deploymentNameMini` and `deploymentName5p1`, removed from `OpenAIAPIKey`, are not referenced anywhere in `cognitive/src`, `core/src` or `docs`.
- [x] **Fixture-sharing convention:** the repository shares fixtures through traits that extend `TestBase`:
  - `HTTPTestUtils` (`core/.../split2/DistributedHTTPSuite.scala:32`)
  - `WithServer` (`core/.../split1/HTTPTransformerSuite.scala:89`)
  - `OCRUtils` (`cognitive/.../vision/ComputerVisionSuite.scala:17`)
  - `FormRecognizerUtils` (`cognitive/.../form/FormRecognizerSuite.scala:99`)

  The new `abstract class OpenAIPromptHttpTestBase extends TestBase` (line 15) has one subclass (`OpenAIPromptMultimodalRequestSuite.scala:69`).
- [x] **Precedents I did not flag:**
  - Reflective `classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")` is already used in `OpenAIV1EndpointSuite` and `OpenAIRuntimeDefaultsSuite`.
  - The bare `spark` initialization idiom appears in more than 10 suites.
  - `spark.sparkContext.longAccumulator` in tests does not fall under the AGENTS.md rule against RDD implementations.
- [x] **Persistence interfaces the tests use:**
  - `paramsWithImplicitDefaults` (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:21`)
  - the `deploymentName` addition in `HasOpenAITextParams` (`OpenAI.scala:206`)
  - the `handler` opt-in in `OpenAIServicesBase` (`OpenAI.scala:584`)

  The save/load tests (cache suite lines 84–141 and 273–338) use them consistently.
- [x] **Visibility:** `getOptionalParams` is `private[ml]`, `getStringEntity` is `private[openai]`, and `UsageMappings.ChatCompletionsDetailed` is `private[openai]` (`UsageUtils.scala:49`). That is appropriate for same-package tests, with no public API widening.
- [x] **Explicit versus default selection outside this partition:** `OpenAIRuntimeDefaultsSuite` lines 48–130 use distinct sentinels. This limits the impact of Issue 4.
- [ ] **No tests were run.** This is a read-only review. The compile and style evidence supplied with the request was not reproduced.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` | assessed | Merges two deployments into `deploymentName = "gpt-6-luna"` (line 14). Searches find no remaining references to the removed symbols. Fixture now equals the default (Issue 4). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionMultimodalSuite.scala` | assessed | Literal-only changes (lines 29, 94, 139, 215). The Foundry `setModel` path is kept; the explicit literal equals the default (Issue 4). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala` | assessed | `deploymentNameMini` becomes `deploymentName`, and prompts and test names change. Renamed tests assert only parameters that were set, consistent with removing name gating. No issue. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala` | assessed | Prompt text changes only. The global setter and restore pattern is unchanged, with no architectural impact. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala` | assessed | Only a global literal changes. Embedding setter flow and assertions are unchanged, which matches the embedding-preservation contract. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` | assessed | New 775-line suite following `TestBase`/`tmpDir`/reflection precedents. Fail-fast contracts are tested only through helpers (Issue 1); concerns are mixed and harnesses duplicated (Issue 3); default literals (Issue 4). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala` | assessed | Single-subclass abstract base (line 15), unlike the repository's trait fixtures, with a multimodal `columnTypes` default (line 126) (Issue 2). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` | assessed | Now extends the base (line 69). Cache and storage tests at lines 73–276 reset `columnTypes` at lines 86, 122, 140, 163 and 186; the suite is 782 of 800 lines (Issue 2). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptResponsesSuite.scala` | assessed | Reasoning and verbosity are now unconditional, and the deployment-parameterized helper has one caller. This matches removing the name filter. No issue. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala` | assessed | Removes the reflective payload test and its imports. The Chat store test checks schema only, following an existing suite pattern; payload coverage moved to the cache suite (lines 79–141). No issue. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala` | assessed | The unsupported-type case uses `new Object()`, and exact-decimal acceptance tests are added. This matches "do not broaden the serializer." No issue. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesMultimodalSuite.scala` | assessed | Literal changes only. The explicit deployment literal equals the default (Issue 4). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala` | assessed | Prompt and literal changes only, with no new abstraction or fixture pattern. No issue. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala` | assessed | 797 of 800 lines. Has the `transformWithThrowingHandler` helper (line 168). Its breakpoint test at lines 455–474 overlaps the cache suite (Issue 3), and default literals in fixtures (Issue 4). |

## Issues

### Issue 1: Approved fail-fast compatibility contracts are tested only through helpers, and some are untested
- **Severity**: Medium
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`
- **Line(s)**: 113–141, 341–356, 431–480
- **Description**: The contract sends two kinds of rejection through different paths:
  - **Breakpoint failures** are caught in a message-only pre-pass and become per-row errors. Public-transform tests cover this at lines 536 and 567.
  - **Request-option rejections** should fail the Spark task. These are `echo`, `bestOf` and `cacheLevel` on Chat and Prompt Chat; any resolved Foundry `store`; and non-finite cache-option or reasoning values. They are raised in `getOptionalParams` (`OpenAIChatCompletion.scala` lines 141–150), and `CognitiveServiceBase.scala` lines 597–599 call `prepareEntity` without a catch.

  The fail-fast side is tested only through `getOptionalParams` or the reflective `payload` helper:
  - Line 348 covers only `setEcho(true)`. `bestOf` and `cacheLevel` have no test anywhere under `cognitive/src/test`.
  - No test covers the Prompt Chat route, a `customUrlRoot` endpoint, or the documented sequence: saved stage loads, transform fails, settings cleared, transform succeeds.
  - No test sets `store(false)` directly on `AIFoundryChatCompletion`. Lines 113–141 cover only Prompt-routed false (omitted), true and column binding.
  - No OpenAI suite intercepts `SparkException` for any public transform.
- **Risk**: These are explicitly approved, disclosed behavior changes, and the failure mode (task failure, not an errorCol row) is part of the contract. The suite would still pass after any of these refactors:
  - dropping a name from `rejectUnsupportedParams`;
  - moving option validation into the row-local pre-pass;
  - adding a catch around `prepareEntity`.

  This conflicts with the AGENTS.md rule that "a helper-only test is not proof that the public transformer path works."
- **Suggested Fix**:
  - Extend the `invalid` table (line 342) with `setBestOf(...)` and `setCacheLevel(...)`, a Prompt Chat case and a `customUrlRoot` case.
  - Add a direct `new AIFoundryChatCompletion().setStore(false)` rejection case.
  - Add one public `transform(...).collect()` test that intercepts `SparkException` for a legacy control and for a non-finite cache option.
  - Add one save → load → fail → clear → succeed test using the existing local HTTP base.

### Issue 2: Cache and storage HTTP tests sit in the multimodal suite, and the new single-subclass abstract base departs from the trait convention
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- **Line(s)**: Base: 15, 126. Multimodal suite: 69, 73–276 (resets at 86, 122, 140, 163, 186).
- **Description**: The tests at lines 73–276 of `OpenAIPromptMultimodalRequestSuite` cover:
  - Responses prewarm;
  - storage forwarding and storage column replacement;
  - scalar and per-row cache options;
  - provider cache errors;
  - struct options;
  - breakpoint retention.

  These are not multimodal concerns. Five of them start by undoing the base fixture's multimodal `setColumnTypes(Map("image" -> "path"))` (base line 126).

  The base is an `abstract class` with one subclass. Comparable shared fixtures in the repository are traits extending `TestBase`: `HTTPTestUtils`, `WithServer`, `OCRUtils` and `FormRecognizerUtils`. The split looks driven by the 800-line limit (the suite is now at 782) rather than by concern.
- **Risk**: The cache tests depend on a multimodal default they must keep overriding, so a change to the fixture silently changes what they test. The next multimodal test will hit the file-length limit and force another ad hoc split.
- **Suggested Fix**:
  - Move lines 73–276 into a dedicated `OpenAIPromptCacheHttpSuite` that shares the base.
  - Make `columnTypes` a `prompt(...)` parameter that defaults to empty.
  - Prefer `trait OpenAIPromptHttpTestUtils extends TestBase` to match repository convention.

### Issue 3: `OpenAIPromptCacheSuite` mixes unrelated concerns near the file-length limit and duplicates harnesses and tests
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala`
- **Line(s)**: Cache suite: 15, 44–66, 72, 79–175, 317–338, 341–398, 121, 289, 542, 573, 755, 726–774. V1 suite: 168, 455–474.
- **Description**:
  - **Mixed concerns.** Besides cache options and breakpoints, the 775-line "PromptCache" suite covers log probabilities (72), storage (79–141), seed rejection (144–175), Java-deserialization guards for saved handlers (317–338), conflicting-control validation (341–362) and response parsing (364–398).
  - **Duplicated harnesses.** The throwing-handler and accumulator harness is reimplemented three times (lines 542, 573 and 755), although `OpenAIV1EndpointSuite.transformWithThrowingHandler` (line 168) already exists. The reflective `payload` helper (lines 44–66) repeats V1 and runtime-defaults helpers.
  - **Duplicated test.** The map-backed Chat breakpoint rejection at V1 lines 455–474 overlaps cache-suite lines 726–774.
  - **Minor inconsistency.** Lines 121 and 289 write out `org.apache.spark.ml.param.ParamMap.empty` in full, even though `ParamMap` is imported at line 15.
  - **Near the limit.** The V1 suite is at 797 of 800 lines.
- **Risk**: Duplicate harnesses can drift; for example, one may assert zero HTTP calls while another does not. The suite name hides what it covers, and two suites are close to the style limit, which encourages more ad hoc splits.
- **Suggested Fix**:
  - Extract a package-private test trait that provides `payload` and `transformWithThrowingHandler`.
  - Split the cache suite into storage and controls versus cache options and breakpoints.
  - Keep one copy of the map-backed breakpoint rejection.
  - Use the imported `ParamMap`.

### Issue 4: Explicit fixtures now use the default value, so they cannot distinguish explicit selection from the fallback
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala`; `OpenAIV1EndpointSuite.scala`; `OpenAIPromptCacheSuite.scala`; `OpenAIChatCompletionMultimodalSuite.scala`; `OpenAIResponsesMultimodalSuite.scala` (all in the same package)
- **Line(s)**:
  - `OpenAIAPIKey`: 14
  - V1 suite: 149–152, 223–251, 276–352, 743–753
  - Cache suite: 50–55
  - Chat multimodal suite: 29, 94, 139, 215
- **Description**:
  - About 50 literals changed from `gpt-5.1` or `gpt-5-mini` to `gpt-6-luna`, which is now the default for an unset `deploymentName`.
  - `OpenAIAPIKey` collapses the two live deployments into one value equal to the default.
  - Fixtures that explicitly set the default would still pass if an explicit `setDeploymentName` or `setModel` were ignored and the fallback used.
  - The literal is hard-coded rather than referencing the package-visible `OpenAIDefaults.DefaultTextDeploymentName` (`OpenAIDefaults.scala:12`).
  - Other tests use `custom-model` sentinels (cache suite line 35; V1 line 786), so the approach is inconsistent.

  Dedicated sentinel tests (`OpenAIRuntimeDefaultsSuite` lines 48–130) and the Python bridge and serializer tests reduce the coverage risk.
- **Risk**: Tests of URL and payload shape lose the ability to catch a regression in explicit selection. Any future default change will require editing about 50 literals again.
- **Suggested Fix**:
  - Use a non-default sentinel, such as `explicit-deployment`, in request-shape fixtures.
  - Reference `OpenAIDefaults.DefaultTextDeploymentName` where the default is intended.
  - Keep one test that pins the literal default.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Trusted repository policy requires six independent lenses.",
    "model": "Current live catalog complementary frontier model, not an unavailable historical alias.",
    "schedule": "Six sequential lenses, each with three sequential, feedback-isolated whole-file partitions.",
    "budget": "Explicit human authorization: one replacement complete split pass29, 120 minutes total per lens; no waiver or automatic renewal."
  },
  "changeSize": "48 complete source/test/notebook paths, 280010 bytes, partitioned without omission.",
  "cyclomaticComplexity": "Moderate cross-surface interaction risk in request encoding, schema validation and persistence.",
  "rationale": "Review the complete feature under one immutable manifest, including cross-file interactions; each lens covers every partition.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "mode": "sequential",
  "reviewAttempt": 29,
  "stageAttempt": 1,
  "manifestSha256": "f6dfb310f37cf1ff83748f8fec3bed067aab6c288bf6f0ff96fb336a5d8d315e",
  "diffSha256": "83643a47064c6f4ab76e8cfa95c6da7e690f069e9941ce52d4f93ce7209de5b8",
  "dispatchStatus": "prepared_not_started",
  "split": {
    "parts_per_lens": 3,
    "part_timeout_seconds": 2400,
    "total_timeout_seconds_per_lens": 7200
  },
  "finalReview": "A distinct final CI-qualified pass remains required; zero of two final passes consumed."
}
