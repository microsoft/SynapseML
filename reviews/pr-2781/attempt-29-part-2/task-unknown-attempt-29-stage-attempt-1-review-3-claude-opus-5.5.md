---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 3
timestamp: "2026-11-10T14:12:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6c6f4ffe06f0ec2c639c8317e2f69f77903a66ec6a217263127ae324903d28e1"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-2/task-unknown-attempt-29-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the complete diff for all 14 assigned paths and checked the related production and test code. I did not run any tests or commands; every finding below comes from reading the source.
- [x] The removed `deploymentNameMini` and `deploymentName5p1` constants are no longer referenced anywhere in the module `src` trees. Production code has no model-name heuristics left, and the only `gpt-6-luna` constant is in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala:12`.
- [x] I checked how test isolation and global state behave together:
  - `build.sbt:282` sets `Test / parallelExecution := false` and does not fork test JVMs.
  - `pipeline.yaml:977-979` runs every `services.openai.**` suite in one `testOnly` invocation, so those suites share one JVM.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala:113-121`: a `Flaky` retry reruns the test body without any reset between attempts.
  - `OpenAIRuntimeDefaultsSuite.scala:49,59` asserts that the global deployment is empty.
- [x] These global and session changes are restored or isolated safely:
  - `OpenAIDefaultsSuite.scala:14-39` resets globals in `beforeEach` and in a `finally` inside `afterEach`.
  - `OpenAIPromptCacheSuite.scala:153-175` restores the global seed in `finally`.
  - `OpenAIPromptMultimodalRequestSuite.scala:73-115` restores ANSI mode in `finally`.
  - `OpenAIPromptCacheSuite.scala:294-339` and `567-602` change the deserialization and `java8API` settings only on a `spark.newSession()`.
- [x] Invalid rows never reach HTTP. Each of these tests uses a handler that throws, plus an accumulator asserted to be `0` (`OpenAIPromptCacheSuite.scala:536-602,727-773`; `OpenAIV1EndpointSuite.scala:455-474`). A task failure would surface through `collect`, so it cannot be mistaken for a pass.
- [x] Numeric boundaries are covered:
  - `OpenAIPromptCacheSuite.scala:399-416`: `Double.MaxValue`, `Float.MaxValue` and `1e400`.
  - `431-484` and `517-534`: NaN and ±Infinity, as Double and Float, for scalar, column and nested bindings.
  - `604-627`: `DECIMAL(38,18)`.
  - `668-685`: 2^63 and 2^64 through save, load and resave.
  - `OpenAIResponseSchemaSuite.scala:132-170`: BigInt beyond Long range and arbitrary objects are rejected, and Scala/Java decimals stay exact.
- [x] Null semantics are covered for whole-column null options (`OpenAIPromptCacheSuite.scala:418-429`), breakpoint null omission and array-null preservation (`495-515`), and storage-column nulls (`98-111`; `OpenAIPromptMultimodalRequestSuite.scala:136-157`).
- [x] I cross-checked the tests against production code:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala:41-75,498-504`
  - `OpenAIChatCompletion.scala:141-149`
  - `HasOpenAIModelParams.scala:156-159`
  - `OpenAIPrompt.scala:207-210,674-675`
  - `OpenAIResponses.scala:173-187`
  - `OpenAIRequestBody.scala:16,42`
- [x] These risks already existed before this PR, so I did not count them as issues:
  - `OpenAIChatCompletionSuite.scala:496-522`: the json_object live test still has a 500-token budget.
  - `OpenAIChatCompletionSuite.scala:589-594`: `testCompletion` throws an NPE when output is null.
  - `OpenAIAPIKey.scala`: `deploymentName` has no environment override.
  - `OpenAIPromptSuite.scala:31-35,116-165`: tests mutate a shared `lazy val prompt`.
  - `OpenAIResponses.scala:182`: the empty-name check uses `nonEmpty` and does not trim whitespace.
- [ ] I did not verify live or hosted provider behavior. This review is read-only, and live tests are opt-in.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` | assessed | Removing the constants is safe because nothing references them any more. The hard-coded `deploymentName` without an environment override was already the pattern; the key and service name do have overrides. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionMultimodalSuite.scala` | assessed | Only renames in the mocked model, the explicit deployment and the Foundry `setModel`. The payload-equality and request-body assertions are unchanged, and the ANSI setting is restored in `finally`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala` | assessed | The clearer prompt fits `requiredLength=10`. The token-budget risk in the json_object test (`496-522`) and the NPE on null output (`589-594`) already existed and are not counted. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala` | assessed | Globals are reset in `beforeEach` and `afterEach` (`14-39`), and the globals each test sets are the same on every `Flaky` retry. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala` | assessed | Issue 1: a failure or retry can leak the global deployment (`89-113`), and `afterAll` restores only the embedding default (`28-35`). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` | assessed | Covers non-finite numbers, decimals, nulls, persistence guards, row errors with no HTTP request, the seed restore and storage. Issues 3 and 4 are coverage gaps for explicitly null cache-option members and Foundry false/null store values. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala` | assessed | The server binds to loopback on an ephemeral port and `stop(0)` runs in `finally`. Issue 2: request bodies are not checked for null and are not tied to rows. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` | assessed | ANSI is restored, Responses storage nulls default to false and Chat omits them, and provider 400 errors are retained. Issue 2 applies to the `poll()` calls at `106,131,152,167,173,189,224,252`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptResponsesSuite.scala` | assessed | Explicit deployment, low reasoning effort and low verbosity, and a single live test. No global or temp-directory side effects were introduced. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala` | assessed | The store test checks only the schema, but forwarding over the wire is covered at `OpenAIPromptCacheSuite.scala:79-82` and `OpenAIPromptMultimodalRequestSuite.scala:117-134`. The shared mutable `prompt` already existed. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala` | assessed | Out-of-range BigInt values and arbitrary objects are rejected without changing the stored format (`132-154`), and Scala/Java decimals stay exact on all three routes (`156-170`). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesMultimodalSuite.scala` | assessed | Only model renames in mocked payloads and explicit deployments. Failure-mode assertions are unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala` | assessed | Model renames in mocked JSON parsing only. No new global, retry or failure-path behavior. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala` | assessed | Map-backed breakpoints are rejected both directly and through `transform`, with zero handler calls (`455-474`). An unset deployment falls back to the default and an empty one is rejected (`759-770`, production `OpenAIResponses.scala:180-187`). |

## Issues

### Issue 1: OpenAIEmbeddingsSuite can leak the global deployment name to later suites after a transient failure
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala`
- **Line(s)**: 28-35, 89-113
- **Description**: The test sets `OpenAIDefaults.setDeploymentName("gpt-6-luna")` at line 94 and restores the original only after a successful live transform (lines 108-112), not in `finally`. The suite is `Flaky`, and retries rerun the test body with no reset in between (`TestBase.scala:113-121`).
  - If the first attempt fails, the retry reads the leaked value as the "original" (line 90) and later restores it permanently. The test then passes with the global still set.
  - `afterAll` restores only the embedding default, not the general deployment name.

  CI runs every `services.openai.**` suite in one JVM (`pipeline.yaml:977-979`, `build.sbt:282`). In that JVM, `OpenAIRuntimeDefaultsSuite.scala:49,59` asserts `OpenAIDefaults.getDeploymentName.isEmpty`. The leak pattern already existed, but the PR edited this line and added the strict assertion, which makes the leak matter.
- **Risk**: A misleading, order-dependent CI failure in a different suite. Worse, the leaked global equals the default value, so default-fallback payload assertions would pass because of the global even if the fallback itself were broken.
- **Suggested Fix**: Wrap the body in `try { ... } finally { restore both globals }` so every attempt restores state before a retry. Optionally, also capture and restore the general deployment name in `beforeAll` and `afterAll`. Apply the same `finally` pattern to lines 65-87.

### Issue 2: Echo-server request assertions are not checked for null and are not tied to rows
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- **Line(s)**: Base 63-112; consumers 106, 131, 152, 167, 173, 189, 224, 252
- **Description**:
  - `ConcurrentLinkedQueue.poll()` returns null when no request was sent, so `.parseJson` throws an NPE. At line 189, for example, that NPE hides the row's error-column diagnostic.
  - The queue is shared across loop iterations (`136-157`, `179-197`), so an unexpected extra or missing request shifts bodies into later iterations.
  - The set-based checks at 131 and 152 do not confirm which row sent which `store` value. The `response_id` assertions come from client-side row state, not from the request that was actually sent.
- **Risk**: Failures are hard to diagnose, and the tests would not catch a regression that swaps per-row values on the wire.
- **Suggested Fix**: Add a base-class helper that polls a body and asserts it is non-null with context. Assert the queue is empty after each iteration. Tie each body to its row, for example by keying on the prompt text.

### Issue 3: No regression test for preserving explicitly null cache-option members
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`
- **Line(s)**: 418-429, 687-702 (compare with 495-515)
- **Description**: The contract says cache-option structs keep explicitly null members as JSON `null`, while absent keys and breakpoint objects omit them. Production does this: `OpenAI.scala:60-75` keeps null entries, and `OpenAIRequestBody.scala:42` writes them through `anyFormat`. However, the tests cover only a null whole column and non-null struct members. I found no test that sends a null member, such as `named_struct('ttl', CAST(NULL AS STRING))` or `Map("ttl" -> null)`, through either the Scala or Python test paths.
- **Risk**: A future refactor that aligns cache options with the breakpoint rule (omit null members) would silently change the request sent to providers without any test failing.
- **Suggested Fix**: Add a scalar-map case and a struct-column case that expect `{"ttl":null,...}`. Run them through Chat, Responses and Prompt, and include a save/load roundtrip for the scalar map.

### Issue 4: Foundry store rejection is untested for false or null values and for direct AIFoundryChatCompletion
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`
- **Line(s)**: 113-142
- **Description**: The contract says SynapseML rejects any resolved Foundry store value, including `false` set directly on `AIFoundryChatCompletion`, and that Prompt omits only a no-op scalar `false`. Production rejects store values per row (`OpenAIChatCompletion.scala:142-143`; `HasOpenAIModelParams.scala:156-159`). Prompt passes any column binding through to Foundry (`OpenAIPrompt.scala:207-210,674-675`).

  The tests cover only scalar `true` and a column value of `true`. I found no test anywhere for:
  - a direct `new AIFoundryChatCompletion().setStore(false)` being rejected;
  - a Prompt Foundry store column containing `false`, which should fail the task;
  - a Prompt Foundry store column containing null, which should omit the field and still send the request.
- **Risk**: A change to how no-op values are skipped, or to the defaults, could quietly start forwarding or dropping Foundry store values in these edge cases without any test failing.
- **Suggested Fix**: Add direct `AIFoundryChatCompletion` cases for `false` and `true`, and Prompt Foundry store-column rows with `false` (rejected) and null (store omitted).

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
