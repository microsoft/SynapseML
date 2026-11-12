---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 4
timestamp: "2026-11-10T15:16:53Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6c22851d74b806b12ae46b61e5b33df0fb8d794ec1fd8339f51f37399f72a05f"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-2/task-unknown-attempt-29-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Test deployment constants.** A grep of `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai` finds no remaining `deploymentNameMini` or `deploymentName5p1` references. The only remaining `gpt-5` literal is in `OpenAICoreOfflineSuite.scala:121`, which is outside this partition and is the intended sampling-forwarding fixture. The shared `deploymentName` value, `gpt-6-luna`, equals `OpenAIDefaults.DefaultTextDeploymentName` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala:12`.
- [x] **OpenAIPromptCacheSuite.** Traced line by line against production:
  - Breakpoint and normalization diagnostics: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala:42-74`.
  - Shared parameter diagnostics: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala:148-190`.
  - Unsupported Responses controls, the `store=false` base and the reasoning merge: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala:170-215`.
  - Rejection of Foundry `store` and of the legacy Chat controls (`echo`, `bestOf`, `cacheLevel`): `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala:141-150`.
- [x] **Persistence assertions.**
  - `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:21,69-71`: the opt-in set is empty by default, and an opted-in parameter is excluded only when it is unset.
  - The same file at lines 133, 136 and 175 keeps the exact BigDecimal parse.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` opts in `deploymentName` at line 206 and the built-in handler at line 584.
  - The handler-guard message the test asserts comes from `core/src/main/scala/org/apache/spark/ml/Serializer.scala`.
- [x] **Map-backed breakpoint test** (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala:455-474`). In `OpenAIChatCompletion.scala`, all four part placements throw `IllegalArgumentException` at lines 325-333, before any `image_url` handling. `validateMessages` (lines 499-524) reaches that code. The validation UDF in `transform` (lines 176-221) records the error and nulls the messages, so the throwing handler is never called.
- [x] **Unset versus empty deployment test** (`OpenAIV1EndpointSuite.scala:759-770`). The Responses default (`OpenAIResponses.scala:147`) supplies `gpt-6-luna` through `getValueOpt` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:102-107`). An explicit `""` skips the non-empty case in `mergeModel` (`OpenAIResponses.scala:180-188`) and raises the v1 error. `deploymentName` has no set-time validator (`OpenAI.scala:151-152`), so `setDeploymentName("")` does not throw outside the `intercept`.
- [x] **Response-schema rejection and decimals** (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala:132-170`). `HasOpenAIResponseSchema.scala:35` serializes the schema before any parameter changes. `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:17-45` sends BigInt and `Object` to `throwFailure`, which throws `IllegalArgumentException`. Scala and Java BigDecimal both become exact `JsNumber` values. The 29-digit test value is beyond Double precision and within the default precision, so the equality check is valid.
- [x] **Prompt Chat store schema** (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala:682-694`). In `OpenAIPrompt.scala`, `transformSchema` resolves globals on a copy with the same uid (lines 761-765), so the default `responseIdCol` name is identical. `mayStoreResponses` (lines 207-210) is true for a scalar `true`, and the schema then appends `responseIdCol` (lines 791-793). The runtime path at lines 371-395 matches.
- [x] **OpenAIDefaultsSuite global tests.** Their deployment assertion now matches the built-in default, so on its own it no longer proves the global value is applied. Precedence with distinct global, scalar and column values is still covered offline in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala:62-79`, so no guarantee is lost. `OpenAIPrompt.transform` copies globals onto the instance (`OpenAIPrompt.scala:534-535`), so the getter assertions hold.
- [x] **Embeddings.** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbedding.scala:76-90` sets no text default and prefers the embedding global, so the renamed literal in `OpenAIEmbeddingsSuite.scala:94` keeps the test's intent.
- [x] **RDD use in test setup is not flagged.** The new tests build input with `spark.sparkContext.parallelize`. This matches existing suite helpers (`OpenAIV1EndpointSuite.scala:174-177`, `OpenAIChatCompletionMultimodalSuite.scala:206-211`) and is not an RDD-based product implementation.
- [ ] **No tests were executed.** This lens is read-only. The live suites (`OpenAIPromptResponsesSuite`, the `OpenAIDefaultsSuite` global tests, the live `OpenAIChatCompletionSuite` tests and the `store=true` Prompt tests) need credentials. Whether the provider accepts Luna reasoning and verbosity controls cannot be checked statically.
- [ ] **Deleted lines could not be re-read.** After context compaction, the inline hunks for later files were re-assessed from working-tree contents in this invocation. Lines that were only deleted could not be re-read: the removed test(s) in `OpenAIPromptSuite.scala`, the consolidated gpt-5.x variants in `OpenAIPromptResponsesSuite.scala`, and the echo-server code moved out of `OpenAIPromptMultimodalRequestSuite.scala`.
  - Deleted lines carry no logic to check for correctness.
  - Remaining coverage was re-checked against the intended behaviors: Chat and Responses store, previousResponseId gating, schema handling, Foundry, usage, and the moved helpers now in `OpenAIPromptHttpTestBase.scala`.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` | assessed | `deploymentName` equals the production Luna default (`OpenAIDefaults.scala:12`). The removed constants have no remaining references, and the credential lookup is unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionMultimodalSuite.scala` | assessed | Only model literals are renamed, consistently across the fake response, the request and the payload assertion. The Foundry test sets no `store`, so the new Foundry store rejection (`OpenAIChatCompletion.scala:141-150`) does not affect it. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala` | assessed | Deployments now use the shared `deploymentName`. The longer prompt keeps the `length > 10` check meaningful. The offline reasoning-controls test asserts only parameters that were set, and Chat has no model-name filter. The `json_schema` string rejection is unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala` | assessed | Resets run in `try`/`finally`. Global assertions now coincide with the Luna default, but precedence with distinct values is covered offline (`OpenAIRuntimeDefaultsSuite.scala:62-79`). `transform` copies globals onto the instance (`OpenAIPrompt.scala:534-535`). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala` | assessed | Only the general-global literal changed. Embedding resolution (`OpenAIEmbedding.scala:76-90`) has no text default and prefers the embedding global, so the test's intent is preserved. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` | assessed | Every test was traced to production: diagnostics (`OpenAI.scala:42-74`, `HasOpenAIModelParams.scala:148-190`, `OpenAIResponses.scala:170-215`), persistence (`ComplexParamsSerializer.scala:69-71`), handler guard text, null usage details from `map_from_arrays`, and BigDecimal reads from `AnyJsonFormat.read`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala` | assessed | The server binds to a loopback ephemeral port and stops in `finally`. Overloads resolve by lambda arity, and only one alternative has defaults. The `prompt` helper pins `gpt-6-luna`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` | assessed | New tests at lines 73-259: the ANSI setting is restored in `finally`; storage-flag and response-ID gating match `OpenAIPrompt.scala:371-395`; struct cache options and breakpoints match the encoders. Existing tests use the moved base helpers (`requestPayload`, lines 261-276). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptResponsesSuite.scala` | assessed | The single live test uses the shared deployment with the Responses API type. Reasoning and verbosity map to their nested Responses fields. The output assertion is case-insensitive and null-guarded. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala` | assessed | The Chat store schema test matches the resolved-copy `transformSchema` (lines 761-765) and `mayStoreResponses` (lines 207-210, 791-793). The previousResponseId gating and Responses store tests are consistent. The deleted-hunk limitation is noted in the checklist. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala` | assessed | BigInt and `Object` are rejected before the format changes (`HasOpenAIResponseSchema.scala:35`, `UntypedArrayParam.scala:17-45`). A 29-digit Scala or Java decimal is preserved exactly. Request-order and format assertions agree with the encoding paths. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesMultimodalSuite.scala` | assessed | Only model literals are renamed, in the fake response, the helper and an explicit `Map("model" -> ...)`. Struct-part encoding assertions are unaffected. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala` | assessed | Deployments are renamed. The store tests match the `store=false` base merged with explicit values (`OpenAIResponses.scala:172`), and the reasoning mapping removes the flat `reasoning_effort`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala` | assessed | Map-backed breakpoints throw at `OpenAIChatCompletion.scala:325-333` in every placement, and validation (lines 176-221, 499-524) prevents the HTTP call. An unset deployment defaults through `OpenAIResponses.scala:147`, while an explicit empty name reaches the v1 error (lines 180-188). |

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
