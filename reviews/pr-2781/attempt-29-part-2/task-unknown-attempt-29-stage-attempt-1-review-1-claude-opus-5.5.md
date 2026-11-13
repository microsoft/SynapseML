---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 1
timestamp: "2026-11-10T12:29:40Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d788fb81973f95f34f3d49028abc7ce505119a8608e8b7619bb38f22fcd98975"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-2/task-unknown-attempt-29-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I compared all 14 assigned whole-file diffs with the production code they exercise. I only read code with view, grep and glob. I ran no tests, builds or commands. For validation I relied on the supplied note: after the visibility-only repair, compilation, both style checks and UsageUtilsSuite passed. The 61 cognitive Scala tests ran before that repair.
- [x] The removed `deploymentNameMini` and `deploymentName5p1` have no remaining references under `cognitive/src`, `core/src` or `docs`, so removing them cannot break test compilation. The test value `deploymentName = "gpt-6-luna"` equals `DefaultTextDeploymentName` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` line 12).
- [x] The store tests (`OpenAIPromptCacheSuite.scala`, about lines 82–140) match production:
  - `skipNoOpStore` (`OpenAIPrompt.scala` lines 674–675) drops default, explicit and restored scalar false for chat_completions and Foundry.
  - Foundry rejects any store value (`OpenAIChatCompletion.scala` lines 141–146).
  - Responses starts from `Map("store" -> false)` (`OpenAIResponses.scala` line 173), so a null storage column still sends false.
- [x] The seed and conflicting-control tests (about lines 142–173 and 341–363) match the message format of `rejectUnsupportedParams` (`HasOpenAIModelParams.scala` lines 156–159) and the Responses rejection list (`OpenAIResponses.scala` lines 171–172). The global seed is restored in `finally`. Tests run serially (`build.sbt` line 282), so changes to global defaults cannot leak into a concurrently running suite.
- [x] The breakpoint tests (lines 485–626) match production:
  - Null object members are dropped before validation (`OpenAIChatCompletion.scala` lines 232–266); array nulls are kept.
  - `validatePromptCacheBreakpoint` (`OpenAI.scala` lines 42–58) wraps the `IllegalArgumentException` from `AnyJsonFormat` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` lines 17–45) and includes the content location in the message.
  - NaN and infinities fail in `normalizeValue` (`OpenAI.scala` lines 61–75).
  - `AnyJsonFormat` rejects both java.sql and java.time timestamp/date values and `Array[Byte]`, whichever representation Spark produces.
  - A counting handler confirms that invalid rows send no HTTP request.
- [x] The persistence and guard tests (lines 294–339) match the spec. A restored stage does not have `handler` set, and the set state of `deploymentName` is preserved. An explicitly saved handler, custom or equal to the default, raises a `SecurityException` naming `Serializer.LegacyObjectDeserializationConfig` (`core/src/main/scala/org/apache/spark/ml/Serializer.scala` lines 31 and 89–90). That guard is off by default.
- [x] The decimal tests (`OpenAIPromptCacheSuite.scala` lines 604–702; `OpenAIResponseSchemaSuite.scala` lines 132–170) match exact `BigDecimal` support in `AnyJsonFormat`. `BigInt` and arbitrary objects are still rejected, consistent with the "do not broaden type support" requirement.
- [x] No coverage was lost when the offline test was deleted from `OpenAIPromptSuite.scala`. The non-v1 Responses model alias, verbosity/reasoning nesting and store=false are now covered in `OpenAIRuntimeDefaultsSuite.scala` lines 130–140. Forwarding of temperature/top_p for a gpt-5-named deployment is covered in `OpenAICoreOfflineSuite.scala` lines 119–135.
- [x] `OpenAIPrompt.scala` has no code that inserts breakpoints (grep), so ordinary templates cannot add one. `OpenAIPromptMultimodalRequestSuite.scala` lines 233–256 confirms breakpoints appear only on marked content. The extracted HTTP base binds to 127.0.0.1 on an ephemeral port and stops the server in `finally`. No credentials are embedded.
- [ ] Not assessed: live and hosted qualification. Source code cannot show whether a gpt-6-luna deployment exists on the CI resource, or whether the provider accepts the live reasoning, verbosity, cache and storage settings. CI on the current head is still required.

## Issues

### Issue 1: The approved rejection of legacy Chat controls is only tested for `echo` on the direct stage
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`
- **Line(s)**: 341–363 (`setEcho(true)` at 348)
- **Description**: The approved breaking change says Chat and Prompt Chat reject echo, bestOf and cacheLevel, including on custom endpoints, and that saved stages still load but fail when transformed.
  - Production implements this with a single `rejectUnsupportedParams(... echo, cacheLevel, bestOf)` call (`OpenAIChatCompletion.scala` line 145).
  - Prompt Chat inherits it only through the generic parameter copy in `getOpenAIChatService` (`OpenAIPrompt.scala` lines 690–693).
  - Searching every Scala and Python test finds no use of `bestOf` or `cacheLevel`. No test covers the Prompt chat_completions route, a custom endpoint, or a saved-then-loaded stage.
  - Foundry deliberately skips this rejection (lines 142–143), and nothing tests that either.
- **Risk**: The documented breaking change (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb` line 1100) could silently regress while tests stay green. Examples: a control is dropped from the rejection list, or the Prompt parameter copy gains a new filter. I found no production defect.
- **Suggested Fix**:
  - Add `setBestOf(...)` and `setCacheLevel(...)` rows to the table.
  - Add a Prompt `chat_completions` case run through `payload(...)`.
  - Add a custom-endpoint (`customUrlRoot`) case.
  - Add a save/load case asserting that loading succeeds and request construction throws.
  - Optionally pin the Foundry exemption.

### Issue 2: No regression test for explicitly null members in cache-options structs
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala`
- **Line(s)**: 418–428, 687–702
- **Description**: The spec says cache-options structs keep explicitly null members, unlike breakpoint objects, which drop them. The tests cover:
  - a whole-column null (418–428),
  - struct options with no null members (687–702),
  - null omission in breakpoints (495–515).

  No test sends an options struct or map with a null member and checks that `JsNull` is forwarded. Production does forward it today:
  - `normalizeValue` copies Row fields without filtering (`OpenAI.scala` lines 69–70).
  - `AnyJsonFormat` writes `null` as `JsNull`.
  - `OpenAIRequestBody.encode` serializes through `anyFormat`.

  Meanwhile, the neighbouring content encoder does filter nulls (`OpenAIChatCompletion.scala` lines 232–266). I found no equivalent test in `OpenAICoreOfflineSuite.scala`, `OpenAIRuntimeDefaultsSuite.scala`, `OpenAIRequestBodySuite.scala`, or the Python cache and bridge tests.
- **Risk**: A future change that merges cache-option and breakpoint encoding would silently drop explicit nulls, contradicting the spec, and no test would catch it.
- **Suggested Fix**: Add a column case such as `named_struct('mode', 'explicit', 'ttl', CAST(NULL AS STRING))` across `stages`. Assert that `prompt_cache_options == {"mode":"explicit","ttl":null}`.

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

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` | assessed | Only `deploymentName = "gpt-6-luna"` remains, and grep finds no remaining users of the removed constants. Credentials still come from the environment or `Secrets`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionMultimodalSuite.scala` | assessed | Only the model literals change (lines 29, 94, 139, 215). Offline payload assertions and the Foundry `setModel` path are logically unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala` | assessed | Live tests now use `deploymentName`. Offline tests for reasoning controls, verbosity and rejection of a bare `json_schema` string are unchanged in logic. The prompt rewording only affects live output. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala` | assessed | The global text deployment uses `deploymentName`, and embedding defaults stay separate (lines 115, 128, 143). Tests run serially. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala` | assessed | The general default becomes gpt-6-luna (line 94) while the embedding default is kept, preserving embedding behavior. The reset outside `finally` is pre-existing, not a regression. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` | assessed | Store, seed, controls, cache options, breakpoints, decimals, persistence and the guard all match production (see checklist). Coverage gaps are Issues 1 and 2. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala` | assessed | Echo server extracted from the suite. Binds to 127.0.0.1 on an ephemeral port, stops in `finally`, and has no secrets. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` | assessed | Prewarm, content filter and usage assertions match `isContentFiltered` and `generateText`. Store-column response IDs match `mayStoreResponses`. The ANSI setting is restored in `finally`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptResponsesSuite.scala` | assessed | Live-only: low reasoning effort and verbosity are now always set (lines 33–34). Provider support is the provider's concern, consistent with removing the gpt-5 name filter. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala` | assessed | The deleted offline test is re-covered in `OpenAIRuntimeDefaultsSuite.scala` lines 130–140. The Chat store test (lines 682–694) matches the response-ID column logic. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala` | assessed | `BigInt` and arbitrary objects are still rejected while Scala and Java `BigDecimal` keep their exact values (lines 132–170), consistent with not broadening `AnyJsonFormat`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesMultimodalSuite.scala` | assessed | Only the model literals change (lines 28, 125, 148, 214). Encoding and validation assertions are unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala` | assessed | Only deployment and fixture model literals change. Offline parsing fixtures use gpt-6-luna, and the request logic is unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala` | assessed | The map-backed breakpoint test (lines 455–475) matches the "requires a nested struct content schema" error and makes no HTTP call. The empty-deployment test (760–771) matches `mergeModel`. The `setModel` alias test (783–797) distinguishes an alias from the default. |

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
