---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 1
timestamp: "2026-11-10T12:13:47Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b97ce83bc79ebd2ef97b6fbc35ba5fb2aca2c448adff671b15ed79afc0734f99"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-1/task-unknown-attempt-29-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

**Scope:** partition 1 of 3 (20 assigned paths).

**Result:** I found one Low issue: an error message that is less helpful in Responses than in Chat. I found no Critical, High or Medium regressions in the assigned paths. For the inputs in that issue, the failing row still gets a row-level error and no HTTP request is sent.

## Evidence Checklist
- [x] **Breakpoint validation**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` 42–58 requires the breakpoint marker to be an object and reuses `AnyJsonFormat.write`. Serializer failures are rethrown as `<location>.prompt_cache_breakpoint must contain only JSON-compatible values`.
  - `normalizeValue` (62–76) recursively requires finite Double/Float values and keeps null members in Row and Map values. It recurses into Seq values and passes everything else to the existing serializer, so type support is not broadened.
- [x] **Chat row-level errors**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` 177–222 runs a validation UDF that catches `IllegalArgumentException`. It sets invalid messages to null so `shouldSkip` (162–163) skips request building and HTTP. It then merges the error into errorCol and restores the original messages.
  - `encodedContentPart` (428–451) wraps errors with the content location.
  - Encoders (232–287) omit null struct and map members and keep null array elements.
  - Breakpoints in map-backed content are rejected at 325–333.
- [x] **Responses row-level errors**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` `validateContentPart` (464–482) calls the shared breakpoint check at 472. `validateMessages` and `structuredMessageValidationError` (520–543) feed the same errorCol pattern.
  - Encoders (230–285) follow the same null rules as Chat.
  - Cross-checked against the context-only `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` 566–600. That test covers timestamp, date and binary values, with Java 8 datetime on and off, for both APIs. It asserts zero HTTP calls, the content location and "JSON-compatible" in the error.
- [x] **Luna default and precedence**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` line 12 defines the default.
  - Chat sets the default only when the service type is `openai` (`OpenAIChatCompletion.scala` 124–127), so Foundry is unaffected.
  - Responses and Prompt set their defaults at `OpenAIResponses.scala` ~145–149 and `OpenAIPrompt.scala` 272–285.
  - Global settings are copied only into params that are not already set (`GlobalParams.scala` 43–52).
  - `OpenAIRuntimeDefaultsSuite.scala` tests at 48, 62, 82, 91, 113 and 165 cover:
    - unset deployment, scalar, column and global settings, and a null column value;
    - Prompt model aliases;
    - embedding and Foundry model selection;
    - rejection of legacy model-only selection.
- [x] **Persistence opt-in**
  - The base set `paramsWithImplicitDefaults` is empty (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` 21).
  - `paramsToPersist` (69–77) excludes an opted-in param only when it is unset. This applies to complex params, paramMap and defaultParamMap.
  - deploymentName opts in at `OpenAI.scala` ~206 and the built-in handler at ~584.
  - Tests: `ValidateComplexParamSerializer.scala` 230 and 245, and `VerifyUDFTransformerPersistence.scala` 21 and 58.
- [x] **store and unsupported-control rules**
  - Non-Foundry Chat rejects echo, cacheLevel and bestOf, and Foundry rejects any resolved store (`OpenAIChatCompletion.scala` 142–151). This is the approved fail-fast change.
  - Prompt's `skipNoOpStore` (`OpenAIPrompt.scala` 674–675) omits only a scalar `false` for Chat and Foundry.
  - Responses keeps `store=false` as its default, including for null column values (~168–176). It rejects an inherited seed and requires the reasoning aliases to agree (203–214).
  - The gpt-5 name filter is removed, as approved (`OpenAICoreOfflineSuite.scala` 119 and 144).
- [x] **Prompt model and schema handling**
  - `getOpenAIChatService` (`OpenAIPrompt.scala` 678–707) applies the model alias only when deploymentName is unset and the endpoint is v1 or Responses. Otherwise it requires `customUrlRoot`.
  - `OpenAIRuntimeDefaultsSuite.scala` 243, 258 and 275 cover schema checks that neither copy global credentials onto the Prompt nor persist them.
- [x] **Core serialization**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` 15–62 writes BigDecimal values exactly. It rejects non-string keys and unsupported types.
  - The precise metadata reader is at `ComplexParamsSerializer.scala` 172–186.
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` 21–68 keeps the ModelNotFound failure.
- [x] **Usage accounting**
  - `OpenAISchemas.scala` `extend` (81–97) appends fields after the existing ones.
  - `UsageUtils.scala` 49–121 keeps the public literal-key mapping. Its `private[openai]` detailed mappings feed Prompt (`OpenAIPrompt.scala` 351–358).
- [x] **Security:** the assigned paths add no secrets or new deserialization surfaces. Test endpoints and keys are synthetic, and the Java-deserialization guard still applies to explicit handlers.
- [ ] **Tests were not run** (read-only review). I did not re-verify the supplied validation claims.

## Issues

### Issue 1: Responses encoding errors inside content parts don't say which part failed (Chat does)
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala
- **Line(s)**: 259–285 (throws at 264 and 281) and 321–326. For comparison: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala 428–451.
- **Description**: Responses encodes the whole content array at once with `encodeStructuredValue(message.get(contentIndex), arrayType)`, without recording which part is being encoded. Suppose a `prompt_cache_breakpoint` struct contains a map with non-string keys, such as SQL `map(1, 'x')`, or has another structural mismatch. The row error is then only `Content part map keys must have string type`. It names neither `messages[i].content[j]` nor `prompt_cache_breakpoint`. Chat's `encodedContentPart` reports the same failure as `messages[i].content[j] is invalid: ...`.
- **Not a functional regression**: both stages still set errorCol, return null output and send no HTTP request. The encoder messages appear to predate this PR, but the new nested breakpoint objects make this path easier to reach. No test covers it: there are no `map keys must` assertions under `cognitive/src/test`.
- **Risk**: Responses users with complex breakpoint objects get errors that are harder to trace, and Chat and Responses report the same mistake differently. The spec stresses including the content location in these errors. There is no security or request-leak risk.
- **Suggested Fix**: In Responses `encodedMessageContent`, encode `ArrayType(StructType)` content one part at a time, as Chat does. Rethrow failures as `IllegalArgumentException(s"messages[$messageIndex].content[$partIndex] is invalid: ${e.getMessage}")`. Add one test for both APIs with `map(1,'x')` inside a breakpoint, asserting that the error includes the location and the handler is never called. Alternatively, record this as an accepted existing limitation of the error messages.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala` | assessed | Adds `promptCacheOptions` to Foundry `sharedTextParams` (~38). Inherits Chat validation, encoders and `responseDataType`. Any resolved store is rejected at `OpenAIChatCompletion.scala` 143–144. The echo/cacheLevel/bestOf rejection applies only to non-Foundry Chat, as the spec requires. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala` | assessed | New ServiceParams use the documented payload names. `reasoning`'s scalar `isValid` reuses `normalizeCacheOptionValue` (~70–78). `getOptionalParams` normalizes column values with parameter-specific names (~147–153). Rejection, stop and logprob helpers reviewed; no type support added. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala` | assessed | The schema is validated with the existing `AnyJsonFormat.write` before it is set (~30–42). No new type support. spray-json writing NaN as null predates this change. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` | assessed | Breakpoint check at 42–58 and `normalizeValue` at 62–76. Scalar `promptCacheOptions` validation at ~358–386. deploymentName and handler opt-ins at ~206 and ~584. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | assessed | Luna default only for the `openai` service type (124–127). Approved fail-fast and Foundry store rejection (142–151). The validation UDF plus `shouldSkip` prevents HTTP for invalid rows (162–163, 177–222). Encoders at 232–287; map-backed breakpoint rejection at 325–333. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` | assessed | `DefaultTextDeploymentName = "gpt-6-luna"` (12) is used only by text stages; embeddings and Foundry are unaffected. The seed documentation matches Responses' rejection. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` | assessed | Defaults at 272–285. `skipNoOpStore` (674–675) omits only a scalar `false` for Chat and Foundry. `getOpenAIChatService` (678–707) keeps alias and explicit-deployment precedence and rejects legacy Chat model-only selection. Detailed usage normalizers at 351–358. Schema checks run on a copy. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | assessed | `store=false` default, including null column values (~145–149, ~168–176). Seed and Chat-only controls rejected. Reasoning aliases must agree (203–214). Approved sampling forwarding. Breakpoint validation at 464–482 with row-level errors (520–543). Issue 1 concerns 259–285 and 321–326. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAISchemas.scala` | assessed | `extend` (81–97) appends usage-detail fields without reordering existing ones. Every path that `UsageUtils` uses exists. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala` | assessed | Public `normalize` keeps literal-key behavior. The `private[openai]` detailed mappings (49–121) are used only by Prompt. The visibility repair leaves the public mapping unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` | assessed | Tests at 53–182 use synthetic inputs. Tests at 119 and 144 assert the approved sampling forwarding and reasoning merge; 182 checks response schemas. No live calls or secrets. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` | assessed | Tests from 48 to 275 cover the Luna fallback and precedence (scalar, column, global, null column), aliases and embedding/Foundry selection. They also cover rejection of legacy model-only selection and that schema checks don't store credentials. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtilsSuite.scala` | assessed | Tests at 18, 64, 85 and 117 cover Chat and Responses mapping, missing details and literal dotted keys, consistent with `UsageUtils.scala`. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` | assessed | Status handling (21–68) keeps the ModelNotFound failure and the other checks and adds guidance. No credential exposure. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` | assessed | `AnyJsonFormat` (15–62) writes BigDecimal exactly, rejects non-string keys and unknown types, and keeps key order. No added coercion. |
| `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` | assessed | Opt-in set is empty by default (21). `paramsToPersist` (69–77) excludes only unset opted-in params, in every location. The precise reader (172–186) keeps metadata validation. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` | assessed | Tests at 230 and 245 cover existing defaults and unset, explicit-default and custom values across save, load and resave. Tests at 344–411 keep the deserialization-guard coverage. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala` | assessed | Tests at 13 and 28 cover the missing-model failure with guidance and the unchanged handling of other statuses. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala` | assessed | Tests at 37, 93, 105, 129, 141 and 160 cover exact decimals, ordering, nulls, rejection of bad keys and types, and wide numbers. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` | assessed | Test 21 confirms complex defaults that don't opt in are still saved and guarded. Test 58 confirms saved UDFs still require explicit trusted opt-in. |

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
