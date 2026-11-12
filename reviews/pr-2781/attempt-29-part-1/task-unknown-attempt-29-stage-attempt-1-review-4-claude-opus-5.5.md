---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 4
timestamp: "2026-11-10T14:55:21Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "aebceaca1916f7abb3477f8bf472e53ea54fe229bacf69b758d520f9edf9335b"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-1/task-unknown-attempt-29-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Persistence opt-in:** `paramsToPersist` removes an opted-in parameter only when `!instance.isSet(...)`. This filter is used for complex-parameter locations, `paramMap` and `defaultParamMap` (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:58-75,128-137`). Spark `Param` equality uses the parent UID and parameter name, so `Set.contains` matches correctly.
- [x] **Opt-in composition:** `HasOpenAITextParams` adds `deploymentName` (`OpenAI.scala:206`) and `OpenAIServicesBase` adds `handler` (`OpenAI.scala:584`). In the linearization, Chat and Responses get {`handler`, `deploymentName`}. `OpenAIPrompt` gets {`deploymentName`} and has no `handler` (`OpenAIPrompt.scala:34-41`).
- [x] **Default deployment:** The Chat default is guarded by `internalServiceType == "openai"` (`OpenAIChatCompletion.scala:122-127`). `internalServiceType` is a literal-returning `def`, so its override is safe during construction, and Foundry gets no default.
- [x] **Chat option rules:** Foundry rejects any resolved `store`. Non-Foundry Chat rejects `echo`, `cacheLevel` and `bestOf`, which is the approved change. Both reject `reasoning`, `include` and `truncation` (`OpenAIChatCompletion.scala:141-150`).
- [x] **Responses option rules:** Responses rejects the listed inherited controls, including `seed` and `logitBias` (`OpenAIResponses.scala:170-177`). `Map("store" -> false) ++ super` keeps `false` when a storage-column value is null. The `sharedTextParams` override (`OpenAIResponses.scala:108-126`) leaves out top-level `verbosity`, `seed` and `reasoningEffort`, so `verbosity` is sent only as `text.verbosity` (`:189-201`). `mergeReasoning` rejects conflicting effort values (`:203-214`).
- [x] **Log-probability and stop handling:** `resolveLogProbParams` sends Chat a boolean `logprobs` plus `top_logprobs`. For Responses it sends `top_logprobs` and adds `message.output_text.logprobs` to `include`. Enable/disable conflicts are rejected (`HasOpenAIModelParams.scala:167-194`). `rejectUnsupportedParams` ignores null column values through `getValueOpt` (`:156-159`).
- [x] **Breakpoint handling:** Null struct members are omitted, null map entries are filtered and null array elements are kept in both APIs (`OpenAIResponses.scala:230-283`, `OpenAIChatCompletion.scala:232-287`). `validatePromptCacheBreakpoint` (`OpenAI.scala:42-58`) gives location-qualified errors for non-finite numbers and wraps `anyFormat` failures for timestamp, date and binary values with the location.
- [x] **Chat map-backed content:** Any non-null breakpoint key is rejected (`OpenAIChatCompletion.scala:326-333`).
- [x] **Positional row conversion is safe:** `OpenAIResponseSchemas.extend` only appends fields (`OpenAISchemas.scala:84-99,109-117`). Chat's `isContentFiltered` and `getFilterReason` convert rows by position through `ChatModelResponse.makeFromRowConverter` (`OpenAIChatCompletion.scala:591-600`, `core/.../schema/SparkBindings.scala:24-28`), so existing positions stay aligned.
- [x] **Usage mappings:** The added mapping keys match fields in the extended schemas. Nested paths are applied only in `normalizeResponsesDetailed`, and public `normalize` treats keys literally (`UsageUtils.scala:48-60,107-127`).
- [x] **Output-text extraction:** `getOutputMessageText` checks that arrays are non-empty and uses 1-based `element_at`, so there is no off-by-one error (`OpenAIResponses.scala:565-571`).
- [x] **Prompt store handling:** `storeFlagCol` is computed before the output column is overwritten (`OpenAIPrompt.scala:371-389`). `skipNoOpStore` drops only a no-op scalar `false` for Chat and Foundry (`:683-684`). `apiType` is limited to two values (`:186-188`). `transformSchema` resolves globals on `copy(ParamMap.empty)`, so the original stage is not modified (`:765-769`).
- [x] **Number handling in `AnyJsonFormat`:** Scala and Java `BigDecimal` values are written exactly (`UntypedArrayParam.scala:31-32`). Reads narrow to `Double` only when `isDecimalDouble` (`:50-57`).
- [x] **Pre-existing, not counted:** With a column-bound `deploymentName`, `getInternalTransformer` → `assertModelStatus(getDeploymentName)` still calls `.left.get` on the Fabric default endpoint (`OpenAI.scala:627-632`). This change only makes an unset deployment check `gpt-6-luna`, which is intended.
- [ ] **Tests not run, provider behavior not checked:** This was a read-only review with no command execution. I relied on the supplied validation evidence and did not run any tests.

## Issues

### Issue 1: Precise metadata parsing loses the sign of `-0.0` for Double/Float params
- **Severity**: Low
- **File**: core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala
- **Line(s)**: 133, 136 (writer); 175-180 (reader)
- **Description**: The writer now parses `p.jsonEncode(v)` with `useBigDecimalForDouble = true`, and the reader replaces `params` and `defaultParams` with a `BigDecimal`-precise parse. `java.math.BigDecimal` has no negative zero, so a `DoubleParam` or `FloatParam` value of `-0.0` becomes `JDecimal(0.0)` and loads back as `0.0`. Previously, json4s `JDouble` kept `-0.0`. The reader change also affects artifacts that were already saved.
- **Risk**: The impact in practice is negligible. It is still a real difference from the previous reader and goes against the contract that older artifacts keep their reader semantics. Any parameter where `1/x` or `copySign` matters would change.
- **Suggested Fix**: Keep the precise parse only where it is needed. For example, in both the writer and the reader, use the standard parse for Spark numeric parameters (`DoubleParam`, `FloatParam`, `DoubleArrayParam`). Alternatively, fall back to the `JDouble` value when the text is a negative zero. Add a round-trip test for `-0.0`.

### Issue 2: `normalizeValue` loses insertion order and collapses duplicate struct fields
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**: 69-72 (used at `HasOpenAIModelParams.scala:148-154`, `OpenAI.scala:381-385,498-504`)
- **Description**: Rows are rebuilt with `fieldNames.zip(...).toMap` and maps with `.iterator.map(...).toMap`. When there are more than four entries, the result is a hash-based map, so the order of a caller's `ListMap` or struct fields is lost. Duplicate struct field names also collapse to the last value. The normalized value is what gets sent for `promptCacheOptions`, `reasoning`, `metadata`, `logitBias` and the other model parameters. This goes against `AnyJsonFormat`'s deliberate ordering guarantee (`UntypedArrayParam.scala:37-44`; test "preserves insertion order beyond the four-field Map boundary").
- **Risk**: JSON meaning is unchanged and the order is stable for a given set of keys. The wire order can still differ from what the user supplied, and duplicate fields are dropped without notice.
- **Suggested Fix**: Build order-preserving maps, for example `ListMap(values.iterator.map(...).toSeq: _*)` and `ListMap(row.schema.fieldNames.zip(...): _*)`. Optionally reject duplicate struct field names.

### Issue 3: Double space in the generated `logProbs` documentation
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**: 316-317
- **Description**: `"... sent as top_logprobs. " + " Prefer ..."` produces "top_logprobs.  Prefer" (two spaces) in the Scala and generated Python documentation.
- **Risk**: Cosmetic only.
- **Suggested Fix**: Remove the trailing or leading space from one of the two strings.

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

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala` | assessed | `promptCacheOptions` was added to the Foundry `sharedTextParams`, so it is normalized at `OpenAI.scala:498-504`. `internalServiceType="foundry"` stops the Luna default and triggers the store rejection at `OpenAIChatCompletion.scala:142-143`. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala` | assessed | `getOptionalParams` (148-154), the null-safe `rejectUnsupportedParams` (156-159), stop/stopSequences exclusivity (161-165) and the log-probability conflict and include merge (167-194) are correct. Issue 2 shows up here through `normalizeValue`. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala` | assessed | `anyFormat.write` validates before the stage is changed. The name regex allows 1-64 characters. The Python `_convert` uses `LinkedHashMap`, so order is kept and `Decimal` passes through to Java `BigDecimal`. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` | assessed | Breakpoint validator (42-58), finite checks for boxed values (62-75), opt-in composition (206, 584) and `promptCacheOptions` `isValid` (358-371) are correct. Issues 2 and 3 are here. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | assessed | Guarded default (124-127), rejection branches and store (141-150), null-safe encoding (232-287), map-backed breakpoint rejection (326-333) and safe positional row conversion (591-600) are correct. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` | assessed | `DefaultTextDeploymentName` is `private[openai]` (12) and is not a global. `getDeploymentName` stays empty until set, as checked by `OpenAIRuntimeDefaultsSuite:48-60`. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` | assessed | Defaults (271-284), alias versus explicit or global precedence and legacy `customUrlRoot` handling (693-704), `skipNoOpStore` (683-684), store flag computed before overwrite (371-389) and schema resolution on a copy (765-769) are correct. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | assessed | Defaults (145-149), rejection list (170-177), null-column store fallback, `sharedTextParams` with no top-level `verbosity` (108-126), effort-conflict check (203-214) and 1-based output-text extraction (565-571) are correct. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAISchemas.scala` | assessed | `extend` only appends (84-99), so positional row conversion keeps working. The added usage fields match the `UsageUtils` detailed keys (109-143). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala` | assessed | Nested paths apply only to `ResponsesDetailed` (123-127). Public `normalize` keeps keys literal. `map_from_arrays` keys are distinct. Detailed mappings are `private[openai]`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` | assessed | Tests at 119-156 check the approved sampling forwarding and the text/reasoning merge. The test at 182 checks the extended schemas. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` | assessed | Covers unset, scalar, column and global precedence (48-80), no fallback for null columns (82-89), aliases (91-111), Foundry (113-128) and schema-only validation without side effects (243+). Globals are reset in `finally`. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtilsSuite.scala` | assessed | Four tests cover Chat, Responses, missing details and literal dotted keys (117), matching the 2-argument versus private overload split. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` | assessed | `assertModelStatus(modelName, JsValue)` (40-67) keeps every status branch. Only the `ModelNotFound` message is extended. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` | assessed | Exact `BigDecimal` writes (31-32), ordered `ListMap` objects (37-44), `isDecimalDouble`-only narrowing on read (50-57) and failures on non-string keys are correct. |
| `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` | assessed | `paramsToPersist` (67-75) is applied consistently to all three locations. The precise reader still runs Spark's `getAndSetParams` checks. Issue 1 is here. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` | assessed | Tests at 230 and 245 separate legacy persisted defaults from opted-in defaults left out of both maps. Legacy and guard tests are kept. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala` | assessed | Tests at 13 and 28 cover the `ModelNotFound` guidance and the unchanged behavior of the other statuses. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala` | assessed | Exact decimals (37), order beyond four fields (93), wide integers (115) and rejection of unsupported values and keys (129). There is no `-0.0` case (Issue 1). |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` | assessed | The test at 21 confirms that complex defaults without the opt-in stay persisted and guarded. Tests at 33 and 58 cover the session policy and the explicit legacy opt-in. |

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
