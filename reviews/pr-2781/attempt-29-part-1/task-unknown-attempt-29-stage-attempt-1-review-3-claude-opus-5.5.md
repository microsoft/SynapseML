---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 3
timestamp: "2026-11-10T13:52:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d7f60caa22b1389b525e7be153f16cad38b132d4e07ef62d7413d74bb4121f9d"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-1/task-unknown-attempt-29-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read every changed hunk in the 20 assigned paths against the working-tree source. I also read the interfaces they depend on:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:98-122,722-760` (`getValueOpt` and `shouldSkip`, dynamic-column packing)
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala` (`ServiceParam` `isValid` wiring)
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala:43-52`
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.scala:138-145`
- [x] **Per-row validation path.**
  - The Chat and Responses validation UDFs catch `IllegalArgumentException` (`OpenAIChatCompletion.scala:560-569`, `OpenAIResponses.scala:533-542`).
  - Invalid rows get null messages (`OpenAIResponses.scala:376-382`), and `shouldSkip` then prevents the HTTP request (`OpenAIResponses.scala:218-219`).
  - `SimpleHTTPTransformer.scala:138-145` coalesces with an existing errorCol, so validation errors survive the HTTP stage.
- [x] **Breakpoint edge values.**
  - `OpenAI.scala:42-59` wraps serializer failures with `<location>.prompt_cache_breakpoint`.
  - `OpenAI.scala:62-76` rejects NaN and infinities, with the location, before spray-json can silently turn them into null.
  - The serializer's catch-all (`UntypedArrayParam.scala:23-45`) throws `IllegalArgumentException` for timestamps, dates, binary values and non-string keys.
  - Responses encoders drop null struct members, filter null map values and keep null array elements (`OpenAIResponses.scala:230-270`).
  - Chat rejects map-backed breakpoints (`OpenAIChatCompletion.scala:325-333`).
- [x] **Store defaults and rejections.**
  - Responses starts from `Map("store" -> false)`, so a null store column still sends false (`OpenAIResponses.scala:145-149,173`).
  - Rejections are evaluated per row through `getValueOpt` (`HasOpenAIModelParams.scala:156-159`, `OpenAIChatCompletion.scala:141-150`, `OpenAIResponses.scala:170-178`).
  - Prompt skips only a no-op scalar store (`OpenAIPrompt.scala:674-675`) and gates the response ID on the per-row store flag (`OpenAIPrompt.scala:376-393`).
- [x] **Deployment default precedence.**
  - The constructor default is guarded by `internalServiceType` (`OpenAIChatCompletion.scala:124-127`). Foundry overrides it with a constant `def` (`AIFoundryChatCompletion.scala:56`), and Foundry's URL ignores the deployment that Prompt copies in (`:62-63`).
  - Globals fill only params that are not set (`GlobalParams.scala:46`), so a global deployment still overrides the gpt-6-luna default.
  - Legacy Chat with only a model selected fails fast (`OpenAIPrompt.scala:695-705`).
  - `OpenAIDefaults.scala:12` keeps the default private, so no global default is created.
- [x] **Persistence.**
  - `ComplexParamsSerializer.scala:59-77` drops unset opted-in params from paramMap, defaultParamMap and complex-param locations.
  - `ComplexParamsWriter` is `private[ml]`, and its only call site (`:42`) passes no explicit `paramMap`, so that branch cannot bypass the filter.
  - The precise parse (`:133,136,175-180`) re-renders strings before Spark's own decoders run. A grep of product code found no code that pattern-matches `JDouble` on raw metadata.
- [x] **Test isolation and coverage.**
  - `build.sbt:282` sets `Test / parallelExecution := false`, so the global-state changes in `OpenAIRuntimeDefaultsSuite.scala` run one suite at a time.
  - Grep found no test for non-string-keyed maps inside Responses breakpoints and no -0.0 roundtrip test.
- [ ] Tests, builds and live endpoints were not executed: this is a read-only review. The validation results quoted in the prompt were not re-verified.
- [ ] Frontmatter provenance (timestamp, execution backend) is left to the script-owned evidence writer rather than invented here.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala` | assessed | `promptCacheOptions` is added to the Foundry params. The constant `def internalServiceType` (`:56`) makes the constructor guard skip the gpt-6-luna default. The URL (`:62-63`) ignores the deployment that Prompt copies in. Any resolved store value is rejected per row, which fails the task as approved. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala` | assessed | Issues 1 and 4. The scalar finite-number check for `reasoning` runs in `isValid` (`:74-77`). Logprob and stop conflicts (`:161-195`) throw `IllegalArgumentException` during request construction. A non-array `include` is rejected. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala` | assessed | Documentation-only change; no runtime branch, boundary or failure mode changes. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` | assessed | Breakpoint errors carry the location (`:42-59`); finite-number checks are at `:62-76`; the `promptCacheOptions` scalar `isValid` is at `:358-372`; the handler opt-in is at `:584`. The Fabric `assertModelStatus` path is unchanged. A `Row` without a schema can only come from a user-built scalar, which the spec puts out of scope. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | assessed | Rejections are at `:141-150` (Issue 1). Map-backed breakpoints are rejected at `:325-333`. Content-location wrapping is at `:428-443` (the comparison point for Issue 2). The validation UDF catches only `IllegalArgumentException`; `:560-569` was checked for other exception types. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` | assessed | The `DefaultTextDeploymentName` constant is `private[openai]` (`:12`), and `getDeploymentName` returns only the global value. A global seed (`:66`) reaches Responses through `transferGlobalParamsToParamMap` and is rejected at the first row, before any request. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` | assessed | Model-alias and legacy-Chat `require` logic is at `:695-705`; Foundry routing is at `:678-693`. The store flag and response ID are handled at `:376-393`; a null store column gives a null ID. The column-name resolution there matches the existing `col(name)` packing. `transformSchema` resolves globals on a copy. `addRAIErrors` only touches non-null outputs. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | assessed | Issues 2 and 4. The store default stays false for null columns (`:145-149,173`). A v1 endpoint without a model fails with a clear error (`:180-187`). The validation transform is at `:369-398`; `shouldSkip` checks for null messages at `:218-219`. The content-filter guard handles empty output. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAISchemas.scala` | assessed | New fields are appended at the end of each struct and do not collide with case-class fields, so the ordinal-based `makeFromRowConverter` stays compatible. New numeric fields match the integers providers return; no defect found. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala` | assessed | The detailed mappings are supersets of the public ones. Missing or empty detail paths resolve to null without throwing. Public literal dotted-name semantics are kept; nested Responses paths are internal only. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` | assessed | Offline tests (no network) cover encoding, collapsing, `response_format`, and the approved forwarding of explicit and global sampling values (`:119-156`). There is no case for column-bound rejected controls (Issue 1). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` | assessed | Covers precedence of unset, explicit, column and global deployments (`:48-81`); a null bound deployment never falls back to the default (`:82`); legacy Chat model-only rejection (`:165`); and schema-only global isolation (`:243-290`). It restores globals in `try`/`finally`, and test parallelism is disabled. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtilsSuite.scala` | assessed | Four tests, including missing or empty detail mappings (`:85`) and literal dotted names (`:117`), which cover the boundary cases of the mapping changes. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` | assessed | The refactor keeps every status branch and adds subprocessor guidance only for model-not-found. The existing `.get` on the model lookup is unchanged and not a regression. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` | assessed | Non-string keys (`:37`) and unsupported types (`:44`) throw `IllegalArgumentException`, which keeps per-row errors working. On read, `isDecimalDouble` keeps decimals that are not exact doubles as exact values. NaN is handled earlier by `normalizeValue`. |
| `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` | assessed | Issue 3. The opt-in filter (`:59-77`) covers all three save locations. An empty set of complex params gives `None` and is handled when reading. The single `private[ml]` writer call site rules out bypassing the filter. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` | assessed | Legacy defaults are kept (`:230-243`); unset implicit defaults are omitted from both maps and survive a resave (`:245-267`); malformed or crafted payload guards are tested (`:344-411`). No -0.0 case (Issue 3). |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala` | assessed | Covers model-not-found guidance (`:13`) and confirms the other status gates still apply (`:28`). |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala` | assessed | Covers exact decimals (`:37`), wide numbers (`:115,141`), nested nulls (`:105`), and rejection of non-string keys and unsupported nested values (`:129`). |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` | assessed | Complex defaults that did not opt in stay persisted and guarded (`:21`); each reader uses its own session's policy (`:33`); persisted UDFs still need an explicit trusted opt-in (`:58`). |

## Issues

### Issue 1: Column-bound rejected controls fail the Spark task only after earlier rows were already sent
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala`
- **Line(s)**: 156-159 and 161-195; also `OpenAIChatCompletion.scala:141-150` and `OpenAIResponses.scala:170-178,203-214`
- **Description**: `rejectUnsupportedParams` checks each row with `getValueOpt(r, param).isDefined`, inside `getOptionalParams`, which runs lazily from `prepareEntity` on executors. Scalar and global values fail on the first row, before any request is sent. Column-bound values behave differently: null rows pass and the first non-null row throws. Affected bindings include:
  - `echo`, `bestOf` and `cacheLevel` columns on Chat
  - `seed` and other inherited Chat controls on Responses
  - a store column on Foundry
  - columns that trigger the stop, logprob or `reasoningEffort` conflict checks

  By the time a row throws, earlier rows in that partition, and rows in other partitions, may already have sent requests. With store enabled they may also have been stored. Spark task retries then send those requests again. Failing the task is approved, and the spec acknowledges retry duplication for cache-option and reasoning column encoding. But this scalar-versus-column asymmetry for the newly rejected controls is neither handled nor tested.
- **Risk**: Duplicate billed requests and stored responses, followed by a job failure on a late row, with nondeterministic side effects.
- **Suggested Fix**: In `transform` (and `transformSchema`), reject `Right(column)` bindings of controls the selected API never supports, on the driver, so the failure happens before any request. This is not a return to forwarding. Alternatively, document the column-binding behavior next to the approved fail-fast disclosure and add a test with a column-bound rejected control.

### Issue 2: Responses content-part encoding errors omit the content location that Chat reports
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala`
- **Line(s)**: 230-286 and 321-326 (compare `OpenAIChatCompletion.scala:428-443`)
- **Description**: `encodedMessageContent` passes the whole content array to `encodeStructuredValue` (`:326`) with no location wrapper. Encoder failures therefore surface without any `messages[i].content[j]` context. One example is a `MapType` with non-string keys inside a struct part, including inside `prompt_cache_breakpoint`, which produces "Content part map keys must have string type" (`:281`). Others are a struct length mismatch or an unexpected element type (`:233-245`). Chat's `encodedContentPart` prefixes the same errors with `messages[i].content[j] is invalid:`. The Responses row still fails per row without an HTTP request, because the exception is caught at `:538-539`. But the same breakpoint schema gives different diagnostics in Chat and Responses, and no test covers this case.
- **Risk**: Users with many messages and content parts cannot find the offending part. The behavior also diverges from the intended consistency of breakpoint handling between Chat and Responses.
- **Suggested Fix**: Encode Responses array content one element at a time, using the same location wrapper as Chat. Add a Responses test with a `map<int, …>` member inside `prompt_cache_breakpoint` that checks the errorCol message includes the location and that no request is sent.

### Issue 3: Precise metadata parsing loses the sign of negative-zero Double and Float params
- **Severity**: Low
- **File**: `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`
- **Line(s)**: 131-137 and 175-180
- **Description**: The writer now re-parses each `jsonEncode` result with `useBigDecimalForDouble = true`, and the reader does the same for `paramMap` and `defaultParamMap`. JSON `-0.0` becomes a `BigDecimal`, which has no negative zero, so it is re-rendered as `0.0`. As a result, any Spark `DoubleParam` or `FloatParam` set to `-0.0` on any `ComplexParamsWritable` stage, in every module, comes back as `+0.0`. This affects new saves and older artifacts read by the new reader. Previously, json4s rendered `JDouble(-0.0)` as `-0.0`. This conclusion comes from json4s and Jackson semantics; I did not run it.
- **Risk**: Very small in practice. It is still a silent value change in a core serializer shared by every module.
- **Suggested Fix**: Use the precise parse only where it is needed, for example only for numbers that cannot be represented exactly as a double. Alternatively, map a zero parsed from text with a leading minus sign back to `JDouble(-0.0)`. Add a save/load regression test for a `DoubleParam` holding -0.0.

### Issue 4: A non-object `reasoning` value is silently replaced when `reasoningEffort` is also set
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala`
- **Line(s)**: 203-214; also `HasOpenAIModelParams.scala:70-83,148-154` and `OpenAI.scala:62-76`
- **Description**: `mergeReasoning` keeps `params("reasoning")` only when it is a `Map`. Any other value falls back to `Map.empty` and is replaced by `{"effort": reasoningEffort}`. `reasoning` can be bound to a column (`setReasoningCol`), and column values are type-erased, so a string or other non-struct column passes through `normalizeValue` unchanged. Without `reasoningEffort`, that value is forwarded and the provider rejects it per row, so the problem is visible. With `reasoningEffort` set, the configured reasoning (for example summary or context settings) is dropped with no error and the request succeeds.
- **Risk**: A configured control is silently lost. Elsewhere this change deliberately rejects controls rather than pretending they took effect.
- **Suggested Fix**: In `mergeReasoning`, and preferably when the reasoning column is encoded, throw `IllegalArgumentException("reasoning must be an object")` when `reasoning` is present but is not a `Map`. Add a negative test.

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
