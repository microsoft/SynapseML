---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 2
timestamp: "2026-11-10T12:59:32Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4653d14c193f75e511c89d1527ba66f4a3010b33285c9f06f5d4b172e6c98ae2"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-1/task-unknown-attempt-29-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] This review was read-only and used only view, grep and glob on product source, tests and `pipeline.yaml`. I ran no commands or tests. I did not re-check the validation results stated in the prompt. I did not open any review or artifact directories.
- [x] **Mixin structure, checked with grep.** `HasOpenAIModelParams` is the only trait that extends `HasOpenAITextParams` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala:15`). Two traits extend `HasOpenAIModelParams`:
  - `HasOpenAITextParamsExtended` (`.../openai/OpenAIChatCompletion.scala:24`)
  - `HasOpenAITextParamsResponses` (`.../openai/OpenAIResponses.scala:41`)

  The concrete classes that mix them in are Chat (`OpenAIChatCompletion.scala:103`), Prompt (`OpenAIPrompt.scala:36`) and Responses (`OpenAIResponses.scala:132`). Foundry gets them by extending Chat.
- [x] **Persistence opt-in.** The hook defaults to an empty set (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:20-21`). `paramsToPersist` (lines 69-77) drops an opted-in param only when it is unset, and that one filter covers `paramMap`, the default map and complex-param locations. `OpenAI.scala:206` (adds `deploymentName`) and `OpenAI.scala:584` (adds `handler`) both build on `super`, so the sets combine correctly through trait linearization. Spark `Param` equality uses parent uid plus name, so checking membership in a `Set` is safe.
- [x] **Request-time rejection.** Unsupported params are rejected at `OpenAIChatCompletion.scala:141-150` and `OpenAIResponses.scala:170-177`. Every current caller resolves the internal placeholder payload keys `stop_sequences` and `log_probs_enabled` (`HasOpenAIModelParams.scala:117,129,161-195`), using the calls at `OpenAIChatCompletion.scala:149` and `OpenAIResponses.scala:177`.
- [x] **Prompt delegation.**
  - Params are forwarded to the inner stage by name with `hasParam` (`OpenAIPrompt.scala:690-693`).
  - `skipNoOpStore` (674-675) chooses the API with the same `getApiType` as service selection (684).
  - The new legacy-Chat model-only `require` is at 695-705.
  - Schema-only checks copy the stage before resolving globals (761-765). Full `transform` still modifies the stage itself (535); that was already the case.
- [x] **Default deployment value.** It is defined once, at `OpenAIDefaults.scala:12`, and used at `OpenAIChatCompletion.scala:126`, `OpenAIResponses.scala:147` and `OpenAIPrompt.scala:281`. The `setSeed` doc tells users that Responses rejects a global seed (`OpenAIDefaults.scala:63-64`).
- [x] **Usage mapping.** The detailed mappings are at `UsageUtils.scala:49-60`. The separate nested-path table is at 108-112. `ResponsesDetailed` is used only at line 109. `OpenAIPrompt.scala:352-359` picks between the two kinds of usage normalizer.
- [x] **Core JSON changes.**
  - `AnyJsonFormat` writes BigDecimal values exactly and reads a number as Double only when its decimal value is exact (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:31-32,52-57`).
  - The writer and reader parse with BigDecimal (`ComplexParamsSerializer.scala:128-137,171-180`).

  Both match the intended exact-decimal behavior, so I did not flag them.
- [x] **Test conventions and CI.**
  - Offline `AnyFunSuite` suites follow existing examples (`core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/FabricArtifactNamesSuite.scala:11`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/geospatial/AzureMapsTraitsSuite.scala:20`).
  - Calling `prepareEntity` and `prepareUrl` through reflection follows `OpenAIV1EndpointSuite.scala:23-27`.
  - The packages of the suites assigned to this part are all covered by CI legs: `fabric.**` and `param.**` (`pipeline.yaml:931-933`), `services.openai` (834), `stages` (919) and `core` (628, 709, 867).
- [x] I treated the explicitly approved behavior changes as approved, not as findings: Chat rejecting echo, bestOf and cacheLevel; Responses forwarding temperature and top_p for gpt-5 names; and the store semantics.
- [x] I did not flag the already-existing duplication of the Chat and Responses encoders. This PR keeps them in step: the Chat null guard (`OpenAIChatCompletion.scala:268-286`) matches Responses (`OpenAIResponses.scala:286-299`), and both reuse `HasMessagesInput.validatePromptCacheBreakpoint` (`OpenAI.scala:42-58`).
- [ ] I did not run or execute anything at runtime: this lens is read-only and no commands were allowed.

## Issues

### Issue 1: One shared model-params trait gives each stage public setters for the other API's controls, which then always fail at transform time
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala`
- **Line(s)**: 15, 69-123, 144-155. Related: `OpenAIChatCompletion.scala:24,147`, `OpenAIResponses.scala:41,170-171`, `OpenAIPrompt.scala:690-693`.
- **Description**: The new public trait `HasOpenAIModelParams` is mixed into both the Chat family and the Responses family. It contains params whose own descriptions say they belong to one API:
  - `reasoning`, `include` and `truncation` are described as Responses settings.
  - `logitBias` and `stopSequences` are described as Chat settings.

  Each concrete class then rejects the other API's params while building the request. Chat rejects `reasoning`, `include` and `truncation`. Responses rejects `stopSequences` and `logitBias`.

  The trait also gives `stopSequences` and `logProbsEnabled` `payloadName` values that are not real wire field names (`stop_sequences`, `log_probs_enabled`). They are correct only because every current `getOptionalParams` override remembers to call `resolveStopSequences` and/or `resolveLogProbParams` afterwards.

  This is a new design choice in this PR. The existing rejection of inherited Chat controls (`n`, `echo`, `seed`, and so on) in Responses was already there and is approved, so it is not part of this finding.
- **Risk**: `OpenAIChatCompletion` and `OpenAIResponses` are `Wrappable`, so the generated Python wrappers include these setters too. The affected setters are `OpenAIChatCompletion.setReasoning/setInclude/setTruncation(+Col)` and `OpenAIResponses.setLogitBias/setStopSequences(+Col)`.
  - **Permanent public API:** These setters can never succeed, yet they become public Scala and Python API, and the param names become part of the saved format. AGENTS.md requires keeping public JVM signatures and serialized param shapes, so removing them later would be a breaking change.
  - **Late failure:** A misconfiguration is not caught by the setter or by `transformSchema`. Save and load round-trip it without complaint, and it only fails a Spark task when the stage runs.
  - **Placeholder keys can leak:** If a future subclass overrides `getOptionalParams` without calling the resolvers, the placeholder keys go into the request body.
- **Suggested fix**: Split the trait into three:
  - A common trait: `promptCacheKey`, `promptCacheRetention`, `safetyIdentifier`, `serviceTier`, `metadata`, `logProbsEnabled`, `topLogProbs`.
  - A Chat trait: `logitBias`, `stopSequences`.
  - A Responses trait: `reasoning`, `include`, `truncation`.

  Chat and Foundry would mix in common plus Chat, Responses would mix in common plus Responses, and `OpenAIPrompt` would mix in all three. Prompt currently forwards params by name using `completion.hasParam` (`OpenAIPrompt.scala:690-693`). After a split, that filter would silently drop a param the selected service lacks, so Prompt must explicitly reject set params that the chosen `apiType` does not support.

  Smaller alternative: keep the single trait, but check for unsupported params in `transformSchema` so they fail before any task runs. Also move the placeholder-key resolution into a `final` method on the trait so subclasses cannot skip it.

### Issue 2: `internalServiceType`, a Fabric URL route value, is reused as a subclass type check for defaults and rejection rules
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`
- **Line(s)**: 120-127, 141-146. Related: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala:56`; the existing route use is in `CognitiveServiceBase.scala` around 272-276.
- **Description**: Until now, `internalServiceType` was used only as a segment of the Fabric URL. The base class now compares its value in two places:
  - The constructor does `if (internalServiceType == "openai") setDefault(deploymentName -> ...)`. This is a virtual call made while the base class is still being constructed.
  - `getOptionalParams` checks for `"foundry"` to choose which params to reject.

  This hierarchy normally customizes behavior by overriding methods. `AIFoundryChatCompletion` already overrides `sharedTextParams`, `setCustomServiceName` and `prepareUrlRoot` (`AIFoundryChatCompletion.scala:28-45,58-64`).
- **Risk**: Request validation and default-deployment behavior now depend on a routing value. Changing a route value, or adding a subclass that routes like `"openai"` but needs a different default or different rejection rules, would silently change behavior. The constructor check is also fragile. It works today only because the override is a `def` that returns a constant; if a subclass overrode it with a `val`, the check would see `null` while the base class is being built. `OpenAIRuntimeDefaultsSuite` asserts that Foundry has no default deployment, which partly covers this.
- **Suggested fix**: Add explicit protected methods and let Foundry override them:
  - `protected def defaultTextDeployment: Option[String] = Some(OpenAIDefaults.DefaultTextDeploymentName)`, written as a `def` that returns a constant so calling it from the constructor stays safe.
  - `protected def unsupportedChatParams: Seq[ServiceParam[_]] = Seq(echo, cacheLevel, bestOf)`, which Foundry overrides with `Seq(store)`.

  `internalServiceType` would then be used only for routing.

### Issue 3: Prompt-cache settings are forwarded by two different mechanisms, and the general JSON normalizer has a cache-specific name
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`
- **Line(s)**: 358-385, 479-504. Related: `HasOpenAIModelParams.scala:73,144-150`, `AIFoundryChatCompletion.scala:38`, `OpenAIChatCompletion.scala:77-96`, `OpenAIResponses.scala:106-124`.
- **Description**: `promptCacheOptions` is declared in `HasOpenAITextParams`. It is forwarded through `sharedTextParams`, and `getOptionalParams` checks `if (param == promptCacheOptions)` to treat it specially. Because each `sharedTextParams` override replaces the base list entirely, the param had to be added to four separate lists.

  The related cache params `promptCacheKey` and `promptCacheRetention` live in `HasOpenAIModelParams` instead. They go through the separate `modelParams` list, which normalizes every value the same way.

  The helper `normalizeCacheOptionValue(value, paramName)` is in fact the general JSON normalizer for all model params, including `reasoning` and `metadata` (`HasOpenAIModelParams.scala:73,148`).
- **Risk**: The next cache-related param has two plausible homes. If an override omits `promptCacheOptions`, that stage silently stops sending `prompt_cache_options`. The cache-specific name also hides the fact that unrelated params go through the same validation.
- **Suggested fix**: Move `promptCacheOptions` into `HasOpenAIModelParams` and add it to `modelParams`. Then remove it from the four `sharedTextParams` lists and delete the equality special case. Grep shows `HasOpenAIModelParams` is the only trait that extends `HasOpenAITextParams`, so the move does not change which stages expose the param. Rename the helper to something like `normalizeJsonParamValue`, or call `OpenAIJsonUtils.normalizeValue` directly.

### Issue 4: `UsageMappings.ResponsesDetailed` only works with a separate nested-path table and is visible more widely than it needs to be
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala`
- **Line(s)**: 49-60, 98-112. Related: `OpenAIPrompt.scala:352-359`.
- **Description**: `ResponsesDetailed` contains dotted keys such as `cache_write_tokens_details.ttl_30m_tokens`. These can only be resolved through the separate `inputDetailPaths` map that `normalizeResponsesDetailed` passes in. The public `normalize(usage, mapping)` treats detail keys as literal field names, as its doc at line 101 says.

  The mapping is `private[openai]`, but it is used only inside `UsageUtils` (line 109). `ChatCompletionsDetailed` has the same type but is used with the public `normalize` (`OpenAIPrompt.scala:355`). So the two "Detailed" mappings look alike but must be used through different functions.
- **Risk**: Code elsewhere in the package could call `UsageUtils.normalize(col, UsageMappings.ResponsesDetailed)`. That compiles, but it fails Spark analysis because no struct field has that literal dotted name. The list of keys and the path table can also change independently and drift apart.
- **Suggested fix**: Make `ResponsesDetailed` `private` to `UsageUtils`. Alternatively, build both the key list and the path table from one private list of `(outputKey, path)` pairs, so the public `UsageFieldMapping` shape stays the same.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala` | assessed | Its `sharedTextParams` override must also list `promptCacheOptions` (line 38; Issue 3). Its `"foundry"` value (56) now also drives the parent's default and rejection rules (Issue 2). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala` | assessed | One shared trait holds both APIs' params, and each class rejects the other API's params only at transform time. Placeholder payload names need later resolution (Issue 1). The general normalizer has a cache-specific name (Issue 3). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala` | assessed | Only doc text changed: exact BigDecimal and `decimal.Decimal` schema values (26, 54). No change to structure or public API. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` | assessed | Opt-in sets combine through `super` (206, 584). `promptCacheOptions` placement and the equality special case (358-385, 479-504; Issue 3). The breakpoint validator is shared by Chat and Responses (42-58). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | assessed | `internalServiceType` checks in the constructor and in rejection logic (122-127, 141-146; Issue 2). Responses-only params rejected (147; Issue 1). Null-guard and encoder logic kept in step with Responses (268-286). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` | assessed | The default deployment is one `private[openai]` constant (12), used by three stages. No global default is created. The `setSeed` doc discloses that Responses rejects a global seed (63-64). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` | assessed | Forwarding params by name (690-693) interacts with the shared trait (Issue 1). Schema-only checks use a copy (761-765), while full `transform` still modifies the stage (already the case). `skipNoOpStore` matches service selection (674-684). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | assessed | Chat-only params rejected at request time (170-171; Issue 1). `store` defaults to false (173). Placeholder keys resolved (177). Breakpoint validator reused (around 472). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAISchemas.scala` | assessed | A `private[openai]` object extends the wire schemas without changing the public case classes, which matches the AGENTS.md rule on public shapes. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala` | assessed | Detailed mappings (49-60). A separate nested-path table that must be used together with the mapping (108-112; Issue 4). The public `normalize` contract is unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` | assessed | Offline `AnyFunSuite`, following existing examples. Checks the field-order prefix of the extended Responses schemas. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` | assessed | Extends `TestBase`. Reflection follows `OpenAIV1EndpointSuite` (26-29). Covers defaults, precedence, Foundry having no default (partly covers Issue 2) and schema-only checks not modifying the stage. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtilsSuite.scala` | assessed | Adds a test for literal dotted keys in the public `normalize`. Nested-path behavior is tested only in `OpenAIPromptCacheSuite.scala:196`, which is context for Issue 4. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` | assessed | `assertModelStatus` is pulled out as a `private[fabric]` method so it can be tested. Subprocessor guidance appears only in the ModelNotFound message. No layering change. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` | assessed | Exact BigDecimal write (31-32). Reads Double only when the decimal value is exact (52-57). The change to this shared utility stays within the intended exact-decimal behavior. |
| `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` | assessed | Public hook that defaults to empty (20-21). One filter drops opted-in params only when unset (69-77). The BigDecimal writer and reader keep Spark metadata semantics (128-137, 171-180). |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` | assessed | Adds `ImplicitDefaultParamTest`, separating legacy saved defaults from opted-in unset defaults and explicitly set values. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala` | assessed | Offline `AnyFunSuite`, consistent with `FabricArtifactNamesSuite`. Runs in the `fabric.**` misc CI leg (`pipeline.yaml:933`). Tests both status paths. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala` | assessed | Adds a decimal round-trip. Removing BigDecimal from the unsupported list matches the writer change. Runs in the `param.**` CI leg. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` | assessed | Confirms that complex defaults outside OpenAI are still persisted and that the Java-deserialization guard is still in place. The opt-in applies only to the OpenAI handler. |

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
