---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 26
stage_attempt: 1
round: 2
timestamp: "2026-11-10T06:29:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d290e79e670378a08e81e1e8e2b33849949970478831452be4b1a8aafdd49ae8"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-26-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] All 48 frozen-manifest paths were checked against the supplied diff and assessed for this lens. They are 10 cognitive production files, 3 core production files, 17 cognitive Scala test files, 4 core Scala test files, 9 Python test files and 5 notebooks. After context compaction, I verified production findings against the working tree, which includes the staged patch. I assessed test and notebook paths at the level of placement, fixtures and conventions.
- [x] `internalServiceType` was only a URL routing segment. It builds `/cognitive/<type>/` at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala:272-276`. This PR is the first to branch on it for behavior, at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala:122-127` and `:141-147`. The Foundry override is a constant `def` at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala:56`. There is no Foundry `getOptionalParams` override.
- [x] The persistence hook is sound:
  - The empty opt-in and `paramsToPersist` filter are at `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:17-24,68-76`.
  - The `deploymentName` and `handler` opt-ins compose through `super` at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala:205-206,584`.
  - The `handler` default is recreated by the constructor at `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala:24-37`.
  - `OpenAIPrompt` has no `HasHandler` (`OpenAIPrompt.scala:34-41`), so leaving the handler opt-in on `OpenAIServicesBase` is consistent.
  - All mixers already use `ComplexParamsWritable`, so there is no writer conflict. I treat placing the hook on `HasOpenAITextParams` as an acceptable composition choice.
- [x] The append-only response schema extension (`OpenAISchemas.scala:79-140`) adds no duplicate fields to the case classes (`OpenAISchemas.scala:26-76`). The legacy converters stay positionally prefix-compatible (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/schema/SparkBindings.scala:24-28`), and public case-class signatures are unchanged, as AGENTS.md requires.
- [x] Breakpoint validation reuses the existing serializer. `OpenAI.scala:42-58` calls `anyFormat.write`, which throws `IllegalArgumentException` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:17-45`). It runs inside per-part validation (`OpenAIChatCompletion.scala:392-407`), and the public transform converts those failures to per-row errors (`OpenAIChatCompletion.scala:176-226,556-566`).
- [x] Validating by throwing inside `isValid` (`OpenAI.scala:365-368`, `HasOpenAIModelParams.scala:74-77`) has precedent at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/speech/TextToSpeech.scala:48-51`.
- [x] A grep for `.rdd`, `SparkContext` and `mapPartitions` found no RDD or SparkContext use added to the changed production Scala. The Python `SparkContext._active_spark_context._jvm` use at `HasOpenAIResponseSchema.scala:65` follows the existing pattern at `OpenAIPromptPythonOverrides.scala:146`.
- [x] The notebook's `openai` SDK import (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb`, cell near line 870) uses the version pinned in `environment.yml:56` and `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala:196`. The cache-hit assertions are explicitly described as qualification checks near line 1083.
- [x] I did not flag the approved compatibility changes: Chat and Prompt Chat failing fast on echo/bestOf/cacheLevel, and Responses forwarding temperature/top_p.
- [ ] Runtime validation was not performed. This was a read-only review with no commands or tests run; I relied only on the supplied validation statement.

## Issues

### Issue 1: API-specific controls in one shared trait add public setters that always fail
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala
- **Line(s)**: 70-123, 148-159. See also `OpenAIChatCompletion.scala:24,110-116,141-147` and `OpenAIResponses.scala:41,170-172`.
- **Description**: `HasOpenAIModelParams` is mixed into both the Chat trait (`HasOpenAITextParamsExtended`) and the Responses trait (`HasOpenAITextParamsResponses`).
  - It defines Responses-only `reasoning`, `include` and `truncation`, and Chat-only `logitBias` and `stopSequences`.
  - Each stage then rejects the other API's controls row by row inside `getOptionalParams`.
  - The new Chat `store` is declared in the `OpenAIChatCompletion` class body, so `AIFoundryChatCompletion` inherits it and always rejects it (line 143).

  Because codegen wraps every param, the PR adds new public Scala and generated Python setters that can only fail the task:
  - `OpenAIChatCompletion` and `AIFoundryChatCompletion`: `setReasoning`, `setInclude`, `setTruncation` and their `*Col` variants.
  - `AIFoundryChatCompletion`: `setStore`.
  - `OpenAIResponses`: `setLogitBias` and `setStopSequences`.

  The repository already has per-API traits; this design bypasses them. The inherited-union pattern exists already (echo/bestOf/cacheLevel). This is a new-surface concern, not a demonstrated runtime regression.
- **Risk**: AGENTS.md asks contributors to keep existing public JVM signatures and serialized param shapes, so these setters would be hard to remove after release. Misconfiguration also surfaces late, as an executor task failure during request construction, rather than at configuration time. The generated API docs would advertise controls the stage can never honor.
- **Suggested Fix**: Split the trait into three:
  - A common trait: cache key/retention, safety identifier, service tier, metadata and log-prob controls.
  - A Chat trait (`logitBias`, `stopSequences`) mixed into `HasOpenAITextParamsExtended`.
  - A Responses trait (`reasoning`, `include`, `truncation`) mixed into `HasOpenAITextParamsResponses` and `OpenAIPrompt`.

  Prompt forwards only params that the child stage has (`OpenAIPrompt.scala:690-693`). So Prompt also needs an explicit rejection of Responses-only controls when routing to Chat or Foundry; otherwise they would be silently dropped. If the union is intentional instead, validate scalar-set unsupported controls on the driver in `transform`/`transformSchema` and document which stages do not support them.

### Issue 2: A URL routing tag is used as a behavior switch inside the Chat base class
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala
- **Line(s)**: 122-127, 141-147. See also `AIFoundryChatCompletion.scala:56` and `CognitiveServiceBase.scala:272-276`.
- **Description**: `internalServiceType` exists to build an internal URL segment. The parent class now branches on that string in two places:
  - In its constructor: `if (internalServiceType == "openai") setDefault(deploymentName ...)`.
  - In `getOptionalParams`: `if (internalServiceType == "foundry")`.

  Foundry-specific policy therefore lives in the parent and depends on a virtual call during parent construction. The shared rejection at line 147 also reports Foundry failures as "Chat Completions". This is new in this PR; there is no current defect.
- **Risk**: Renaming the routing segment, or adding a Chat subclass with a different segment, would silently change default deployment selection and parameter policy. If a subclass changed the override to a `val`, it would be null during parent construction and the default would be dropped without an error.
- **Suggested Fix**: Add dedicated protected hooks with constant `def` overrides in `AIFoundryChatCompletion`, for example `builtInDeploymentDefault: Option[String]`, `unsupportedRequestParams` and `requestApiLabel`. Alternatively, override `getOptionalParams` in the Foundry class. Keep `internalServiceType` for URL routing only.

### Issue 3: Payload assembly now has competing allowlists and denylists, and some payload names are not wire names
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala
- **Line(s)**: 108-126, 170-173. See also:
  - `OpenAI.scala:62-71,381-385,478-504`
  - `HasOpenAIModelParams.scala:117,129,148-154`
  - `OpenAIChatCompletion.scala:78-97,232-243`
  - `AIFoundryChatCompletion.scala:28-45`
- **Description**: Several inconsistencies sit around request-payload assembly:
  - **Duplicated lists**: four existing `sharedTextParams` copies each needed the same `promptCacheOptions` edit.
  - **A second mechanism**: the PR adds a separate, non-overridable `modelParams` list plus per-stage `rejectUnsupportedParams` denylists.
  - **Contradicting lists in Responses**: `n`, `echo`, `stop`, `cacheLevel`, `presencePenalty`, `frequencyPenalty` and `bestOf` remain in the forwarding allowlist but are always rejected first. The allowlist no longer describes what is actually forwarded.
  - **Two normalization policies**: the base builder normalizes only `param == promptCacheOptions` inline (`OpenAI.scala:501`). Model params all go through `normalizeCacheOptionValue`, a misleading name since it also normalizes reasoning, metadata and logitBias and converts struct Rows to maps.
  - **Two null conventions for structs**: struct-typed option columns keep null fields as explicit JSON nulls (`OpenAI.scala:68-69`), while content-part and breakpoint structs omit them (`OpenAIChatCompletion.scala:232-243`). Neither convention is documented, and the options-struct null case is untested (`OpenAIPromptMultimodalRequestSuite.scala:202-230`).
  - **Non-wire payload names**: `log_probs_enabled` (line 129) and `stop_sequences` (line 117) are internal intermediate keys. They are correct only because every stage remembers to call `resolveStopSequences` and `resolveLogProbParams`.
- **Risk**: Each future param needs edits across four lists and two mechanisms. A stage or override that reuses the builder without the resolve steps would leak non-wire keys. Readers cannot tell what a stage supports from its allowlist.
- **Suggested Fix**:
  - Use one declarative per-stage spec (forwarded, rejected, renamed) consumed by a single builder.
  - Drop the rejected entries from the Responses allowlist.
  - Rename the normalizer (for example, `normalizeJsonParamValue`) and apply it uniformly.
  - Document the struct-null convention for options.
  - Keep `payloadName` as the wire name, and have the resolve steps match on param identity.

### Issue 4: `store` is three unrelated params reconciled by name
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 196-210, 284, 674-693. See also `OpenAIChatCompletion.scala:110-116,148` and `OpenAIResponses.scala:76,145-149,173`.
- **Description**: Three separate `store` params exist with different docs and defaults:
  - `HasOpenAITextParamsResponses`, defaulting to false.
  - `OpenAIChatCompletion`, new and with no default.
  - `OpenAIPrompt`, defaulting to false.

  Prompt reconciles them through name-based forwarding plus a `param.name == store.name` special case (`skipNoOpStore`) inside its generic forwarding loop. Responses defines its default twice: `setDefault(store -> Left(false))` and `Map("store" -> false)` for null-column rows. As a result, `setStore(false)` sends `store:false` on direct Chat but omits it on Prompt Chat. That is not a defect under the approved contract, but the asymmetry is undocumented.
- **Risk**: Store semantics are spread across three classes and kept consistent only by convention. These include the default, null-column handling, Foundry rejection and no-op omission.
- **Suggested Fix**: Define `store` once in a shared trait with a single documented contract. Express each API's policy as an overridable hook in that service's request builder rather than in Prompt's forwarding loop: omit scalar false for Chat, fall back to false for Responses, and reject for Foundry.

### Issue 5: The detailed usage mappings differ in visibility and hide nested-path resolution
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala
- **Line(s)**: 49-60, 98-112
- **Description**: `ChatCompletionsDetailed` is new public API, while `ResponsesDetailed` is `private[openai]`. `ResponsesDetailed` encodes nested fields as dotted pseudo-keys, such as `cache_write_tokens_details.ttl_30m_tokens`. Those keys resolve only through a separately maintained path map in `normalizeResponsesDetailed`. The public `normalize(usage, mapping)` documents literal keys, so passing this mapping to it would silently return null TTL counters. The dotted strings also become user-visible keys in `input_token_details`.
- **Risk**: The mapping and path data can drift apart, and the same feature has an asymmetric public surface.
- **Suggested Fix**: Give both detailed mappings the same visibility. Keep nested-path data with the mapping, either through `(outputKey, path)` entries or by deriving paths from the key, so one internal `normalize` handles both.

### Issue 6: Cache-feature tests are split across unrelated suites and conventions
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala
- **Line(s)**: 69, 73, 160, 179, 202, 233. See also:
  - `OpenAIPromptCacheSuite.scala:24,43-44`
  - `OpenAIResponseSchemaSuite.scala:68-70`
  - `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py`
- **Description**:
  - HTTP tests for prewarm, cache options, provider cache errors and breakpoints were added to the multimodal request suite. It is the only user of the new `OpenAIPromptHttpTestBase`.
  - The dedicated `OpenAIPromptCacheSuite` extends `TestBase` directly and calls the protected `prepareEntity` through Java reflection.
  - `OpenAIResponseSchemaSuite` keeps its own local `HttpServer` helper.
  - The OpenAI-specific live test sits under `synapsemltest/services/`, while every other OpenAI Python test is under `synapsemltest/services/openai/`.
- **Risk**: The tests are harder to find and maintain. String-based reflection bypasses compile-time checks, and the HTTP fixtures are diverging.
- **Suggested Fix**: Move the cache HTTP tests into `OpenAIPromptCacheSuite` on top of `OpenAIPromptHttpTestBase`. Replace the reflection with a `private[openai]` test hook; Chat and Responses already expose `protected[openai] prepareEntity`. Move the live test to `synapsemltest/services/openai/`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

### Issue 6
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
- **How verified**: Not applicable

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

## Driver Reconciliation Addendum - 2026-11-10

The original review and verdict above are preserved. These dispositions apply
to the reported findings, not to the incomplete attempt's overall qualification.

| Issue | Disposition | Rationale and evidence |
|---|---|---|
| 1 | Design recommendation; not a demonstrated correctness defect | The shared parameter surface deliberately validates support in the selected request builder. Existing negative tests cover API-specific rejection. The notebook's control table distinguishes Chat-only and Responses-only controls, and its local-validation paragraph now discloses task failure, sparse-column behavior and retry risk. Splitting public traits is not required to fix a supported request and would expand this patch. |
| 2 | Nonblocking future-maintenance recommendation | The review identifies no current defect: Foundry's routing override is a constant `def`. A hypothetical future mutable override or routing rename is not a regression in the current patch. No additional policy-hook abstraction was added. |
| 3 | Documentation clarified; structural refactor deferred | The notebook now distinguishes explicit null fields in top-level option structs from omitted null fields in breakpoint objects. The review identifies no request with an incorrect wire key: existing builders run the stop/log-prob resolution steps, and direct/Prompt payload tests cover their output. Consolidating lists and renaming internal helpers are maintenance suggestions, not necessary behavioral repairs. |
| 4 | Documentation clarified | The notebook now states the direct-Chat versus Prompt no-op scalar-false difference and the Foundry boundary. The review explicitly recognizes this asymmetry as valid under the contract; existing copy/save/load and request tests cover it. |
| 5 | Nonblocking internal-design recommendation | `ResponsesDetailed` is package-private and used with its dedicated normalizer, not exposed as a supported argument to the public literal-key normalizer. Existing detailed-usage tests check TTL counters. No missing counter or supported public call failure was demonstrated. |
| 6 | Nonblocking test-organization recommendation | Moving tests or replacing the existing reflection helper does not repair production behavior. The suites run and assert request payloads and errors; a fixture/layout refactor would enlarge the change without addressing a demonstrated failure. |

No finding has been silently deleted, and the raw `issues_found` result remains.
Attempt 26 is incomplete and is not being counted as a clean pass.
