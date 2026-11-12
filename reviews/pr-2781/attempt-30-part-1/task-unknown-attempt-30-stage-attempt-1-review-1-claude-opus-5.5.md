---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 30
stage_attempt: 1
round: 1
timestamp: "2026-11-11T12:01:20Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "35f2691deb15e448c3f08cccd58b9027e278af1c29b8359eb41884e7cb2d69f1"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-30-part-1/task-unknown-attempt-30-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and method.** I assessed all 20 assigned paths against the frozen partition patch `reviews/pr-2781/inputs30/part1.patch` and the current product source.
  - The patch included deleted hunks: the removed Prompt `store` guard in `validateResponsesOnlyParams`, the removed `isExactDouble || isBinaryDouble` read branch, and the Fabric status match that was previously inline.
  - Only read-only view, grep and glob were used. I did not execute any commands, builds or tests. The local validation results in the plan were not re-run here.
  - I did not read any sibling or prior review output.
- [x] **Luna default scope.**
  - `OpenAIChatCompletion.scala` lines 125–126: the default is set only when `internalServiceType == "openai"`.
  - Foundry overrides that `def` to return "foundry", so `AIFoundryChatCompletion` gets no Luna default.
  - `OpenAIPrompt.scala` adds `deploymentName -> Left(OpenAIDefaults.DefaultTextDeploymentName)` to its defaults.
  - Precedence is preserved. `getValueOpt` resolves `get(p).orElse(getDefault(p))`, and `transferGlobalParamsToParamMap` fills only unset params, so explicit scalar, column and global deployments all win.
  - A null column binding resolves to None and does not fall back to Luna. `OpenAIRuntimeDefaultsSuite` covers this ("a null explicitly bound deployment never falls back to Luna").
- [x] **Prompt alias and legacy Chat rejection.** `OpenAIPrompt.scala` lines 695–705 apply the model alias only when `!isSet(deploymentName) && !hasAIFoundryModel`. Model-only selection on legacy Chat requires `customUrlRoot`. The `model` param (`AIFoundryChatCompletion.scala` line 19) has no default, so this `require` cannot fire for configurations that leave `model` unset.
- [x] **Store semantics.**
  - Prompt: `skipNoOpStore` (`OpenAIPrompt.scala` lines 674–675) together with `mayStoreResponses` drops only scalar false, including restored values, for Chat and Foundry. True and column bindings are still copied. `responseIdCol` is gated by the per-row store flag at runtime, and the schema matches through `replaceOrAppendField`.
  - Chat: `OpenAIChatCompletion.scala` lines 141–150 reject any resolved `store` for Foundry. Non-Foundry Chat rejects echo, cacheLevel and bestOf (the approved fail-fast change). Store is forwarded only when it resolves.
- [x] **Breakpoint validation.**
  - `OpenAI.scala` lines 43–59 apply finite checks through `OpenAIJsonUtils.normalizeValue`, then call `anyFormat.write`.
  - `AnyJsonFormat.write` (`UntypedArrayParam.scala` lines 17–20 and 45) throws `IllegalArgumentException` for Timestamp, Date and binary values. The validator wraps these with the content location plus `prompt_cache_breakpoint`. Non-object markers are rejected.
  - Chat runs `encodedContentPart` (lines 428–451), then `validateContentPart` (lines 396–412). `structuredMessageValidationError` (lines 561–571) catches `IllegalArgumentException` and turns it into per-row errors.
  - Responses has the equivalent catch at `OpenAIResponses.scala` lines 540–541. Invalid rows therefore skip HTTP.
- [x] **Cache options.**
  - The `promptCacheOptions` `isValid` runs the parameter-named finite check before scalar persistence.
  - `HasOpenAITextParams.getOptionalParams` normalizes column values at encode time.
  - `normalizeValue(Row)` keeps explicit struct null members, and a whole-column null resolves to None, so the field is omitted.
  - Model params are normalized with their own names (`HasOpenAIModelParams.scala` lines 151–153).
- [x] **Responses controls.**
  - Unsupported inherited Chat controls are rejected, including a seed that resolves from globals.
  - `store=false` is merged as the default.
  - `reasoning` is checked to be an object before `reasoningEffort` is merged, and the two aliases must agree.
  - The removed gpt-5 temperature/top_p filter matches the approved keep-forwarding decision.
- [x] **Persistence.**
  - `ComplexParamsSerializer.scala` line 21 defines the empty opt-in. `paramsToPersist` (lines 87–95) filters `extractParamMap()`, `defaultParamMap` and `getComplexParamLocations` (lines 77–85) only when the param is unset.
  - The reader loads complex params from the saved `complexParamLocs` metadata (lines 216–235), not from the fresh instance. Explicitly saved handlers and older saved handlers therefore still load and stay subject to the existing guard.
  - Linearization gives Chat, Responses and Foundry {deploymentName, handler} (`OpenAI.scala` lines 207 and 585).
  - Every class that mixes in `HasOpenAITextParams` already used `ComplexParamsWritable` (`OpenAIPrompt.scala` line 41, and the services via `CognitiveServicesBase`), so no writer is swapped.
- [x] **Exact decimals and signed zero.**
  - `UntypedArrayParam.scala` lines 31–32 add BigDecimal writes. Line 55 narrows reads to `isDecimalDouble` only. Doubles encoded by anyFormat (shortest decimal) still read back as Double, and values outside that, such as 2^63, stay exact.
  - `ComplexParamsJson.parseExact` and `preserveSignedZero` keep IEEE -0.0. The two parses have the same structure, so the field-wise zip is safe.
  - Spark's per-param `jsonDecode` re-parses rendered JDecimal text, and `JNothing` handling matches `parseMetadata`.
- [x] **Usage and Fabric.**
  - Prompt `usageNormalizerFor` (`OpenAIPrompt.scala` lines 352–359) uses the private detailed mappings. The public `UsageMappings` are unchanged.
  - `OpenAIFabricSetting.scala` lines 40–68 match the deleted inline match branch for branch; only the ModelNotFound guidance is extended. The new overload is `private[fabric]`, and the call site at `OpenAI.scala` line 630 is unchanged.
- [ ] **Non-blocking observations (not counted as issues):**
  - Foundry's rejection of `reasoning`, `include` and `truncation` uses the "Chat Completions" label (`OpenAIChatCompletion.scala` line 147). This is cosmetic and arguably accurate for the chat protocol.
  - New typed response-schema fields in `OpenAISchemas.scala` add surface for provider type drift during JSON parsing. This is speculative and cannot be verified without a live provider.
  - `normalizeValue` assumes Rows that carry a schema (`OpenAI.scala` line 70). Spark struct values satisfy this. A hand-built schema-less Row inside a scalar map would raise an NPE, which falls outside the spec's set-time validation promise.
- [ ] **Pre-existing behavior, not regressions:** `transform` mutates the Prompt by transferring globals (`OpenAIPrompt.scala` line 535), and Chat's `isContentFiltered` uses `choices.head`. Notebook and PR-description disclosure belongs to other partitions and was not assessed here.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala` | assessed | Adds `promptCacheOptions` to the Foundry text params; normalized by the inherited `getOptionalParams`. No Luna default (service type "foundry"). `model` (line 19) has no default. Any resolved store is still rejected. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIModelParams.scala` | assessed | New ServiceParams with correct payload names. Parameter-named normalization (lines 151–153). `rejectUnsupportedParams` uses resolved values, including globals (156–159). stop/stopSequences and logprob alias conflicts are rejected (161–194). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala` | assessed | Documentation only. BigDecimal and `decimal.Decimal` claims match the `AnyJsonFormat` write cases. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` | assessed | Breakpoint validator and `OpenAIJsonUtils` (43–77). `promptCacheOptions` is validated at set time and normalized at encode time. Implicit-default composition (207, 585). Fabric call site unchanged. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | assessed | Luna default gated to "openai" (125–126). Approved echo/cacheLevel/bestOf rejection and Foundry store rejection (141–150). Per-row breakpoint and message validation with no request for invalid rows (396–412, 428–451, 561–571). |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` | assessed | Default deployment constant plus seed documentation. No global default is materialized; the runtime-defaults suite covers this. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` | assessed | Luna default. `mayStoreResponses`/`skipNoOpStore` drop only no-op false. Alias and legacy-Chat `require` (695–705). Store-gated response IDs. Non-mutating schema copy (761–765). Schema and runtime columns agree. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | assessed | Inherited controls rejected, including a global seed. `store=false` default. Reasoning object/alias validation. Approved sampling forwarding. Per-row validation at 540–541. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAISchemas.scala` | assessed | Fields are appended only, so ordinal-based converters still work. Provider type-drift note is speculative only. |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtils.scala` | assessed | Detailed mappings are private and reference fields that exist in the extended schemas. Public mappings and normalization semantics are unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAICoreOfflineSuite.scala` | assessed | New test (patch line 1122) checks that explicit sampling is preserved alongside the text/reasoning merge, consistent with the approved forwarding. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` | assessed | Tests cover the Luna fallback, scalar/column/global precedence, null bindings, aliases, Foundry and embedding selection, legacy-Chat rejection, `customUrlRoot`, and schema-only global resolution without persisting credentials. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/UsageUtilsSuite.scala` | assessed | Test that custom detail mappings keep literal dotted field names. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` | assessed | Pure extraction into a `private[fabric]` overload. Every status branch is unchanged except the extended ModelNotFound guidance. |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` | assessed | BigDecimal write cases (31–32). Reads use `isDecimalDouble` only (55), which keeps non-shortest values exact. Non-string keys and unsupported values throw `IllegalArgumentException`. |
| `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` | assessed | Opt-in filter applies only to unset params, across paramMap, defaultParamMap and complex locations. The reader loads from saved locations. The precise parse preserves -0.0 and existing metadata semantics. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` | assessed | Tests cover legacy default persistence, omission of unset opted-in defaults from both maps, signed-zero save/load/resave, and older signed-zero metadata. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSettingSuite.scala` | assessed | Tests the ModelNotFound guidance and that the remaining status gates are unchanged, through the extracted overload. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala` | assessed | Test that exact Scala and Java decimals survive a JSON roundtrip. |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` | assessed | Confirms that complex defaults outside the opt-in remain persisted and guarded. |

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate, satisfying trusted repository six-lens requirement.",
    "model": "Installed review-code complementary-family rule and current task-tool live catalog.",
    "schedule": "Trusted public SynapseML PR-loop sequential override; retained human-approved three-part split.",
    "budget": "Human authorization for exactly one additional complete pass, total cap30; no retry or reset."
  },
  "changeSize": "Broad 49-path, 300518-byte cross-module source/test/notebook patch.",
  "cyclomaticComplexity": "Moderate: branching in request validation and persistence; existing bounded request and Spark mechanisms remain, with no new scheduler or lifecycle state machine.",
  "rationale": "Six independent lenses assess the entire frozen patch, each via three whole-file partitions. Broad size is handled by exact disjoint coverage, not omitted hunks or partial signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoningEffort": "max",
  "modelEligibility": "Current task catalog exposes claude-opus-5.5 with max reasoning; newest available Opus is complementary to the OpenAI primary. Runtime identity must match.",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md and explicit pass30 approval with retained split bounds",
  "mode": "sequential",
  "reviewAttempt": 30,
  "stageAttempt": 1,
  "manifestSha256": "61cad5891e700d843392710b357fe34ac3852511744752a256911df68b5f4196",
  "diffSha256": "643efb8706e912bf3feafe7fe02829b3586b8f72a20d11dad2f2463f032c9a10",
  "dispatchStatus": "prepared_not_started",
  "split": {
    "parts_per_lens": 3,
    "part_timeout_seconds": 2400,
    "total_timeout_seconds_per_lens": 7200
  },
  "finalReview": "Distinct final CI-qualified pass required; zero of two final passes consumed."
}

## Driver closeout (2026-11-12 UTC)

The original review and driver plan above are preserved. This partition
assessed all 20 assigned paths and reported no findings. Together, the three
completed broad-sweep partitions assessed all 49 assigned source paths.
This is one completed lens, not a completed six-lens pre-commit review.

The owner explicitly authorized: "Authorize bounded closeout; waive further
full pre-commit passes." The five low-severity findings in partitions 2 and 3
were fixed in one bounded batch and targeted validation passed. Lenses 2-6
were not dispatched. No clean full-pass result is claimed, and historical
reviews are not combined into one.

The patch is now based on master
`80191a61e743b8ba462b692d2ffa2e95ac2df91d`. The intervening target change only
updates the semantic-PR action pin; the rebase preserves the product patch.
Publication is authorized under the explicit pre-commit exception, not as
merge approval. Current-head CI, a distinct final CI-qualified review and
human approval remain separate gates.
