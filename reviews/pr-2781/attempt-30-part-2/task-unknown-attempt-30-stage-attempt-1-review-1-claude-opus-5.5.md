---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 30
stage_attempt: 1
round: 1
timestamp: "2026-11-11T12:15:46Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1d2e5a7eac9ac400584c5646c9e61e07987bdec4ab7f37f4775054587304f018"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-30-part-2/task-unknown-attempt-30-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Patch scope:** I read the assigned hunks from `reviews/pr-2781/inputs30/part2.patch`, including the deleted ones. The deletions are:
  - `deploymentName5p1` and `deploymentNameMini`
  - the gpt-5-mini Prompt payload test
  - the in-suite echo server and helpers
  - the "store requires responses" rejection test
- [x] **Default deployment:** `OpenAIAPIKey.deploymentName = "gpt-6-luna"` matches `OpenAIDefaults.DefaultTextDeploymentName` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala:12`).
  - It is applied through `setDefault` in `OpenAIChatCompletion.scala:126` (OpenAI service type only, so Foundry gets no default), `OpenAIPrompt.scala:281` and `OpenAIResponses.scala:147`.
  - A grep of `cognitive/src/test` finds no remaining uses of the removed symbols. The only `gpt-5-mini` hits are literals in the context-only `OpenAICoreOfflineSuite.scala:121,130`.
- [x] **Global-deployment precedence:** `OpenAIDefaultsSuite` now sets a global deployment equal to the built-in default. That suite's assertion therefore can't tell global transfer from fallback, but precedence is still covered with a distinct name in the context-only `OpenAIRuntimeDefaultsSuite.scala:61-79`.
- [x] **Persistence:** The persistence tests (`OpenAIPromptCacheSuite.scala:84-140, 273-340`) trace correctly to:
  - `HasOpenAITextParams.paramsWithImplicitDefaults` (`OpenAI.scala:207`) and the handler opt-in (`OpenAI.scala:585`)
  - `ComplexParamsWriter.paramsToPersist` (`core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:87-95`)
  - the Java-deserialization guard in `core/src/main/scala/org/apache/spark/ml/Serializer.scala`

  Explicitly saved handlers stay guarded on reload (line 318).
- [x] **Store semantics:**
  - Prompt Chat and Foundry omit a no-op scalar `false` (`OpenAIPrompt.scala:207, 674-692`).
  - Responses defaults to `false`, including for null columns (`OpenAIResponses.scala:170-177`).
  - Direct Foundry rejects any store value.

  These match `OpenAIRequestOptionsSuite.scala:108-147, 215-254` and `OpenAIPromptCacheSuite.scala:79-140`. `OpenAIPromptSuite.scala:682-695` now asserts that Chat accepts store. That relaxes an earlier rejection, so no previously working pipeline breaks.
- [x] **Rejected controls:** The rejections in `OpenAIChatCompletion.scala:141-150` (echo, bestOf, cacheLevel) and `OpenAIResponses.scala:170-177` (seed, including a global seed) match `OpenAIRequestOptionsSuite.scala:72-106` and `OpenAIPromptCacheSuite.scala:144-176, 342-364`. The reasoning object check is covered at `OpenAIRequestOptionsSuite.scala:54-70`.
- [x] **Breakpoint and option validation:**
  - The tests at `OpenAIPromptCacheSuite.scala:435-498, 499-607, 737-778` trace to `validatePromptCacheBreakpoint` and `normalizeValue` (`OpenAI.scala:24-76`) and to the transform validation UDFs in Chat and Responses.
  - The no-HTTP accumulator checks are sound because each handler increments its accumulator before throwing.
- [x] **Map-backed breakpoint test:** `OpenAIV1EndpointSuite.scala:458-472` uses the existing helpers `compositeMessageRow` (119), `chatSerializationError` (159) and `transformWithThrowingHandler` (168). Its expected message matches `OpenAIChatCompletion.scala:328-331`.
- [x] **Unset Responses deployment:** The unset-default versus explicit-empty test (`OpenAIV1EndpointSuite.scala:759-770`) matches `mergeModel`.
- [x] **Option values:** Explicit null struct members are preserved (`OpenAIRequestOptionsSuite.scala:298-327`). A whole-column null omits the field (`OpenAIPromptCacheSuite.scala:421-433`). Exact decimals are preserved (`OpenAIPromptCacheSuite.scala:658-690` and the new `OpenAIResponseSchemaSuite` decimal test). All three are consistent with `AnyJsonFormat` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala`.
- [x] **Security and isolation:**
  - The loopback server (`OpenAIPromptHttpTestBase.scala:63-117`) binds `127.0.0.1` on an ephemeral port and stops in `finally`. Only one `withEchoServer` overload declares defaults, which is legal.
  - No new secrets were added; offline tests use `"unused"` keys.
  - `OpenAIEmbeddingsSuite` now restores global defaults in `finally`.
- [ ] **Not verifiable here: live deployment.** The live suites now need a `gpt-6-luna` deployment on the live test resource, which read-only review can't confirm. I ran no tests; the validation results are as supplied by the controller.
- [ ] **Not verifiable here: datetime toggle.** In `OpenAIPromptCacheSuite.scala:585-586`, both datetime representations are unsupported, so the assertions pass either way. I couldn't confirm read-only that the session conf reaches the executor converters. This is a test-strength note only, not raised as an issue.

## Issues

### Issue 1: Unused `ConcurrentLinkedQueue` import left after helper extraction
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- **Line(s)**: 22
- **Description**: The refactor moved `withEchoServer`, the only code that named `ConcurrentLinkedQueue`, into `OpenAIPromptHttpTestBase`. It removed the other imports that became unused (`TestBase`, `HttpServer`, `IOUtils`, `InetSocketAddress`) but left `ConcurrentLinkedQueue` in the `java.util.concurrent` import. All remaining callbacks use untyped lambda parameters, and a grep finds the name only on line 22.
- **Risk**: Hygiene only. `build.sbt` enables no unused-import or fatal-warning flags, so nothing breaks.
- **Suggested Fix**: Change the import to `import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit}`.

### Issue 2: Several loopback tests don't assert exact HTTP request counts
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRequestOptionsSuite.scala`
- **Line(s)**: 202, 263, 269, 285 (the `takeBodies` helper is at 35-39)
- **Description**: The suite's `takeBodies(bodies, expected)` helper asserts an exact count and an empty queue. The storage tests use it, but these call `bodies.poll()` directly:
  - the prewarm test (202)
  - the scalar and per-row cache-options test (263, 269)
  - the provider-error test (285)

  The tests at 256-271 and 275-295 share one server across both APIs. A duplicate Chat request would be picked up by the Responses iteration and still match. An empty queue would cause an NPE instead of a clear failure.
- **Risk**: The tests are less able to catch duplicate or misrouted requests, which matters given the spec's no-HTTP guarantees and its retry disclosure. No product defect is demonstrated.
- **Suggested Fix**: Replace these `bodies.poll()` calls with `takeBodies(bodies, 1).head`. Optionally also assert the request path for each API.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAPIKey.scala` | assessed | Removed symbols have no remaining users. The default matches `OpenAIDefaults.scala:12`. No new secrets; the env override is kept. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionMultimodalSuite.scala` | assessed | Model-string renames only. The Foundry test still selects its model explicitly. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletionSuite.scala` | assessed | Live tests now use `deploymentName`. Test-name and prompt-text changes are harmless. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaultsSuite.scala` | assessed | Prompt-text change only. Globals reset in before/afterEach. Distinct global precedence is covered elsewhere. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIEmbeddingsSuite.scala` | assessed | Global restoration now in `try`/`finally`. Embedding-default precedence over the general default is kept. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptCacheSuite.scala` | assessed | All 32 tests traced to source. Assertions match store, seed, persistence, non-finite, non-JSON and decimal behavior. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala` | assessed | Loopback-only server, ephemeral port, stopped in `finally`. Overload and defaults are legal. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` | assessed | Clean extraction to the base class. Stale import (Issue 1). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptResponsesSuite.scala` | assessed | Low reasoning effort and verbosity applied unconditionally. Single live Luna test. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptSuite.scala` | assessed | The removed Responses payload test is covered in context-only suites. Chat store acceptance (682-695) is consistent with source. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRequestOptionsSuite.scala` | assessed | All tests traced. Count-assertion gap (Issue 2). |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala` | assessed | BigInt and `new Object()` rejected without mutating state. Exact Scala and Java decimals tested across Chat, Responses and Prompt. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesMultimodalSuite.scala` | assessed | Model-string renames only. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponsesSuite.scala` | assessed | Fixture renames and the prompt-text change. Parsing assertions unchanged. |
| `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIV1EndpointSuite.scala` | assessed | New map-backed breakpoint test and unset/empty Responses deployment test match source. Prompt test uses `custom-model`. |

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

The original review, open-at-review-time resolution log and driver plan above
are preserved. This partition assessed all 15 assigned paths.

### Issue 1: Resolved

Removed the unused `ConcurrentLinkedQueue` import from
`OpenAIPromptMultimodalRequestSuite.scala`. No production behavior changed.
Targeted Scala compilation and style checks passed.

### Issue 2: Resolved

Replaced the four direct `bodies.poll()` reads in
`OpenAIRequestOptionsSuite.scala` with `takeBodies(bodies, 1).head`.
Each request assertion now checks an exact count and an empty remaining
queue, preventing a duplicate request from satisfying the next API's case.
All 11 tests in `OpenAIRequestOptionsSuite` passed after this change.

The owner explicitly authorized: "Authorize bounded closeout; waive further
full pre-commit passes." All three broad-sweep partitions completed, covering
49 source paths; lenses 2-6 were not dispatched. The five findings across
partitions 2 and 3 were fixed in one bounded batch. This is not a clean
six-lens pre-commit pass.

The rebased target is master
`80191a61e743b8ba462b692d2ffa2e95ac2df91d`. Publication uses the explicit
pre-commit exception; current-head CI, the distinct final CI-qualified review
and human approval have not been waived.
