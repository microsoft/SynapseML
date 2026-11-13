---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 30
stage_attempt: 1
round: 1
timestamp: "2026-11-11T12:34:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "54807c222937040dee904707cc8210fd05e0fc67b3c4dea73c0782c38f411ad8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-30-part-3/task-unknown-attempt-30-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] All 14 assigned whole-file diffs were assessed, including deleted hunks. I used the frozen partition patch `reviews/pr-2781/inputs30/part3.patch` (the permitted own-patch exception) and cross-checked it against current sources and tests.
- [x] The bridge loopback test's expectations match production code:
  - `OpenAIResponses.scala:170-178` rejects Chat-only controls, defaults `store` to false and maps logprobs to `include`/`top_logprobs`.
  - `OpenAIResponses.scala:203-215` merges reasoning.
  - `OpenAIPrompt.scala` handles `skipNoOpStore` and response IDs for stored rows.
  - `UsageUtils.scala` provides the normalized `cached_tokens`/`cache_write_tokens` keys, and `OpenAISchemas.scala` provides `prompt_cache_diagnostics`.
  - The server binds to `127.0.0.1` on an ephemeral port and shuts down in `finally`.
  - `SelectColumns` and `OpenAIPrompt` are already imported (`test_ServiceParamPythonBridge.py:20-22`).
- [x] The notebook's breakpoint and option semantics match the code:
  - Struct and map-backed rejections produce per-row `errorCol` values with nulled messages, so no HTTP request is sent (`OpenAIChatCompletion.scala:176-220`, `:499-525`, `:561-568`).
  - `OpenAI.scala:63-76` checks numbers are finite and keeps null option members.
  - `OpenAIRequestOptionsSuite.scala:320-325` asserts explicit `null` in options structs.
- [x] Security:
  - The live cache test only runs when explicitly enabled (`test_OpenAICacheLive.py:27-30`).
  - Credentials are set only after save/load (`:178-180`), and the key travels only in a request header (`:99`).
  - The evidence JSONL records usage, status, IDs and the error struct. The error struct holds only the response body and status (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.scala:34-36`), so no headers or keys are recorded.
  - `test_OpenAIResponseSchema.py` now keeps `allowUnsafeJavaDeserialization` at `"false"` on load, which matches the handler-persistence spec.
- [x] The Decimal test is sound. Python `Decimal` crosses py4j as JVM `BigDecimal` and is written exactly. `json.loads(..., parse_float=Decimal)` compares exactly, and the integral limit parses as an `int` that compares equal to the `Decimal`.
- [x] Deployment precedence is covered:
  - The Python bridge test covers default, explicit-default, scalar and column bindings across copy, save and load, including `isSet` checks.
  - Global precedence is covered in Scala (`OpenAIRuntimeDefaultsSuite.scala:64-67`).
  - The live `test_OpenAIDefaults.py` value now equals the default, so it no longer discriminates. That is acceptable given the Scala coverage.
- [x] Deployment-string-only edits are inert:
  - The Langchain tests assert transport (auth and path), not model-specific payloads.
  - The multimodal Chat/Responses tests check only local row validation (`test_OpenAIChatCompletionMultimodal.py:16,57`; `test_OpenAIResponsesMultimodal.py:16,69`).
  - No stale `gpt-5` strings remain in the Python tests. The only notebook mention is the intended disclosure (`OpenAI.ipynb:722`).
- [x] Both approved behavior changes (Chat echo/bestOf/cacheLevel rejection; Responses temperature/top_p forwarding) are prominently labelled at `OpenAI.ipynb:719-722`. The store row (`:734`) matches the Responses default, Prompt's no-op false omission and Foundry's rejection.
- [x] The notebook's SDK cell dependency is available: `openai==2.47.0` is pinned in `environment.yml:56` and `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala:196`.
- [x] Non-blocking observations, not counted as issues:
  - The SynapseML prewarm subtest reuses the prefix the native prewarm already cached. Its empty output still proves the option was forwarded.
  - The live-test docstring does not say `OPENAI_BASE_URL` must be the `/openai/v1/` base.
- [ ] Nothing was executed (read-only review: no tests, notebooks, builds or hosted requests). Provider cache hits and `gpt-6-luna` multimodal capability cannot be checked here and are not reported as defects.

## Issues

### Issue 1: Seed-reset guidance uses a `defaults` import that doesn't exist
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 738
- **Description**: The Responses global-seed guidance says to call `defaults.reset_seed()` after `from synapse.ml.services.openai import defaults`. That package has no `defaults` submodule or module-level instance:
  - The hand-written `cognitive/src/main/python/synapse/ml/services/openai/` contains an empty `__init__.py`, `OpenAICompletion.py` and `OpenAIDefaults.py`.
  - `OpenAIDefaults.py` defines only `getOption` (line 12) and class `OpenAIDefaults` (line 19).
  - The notebook's own cell (lines 326-329) uses the working pattern instead.
- **Risk**: Users following this guidance for the newly rejected global seed hit `ImportError`. Their Responses pipelines keep failing during request construction.
- **Suggested Fix**: Use `from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults` followed by `OpenAIDefaults().reset_seed()`.

### Issue 2: Notebook-global reasoning effort conflicts with the documented `setReasoning` example
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 333-334, 729
- **Description**:
  - Lines 333-334 add process-wide `reasoning_effort` and `verbosity` defaults of `"low"`. Nothing in the notebook resets them; only lines 326-334 and 738 reference `defaults`.
  - At transform time, `transferGlobalParamsToParamMap` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala:43-52`, called at `OpenAIResponses.scala:359`) fills unset stage params from these globals.
  - `mergeReasoning` (`OpenAIResponses.scala:203-215`) then requires `reasoning.effort` to match the resolved effort.
  - So in the same session, the documented `setReasoning({"effort": "max", ...})` (line 729) on Responses or Prompt-Responses fails the Spark task with "reasoningEffort conflicts with reasoning.effort".
  - Line 729 only mentions conflicts with `setReasoningEffort(...)`. The globals also silently apply to every later text stage, including any non-reasoning deployment a reader substitutes.
- **Risk**: A documented example fails in the notebook's own session, with a task-level error. This is documentation only, not a product defect.
- **Suggested Fix**: Either set effort and verbosity on the stage in that cell, or reset them at the end of the cell (`defaults.reset_reasoning_effort()`, `defaults.reset_verbosity()`). Also state that a global `OpenAIDefaults` reasoning effort counts in the conflict check.

### Issue 3: Map-backed Chat breakpoint rejection is called a request-construction failure, but it is a per-row error
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 1122 (contrast with 1021)
- **Description**: Line 1122 says these rows are "rejected locally during request construction". Line 1021 uses "request-construction exceptions" to mean errors that fail the Spark task. In the code, though:
  - The rejection happens inside `validateMessages` (`OpenAIChatCompletion.scala:516-517`, which calls the check at `:325-333`).
  - `structuredMessageValidationError` catches it (`:561-568`).
  - `transform` (`:188-217`) then writes a per-row `errorCol` value and nulls the messages, so no HTTP request is sent.
- **Risk**: Readers may add task-level exception handling for this case and miss errors that actually arrive as `errorCol` rows. This is a wording issue only.
- **Suggested Fix**: Reword to: "those rows receive a per-row validation error in `errorCol` with null output and no HTTP request; the message directs you to a nested struct content schema."

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
| `cognitive/src/test/python/synapsemltest/services/langchain/test_LangchainTransform.py` | assessed | Every hunk only renames the model string. Assertions cover transport, auth, path and persistence sanitization, not model-specific payloads. No stale model strings remain. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIChatCompletionMultimodal.py` | assessed | Explicit deployment renamed in a row-validation test. Invalid rows are nulled before HTTP (`OpenAIChatCompletion.scala:188-205`), so there is no network dependency. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIDefaults.py` | assessed | The live global-deployment value now equals the default and no longer discriminates. Precedence is covered by `OpenAIRuntimeDefaultsSuite.scala:64-67`; tearDown resets globals. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIMultimodalRequest.py` | assessed | Local echo-server fixture model and explicit deployment renamed. Request-shape assertions are unaffected. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` | assessed | The new Decimal exactness test is sound across stages. Loading with the deserialization guard at "false" matches the spec. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponsesMultimodal.py` | assessed | Explicit deployment renamed in a local row-validation test. No HTTP request is made. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_StructuredOutput.py` | assessed | Live deployment rename only. No sampling or parameter changes in the diff. |
| `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` | assessed | Runs only when explicitly enabled; credentials are set after save/load; no secrets reach the evidence file. Usage and TTL key checks match the schemas. Non-blocking prewarm and base-URL notes are in the checklist. |
| `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` | assessed | Loopback assertions verified against Chat, Responses and Prompt request building, store semantics and usage normalization. Deployment bindings checked across copy, save and load with `isSet`. |
| `docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs.ipynb` | assessed | Explicit deployment and model rename plus a comment. `aoai_model_name_query` is not used elsewhere; the LangChain call uses the explicit deployment (line 899). |
| `docs/Explore Algorithms/OpenAI/Langchain.ipynb` | assessed | Explicit deployment rename only. The `ChatOpenAI` v1 base-URL usage and arguments are unchanged. |
| `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` | assessed | Default-deployment, compatibility-disclosure, controls table, cache cells and field mapping checked against code. Issues 1-3 come from this file. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb` | assessed | Explicit deployment rename (line 52). Whether the model supports multimodal input is decided by the provider and cannot be checked read-only; not reported. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms.ipynb` | assessed | Explicit deployment rename (line 101). The SDK call arguments (lines 772-777) are unchanged. |

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
are preserved. This partition assessed all 14 assigned paths.

### Issue 1: Resolved

The seed-reset guidance now imports `OpenAIDefaults` from its real module and
calls `OpenAIDefaults().reset_seed()`. The corrected import and seed reset
were exercised with the installed package without hosted requests.

### Issue 2: Resolved

Moved the example's reasoning effort and verbosity from process-wide defaults
onto that example's stage. The controls documentation now explicitly says a
global reasoning effort also participates in the conflict check.
Local notebook checks confirmed stage-local values, unchanged reasoning and
verbosity globals, and successful seed reset. No hosted-model result is
claimed.

### Issue 3: Resolved

The map-backed Chat breakpoint guidance now describes the actual per-row
validation error in `errorCol`, null output and no HTTP request, rather than
a task-level request-construction failure. Notebook source-format and Python
formatting checks passed.

The owner explicitly authorized: "Authorize bounded closeout; waive further
full pre-commit passes." All three broad-sweep partitions completed, covering
49 source paths; lenses 2-6 were not dispatched. All five findings were fixed
in one bounded batch. This is not a clean six-lens pre-commit pass.

The rebased target is master
`80191a61e743b8ba462b692d2ffa2e95ac2df91d`. Publication uses the explicit
pre-commit exception; current-head CI, the distinct final CI-qualified review
and human approval have not been waived.
