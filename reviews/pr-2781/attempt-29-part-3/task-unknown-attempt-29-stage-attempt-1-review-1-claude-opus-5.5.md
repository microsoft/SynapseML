---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 1
timestamp: "2026-11-10T12:46:24Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9611b754e67293004604ec42732c7a45d4e47010232ed8878be3294c01c32278"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-3/task-unknown-attempt-29-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] This was a read-only static review of the full diff for all 14 assigned paths, plus the related production code. No commands, builds or tests were run, and no test result is claimed here. I did not read any review directories, sibling artifacts or other reviewers' output.
- [x] **Python bridge test that loads a Scala-saved stage.** `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py:266-293` (`test_scala_saved_default_columns_survive_python_pipeline_load`) is consistent with the unchanged bridge:
  - `Wrappable.scala:173-199` fixes the uid-derived Python defaults at code-generation time.
  - `ComplexParamsSerializer.scala:69-77,128-131` writes `extractParamMap()`, which includes defaults, into `paramMap`. On load, the reader (`ComplexParamsSerializer.scala:172-184`) therefore marks `outputCol` and `errorCol` as set.
  - The set-only transfer in `core/src/main/python/synapse/ml/core/schema/Utils.py` (`_transfer_params_from_java`) then copies the Java uid-derived values into Python.
  - I found no defect.
- [x] **Default deployment through copy and load.** `test_text_deployment_defaults_and_bindings_survive_copy_and_load` (lines 218-264) matches the rule that opted-in parameters are dropped only when unset (`ComplexParamsSerializer.scala:76`). It also matches the constructor default `deploymentName -> Left(DefaultTextDeploymentName)` (`OpenAIPrompt.scala:272-285`). The test loads with the Java-deserialization guard forced to `false`, so it exercises exclusion of the unset handler.
- [x] **Mock HTTP assertions** in `test_ServiceParamPythonBridge.py:168-211` match the mapping code:
  - logprobs plus `top_logprobs` for Chat;
  - `include` gets `message.output_text.logprobs` appended for Responses;
  - `stopSequences` is sent as `stop`;
  - a Responses-only column binding for `prompt_cache_key` and `metadata`;
  - Prompt returns null output for an empty completed Responses output, per `OpenAIResponses.scala` `isContentFiltered`.
- [x] **Notebook disclosures** in `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`:
  - line 1100 prominently labels the Chat/Prompt Chat rejection of `echo`, `bestOf` and `cacheLevel` as a breaking change that fails the task;
  - line 1102 labels the Responses `temperature`/`top_p` forwarding as a behavior change, including Prompt in Responses mode;
  - line 1114 states the store semantics, including local rejection on Foundry;
  - line 804 covers task failure and retries that can repeat requests;
  - lines 799, 802 and 978 cover null and non-finite handling for cache options and breakpoints;
  - line 308 covers the default deployment, no modality routing and rejection of model-only legacy Chat.
  - All of these match the spec.
- [x] **Live-test safety** in `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py`:
  - Live tests require opt-in (lines 26-38). Foundry (318-321) and retention (370-373) each have their own gate.
  - Credentials are set only in `collect` (lines 177-179), after every save/load (209-213, 276-279, 307-310).
  - `record` and `native` log only usage, status, IDs and provider error bodies, never keys.
- [x] **Rename-only paths** (Langchain, both multimodal tests, the multimodal request test, StructuredOutput) and the four non-OpenAI notebooks were checked. Deployment names are explicit (`Langchain.ipynb:161`, `Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb:52`, `Quickstart - Understand and Search Forms.ipynb:101`, `Quickstart - Document Question and Answering with PDFs.ipynb:155-157`). No new credentials were added.
- [x] **Considered and not raised:**
  - In `test_OpenAIDefaults.py`, the global deployment equals the built-in default. Offline precedence is covered by `OpenAIRuntimeDefaultsSuite.scala:64`.
  - `test_responses_prewarm` fails on endpoints without prewarm support. This is intentional per `OpenAI.ipynb:1089-1091` ("unsupported options fail qualification").
- [ ] Not checked: current-head CI and freshly generated Python wrappers. The driver states these are still required after publication.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/python/synapsemltest/services/langchain/test_LangchainTransform.py` | assessed | Only the model string changes (`gpt-5.1` → `gpt-6-luna`) in the mock server and client constructors. Security and transport assertions are unchanged. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIChatCompletionMultimodal.py` | assessed | Rename only, with an explicit `setDeploymentName`, so it does not depend on modality routing (spec: no automatic routing). |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIDefaults.py` | assessed | The global deployment now equals the built-in default, so the live test cannot tell global from fallback. Offline Scala precedence coverage exists, and tearDown resets the globals. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIMultimodalRequest.py` | assessed | Rename only, with explicit deployment and mock model strings. No change to request logic. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` | assessed | The Decimal exactness check through `jsonEncode` is valid. Load and request exactness are covered by `OpenAIResponseSchemaSuite.scala:156-170`. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponsesMultimodal.py` | assessed | Rename only, with an explicit deployment. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_StructuredOutput.py` | assessed | Explicit `setDeploymentName(self.deploymentName)`. No change to schema logic. |
| `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` | assessed | Opt-in gating, credentials set after save, and evidence that never logs keys were verified. Issues 1 and 2. |
| `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` | assessed | Five new tests traced against the bridge, writer and request mapping. The Scala-saved-defaults test is confirmed sound. Issue 3. |
| `docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs.ipynb` | assessed | Explicit query deployment `gpt-6-luna` plus a hint about a larger model. Embeddings unchanged. No credentials added. |
| `docs/Explore Algorithms/OpenAI/Langchain.ipynb` | assessed | Only the deployment name changes. The secret-loading pattern is unchanged. |
| `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` | assessed | Required disclosures and store, null and finite-number semantics match the spec. Issue 4. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb` | assessed | The deployment is selected explicitly (`setDeploymentName`, line 132). No modality routing is implied. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms.ipynb` | assessed | Only the explicit deployment name changes (line 101). No credentials added. |

## Issues

### Issue 1: The 30-minute retention test asserts a cache hit only after the minimum lifetime has already passed
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py
- **Line(s)**: 370-390 (see also docs/Explore Algorithms/OpenAI/OpenAI.ipynb:1087, 1091)
- **Description**: The test starts its timer after the seed request has completed. The cache write happens before that. It then sleeps 1800 s, runs a Spark job and asserts `cached_tokens > 0`. The read therefore happens strictly after 30 minutes from the write. The notebook defines `prompt_cache_options.ttl` as a **minimum** cache lifetime, so a provider that evicts exactly at 30 minutes is conforming but fails this test. The notebook still describes the test as assertion-backed qualification of "an untouched 30-minute interval".
- **Risk**: The result depends on timing at the boundary. A conforming provider can be reported as a retention failure, and a pass mostly reflects how much extra time the provider allows beyond the TTL.
- **Suggested Fix**: Read just inside the guaranteed window, for example about 29 minutes measured from the start of the seed request, and assert the hit there. Alternatively, record any observation after 30 minutes as informational rather than asserted, and update the notebook wording to match.

### Issue 2: The Foundry live test hard-codes a third-party model with no override
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py
- **Line(s)**: 318-368 (model list at 327)
- **Description**: `test_foundry_model_inference` always iterates over `"Llama-3.3-70B-Instruct"` as well as `OPENAI_CACHE_MODEL`, and no environment variable can change it. On a Foundry endpoint without that deployment, the request fails as not found, which looks the same as a cache-qualification failure.
- **Risk**: The opt-in Foundry qualification cannot be reused across environments. A missing deployment can be mistaken for a SynapseML or provider caching regression.
- **Suggested Fix**: Read the model list from an environment variable (for example `AI_FOUNDRY_CACHE_MODELS`, comma-separated), and skip any model that is not configured. Alternatively, document the required deployment next to the skip condition.

### Issue 3: The column-binding check cannot detect whether reasoning, topLogProbs and store columns replaced the scalars
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py
- **Line(s)**: 95-102 (row values), 122-153 (scalar options and column bindings), 174-182 (assertions)
- **Description**: In the Responses cases, the row holds `reasoning={"context":"all_turns","effort":"max"}`, `probability_count=2` and `persist=True`. These are exactly the same as the scalar `reasoning`, `topLogProbs=2` and `store=True`. Only `promptCacheKey` and `metadata` use different values for the row and the scalar. The assertions on `top_logprobs`, `store` and `reasoning` therefore pass whether the column binding or the scalar was sent. The notebook (line 1104) promises that a column binding replaces the scalar for every control.
- **Risk**: A regression in which those three column bindings are ignored after Python binding or Prompt reload would go undetected.
- **Suggested Fix**: Use row values that differ from the scalars, for example `effort: "low"` in the row, `probability_count=3`, and scalar `store=False` with `persist=True` in the row. Assert the row values.

### Issue 4: The Python notebook tells users to call seed methods that the Python `OpenAIDefaults` does not have
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 1118
- **Description**: The new text tells users to clear a global seed before using Responses with `OpenAIDefaults.setSeed(...)` and `OpenAIDefaults.resetSeed()`. The Python class used in this notebook (`defaults = OpenAIDefaults()`, line 330) has only the snake_case instance methods `set_seed` and `reset_seed` (`cognitive/src/main/python/synapse/ml/services/openai/OpenAIDefaults.py:66,72`).
- **Risk**: Following this guidance, which is part of the documented workaround for Responses rejecting a global seed, raises `AttributeError`.
- **Suggested Fix**: Use `defaults.set_seed(...)` and `defaults.reset_seed()` in the notebook text. Mention the Scala `OpenAIDefaults.setSeed`/`resetSeed` names only if they are explicitly labeled as Scala.

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
