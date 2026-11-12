---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 3
timestamp: "2026-11-10T14:31:11Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d332620f33e2206ffdb4f72c5657ef66e8fc281ecefc7d645189dbd2d31261ce"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-3/task-unknown-attempt-29-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I assessed the full supplied diff for all 14 assigned paths in partition 3 of 3. I used other frozen paths only as interface context and don't claim to have covered them.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala:574-583`: `getPromptsForMessage` always sends the system prompt as a `"system"` role message. A grep for `"developer"` or `"instructions"` under `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/` found no role remapping.
- [x] Prompt-caching cells in `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`:
  - `:839-850`: the prefix is sent as a `developer` role message.
  - `:872-887`: the native SDK request that warms the cache.
  - `:950-967`, `:990-1006`, `:1028-1061`: every row asserts `cached_tokens > 0` (at 966, 1005 and 1059).
  - `:1085`: the text claims cache reads follow the native warm-up.
  - `:1089`: the text says the Azure OpenAI v1 `gpt-6-luna` deployment rejects `prewarm`.
- [x] `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py:265-292`: the PR's own Prompt implicit-mode test expects a cold cache write first and a cache read second.
- [x] The notebooks run in E2E CI:
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala:184-196,256-267`: Databricks CPU E2E includes all five assigned notebooks, and the cluster installs `openai==2.47.0`.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseTests.scala:49-63`: Synapse E2E excludes only the LangChain and Document QA notebooks among those assigned.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseUtilities.scala:276`: the Synapse pool runs Spark 3.5.
  - `docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms.ipynb:61` pins the SDK inside the notebook; `OpenAI.ipynb` has no install cell.
- [x] Seed and reasoning APIs:
  - `cognitive/src/main/python/synapse/ml/services/openai/OpenAIDefaults.py:66-72`: Python uses `set_seed` and `reset_seed`.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala:66,74`: Scala uses `setSeed` and `resetSeed`.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala:203-213`: `mergeReasoning` fails with a `require` error when the two effort values disagree.
- [x] `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py`: compared row values (96-101), scalar options (122-138), column bindings (146-153) and assertions (168-182). The test checks `row.error` (169) before dequeuing the request body (170), and checks for leftover requests only at the end (212).
- [x] `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py:4-11,26-44,129-152,177-189,241-263,318-368,370-391`: reviewed gating, saving before credentials are set, counter validation, and the prewarm, Foundry and retention paths.
- [x] Other assigned tests:
  - `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py:75-89`: the Decimal test parses with `parse_float=Decimal`.
  - `test_OpenAIDefaults.py:21-31,138` and `test_StructuredOutput.py:64-67,78`: reviewed.
  - The LangChain test and three multimodal test files contain only model-name changes; I checked each against its mock-server assertions.
- [ ] Not checked whether the output artifact already exists. The plan forbids listing or viewing the review directory, and the controller persists this response.
- [ ] No tests, notebooks or builds were run (read-only lens). The supplied Python and HTTP results predate the visibility-only repair, so I didn't rely on them.
- [ ] Not verified (provider and environment facts):
  - whether the shared live resource has a `gpt-6-luna` deployment, and whether it accepts multimodal input;
  - whether the provider treats `system` and `developer` role prefixes as the same for caching;
  - whether implicit-mode requests can read entries written in explicit mode;
  - whether `openai==2.47.0` accepts the `prompt_cache_options` keyword;
  - which `openai` version the Synapse runtime bundles.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/python/synapsemltest/services/langchain/test_LangchainTransform.py` | assessed | Only `gpt-5.1` → `gpt-6-luna` renames. The mock server ignores the model. Assertions check auth headers, URL normalization, transport sanitization and `/v1/chat/completions` routing, so SDK rerouting by model name would fail loudly. No new failure mode. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIChatCompletionMultimodal.py` | assessed | Explicit deployment and mock-response model renamed only. The offline echo server and explicit deployment leave the unset-default path untouched. No new error or boundary handling. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIDefaults.py` | assessed | Line 138 sets the live global deployment to `gpt-6-luna`. `tearDown` (21-31) resets deployment, key, URL, sampling and model. It doesn't reset verbosity or reasoning effort (pre-existing, and nothing in the file sets them). Deployment availability is unverified. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIMultimodalRequest.py` | assessed | Model renames only, in offline request-building assertions. Error-path coverage is unchanged. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` | assessed | The Decimal test (75-89) parses Java JSON with `parse_float=Decimal`, so lossy double or long coercion would be detected. Decimal NaN would make py4j raise rather than coerce silently. Save/load and HTTP precision are left to Scala suites in other partitions. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponsesMultimodal.py` | assessed | Explicit deployment and mock model renamed only, using an offline mock. No new failure mode. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_StructuredOutput.py` | assessed | Line 78 deployment is now `gpt-6-luna`. Setup resets model, temperature, top_p and seed globals (64-67), avoiding the Responses global-seed rejection. Live availability is unverified. |
| `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` | assessed | Opt-in gating (26-44, 318-321, 370-373) and setting credentials only after save/load (177-189) are sound. Missing counters fail rather than count as zero. See Issue 4: prewarm isn't gated, the Foundry model is hard-coded, and TTL details are indexed without a clear assertion. |
| `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` | assessed | Each request is taken from the queue with a timeout (170), leftovers are checked at the end (212), and the server shuts down in `finally`. See Issue 5: column values can't be told apart from scalars, and a failed subtest leaves a stale body in the queue. |
| `docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs.ipynb` | assessed | ChatOpenAI uses `reasoning_effort` and `max_completion_tokens` with no sampling parameters. Excluded from Synapse E2E (`SynapseTests.scala:56`) but runs on Databricks. An unused model-name variable is cosmetic. No new error path. |
| `docs/Explore Algorithms/OpenAI/Langchain.ipynb` | assessed | Pins `openai==2.47.0` and `langchain-openai==1.4.0` (line 91). ChatOpenAI uses `gpt-6-luna` with `reasoning_effort` and no temperature. Excluded from Synapse E2E; runs on Databricks. |
| `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` | assessed | Issues 1, 2, 3 and 6. Hard cache-hit assertions include a `system`-role Prompt prefix that is never warmed (1028-1061). The SDK cell has no pinned install (872-887). Global defaults leak (329-334), and the Python seed API names are wrong (1118). Breaking-change disclosures are present (1100, 1102). |
| `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb` | assessed | Uses `gpt-6-luna` explicitly for image, PDF and text attachments (52, 132) with an error column (143) and no result assertion (only 61), which is pre-existing. Multimodal support is unverified, so E2E could pass even if every row errors. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms.ipynb` | assessed | Pins `openai==2.47.0` (61). Uses OpenAIPrompt with an explicit deployment and reasoning effort; the SDK chat section starts at 727. No new boundary risk. |

## Issues

### Issue 1: The OpenAIPrompt implicit-cache cell asserts cache reads on a prefix that was never warmed
- **Severity**: Medium
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 1028-1061 (assert at 1059); related: 839-850, 872-887, 1085
- **Description**: The cell passes `stable_prefix` to `setSystemPrompt`.
  - `OpenAIPrompt.getPromptsForMessage` always sends it as a `"system"` role message (`OpenAIPrompt.scala:574-583`), and no production code remaps the role.
  - Every earlier notebook request (the native SDK warm-up and both explicit-mode Spark cells) sends the prefix as a `developer` message (`cache_input`, 839-850).
  - The rendered prefixes therefore differ from the role marker onward. The Prompt's first requests should be cold implicit writes, yet the cell asserts `cached_tokens > 0` for every row on its only pass.
  - The two rows are probably in separate partitions and sent concurrently, so both can be cold.
  - This contradicts the PR's own test for this configuration (`test_OpenAICacheLive.py:265-292`, which expects `cache_write_tokens > 0` before a read). It also contradicts line 1085, which says reads occur "after the native request warms the shared prefix".
  - Classification: introduced by this diff. The encoding mechanism is verified; the provider outcome is not.
- **Risk**: OpenAI.ipynb runs in both Databricks CPU E2E and Synapse E2E against a shared live resource.
  - An AssertionError fails the notebook job and skips all later cells (inference-control and embedding examples), hiding later regressions.
  - A concurrent CI run could also warm the same `system`-role prefix, so a green run may not reflect this notebook's own behavior.
- **Suggested Fix**: Mirror the live test.
  - Run the Prompt transform once as a warm-up, asserting no errors and non-null usage counters. Run it again and assert `cached_tokens > 0` only on the second pass.
  - Alternatively, assert only that the counters are present and non-negative, and print the read count.
  - Correct line 1085 to say the Prompt example warms its own `system`-role prefix.

### Issue 2: The CI-run tutorial asserts per-row cache hits that the same notebook says are "not guaranteed"
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 795, 950-967 (assert 966), 990-1006 (assert 1005), 1085
- **Description**: After a single native warm-up, the explicit-mode Responses cells assert `cached_tokens > 0` for every row.
  - The notebook itself says hits are not guaranteed (795), and that a failed assertion "does not by itself identify a client defect" (1085).
  - Provider-side variance can cause failures unrelated to SynapseML, such as write-propagation delay after the warm-up, overflow routing across cache hosts, or capacity changes.
  - Classification: introduced by this diff. This is a risk assessment; the failure frequency is unknown.
- **Risk**: This gate fails intermittently in two E2E pipelines without pointing to a client defect. If operators start retrying or ignoring red notebook runs, real regressions can be missed, and each failure also aborts all later cells.
- **Suggested Fix**: In the tutorial, assert error-free output and non-null counters, and print `cached_tokens`. Keep hit qualification in the opt-in `test_OpenAICacheLive.py`, or gate these hit assertions behind `SYNAPSEML_LIVE_CACHE_TESTS`. If they stay, add a bounded retry before asserting.

### Issue 3: The SDK comparison cell relies on an unpinned `openai` package in Synapse E2E
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 872-887
- **Description**: The new cell imports `openai.OpenAI` and passes `prompt_cache_options=` as a typed keyword to `responses.create`.
  - Databricks installs `openai==2.47.0` for the whole cluster (`DatabricksUtilities.scala:196`).
  - Synapse E2E runs this notebook on a Spark 3.5 pool (`SynapseTests.scala:49-63`, `SynapseUtilities.scala:276`) without a notebook-level install. By contrast, `Quickstart - Understand and Search Forms.ipynb:61` runs `%pip install openai==2.47.0`.
  - If the SDK is missing, older than 1.0, or older than this parameter, the cell raises `ImportError` or `TypeError` (unexpected keyword).
  - Classification: introduced by this diff. The Synapse environment is unverified.
- **Risk**: The notebook can fail depending on the environment, halting all later cells, including the SynapseML caching examples this cell is meant to compare against.
- **Suggested Fix**: Add the same pinned `%pip install openai==2.47.0` cell as the Forms quickstart. Alternatively, send the field with `extra_body={"prompt_cache_options": cache_options}`, which produces the same top-level JSON on any SDK 1.x or later, and keep the typed-argument version as prose.

### Issue 4: The opt-in live suite can't pass on its own documented target and has undocumented requirements
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py
- **Line(s)**: 4-11, 145-151, 241-263, 327
- **Description**: Three robustness gaps:
  - (a) `test_responses_prewarm` runs whenever the suite is enabled. The docstring targets an existing Azure OpenAI deployment, but the notebook says an Azure OpenAI v1 `gpt-6-luna` deployment rejects `prewarm` (`OpenAI.ipynb:1089`). On that target the suite is red by design.
  - (b) `test_foundry_model_inference` hard-codes `Llama-3.3-70B-Instruct` and also needs `OPENAI_CACHE_MODEL` deployed on the Foundry endpoint. Neither requirement appears in the docstring or skip reason.
  - (c) `details()` indexes `details["cache_write_tokens_details"]` directly. A missing or null TTL breakdown raises `KeyError` or `TypeError` instead of the descriptive "Provider did not report" assertion used for the other counters.
  - All three fail closed rather than passing silently. Classification: new gaps in test design, not product regressions.
- **Risk**: Qualification runs always show an unexplained expected failure, so operators learn to discount red results and real cache regressions get obscured. Foundry runs also fail on endpoints that legitimately lack the hard-coded model.
- **Suggested Fix**:
  - Gate prewarm behind its own opt-in (for example `SYNAPSEML_CACHE_PREWARM_TEST=1`, like the retention gate).
  - Make the Foundry models configurable (for example a comma-separated `AI_FOUNDRY_CACHE_MODELS`) and document them.
  - Check the TTL breakdown with `assertIn` and `assertIsInstance` before indexing it.

### Issue 5: The Python bridge HTTP test can't show that Responses column bindings override scalars, and one failing subtest corrupts the rest
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py
- **Line(s)**: 96-101, 122-153, 168-182, 212
- **Description**: Two weaknesses in the Responses cases:
  - The row values for `reasoning` (`{"context": "all_turns", "effort": "max"}`), `probability_count` (`2`) and `persist` (`True`) equal the scalar `reasoning`, `topLogProbs` and `store` options. The assertions on `reasoning`, `top_logprobs` and `store` pass whether the column wins, the scalar wins, or `setParams`, Prompt `copy().save()` or `load()` silently drops the column binding. Only `promptCacheKey` and `metadata` discriminate.
  - `assertIsNone(row.error)` (169) runs before `requests.get` (170). If a row fails after its request was sent, the body stays queued and each later subtest compares against the previous stage's body. Only the final `requests.empty()` (212) would notice.
  - Classification: a new weakness in test coverage; I found no product defect.
- **Risk**: A regression in Python-to-JVM transfer or persistence of `reasoningCol`, `topLogProbsCol` or `storeCol` would go undetected, and real failures would be harder to diagnose because of cascading mismatches.
- **Suggested Fix**: Use distinct column values (for example row effort `high` vs scalar `max`, row count `3` vs scalar `2`, scalar `store=False` vs row `True`) and assert the row values. Dequeue the body before checking `row.error`, or drain the queue in a per-subtest `finally`.

### Issue 6: Global defaults leak through the notebook, and the documented seed-recovery call is wrong in Python
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/OpenAI.ipynb
- **Line(s)**: 329-334, 1109, 1118
- **Description**: Two documentation and UX problems:
  - (a) The reasoning cell sets process-wide defaults for `reasoning_effort="low"` and `verbosity="low"` (plus deployment, key and URL), and the notebook never resets them. These defaults are applied at transform time, and `OpenAIResponses.mergeReasoning` requires the resolved effort to match `reasoning.effort` (`OpenAIResponses.scala:203-213`). A reader who extends the notebook with `setReasoning({"effort": "medium"})`, as the control table at 1109 suggests, hits a task-failing conflict caused by a hidden global.
  - (b) Line 1118 says to recover from the Responses global-seed rejection with `OpenAIDefaults.setSeed(...)` and `OpenAIDefaults.resetSeed()`. Those are the Scala method names (`OpenAIDefaults.scala:66,74`). Python exposes instance methods `set_seed` and `reset_seed` (`OpenAIDefaults.py:66-72`), so the documented recovery raises `AttributeError` in this Python notebook.
  - Classification: introduced by this diff. No current cell fails.
- **Risk**: Users following the notebook can hit confusing task failures, or a broken recovery step on exactly the error path this PR introduces.
- **Suggested Fix**: Write the Python form (`OpenAIDefaults().reset_seed()`) and mention the Scala names separately. Set effort and verbosity on the stage in that cell, or add `defaults.reset_reasoning_effort()` and `defaults.reset_verbosity()` after the demo.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
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
