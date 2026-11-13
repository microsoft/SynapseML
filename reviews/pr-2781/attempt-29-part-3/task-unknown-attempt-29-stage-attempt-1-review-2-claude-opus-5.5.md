---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 2
timestamp: "2026-11-10T13:30:15Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0df20509e85fbc231f11dd69fa601af69a4eb3553be499c46089388d9d3c897e"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/attempt-29-part-3/task-unknown-attempt-29-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 7
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the full diff for all 14 assigned paths, judged against AGENTS.md. Rules applied: follow nearby patterns, credential tests skip when credentials are absent, keep core behavior in Scala, never print secrets.
- [x] Confirmed that every docs notebook runs in CI.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala` lines 250–266: every docs `.ipynb` is collected, and `OpenAI.ipynb` is not excluded.
  - `SharedNotebookE2ETestUtilities.scala` lines 93–130 and the Databricks CPU E2E jobs in `pipeline.yaml` lines 230–259.
  - `SynapseTests.scala` lines 47–62 exclude only Langchain and the Document QA notebook.
- [x] Mapped the headings, assertions and disclosures in `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` with grep and targeted views.
  - Headings: 718, 727, 762, 793, 859, 894, 974, 1013, 1067, 1098, 1127, 1161, 1245.
  - Assertions: 883–885, 962–966, 1002–1005, 1055–1059.
  - Disclaimers at 795 and 1085; behavior-change notices at 1100–1102; seed guidance at 1118; defaults cell at 320–335.
- [x] Checked how Prompt builds messages: `OpenAIPrompt.scala` lines 566–583 send the system prompt as role `"system"` with `input_text` content in Responses mode. The notebook's warm-up instead uses a `"developer"` message with an explicit breakpoint (notebook lines 838–852).
- [x] Checked the persistence opt-in: `OpenAI.scala` line 206 adds `deploymentName` and line 584 adds `handler`; `ComplexParamsSerializer.scala` lines 21, 71 and 76; guard key at `Serializer.scala` lines 31–32.
- [x] Checked the Python defaults API: `cognitive/src/main/python/synapse/ml/services/openai/OpenAIDefaults.py` line 19 is an instance class, with `set_seed`/`get_seed`/`reset_seed` at lines 66–73.
- [x] Checked the global-parameter mechanics (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` lines 43–52) and the reasoning conflict check (`OpenAIResponses.scala` lines 203–213). The notebook's explicit `setReasoningEffort("low")` matches the global `"low"`, so there is no conflict.
- [x] Checked the live harness (`test_OpenAICacheLive.py` lines 4–11, 26–44, 129–152, 177–189, 318–368) against the existing Foundry suite (`AIFoundryChatCompletionSuite.scala` lines 16 and 27).
- [x] Checked default-deployment coverage: `OpenAIRuntimeDefaultsSuite.scala` lines 52–67 and `test_OpenAIDefaults.py` line 138.
- [x] Grepped docs, source and website for `gpt-6.1-sol` and `aoai_model_name_query`.
  - `gpt-6.1-sol` appears only at Document QA notebook line 155.
  - `aoai_model_name_query` is unused in the current notebook (lines 893–902). The versioned website docs show its old use as `model_name=`.
- [x] Compared the local stub-server patterns: `test_LangchainTransform.py` `openai_server()` (about lines 25–108), `test_OpenAIMultimodalRequest.py` `EchoOpenAIHandler` (lines 20–93), and `test_ServiceParamPythonBridge.py` lines 33–91.
- [ ] Did not run any commands, builds, tests or notebooks. This was a read-only review, so runtime behavior is inferred from source.
- [ ] Other partitions were consulted only as context, not reviewed exhaustively.

## Issues

### Issue 1: CI-run docs notebook asserts provider cache hits that are not guaranteed
- **Severity**: Medium
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 885, 962–966, 1002–1005, 1055–1059. Contradicting text at 795 and 1085.
- **Description**: The new caching cells assert `row.cached_tokens is not None and row.cached_tokens > 0` for:
  - the scalar Responses rows (966);
  - the column-bound rows (1005);
  - the OpenAIPrompt implicit-cache rows (1059).

  The native SDK cell also asserts a token threshold (885). All docs notebooks run in the Databricks CPU E2E jobs (`DatabricksUtilities.scala` lines 250–266; `pipeline.yaml` lines 230–259).

  Elsewhere the PR treats cache hits as nondeterministic and provider-owned:
  - Cache-hit qualification lives in `test_OpenAICacheLive.py`, gated behind `SYNAPSEML_LIVE_CACHE_TESTS=1` (lines 26–29).
  - The notebook itself says hits are **not guaranteed** (795) and that a failed assertion "does not by itself identify a client defect" (1085).

  The Prompt cell is especially fragile (well-founded but speculative). `OpenAIPrompt` sends `stable_prefix` as a `"system"` message (`OpenAIPrompt.scala` lines 574–577) in implicit mode. The only warm-up sends the prefix as a `"developer"` message with an explicit breakpoint (notebook 838–852). So the first Prompt row, or both rows if they run concurrently, may report `cached_tokens == 0`.

  The existing schema E2E assertions (497–554) check only deterministic, error-free behavior. These new asserts depart from that pattern.
- **Risk**: The new assertions can fail intermittently in the CPU notebook E2E jobs. Provider routing, model support, TTL behavior or partition timing would then block unrelated PRs. This regression is new in this PR.
- **Suggested Fix**:
  - Keep the deterministic asserts: `cache_error is None`, non-empty output, and row counts.
  - Print `cached_tokens` instead of asserting a hit, or gate the hit asserts behind the existing opt-in environment variable.
  - Leave hit qualification to `test_OpenAICacheLive.py`.
  - Update the sentence at line 1085 to match.

### Issue 2: Caching chapter splits the usage chapter and buries the approved behavior-change notices
- **Severity**: Medium
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 718, 727, 762, 793, 1098–1102, 1127, 1161, 1245
- **Description**: The new H2 "## Prompt caching: request options and input breakpoints" (793) is inserted inside "## Working with Usage Statistics" (718). As a result:
  - The usage chapter's "### Chat Completions with `usageCol`" (727) and "### Responses API with `usageCol`" (762) stay under the original H2.
  - "### Embeddings with `usageCol`" (1127), "### Responses API with `store` and Response Chaining" (1161) and "### OpenAIPrompt with `store`…" (1245) now appear under "Prompt caching".

  The two explicitly approved compatibility changes are bold paragraphs inside "### Additional inference controls" (1098), which is itself nested under the caching chapter near the end of the notebook:
  - Chat and Prompt Chat reject `echo`, `bestOf` and `cacheLevel` (1100).
  - Responses now forwards `temperature`/`top_p` for `gpt-5` deployment names (1102).

  The contract requires the notebook to disclose these prominently. The website docs are generated from these notebooks, so the broken outline also reaches the published table of contents.
- **Risk**: Users upgrading Chat pipelines or gpt-5 Responses pipelines will not find the breaking-change notices under the heading where they would look. The usage and storage sections also become hard to find. This regression is new in this PR.
- **Suggested Fix**:
  - Move the caching chapter after the last usage/store section, or make it a sibling H2 that does not split "Working with Usage Statistics".
  - Promote "Additional inference controls" to its own H2.
  - Add a short "Compatibility and behavior changes" H2 near the top (or next to "Chat Completion with Reasoning Models", line 306) that holds both notices.

### Issue 3: Python notebook gives Scala API names as the fix for global-seed rejection
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`
- **Line(s)**: 1118
- **Description**: The text tells users to call `OpenAIDefaults.setSeed(...)` and `OpenAIDefaults.resetSeed()`. Those are the Scala object methods. The Python wrapper is an instance class with snake_case methods `set_seed`/`reset_seed` (`cognitive/src/main/python/synapse/ml/services/openai/OpenAIDefaults.py` lines 19 and 66–73). The same notebook already uses the Python form elsewhere (`OpenAIDefaults.set_deployment_name(...)` at 308; `defaults = OpenAIDefaults()` / `defaults.set_*` at 329–334).
- **Risk**: A Python user who copies the documented fix gets an `AttributeError`. That is exactly when they need the fix: Responses has just rejected a global seed. The guidance is also inconsistent with the rest of the notebook.
- **Suggested Fix**: Use `OpenAIDefaults().reset_seed()` and `set_seed(...)`, or `defaults.reset_seed()` with the instance from the setup cell. Mention the Scala names only as a qualified aside, if at all.

### Issue 4: Opt-in live Foundry check hard-codes models and reuses the Azure OpenAI deployment name
- **Severity**: Low
- **File**: `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py`
- **Line(s)**: 4–11, 129–152, 327, 347
- **Description**: The harness takes all endpoint settings from environment variables, and its docstring lists the required ones (lines 6–11). However, `test_foundry_model_inference` loops over `(self.model, "Llama-3.3-70B-Instruct")` (327):
  - `self.model` is `OPENAI_CACHE_MODEL`, the Azure OpenAI deployment, but here it is also used as a Foundry model id.
  - The third-party model id is hard-coded, and the docstring does not mention either requirement.

  The model name does match the existing Scala Foundry suite (`AIFoundryChatCompletionSuite.scala` line 16). Even so, this opt-in harness is configured through environment variables, and its contract does not cover this coupling.

  Speculative: `details()` (129–152) requires `cache_write_tokens` for every model. Non-OpenAI models may not report it, which would make that subtest fail by construction rather than by provider behavior.
- **Risk**: A qualification run on an endpoint without one of those exact model ids fails in a way that looks like a cache defect. The results are also hard to reproduce across environments.
- **Suggested Fix**:
  - Read the Foundry model list from a documented environment variable, such as a comma-separated list, and skip when it is unset.
  - Document it in the module docstring.
  - Decide explicitly whether missing write counters mean "not qualified" (record and skip) or a failure.

### Issue 5: Bespoke third stub server and scalar/column values that do not prove precedence
- **Severity**: Low
- **File**: `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py`
- **Line(s)**: about 28–91 (inline handler, Queue and thread), 96–101 (row values), 122–153 (scalar options and column bindings), 174–175 and 182 (assertions)
- **Description**:
  - **Structure:** This module was a generic service-parameter bridge test. It now holds OpenAI-specific wire-format tests and adds a third ad-hoc local HTTP server pattern. Existing patterns already cover this: the `openai_server()` context manager in `test_LangchainTransform.py` and the `EchoOpenAIHandler` class fixture in `test_OpenAIMultimodalRequest.py` (lines 20–93). The guard save/restore block is also re-implemented inline; `test_OpenAIResponseSchema.py` has its own copy.
  - **Coverage:** For three Responses controls, the scalar and column values are identical, so the assertions pass whether or not the column binding replaced the scalar:
    - `topLogProbs` 2 vs. column `probability_count` 2;
    - `store` True vs. column `persist` True;
    - the same `reasoning` map in both places.

    Only `promptCacheKey` and `metadata` actually test column precedence. The notebook (around line 1106) states that "a column binding replaces the scalar binding".
- **Risk**: A precedence regression for `topLogProbs`, `store` or `reasoning` would go unnoticed. The duplicated server and guard code raise maintenance cost and drift between suites.
- **Suggested Fix**:
  - Give each control different scalar and row values, with per-API expected values. For example: scalar `topLogProbs=1` vs. row 2; scalar `store=False` vs. row True for Responses; scalar `{"effort":"low"}` vs. a row reasoning map.
  - Extract a shared local OpenAI stub server and a guard-config context manager into a test helper.
  - Consider moving the OpenAI wire-format tests under `services/openai/`.

### Issue 6: Schema persistence test still enables unsafe Java deserialization
- **Severity**: Low
- **File**: `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py`
- **Line(s)**: 224–246 (the override is at 239), 45–49 and 85–88
- **Description**: `test_copy_and_save_load_preserve_the_existing_response_format_parameter` sets `spark.synapseml.legacy.allowUnsafeJavaDeserialization` to `"true"` to load stages it just saved. With this PR, an unset built-in handler is no longer persisted (`OpenAI.scala` line 584; `ComplexParamsSerializer.scala` lines 21, 71 and 76), so the override appears unnecessary. The new bridge test loads all three stage types with the guard set to `"false"`.

  The override also conflicts with the PR's guidance to recreate stages "rather than disabling the protection" (notebook line 808). It means this suite could not catch a regression that re-serializes the default handler.

  This code predates the PR; the PR made it stale. Separately, the new `test_decimal_schema_limits_remain_exact` copies the `response_format()` helper's parsing (85–88 vs. 46–49) only to pass `parse_float=Decimal`.
- **Risk**: The test keeps passing even if the handler is re-serialized. The docs and tests also send mixed signals about the guard.
- **Suggested Fix**: Load with the guard set to `"false"`, or remove the override. Add an optional `parse_float` argument to `response_format()` and reuse it in the decimal test.

### Issue 7: Document QA notebook recommends an unsupported, unexplained alternative model
- **Severity**: Low
- **File**: `docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs.ipynb`
- **Line(s)**: 155–157
- **Description**:
  - The new comment "# Use gpt-6.1-sol instead when the workload needs a larger model." is the only occurrence of that name in docs, source or website content. No other part of the PR explains it.
  - It conflicts with the OpenAI notebook's guidance to use the endpoint's actual deployment name (`OpenAI.ipynb` line 308).
  - The PR also rewrote `aoai_model_name_query`, which is unused in the current notebook (the `ChatOpenAI` call at 893–902 uses `aoai_deployment_name_query`). That dead variable predates the PR.
- **Risk**: Readers may try a deployment name that does not exist on their endpoint. The leftover variable also suggests a model/deployment distinction that the notebook no longer uses.
- **Suggested Fix**: Drop the comment or replace it with generic guidance to use your endpoint's deployment name. Optionally remove the unused `aoai_model_name_query`.

## Assigned-path coverage
| Path | Assessment | Evidence or rationale |
|---|---|---|
| `cognitive/src/test/python/synapsemltest/services/langchain/test_LangchainTransform.py` | assessed | Only model-string swaps in the mock server response and LangChain client constructors; no assertion depends on the model. The existing `openai_server()` context manager is unchanged and serves as the reference fixture for Issue 5. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIChatCompletionMultimodal.py` | assessed | Explicit `setDeploymentName("gpt-6-luna")`, consistent with no modality routing. Breakpoint validation follows the per-row `errorCol` convention. No architectural deviation. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIDefaults.py` | assessed | The live test now sets the global deployment equal to the built-in default (line 138), but global precedence is covered by `OpenAIRuntimeDefaultsSuite.scala` lines 64–67. The snake_case API is used consistently. No finding. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIMultimodalRequest.py` | assessed | Explicit deployment selection. The `EchoOpenAIHandler` class fixture (lines 20–93) follows existing style and is cited as the reusable pattern in Issue 5. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` | assessed | The new exact-Decimal test (75–89) discriminates correctly. Helper duplication and the stale unsafe-deserialization override (239) are reported in Issue 6. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponsesMultimodal.py` | assessed | Mirrors the Chat multimodal row-validation tests for Responses with explicit deployment selection. The two APIs stay symmetric. No finding. |
| `cognitive/src/test/python/synapsemltest/services/openai/test_StructuredOutput.py` | assessed | Deployment/model rename within the existing test structure. No pattern or abstraction change. |
| `cognitive/src/test/python/synapsemltest/services/test_OpenAICacheLive.py` | assessed | Opt-in environment gating (26–37) matches the AGENTS.md credential-skip rule. Credentials are attached after save/load (178). Foundry model coupling is reported in Issue 4. |
| `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` | assessed | The guard-off reload test (about 224–240) correctly tests the persistence opt-in. The bespoke stub server, misplaced OpenAI wire tests and non-discriminating values are reported in Issue 5. |
| `docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs.ipynb` | assessed | The deployment rename and the `ChatOpenAI` v1 base-URL usage (893–902) are consistent. The unexplained `gpt-6.1-sol` comment and dead variable are reported in Issue 7. |
| `docs/Explore Algorithms/OpenAI/Langchain.ipynb` | assessed | Deployment rename only (about line 161), consistent with sibling notebooks. Its Synapse E2E exclusion (`SynapseTests.scala` line 55) is unchanged. |
| `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` | assessed | Issues 1–3: CI-executed cache-hit asserts, broken outline and buried compatibility notices, and Scala API names in Python guidance. The defaults cell's global reasoning effort matches the cells' explicit values (verified against `OpenAIResponses.scala` lines 203–213). |
| `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb` | assessed | Explicit multimodal-capable deployment (about line 52) with a prerequisite note (about line 26). Consistent with no automatic modality routing. No finding. |
| `docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms.ipynb` | assessed | `OpenAIPrompt` uses explicit `openai_deployment_name` (about 446–470). The OpenAI SDK v1 client uses `max_completion_tokens` and `reasoning_effort` (about 727–780). Consistent; no finding. |

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
- **How verified**: pending

### Issue 7
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver action
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
