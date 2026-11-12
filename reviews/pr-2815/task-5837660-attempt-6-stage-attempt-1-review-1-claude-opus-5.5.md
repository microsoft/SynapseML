---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 1
timestamp: "2026-11-10T19:20:18Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "4e2d0b9471812fa74134e25e402ce8792add15416f7dded2f9d5096ad757ee8c"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 32222 (char 32221)
Invalid CLI event on line 448: Expecting ',' delimiter: line 1 column 6370 (char 6369)
Invalid CLI event on line 754: Expecting ',' delimiter: line 1 column 3994 (char 3993)

## Original Response

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Opt-in throttling is off by default.**
  - `retryThrottledRequests` is a `BooleanParam` that defaults to `false` (`ContentUnderstandingParams.scala` 149–156, 173–177).
  - It reaches `Settings.retryThrottledRequests` (`ContentUnderstandingProtocol.scala` 109, default false) only through `getRetryThrottledRequests` (`ContentUnderstanding.scala` 147).
  - With the flag off, `withThrottlingRetries(false)` makes exactly one attempt and never writes `started` (`ContentUnderstandingBudget.scala` 16–32). The default 429 path is therefore the same as the previous `exchange` body.
- [x] **Retry predicate and Retry-After handling.**
  - Only a 429 is retried, and only for a GET or for a POST with no `Operation-Location` (`ContentUnderstandingProtocol.scala` 338–356).
  - `Operation-Location` and `Retry-After` are captured before the body is read (`exchangeBeforeDeadline` ~375–380). A 429 that carries a location is never replayed, even when its body is oversized or unreadable.
  - A failure with code 0 is never retried.
  - `retryDelay` (280–287) parses delta-seconds or an HTTP-date and falls back to `pollingDelay`.
- [x] **Budget refund and per-call budgets.**
  - `elapsed` is captured on entry and restored after each throttled attempt and its wait (Budget 17, 27). The final attempt that isn't a 429 is charged normally.
  - `pollWithClient` decrements `remaining` only after `exchange` returns (Protocol 621–633).
  - `withClient` creates a fresh budget for each call (~418).
- [x] **Cancellation.**
  - Every attempt starts with `checkInterrupted()`.
  - `pause` re-checks every 50 ms for both thread interrupt and `AsyncClient.isTaskCancelled`, and restores the interrupt flag (Budget 37–58; Protocol 271–278).
- [x] **Management requests are unaffected in the current code.**
  - Analyzer creation uses `HttpPut` (`ContentUnderstanding.scala` 481), sent through `exchange` (Protocol 765). It is never eligible under `safeToRetry`.
  - Management operation polls and analyzer GETs pass `managementGet = true` (Protocol 622, 717).
  - The only `HttpPost` is analyze (`ContentUnderstanding.scala` 335).
- [x] **`allowInputTruncation`.**
  - It is a `ServiceParam[Boolean]` with `isURLParam` (Params 138–147), included only in the analysis `activeParams` (`ContentUnderstanding.scala` 84–92).
  - Schema and value checks use BooleanType (168–200, 243–255, 271–288).
  - The query is built with `getValueOpt(...).map(_.toString)` (317–323): unset or null omits the option, and false sends `false`. There is no API-version allowlist.
  - Python kwarg validation uses `is None`, so `False` is kept (`Utils.py` 89–104).
  - Default `concurrency` is 1 (`HTTPTransformer.scala` 76). Together with `coalesce(1)`, this keeps the ordered query assertion in `test_ContentUnderstanding.py` 197–218 deterministic.
- [x] **Python credential handling.**
  - No `isSet`, `isDefined`, `hasDefault`, `getOrDefault` or `extractParamMap` overrides remain in core/cognitive main Scala templates or Python (grep).
  - `_transfer_params_from_java` drops ServiceParams from `_paramMap` (`Utils.py` 205–211). `_transfer_params_to_java` sends them through their setters, then drops them (226–231).
  - Credential `explainParam` redaction is kept (156–179).
  - The MLflow test covers configured, copied and loaded stages for both CU and AnalyzeDocument, and disables autolog in `finally` (`test_ContentUnderstanding.py` 220–297).
  - Other hand-written Python that uses generic inspection (EnsembleByKey, UDFTransformer, CNTKModel, LangchainTransform, deep-learning) works on ordinary or pure-Python params and is unaffected.
- [x] **Persistence in the guide and notebook.**
  - The guide's URL and binary examples write once, then read Parquet (`Content Understanding.md` ~68–69, ~96–97). Asserted by `test_ContentUnderstanding.py` 453–506.
  - The notebook writes handles once, polls only saved handles that can be resumed, and displays from saved reads. Exactly one POST is asserted (508–631).
- [x] **Scan of other Python tests for the generic-inspection change.**
  - Only `test_OpenAIResponseSchema.py:90` contradicts the new semantics.
  - The new semantics are asserted at `test_ContentUnderstanding.py:833,840` and `test_ServiceParamPythonBridge.py:182,196` (Issue 1).
- [x] **Notebook CI harness traced.** `DatabricksUtilities.scala` 248–276, `SynapseTests.scala` 49–73, `pipeline.yaml` 74–77, 230–259, and `tools/ci/e2e_impact.py` (Issue 2).
- [x] **Observation, not a defect:** with the opt-in flag, `Retry-After: 0` or `pollingDelay=0` gives back-to-back retries with no minimum delay. This follows the "respect Retry-After" requirement and the user's own configuration.
- [ ] **No builds, tests or commands were run.** This is a read-only review. Results the driver reported (182 Scala cases, 65 Python tests) were not re-executed. Live CU compatibility can't be checked offline and is not treated as a defect.

## Issues

### Issue 1: Removing the shared generic inspection overrides breaks an existing OpenAI Python test
- **Severity**: Medium
- **File**: cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py (caused by the change to core/src/main/python/synapse/ml/core/schema/Utils.py)
- **Line(s)**: 74–90 (failing assertion at 90); HasOpenAIResponseSchema.scala 81–82
- **Description**:
  - After this diff, nothing overrides `isSet`, so PySpark's `Params.isSet` only checks `param in self._paramMap`.
  - The test calls `setResponseSchema`, which sets the value on the JVM and removes `responseFormat` from `_paramMap` (HasOpenAIResponseSchema.scala 81–82). Line 87 asserts that removal, and `_transfer_params_to_java()` does not put the value back.
  - Line 90, `assertTrue(stage.isSet(stage.responseFormat))`, therefore fails for OpenAIChatCompletion, OpenAIPrompt and OpenAIResponses.
  - The new suites assert the opposite semantics (`test_ContentUnderstanding.py:833,840`; `test_ServiceParamPythonBridge.py:182,196`).
  - This test needs no credentials and is outside the two suites that were validated.
- **Risk**: The cognitive Python job will fail every time in the full build and on master after merge. A behavior change shared by every ServiceParam connector would land with a failing cross-connector test that's easy to dismiss as unrelated.
- **Suggested Fix**:
  - Check JVM state instead: `self.assertTrue(stage._java_obj.isSet(stage._java_obj.getParam("responseFormat")))`.
  - Optionally add `self.assertFalse(stage.isSet(stage.responseFormat))` to lock in the intended semantics.
  - Rerun `test_OpenAIResponseSchema.py` and `test_OpenAIPromptParams.py`, and preferably the full core and cognitive Python suites, since the shared mixin changed.

### Issue 2: The new CU notebook is picked up by the default Databricks CPU E2E job (and SynapseTests), where it cannot run
- **Severity**: High
- **File**: docs/Explore Algorithms/AI Services/Quickstart - Semantic Extraction with Content Understanding.ipynb; core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala; core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseTests.scala
- **Line(s)**: notebook JSON 46–66 (`input()` at 57 and 64, `getpass.getpass()` at 59); DatabricksUtilities.scala 250–267, 269–276; SynapseTests.scala 49–65, 70–73
- **Description**:
  - `NotebookFiles` collects every `.ipynb` under `docs/`. None of the `CPUNotebooks` exclusion substrings match this notebook, so it lands in one of the `DatabricksCPUTests1-5` partitions.
  - `testDatabricksE2E` defaults to true (`pipeline.yaml` 74–77), and impact detection runs every suite when cognitive Scala changes.
  - The cluster's environment sets only `PYSPARK_PYTHON` and provides no `CONTENT_UNDERSTANDING_*` values. The configuration cell therefore falls back to `input()`/`getpass()`, which in a non-interactive job either raise an error or block until the 20-minute per-notebook timeout.
  - Other AI Services quickstarts get their keys in CI through `find_secret`; this notebook has no such path. `SynapseTests.excludedNotebooks` doesn't exclude it either.
  - This is not a complaint about missing live credentials. It is a guaranteed harness failure caused by placing a notebook that needs user-supplied resources, and creates an analyzer, under the auto-executed docs tree.
- **Risk**:
  - A Databricks CPU partition fails on every later full build.
  - The obvious workaround, adding CU credentials to CI, would create a new billable analyzer on every run that the notebook deliberately never deletes. That conflicts with the no-provisioning / no-live-call instruction.
- **Suggested Fix**:
  - Add `.filterNot(_.getAbsolutePath.contains("Content Understanding"))` to `CPUNotebooks`, with a comment that the notebook needs a user-supplied CU resource and storage and is validated offline against loopback.
  - Add `"ContentUnderstanding"` to `SynapseTests.excludedNotebooks` (generated notebook names drop spaces).
  - Optionally add an nbtest assertion that the notebook is excluded.
  - Don't change `pipeline.yaml` or wire a live resource without approval.

### Issue 3: No regression test locks in "analyzer management unchanged" when throttling retries are enabled
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: Whole suite (no management case with `setRetryThrottledRequests(true)`); production code at ContentUnderstandingProtocol.scala 343–355, 622, 717, 765
- **Description**:
  - The requirement holds only implicitly. The creation PUT is excluded by the HTTP-method check in `safeToRetry`, and management GETs are excluded by `managementGet = true`.
  - The suite covers analysis retries, no-replay, unchanged ordinary limits and interrupt cancellation, but never turns the flag on while calling `createAnalyzer` or `getAnalyzer`.
- **Risk**: A later change could silently give management calls unbounded retries that don't count against their budgets. Examples: treating PUT as retryable, or a new management GET caller that omits `managementGet = true`.
- **Suggested Fix**:
  - Add a stub-backed test with the flag enabled that returns 429 for the creation PUT, the management operation GET and the `getAnalyzer` GET. Assert the same outcomes, request counts and `ManagementGetAttempts`/`maxPollAttempts` bounds as with the flag off.
  - Optionally add a cancellation case (`AsyncClient.isTaskCancelled`, without a thread interrupt) during a throttled wait.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; neither human nor repository selects a tier",
    "models": "common:review-code Medium complementary-family rule, resolved against current task tool catalog",
    "stageBehavior": "common:review-code Medium six concurrent independent lenses",
    "safetyStop": "No new timeout, credit, or iteration bound; user prohibits introducing timeouts",
    "delivery": "Human iteration-mode instruction: no commit/push without explicit approval"
  },
  "changeSize": "Thirteen source/test/documentation files relative to PR head, including three untracked additions. Tracked diff is 602 additions and 188 deletions; most volume is tests and documentation. Production changes are localized to four CU Scala files and removal of four Python inspection overrides.",
  "cyclomaticComplexity": "Moderate additions to existing flows: cancellable tail-recursive 429 retry, elapsed-budget refund, GET/POST eligibility, optional Boolean scalar/column query values. Existing transport, request deadlines, task ownership, and management state machines are reused rather than redesigned. Shared Python inspection has cross-connector compatibility implications.",
  "rationale": "Six independent lenses are justified by authentication, budget/cancellation, persistence and notebook interactions. Scope is the incremental uncommitted review fixes, not a new whole-connector implementation. Medium provides complementary review without treating documentation/test volume as a new high-complexity state machine.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "SynapseML AGENTS.md and .github/skills/code-review/SKILL.md require checks and evidence but specify no tier/model override. Existing user iteration-mode policy overrides the skill's automatic commit/PR step.",
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "Source-level offline review cannot establish live CU contract compatibility; live validation remains blocked on endpoint and secure credentials."
}

## Driver Outcomes and Resolution - 2026-11-10

### Review gate: BLOCKED, not clean

All six Medium lenses ran concurrently with `claude-opus-5.5` and maximum
reasoning, through the skill-owned Execute runner. Each CLI process exited zero,
but every runner invocation exited one with `persisted: true` and
`outcome: failed`. All six original artifacts, responses, raw event streams and
receipts are retained. The original reviewer verdicts above and in the sibling
artifacts have not been replaced by this driver assessment.

The malformed JSON records occur at redacted authorization assertions: the
masked text contains an unescaped closing quote. This is consistent with CLI
redaction corrupting serialized JSON. The IDE, npm and WinGet launchers all report
CLI `1.0.108-1`. No masking was disabled, transport records repaired, runner
modified, or unchanged six-review batch retried. Recovered feedback is useful
for investigation, but **none of the six invocations counts as independent
signoff**. A supported transport correction and new stage-attempt identities are
required before retrying against an updated frozen diff.

The source HEAD remains `41b54ec47d514aac82494c70ed2963a8bc69ff5d`.
The original reviewed diff had SHA-256
`E34C782D4A90662A5733B67A17C33C32838499F16B899A79101789A41440987A`.
The four test/harness files changed below were modified only after collection;
they were not part of that snapshot and have not received valid independent
review. Nothing was committed, pushed, posted to GitHub/ADO, or run against CU.

### Confirmed regressions fixed

1. **OpenAI generic inspection contract** (lens 1 issue 1; lens 5 issue 1).
   Reproduced the failing assertion for OpenAIChatCompletion, OpenAIPrompt and
   OpenAIResponses. Updated `test_OpenAIResponseSchema.py` to assert that Python
   `isSet` is false while JVM `isSet` remains true, retaining the actual response
   schema assertion. This tests the intended credential-safe contract without
   restoring the removed inspection overrides. Both OpenAIResponseSchema and
   OpenAIPromptParams suites now pass: **29 tests and 154 subtests**.
2. **Interactive notebook in unattended E2E** (lens 1 issue 2; lens 2 issue 1;
   lens 4 issue 1; lens 5 issue 2; lens 6 issue 1).
   Added the specific semantic-notebook exclusion to
   `DatabricksUtilities.CPUNotebooks` and `SynapseTests.excludedNotebooks`.
   Added a `DatabricksUtilitiesSuite` regression proving that the notebook is
   discovered but absent from CPU selection and all five CPU partitions.
   The test failed before the exclusions and passed afterward. Core test
   compilation and repository-wide test Scala style also passed. SynapseTests
   itself was not run because its constructor performs live provisioning.
   Existing notebook-cell loopback coverage remains enabled.

### Remaining recovered findings

These are preserved as open suggestions, not silently waived or claimed fixed.
The scope of this correction was the two reproduced regressions; it did not
change the user-selected opt-in retry policy or introduce a broader refactor.

| Feedback reference | Finding | Driver disposition |
|---|---|---|
| L1 I3; L2 I3; L3 I2; L5 I3 | Explicit opt-in management non-regression tests; make request eligibility more explicit | Open coverage/design suggestion. Existing method/management gates remain unchanged; no management regression was reproduced. |
| L5 I4 | Preserve time already spent before a throttling refund | Open test gap; no new arithmetic test or production change. |
| L5 I5 | TaskContext-based Spark cancellation during opt-in throttling | Open test gap; current tests cover ordinary Spark cancellation and direct thread interruption during throttling, not this combination. |
| L5 I6, I8 | Explicit GA truncation, poll-mode ignored columns, precise rejection message | Open coverage/assertion suggestions. |
| L5 I7 | Missing/invalid Retry-After fallback coverage | Open test gap. |
| L2 I2; L3 I1; L4 I2; L6 I3 | Minimum retry delay, exponential backoff and jitter | Deferred behavioral change, not implemented. Existing Retry-After and pollingDelay semantics are preserved, including zero delay. |
| L6 I2 | Rate-limited throttling diagnostics and related documentation | Deferred observability expansion; no logging/counters added. |
| L2 I4 | Move retry loop/refund responsibilities and consolidate predicates | Deferred architectural refactor; no demonstrated runtime defect. |
| L2 I5 | Rename/regroup public preview parameter traits | Deferred API/design suggestion. |
| L3 I3 | Guide overwrite mode can replace saved handles on rerun | Open documentation/usage risk. The new notebook already uses errorifexists; guide write modes were not changed in this correction. |
| L3 I4; L4 I3 | Empty or wrong-shaped model-deployments environment JSON | Open notebook configuration improvement. The optional value must currently be absent or valid mapping JSON. |
| L4 I4 | Stale assertion messages recommend the removed shared override | Open test-message clarification. |
| L2 I2; L4 I2; L6 I3 | pollingDelay parameter/table omit its opt-in 429 fallback role | Open documentation clarification; the throttling prose already describes the fallback. |

### Offline validation evidence

Logs are retained under the session `files` directory:
`C:\Users\boraelkin\.copilot\session-state\f5a570d8-0a43-4724-aa89-1085005d69a4\files`.

| Check | Result | Log |
|---|---|---|
| Core/cognitive compile, test compile, production/test Scala style, cognitive codegen | Passed | `synapseml-offline-scala.log` |
| Shared core + CU Scala suites | 66 + 182 passed | `synapseml-offline-scala.log` |
| CU, ServiceParam, actual MLflow, notebook cells/recovery, HTTP exports | 66 passed; 70 subtests passed; 7 live CU tests skipped | `synapseml-offline-python-final.log` |
| OpenAI regression reproduction | 3 expected failing subtests before correction | `synapseml-review-openai-repro.log` |
| OpenAI schema and prompt suites after correction | 29 passed; 154 subtests passed | `synapseml-review-openai-fixed.log` |
| Notebook-selector reproduction | 1 expected failure before exclusions | `synapseml-review-notebook-repro.log` |
| Selector regression, core test compile, test Scala style after correction | Passed; 1 test | `synapseml-review-notebook-fixed.log` |
| Black 22.3.0 on Python sources; corrected OpenAI test formatted | Passed; original sweep checked 219 files | `synapseml-offline-black.log` |
| Changed notebook schema, syntax, empty outputs, plain-Python cell formatting and export | Passed | `synapseml-offline-notebook.log`, `synapseml-offline-notebook-format.log` |
| CI helper suites | 334 passed, 442 subtests passed, 12 skipped, 42 failed due to missing Bash/WSL environment | `synapseml-offline-ci-tests.log` |
| Full Black Jupyter-extra route | Blocked: package-download TLS failures and missing cached dependencies; changed notebook cells checked separately | `synapseml-black-extras-install.log`, `synapseml-black-extras-offline.log` |

The functional total is **249 Scala tests, 95 Python tests and 224 Python
subtests**, across the listed runs, not a single full-repository test run.
This is not an all-green repository validation: the CI-shell failures, full
Jupyter-formatting limitation, open feedback and failed formal review remain
visible. Checks use generated bindings and local class directories, not matching
packaged release artifacts. Final live approval still requires the endpoint,
secure credentials and matching Python/JVM packages from the approved PR head.
