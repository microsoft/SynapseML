---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 2
timestamp: "2026-11-10T19:17:45Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "d76c8afac638a2f9ed1810b92e34d23d43ba9c5528459950beea18e71c35c04e"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 32261 (char 32260)
Invalid CLI event on line 607: Expecting ',' delimiter: line 1 column 3994 (char 3993)

## Original Response

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the changed production code in these files:
  - `ContentUnderstanding.scala`: `activeParams` at 84–92, `settings()` at 121–148, `expectedType`/`validateValue` at 168–197 and `analyzeUrl` at 317–323.
  - `ContentUnderstandingBudget.scala`: lines 13–56.
  - `ContentUnderstandingParams.scala`: lines 12–13, 76–90 and 107–176, plus the existing `private[contentunderstanding] trait ContentUnderstandingPython` at line 179.
  - `ContentUnderstandingProtocol.scala`: `retryDelay` at 280–287, `admissionRejected` at 335–336, `exchange` at 338–356, `exchangeBeforeDeadline` at 358–406, `pollWithClient` at 603–636, `execute` at 638–662, `getAnalyzerResponse` at 713–733 and `createAnalyzer` at 761–797.
- [x] I compared the new 429 loop with the repository's existing 429 handling in `core/.../io/http/HTTPClients.scala`. `MaxBackoffMs` is at line 107. Lines 258–281 use exponential backoff and jitter when Retry-After is missing, and say this is "to prevent thundering herd".
- [x] I checked the Python bridge and code generation:
  - `Wrappable.scala` ServiceParam getter generation still uses `_service_param_scalar_to_python` (line 324).
  - In `Utils.py` (106–240), the remaining helpers are still used. Removing the generic `isSet`/`hasDefault`/`getOrDefault`/`extractParamMap` overrides matches the human requirement.
  - Other cognitive services already use `ServiceParam[Boolean]` with `isURLParam = true`.
- [x] I traced how the notebook E2E harness finds notebooks:
  - `DatabricksUtilities.scala`: 248–276, plus cluster `spark_env_vars` at 458–462.
  - `DatabricksCPUTests.scala`: 19–47, with no base parameters.
  - `SynapseTests.scala`: 49–73.
  - `SharedNotebookE2ETestUtilities.scala`: 89–105.
  - `FabricNotebookTests.scala`: 226–240. It uses an include list, so it is not affected.
  - `tools/ci/e2e_impact.py`: 39–64 returns every suite for any changed `docs/**.ipynb`.
  - `pipeline.yaml`: 230–261.
  - The existing secret pattern is `find_secret` in `Quickstart - Analyze Text.ipynb` and `Platform.py` 57–87.
- [x] I reviewed the tests and test setup:
  - `ContentUnderstandingThrottlingSuite.scala`: all 6 tests. None covers analyzer management with opt-in, and none covers a 429 without Retry-After.
  - `ContentUnderstandingOptionsSuite.scala`.
  - `test_ContentUnderstanding.py`: 170–300 and 508–620. Autologging is turned off in `finally`.
  - `test_ServiceParamPythonBridge.py`.
  - `mlflow==2.21.3` is pinned at `environment.yml:36`, so the module-level import is fine.
- [x] I reviewed the docs and conventions:
  - `Content Understanding.md`: 180–225 and 360–500.
  - `Use with MLFlow/Autologging.md`: 12–17.
  - `website/sidebars.js`: 34–46.
  - `AGENTS.md`.
  - `scalastyle-config.xml`: 56–57 (`maxMethods` = 50).
- [ ] No builds, tests or commands were run. This is a read-only lens, so I relied on the validation evidence you supplied. The live Content Understanding (CU) contract, such as whether 429 responses include Retry-After, can't be checked offline.

## Issues

### Issue 1: The new semantic notebook is picked up and run by the Databricks CPU notebook E2E harness, but it can't run there
- **Severity**: High
- **File**: docs/Explore Algorithms/AI Services/Quickstart - Semantic Extraction with Content Understanding.ipynb; core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala; core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseTests.scala
- **Line(s)**:
  - Notebook: 46–84 (configuration cell), 55–65 (fallback to `input()`/`getpass`), 75 (`analyzer_id` built from the run ID), 214 (`createAnalyzer`), 395 (no automatic deletion).
  - `DatabricksUtilities.scala`: 251–252, 256–267, 272–276, 460–462.
  - `SynapseTests.scala`: 49–65.
  - `tools/ci/e2e_impact.py`: 39–64.
  - `pipeline.yaml`: 230–261.
- **Description**: The repository runs every `docs/**/*.ipynb` as an E2E test:
  - `NotebookFiles` collects all notebooks under docs. `CPUNotebooks` removes only a fixed name list, and `cpuNotebookPartition` feeds the result to `DatabricksCPUTests1`–`5`.
  - The new notebook is not on that list. `e2e_impact.py` selects every notebook suite for any changed `docs/**.ipynb` or cognitive source file, so this PR runs the Databricks CPU E2E job.
  - The test clusters set only `PYSPARK_PYTHON`, and the CPU suites pass no base parameters. The notebook therefore falls back to `input()` and `getpass`, which have no stdin in a job run.
  - Even with values supplied, the notebook needs a live CU resource and durable storage. Each run would also leave behind a new `synapseml-purchase-order-<run_id>` analyzer, which the notebook deliberately does not delete.
  - Other AI Services notebooks use `find_secret(..., keyvault="mmlspark-build-keys")` so the harness can run them. This notebook intentionally relies on user-supplied configuration and local Spark, which matches the human requirement, so it must be excluded from the harness.
- **Risk**: The Databricks CPU E2E job, part of the full `pipeline.yaml` build, would fail on this PR and on master. It would either error immediately or block until the 20-minute `TimeoutInMillis` and then retry once. `SynapseTests` would fail the same way if it is re-enabled. If CI credentials were ever added, every run would leak an analyzer.
- **Suggested Fix**:
  - Add an explicit exclusion with a reason comment to `DatabricksUtilities.CPUNotebooks`, for example: `.filterNot(_.getAbsolutePath.contains("Semantic Extraction with Content Understanding")) // Requires user-supplied CU resource/storage; covered by the loopback notebook test`.
  - Add `"SemanticExtractionwithContentUnderstanding"` to `SynapseTests.excludedNotebooks`.
  - Coverage is kept by `test_semantic_notebook_executes_and_recovers_saved_handles` (`test_ContentUnderstanding.py` 508–620).
  - These are test-code edits, not `pipeline.yaml` or workflow changes.

### Issue 2: Unlimited opt-in 429 retries wait a fixed `pollingDelay`, which can be zero, with no backoff or jitter
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; ContentUnderstandingParams.scala; docs/Explore Algorithms/AI Services/Content Understanding.md
- **Line(s)**:
  - `ContentUnderstandingProtocol.scala`: 351–355 and 280–287.
  - `ContentUnderstandingBudget.scala`: 16–32.
  - `ContentUnderstandingParams.scala`: 114–118.
  - `Content Understanding.md`: 381–382 and 479.
  - For comparison, `core/.../HTTPClients.scala`: 107 and 258–281.
- **Description**:
  - The loop deliberately has no retry-count or time limit, so the wait between retries is its only protection.
  - When Retry-After is missing or invalid, `retryDelay(raw.retryAfter, settings.pollingDelay)` returns the fixed `pollingDelay`. That is a constant 1000 ms by default, with no growth and no jitter.
  - `pollingDelay` may be set to 0, which `ParamValidators.gtEq(0)` allows and the suites use. Then `pause(0)` returns at once and requests go out back-to-back indefinitely.
  - The repository's existing handler for unlimited 429 retries (`HandlingUtils.sendWithRetries`) uses exponential backoff, capped at `MaxBackoffMs`, plus jitter when Retry-After is absent.
  - The `pollingDelay` parameter description, which also becomes the generated Python docstring, and the `setPollingDelay` row in the settings table still describe only polling and management-GET fallback. Only the prose at lines 381–382 mentions the new role.
  - No test covers opt-in 429 responses without Retry-After. The suite uses only Retry-After values of 2, 0 and 60.
- **Risk**: If the service keeps throttling without Retry-After, every partition's concurrent workers retry in lockstep at a fixed rate, or in a busy loop at 0 ms. This extends the throttling, inflates request volume and executor CPU, and works against the purpose of throttling recovery.
- **Suggested Fix**:
  - For opt-in analysis throttling only, when Retry-After is absent or invalid, use capped exponential backoff with jitter. Seed it from `max(pollingDelay, positive floor)`, or reuse the core constants.
  - Keep a valid Retry-After authoritative.
  - Update the `pollingDelay` parameter description and the docs table row.
  - Add a throttling test with `pollingDelay=0` and repeated 429s that lack Retry-After, asserting that waits are non-zero and grow.
  - Leave default, management and non-429 behavior unchanged.

### Issue 3: Limiting the unbounded retries to analysis is implicit and allows retries by default; the management exclusion is untested
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; ContentUnderstanding.scala; cognitive/src/test/scala/.../ContentUnderstandingThrottlingSuite.scala
- **Line(s)**:
  - `ContentUnderstandingProtocol.scala`: 338–343, 352–353, 405, 622, 717, 765, 779–781.
  - `ContentUnderstanding.scala`: 147, 475–492.
  - `ContentUnderstandingParams.scala`: 150–153.
- **Description**:
  - `settings()` passes `retryThrottledRequests` into the management calls too: `createAnalyzer` and `getAnalyzer` use the same settings.
  - Management is excluded only by two incidental mechanisms:
    - The existing `managementGet` flag was an input to transport-failure classification (line 405). It now also decides throttling scope for management GETs and polls (lines 622 and 717).
    - The `createAnalyzer` PUT (line 765) calls `exchange` with the default `managementGet = false`, so retries are enabled for it. It is skipped only because `safeToRetry` accepts just GET and POST.
  - Any new `exchange` caller is eligible for retries by default. A future management POST, such as an analyzer-copy operation, added through `exchange(client, settings, request, budget)` would silently get unbounded 429 replays for users who opted in. That contradicts the parameter description, "Does not affect analyzer management."
  - No test sets `setRetryThrottledRequests(true)` on `createAnalyzer` or `getAnalyzer`. The existing management tests use the default.
- **Risk**: The explicit requirement that management behavior stay unchanged holds today only by accident of the HTTP method, so a regression would not be caught.
- **Suggested Fix**:
  - Make eligibility explicit and off by default. For example, add `exchange(..., throttleRetry: Boolean = false)` or a small request-kind type.
  - Set it from `settings.retryThrottledRequests` only for the analysis POST in `execute` and for analysis `pollWithClient` (`analyzer.isEmpty`). Keep `managementGet` for transport classification only.
  - Add opt-in regression tests:
    - A PUT that gets 429 sends one request and throws.
    - A management GET that gets 429 is still limited by `ManagementGetAttempts`.
    - A management poll that gets 429 still uses up `maxPollAttempts`.

### Issue 4: The retry loop and refund bookkeeping live in the budget class, and the rejection rule is duplicated
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingBudget.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: `ContentUnderstandingBudget.scala` 14, 16–32 (refund at 25–26); `ContentUnderstandingProtocol.scala` 64, 335–336, 352–353
- **Description**:
  - `ContentUnderstandingBudget` used to track time only: start time, `remainingMs` and a cancellable `pause`. It now also holds a generic retry loop and protocol knowledge ("definitive throttling").
  - Refunds work by rewriting `started`, which is now a mutable `@volatile var`. As a hidden side effect, `remainingMs` can go up.
  - Retry logic is split across two classes: the loop and refund in Budget, and eligibility and delay in Protocol.
  - The POST branch of `safeToRetry` re-implements "definitive admission rejection" instead of reusing `admissionRejected(method, status, location)`. That function is what also classifies a response as `Rejected` or `ResponseBodyReadFailed`.
- **Risk**: The guarantee that uncertain acceptance is never replayed depends on the two rules staying identical. If one changes, the connector could replay an uncertain submission, or report a response as `Rejected` without retrying it.
- **Suggested Fix**: Move the loop into Protocol. Give Budget one documented primitive, for example `excludingElapsed[T](body: => T)` or `refund(nanos)`, and state that remaining time can increase. Write POST eligibility as `admissionRejected(method, raw.httpStatus, raw.location) && raw.httpStatus == TooManyRequests`.

### Issue 5: The new public `ContentUnderstandingPreviewParams` trait puts the API's preview status in a public type name, and parameters are grouped to fit the method limit rather than by purpose
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala
- **Line(s)**: 12–13, 76–90, 138–147, 149–176, 179
- **Description**:
  - `ContentUnderstandingParams` already has 48 methods, and the Scala style check allows at most 50.
  - To stay under that limit, `allowInputTruncation` went into a new public trait named "Preview". This separates it from the other Analyze URL query options, `stringEncoding` and `processingLocation`.
  - The retry switch went into `ContentUnderstandingLimits`, which holds size and duration limits, away from `maxPollAttempts` and `pollingDelay`.
  - "Preview" in a public, binary-visible type name will be wrong once the option reaches general availability. It also conflicts with the connector's documented policy of forwarding options without a version allowlist (`Content Understanding.md` ~208–219).
- **Risk**: The public API carries a misleading name that is a breaking change to rename later, and placement of future options becomes arbitrary.
- **Suggested Fix**:
  - Before the API ships, use a neutral, purpose-based trait such as `ContentUnderstandingAnalyzeQueryParams`, holding `stringEncoding`, `processingLocation` and `allowInputTruncation`.
  - Optionally add a retry/polling trait, or make the helper traits `private[contentunderstanding]` like the existing `ContentUnderstandingPython`.
  - Keep "preview" only in the parameter description.

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

### Issue 5
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
