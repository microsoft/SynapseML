---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 2
round: 3
timestamp: "2026-11-10T19:58:52Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "516c63c3c8e3194d494e299f09aa880e1527b60df77412fa50f0275f3ed5e05e"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 33295 (char 33294)
Invalid CLI event on line 314: Expecting ',' delimiter: line 1 column 6656 (char 6655)

## Original Response

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-2-review-3-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Throttling budget refund.**
  - `ContentUnderstandingBudget.scala` 16–32 records `elapsed` once and resets `started` only after a retried 429 and its `pause`. The final attempt that is not a 429 is still charged.
  - GET 429 retries stay inside one `exchange` call (`ContentUnderstandingProtocol.scala` 343–355, call site 622). So `pollWithClient` (603–633) uses up one `maxPollAttempts` per exchange.
  - When `remainingMs == 0`, the code returns a status-0 `notSent` response (345–347), not a 429. An exhausted budget therefore cannot cause a retry loop.
- [x] **No replay of uncertain submissions.**
  - `safeToRetry` (Protocol 352–353) allows a retry only for GET, or for POST when there is no `Operation-Location`. PUT is never retried.
  - Management GETs pass `managementGet = true` (622 through `analyzer.isDefined`, and 717). The createAnalyzer PUT (765) is excluded because of its method.
  - Throttling suite 61–75 checks that only one POST is sent for each of: a 429 with a handle, an invalid handle, 500, 408, and a disconnect.
- [x] **Cancellation.**
  - `pause` checks for cancellation every 50 ms (Budget 37–58).
  - `checkInterrupted` (Protocol 271–278) throws on task end or thread interrupt.
  - `AsyncClient.isTaskCancelled` covers both `isInterrupted` and `isCompleted` (`core\...\io\http\Clients.scala` 56–57).
  - The interrupt test (Throttling suite 105–140) expects an `InterruptedException` after exactly one request.
- [x] **`retryDelay` (Protocol 280–287) cannot overflow.** Seconds values are capped, past HTTP dates are clamped to 0 or more, and anything else falls back to `pollingDelay`. `pollingDelay` is validated with `gtEq(0)` (Params 114–118).
- [x] **`allowInputTruncation`.**
  - It is a `ServiceParam[Boolean]` (Params 139–146) with a BooleanType check (`ContentUnderstanding.scala` 168–200).
  - When unset or null it is left out; an explicit false is sent as `false` (`analyzeUrl` 317–323). It is excluded in poll mode (`activeParams` 84–92).
  - Python test lines 169–218 cover true, false, null rows, clear, and copy/save/load.
  - 24 existing `ServiceParam[Boolean]` declarations in the cognitive module are precedent.
- [x] **`retryThrottledRequests` defaults to false** (Params 150–156, 174). Management calls use the same `settings()` (`ContentUnderstanding.scala` 485–491).
- [x] **Python inspection removal.** ServiceParams have no Python defaults, and `_transfer_params_to_java` removes them from the Python map. The MLflow test restores the tracking URI and autolog in `finally`. `mlflow==2.21.3` is pinned in `environment.yml` line 36, so the module-level import works in CI.
- [x] **Notebook safeguards.**
  - Both snapshots are written with `errorifexists` (notebook JSON 245, 306), and an empty `pending` sends no GET.
  - The notebook is excluded from Synapse runs (`SynapseTests.scala` 50) and Databricks runs (`DatabricksUtilities.scala` 258; `DatabricksUtilitiesSuite.scala` 18–19).
  - It is not on the Fabric allowlist (`FabricNotebookTests.scala` 155, 230).
- [x] **Guide write-then-read examples.** Docs 68–69 and 96–97 are mirrored by `test_saved_analysis_results_can_be_read_without_resubmission` (test 453–506), which checks that no extra POST is sent.
- [ ] **Builds and tests not run by this reviewer** (read-only plan). I relied on the supplied offline evidence, including its stated Bash/WSL and Black-Jupyter gaps. Live CU compatibility cannot be confirmed from source.

## Issues

### Issue 1: Opt-in 429 retry loop can spin with no delay, no backoff and no jitter
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingBudget.scala; ContentUnderstandingProtocol.scala; ContentUnderstandingParams.scala; docs\Explore Algorithms\AI Services\Content Understanding.md
- **Line(s)**: Budget 16–32, 37–39; Protocol 280–287, 343–355; Params 114–118; docs 380–382
- **Description**:
  - **The defect (reproducible).** Each wait is exactly `retryDelay(raw.retryAfter, pollingDelay)`, and `pause` does not sleep when the delay is 0 (`if (delay > 0)`). Because the loop has no limit, any zero delay turns into a tight retry loop. Three things trigger it:
    - **`pollingDelay = 0` with a missing or invalid Retry-After.** The validator allows 0 and the suite fixture uses it (line 22). Sending 429 replies without Retry-After through the existing stub reproduces this.
    - **`Retry-After: 0`.**
    - **A past HTTP-date Retry-After (client clock ahead of the server).** This gives 0 even with the default `pollingDelay = 1000`.
  - **No jitter.** Even with a positive delay, the interval is fixed, so all throttled rows retry at the same moment.
  - **Full re-upload.** Every POST retry sends the whole body again (up to about 43 MiB after base64).
  - **Scope.** Cancellation still works, so the risk is extra load, not a hang. Whether CU actually sends 0 or HTTP-date values is a service-contract uncertainty; the first trigger needs only configuration.
- **Risk**: Executors keep sending full, multi-megabyte uploads to an already throttled resource. This prolongs throttling, affects other users of the same quota, and wastes network and CPU. Since there is intentionally no cutoff, only manual cancellation stops it.
- **Suggested Fix**:
  - Wait `max(honoredRetryAfter, backoff(n))`, where `backoff(n)` is a capped exponential backoff. For example, start at `max(pollingDelay, 1000)` ms, double up to about 60 s, and add random jitter.
  - Use the backoff only when Retry-After is missing, invalid, or 0 or less. Adding jitter on top of Retry-After still respects it.
  - Document that `pollingDelay` is also the fallback wait for analysis 429s; Params 115–117 mentions only management GETs.
  - Add a test with `setPollingDelay(0)` that sends no Retry-After, a 0 value, and a past date. It should override `pause` (as at lines 111–116) and assert that the waits are positive.

### Issue 2: Notebook poll pass drops handles that fail local validation, and the documented loop never resumes them
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Quickstart - Semantic Extraction with Content Understanding.ipynb; cognitive\...\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: notebook 260–262, 286–306; ContentUnderstanding.scala 231, 289–296, 384–386; Protocol 241–251
- **Description**:
  - **How a handle is lost.**
    - In poll mode, each `operationLocation` is checked against the configured endpoint's scheme, host and port (231, Protocol 245–251).
    - A mismatch produces `InvalidInput`. `invalidResponse` then writes `status = "Failed"` with `operationLocation = null` (384–386).
    - The poll-and-save cell keeps only `documentId, analysis, requestError` (305), so the handle is not copied into the new snapshot.
  - **Why the documented loop cannot fix it.** On the next pass, the markdown says to use "the last snapshot". There, `resumable` requires a non-null handle (286–288), so the row is treated as settled.
  - **Trigger.** After a restart, the user re-enters the endpoint and gets it wrong: a typo, the wrong resource, or a different host alias if the resource supports more than one. All accepted operations then become Failed in the new snapshot. Their handles survive only in older snapshots.
  - **Mitigation.** The `diagnosticCode` column does show `InvalidInput`.
- **Risk**: Accepted, billed operations are reported as Failed, and the documented steps cannot recover them.
- **Suggested Fix**:
  - Before writing, compare each pending handle's scheme, host and port with `endpoint` (no network call needed). If any differ, raise an error saying to reuse the submission endpoint.
  - Alternatively, carry `operationHandle` forward into each snapshot and treat `InvalidInput` rows that have a handle as resumable.
  - Either way, document that the endpoint origin must match the one used at submission.

### Issue 3: Guide examples overwrite snapshots that contain accepted-operation handles
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Content Understanding.md; cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py
- **Line(s)**: docs 68, 96, 438; test 486–488
- **Description**:
  - **What happens on a re-run.** The URL, binary and retry examples save output with `.write.mode("overwrite")`, which deletes the existing directory before the new job runs. Re-running a cell:
    - resubmits every document, paying again;
    - deletes handles from the earlier run, including rows still `Running` or `Unknown` that the docs tell users to recover in poll mode.
  - **If the re-run fails part-way, the earlier handles are gone.**
  - **Retry example (438).** A second run sends the same rejected rows again and overwrites the handles from the first retry.
  - **Inconsistency.** The notebook uses `errorifexists` on purpose to avoid this (notebook 232, 245).
  - **Status.** This is optional hardening: the requirement (write once, then read) is met.
- **Risk**: A routine re-run can lose recovery handles and duplicate submissions.
- **Suggested Fix**: Use the default error-if-exists save (`.write.parquet(path)`) or a unique path per run. Add a sentence that a re-run needs a new output path, and update the mirrored test.

### Issue 4: Notebook configuration parsing fails on common environment values and blocks recovery-only sessions
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Quickstart - Semantic Extraction with Content Understanding.ipynb
- **Line(s)**: 66–71, 212–213
- **Description**:
  - **Empty variable.** `json.loads(os.environ.get("CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS", "{}"))` raises `JSONDecodeError` when the variable is set but empty. It runs before the required-settings check, in the configuration cell that recovery depends on. A session that only needs to poll is blocked by an optional setting.
  - **Wrong JSON type.** A list or string passes `if model_deployments:` and then fails inside `setModelDeployments` with an unclear Py4J error.
  - **Unchecked `run_id`.** `run_id` is used in storage paths and the analyzer ID without validation. For example, a `/` in it changes the directory layout.
- **Risk**: Confusing startup errors, recovery blocked by an unrelated setting, and mismatched recovery paths.
- **Suggested Fix**:
  - Use `os.environ.get(...) or "{}"`.
  - Require a dict whose keys and values are non-empty strings, and raise a clear `ValueError` otherwise.
  - Strip `run_id` and validate it (for example `[A-Za-z0-9-]{1,40}`).
  - Optionally, parse model deployments only in the create-analyzer cell.

### Issue 5: Unlimited throttling waits occupy threads in the shared `ExecutionContext.global` pool without `blocking`
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala; ContentUnderstandingBudget.scala
- **Line(s)**: Clients 86–90; Budget 39–52
- **Description**:
  - **Where the threads come from.** `AsyncClient` runs each request on `ExecutionContext.global`, a pool shared across the JVM and sized to the number of cores. The work is not wrapped in `scala.concurrent.blocking`, and `pause` calls `Thread.sleep` on that thread.
  - **What changed.** Before, the time a row could hold a pool thread was bounded by `maxOperationDuration` (600 s by default). With the opt-in, a throttled row holds it for the whole throttling period, which can be any length when Retry-After is honored.
  - **Effect.** Other futures in the same executor JVM, such as other SynapseML HTTP stages, wait in the queue behind it.
  - **Status.** This worsens an existing pattern rather than adding a new one.
- **Risk**: Unrelated work on the same executor slows sharply or appears to hang during long throttling.
- **Suggested Fix**: Wrap the sleep in `pause`, and ideally the HTTP exchange, in `scala.concurrent.blocking { ... }`. Otherwise, document the effect and recommend a modest `concurrency`.

### Issue 6: No regression test shows management calls still do not retry when the opt-in is enabled
- **Severity**: Low
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottlingSuite.scala; ContentUnderstanding.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: Suite 28–140 (all cases cover analysis only); ContentUnderstanding.scala 485–491; Protocol 343, 352–353, 622, 717, 765
- **Description**:
  - **How management stays unchanged.** `createAnalyzer` and `getAnalyzer` use `settings()`, which now includes the opt-in. Their behavior stays the same only because of two implicit guards:
    - PUT is not eligible under `safeToRetry`.
    - The management GET call sites pass `managementGet = true`.
  - **Missing coverage.** The code is correct today. However, no test turns the opt-in on and checks that a management PUT, a management operation GET, or a `getAnalyzer` 429 results in exactly one request. The requirement explicitly says management behavior must stay unchanged.
- **Risk**: A future change could quietly allow unlimited, budget-exempt management retries without any test failing.
- **Suggested Fix**: Add opt-in test cases for these three paths, each asserting one request and the existing error. Optionally, pass `retryThrottledRequests = false` explicitly for management calls as extra protection.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; neither human nor repository selects a tier",
    "models": "Active review-code Medium complementary-family rule, rechecked against current task tool catalog",
    "stageBehavior": "Six concurrent independent lenses; human explicitly requested a rerun",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit/push without explicit approval"
  },
  "changeSize": "Seventeen source/test/documentation files relative to PR head, including three untracked additions. Fourteen tracked files have 616 additions and 189 deletions. Most volume is tests and documentation; production changes remain localized to four CU Scala files and removal of four Python inspection overrides.",
  "cyclomaticComplexity": "Moderate changes to cancellable 429 retry, elapsed-budget refund, GET/POST eligibility and optional Boolean query values; shared inspection changes have cross-connector implications. Existing request and management state machines are reused.",
  "rationale": "Retain Medium and six complementary lenses for authentication, cancellation, persistence and notebook interactions. The user explicitly requested another review after the first batch failed transport validation. Preserve earlier failures and use fresh stage-attempt identities; no invalid result counts as signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: lets rerun the review. Repository policy specifies no tier/model override. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 2,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The CLI JSON transport failure may recur. Source review cannot prove live CU compatibility; endpoint, secure credentials and matching packaged artifacts remain necessary."
}

## Driver resolution update before stage attempt 3

Original findings and failed transport provenance are preserved above.

- Missing/invalid Retry-After receives adaptive fallback; valid headers retain
  exact service-directed delays by explicit user choice, including zero.
- Recovery validates pending-handle endpoint origins before polling or writing
  a new snapshot. The actual-cell regression verifies wrong-origin rejection,
  no service requests and no replacement output, even with an invalid unused
  model-deployment setting.
- Guide examples use `errorifexists` and fresh-path guidance. A regression
  executes the actual guide snippets and proves reruns neither overwrite nor
  submit additional analysis requests.
- Run identifiers and model mappings are validated before service use.
  Model mappings are parsed only for analyzer creation, not recovery.
- Blocking HTTP work and cancellable waits use `scala.concurrent.blocking` to
  notify the shared execution pool without rewriting shared HTTP behavior.
- Management opt-in exclusions have direct PUT, GET and polling coverage.

See the stage-attempt-2 lens-1 consolidated validation update for results.
These driver resolutions await independent stage-attempt-3 review.
