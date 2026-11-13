---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 2
round: 5
timestamp: "2026-11-10T19:57:23Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "7c2739538f4aae7192cf620af4d572a43a0a67ccd4f9e444974f6ce9321b6faa"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 33282 (char 33281)
Invalid CLI event on line 228: Expecting ',' delimiter: line 1 column 6370 (char 6369)

## Original Response

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-2-review-5-claude-opus-5.5.md
- **Issues Found**: 7
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I traced each new production path:
  - `ContentUnderstandingBudget.scala` lines 13–58. The refund is at lines 16–32: `elapsed` is captured at line 17 and `started` is reset at line 26.
  - `ContentUnderstandingProtocol.scala`: `checkInterrupted`/`retryDelay` at 271–287, the throttle predicate in `exchange` at 338–356, the `pollWithClient` exchange at line 622 (`managementGet = analyzer.isDefined`), `getAnalyzerResponse` at 713–733 (line 717 passes `managementGet = true`), and the `createAnalyzer` PUT at line 765 (default `managementGet = false`).
  - `ContentUnderstanding.scala`: `activeParams` 84–92, `settings` 129–148, `expectedType`/`validateValue` 168–199, `analyzeUrl` 317–323, the `prepareUrl` override at 325, and the management entry points 475–492. Both management methods pass `settings()`, which carries `retryThrottledRequests`.
  - `ContentUnderstandingParams.scala` lines 107–177.
- [x] I mapped the throttle tests in `ContentUnderstandingThrottlingSuite.scala`:
  - POST retry beyond a 1000 ms budget, waiting at least 2000 ms and replaying the same body (28–44).
  - GET retry beyond one attempt and 1000 ms (46–59).
  - No replay after a 429 that has an Operation-Location or an invalid one, nor after 500, 408 or a disconnect (61–75).
  - A 429 whose body cannot be read (77–88).
  - Ordinary attempt and duration limits still apply under opt-in (90–103).
  - Interrupt during a wait, with exactly one request sent (105–144).
  - The default-off behavior is covered by the existing `ContentUnderstandingSuite.scala` tests at 195–213 (POST 429 is rejected) and 355–370 (GET 429 consumes the budget).
  - The only management 429 test, `ContentUnderstandingConcurrencySuite.scala` 193–219, uses default settings. A grep shows no test that combines `setRetryThrottledRequests(true)` with management calls.
- [x] I mapped the `allowInputTruncation` tests:
  - `ContentUnderstandingOptionsSuite.scala` 24–42: unset is omitted on both GA and preview; explicit true and false are sent on preview only; the body is clean; `clear` restores omission.
  - `ContentUnderstandingOptionsSuite.scala` 44–59: column values true, false and null; a StringType column is rejected.
  - Python `test_ContentUnderstanding.py` 170–218: configured, copied and loaded stages, plus the column path.
  - The CU `prepareUrl` override (line 325) prevents duplicate query values. Ordered query assertions are deterministic because the default concurrency is 1 (`HTTPTransformer.scala` line 76).
- [x] I checked the removal of generic inspection:
  - The remaining `Utils.py` helpers are `_set_params_via_setters` (142–148) and the transfer functions (190–240).
  - The generated `__init__` drops only `None` values (`Wrappable.scala` 501–523), so an explicit `False` is still set.
  - The MLflow test (220–300) has a positive control (`concurrency == "1"`) and checks that credentials are absent for configured, copied and loaded stages of both CU and AnalyzeDocument.
  - The OpenAI assertion was inverted (`test_OpenAIResponseSchema.py` 90–91).
  - `mlflow==2.21.3` is pinned in `environment.yml`.
  - A grep of the main Python sources found no service-param callers that relied on the removed overrides. `LangchainTransform` uses plain Params.
- [x] I checked the guide and notebook coverage:
  - Saved-results test (about lines 455–502): URL and binary inputs, two POSTs, and no further requests after reading saved results.
  - Notebook execution test (about lines 504–637): one POST, polling resumed and saved, display does not poll again, and grounding and confidence are asserted.
  - Exclusions: Databricks (`DatabricksUtilities.scala` 256–258 and `DatabricksUtilitiesSuite.scala` 18–25), Synapse (`SynapseTests.scala` line 50), and the Fabric include list (`FabricNotebookTests.scala` 230–240).
- [x] I ruled out several non-issues:
  - An empty Operation-Location header yields `Some("")`, so the POST is not replayed (conservative).
  - Removing the refund entirely would already fail the tests at 28–44 and 46–59, because the retried request would be expired before dispatch.
  - No defect was reproducible from the source. All findings below are coverage gaps or optional improvements.
- [x] I did not run any commands, builds or tests. I relied on the driver's evidence: 182 CU Scala tests, 66 core tests, 66 Python tests with 70 subtests, and the OpenAI suites. That evidence is not all green: 42 helper tests failed because there is no Bash/WSL, the Black Jupyter extra could not be installed, and 7 live tests were skipped.
- [ ] Live CU compatibility and packaged artifacts were not reviewed. This was intentionally out of scope (offline review only, no live run requested).

## Issues

### Issue 1: No regression test for excluding analyzer management from opt-in throttle retries
- **Severity**: Medium
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingConcurrencySuite.scala
- **Line(s)**: ConcurrencySuite 193–219 (the stage at 205–206 never opts in). Protocol lines 343, 352–355, 622, 717 and 765. `ContentUnderstanding.scala` 486 and 491. Params 150–153. Docs `Content Understanding.md` 369–372.
- **Description**:
  - `createAnalyzer` and `getAnalyzer` pass `settings()`, which includes `retryThrottledRequests`. The exclusion of management calls depends on three separate mechanisms:
    - The PUT goes through `exchange` with `managementGet = false` (line 765), so the retry loop is enabled. The PUT is excluded only by the method check in `safeToRetry` (352–353).
    - The creation poll depends on `managementGet = analyzer.isDefined` (line 622).
    - `getAnalyzer` depends on `managementGet = true` (line 717).
  - No test runs a management call with `setRetryThrottledRequests(true)`.
- **Risk**:
  - Changing `safeToRetry` to "any 429 without an Operation-Location", or dropping the `managementGet` argument, would make management calls retry without limit and ignore `maxOperationDuration`. That breaks the documented and required "management unchanged" contract.
  - Every current test would still pass. With the stub that repeats its last reply, the regression would show up as a hung test rather than a clear failure.
- **Suggested Fix**:
  - Run test 193–219 with `retryThrottledRequests` set to both false and true, keeping the same assertions.
  - Add two opt-in cases:
    - A PUT that gets a 429 with no Operation-Location and `Retry-After: 0`. Expect a `ContentUnderstandingException` and `Seq("PUT")`.
    - A creation poll where every GET gets a 429 with `Retry-After: 0`, using `setMaxPollAttempts(2)`. Expect exactly two poll GETs.
  - Wrap each case in a timeout (for example `Await` on a `Future`) so a regression fails quickly.

### Issue 2: Exact refund is untested, so a full budget reset would pass every test
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingBudget.scala
- **Line(s)**: Budget 16–32 (lines 17 and 26). ThrottlingSuite 28–44, 46–59 and 90–103. Docs 380–384.
- **Description**:
  - The refund restores the elapsed time measured before the exchange.
  - Current tests prove two things only: throttled time is not charged, and ordinary limits apply when there is no 429.
  - No test has charged non-throttled time followed by a 429. So `started = System.nanoTime()` (a full reset), or measuring `elapsed` after the attempt, would both pass.
- **Risk**: This would silently break "non-429 budgets unchanged". Each 429 would reset `maxOperationDuration`, so intermittent throttling could stretch ordinary polling without bound.
- **Suggested Fix**:
  - Add a same-package unit test:
    - Create `new ContentUnderstandingBudget(1000)` and call `Thread.sleep(300)`.
    - Call `withThrottlingRetries(true)` with a delay function that returns `Some(400L)` once and then `None`.
    - Assert `450 < remainingMs < 800`. A full reset gives about 1000; no refund gives about 300.
  - Also assert that with `enabled = false` the attempt runs exactly once.
  - Optional end-to-end test:
    - Settings: analyze mode, `pollingDelay(600)`, `maxOperationDuration(1000)`, `maxPollAttempts(10)`.
    - Replies: `[accepted, 200 running, 429 Retry-After 0, 200 running]`.
    - Expect `POST, GET, GET, GET` and `OperationTimedOut`. A full reset produces five requests.

### Issue 3: Explicit `allowInputTruncation` on the GA version is not tested
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingOptionsSuite.scala
- **Line(s)**: 25–30 (GA, unset only) and 31–41 (explicit values, preview only, line 33). Docs 208 and 217–218.
- **Description**: The requirement is to forward explicit options with no version allowlist, but no test sends an explicit value on `DefaultApiVersion`.
- **Risk**: A future change that sends the option only for preview versions would silently drop an explicit user setting, and CI would stay green.
- **Suggested Fix**: Run the explicit loop for both `Seq(DefaultApiVersion, "2026-06-01-preview")`, and assert `allowInputTruncation=false&api-version=2025-11-01` for GA.

### Issue 4: Opt-in fallback for missing, invalid or HTTP-date Retry-After is untested, and the zero-delay loop is undocumented
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: ThrottlingSuite 30, 48, 80 and 107 use only "2", "0" and "60". Protocol 280–287 and 354. Params 114–118. Docs 381–382 and 479.
- **Description**:
  - The new call `retryDelay(raw.retryAfter, settings.pollingDelay)` is only exercised with delta-seconds values. The HTTP-date and fallback cases are documented but have no opt-in test. `retryDelay` itself is unit-tested in `ContentUnderstandingSubmissionSuite.scala` around line 691.
  - `pollingDelay` allows 0 (`gtEq(0)`), and the suite's helper uses 0. So a persistent 429 without Retry-After retries in a tight loop until cancelled. This is a design consideration, not a challenge to the requested unlimited behavior.
  - The `pollingDelay` doc (Params 114–117, docs table 479) does not mention the opt-in fallback.
- **Risk**: A wrong fallback value would go undetected, and a zero-delay loop could flood a throttled endpoint.
- **Suggested Fix**:
  - Add opt-in cases where the 429 has no Retry-After and where it has `Retry-After: invalid`, using `setPollingDelay(300)`. Assert at least 300 ms elapsed and two requests.
  - Optionally add an HTTP-date case.
  - Document or set a floor for the zero-delay case, and update the `pollingDelay` doc.

### Issue 5: Poll mode ignoring `allowInputTruncation` is untested
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 84–92 (line 89). Docs 218.
- **Description**: The parameter was added only to the non-poll `activeParams` list, and the docs say poll mode ignores it. No test sets it in poll mode.
- **Risk**: If it were moved into the shared list, poll-mode DataFrames would require the bound column. That would be a schema failure and would go undetected.
- **Suggested Fix**: In OptionsSuite, run poll mode with `setAllowInputTruncationCol("missing")` on a frame that lacks that column. Assert the transform succeeds and the GET query has no `allowInputTruncation`.

### Issue 6: The throttle-wait cancellation test covers only direct interruption and uses unbounded waits
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: 105–144 (lines 133, 135 and 140). Protocol 271–278. Docs 384–385.
- **Description**:
  - The test interrupts a raw thread that uses `budgetOverride`. The documented Spark-task cancellation path (`AsyncClient.isTaskCancelled` raising `CancellationException`) is never exercised during a 429 wait. The Python cancellation test (about lines 302–394) covers only ordinary polling waits.
  - `await()` and both `join()` calls have no timeout.
- **Risk**: The risk is low because `pause` is shared code. However, a regression such as catching `CancellationException` inside the retry loop would go undetected. A wait that cannot be interrupted would stall for about 60 s per mode instead of failing quickly.
- **Suggested Fix**:
  - Use `await(10, SECONDS)` and assert the result, then `join(10000)` and `assert(!worker.isAlive)`.
  - Optionally cancel a Spark job during a `Retry-After: 60` wait and assert it aborts promptly with one request.

### Issue 7: Minor gaps in assertion precision and persistence coverage
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py
- **Line(s)**: Python 179, 186 and 195–218. OptionsSuite 54–56. FuzzingSuite 16–22. `SynapseTests.scala` 50.
- **Description**:
  - `intercept[IllegalArgumentException]` does not check the message.
  - `assertNotIn(..., post[2])` checks only the top-level keys of the parsed body.
  - `assertFalse(getAllowInputTruncation())` would also pass for `None`. The wire assertion at line 185 partly covers this.
  - A column-bound `allowInputTruncation` is never saved and reloaded, and FuzzingSuite does not set either new parameter.
  - The Synapse exclusion token has no unit test, although the Databricks one does.
  - The saved-results test re-implements the guide snippets instead of extracting them, so the docs could drift.
  - The notebook branches for non-empty `MODEL_DEPLOYMENTS`, the configuration `ValueError` and the not-ready `RuntimeError` are not run offline.
- **Risk**: These weaker assertions could hide regressions, though nearby stronger checks mostly cover them.
- **Suggested Fix**:
  - Assert that the exception message names `allowInputTruncation`.
  - Use `assertNotIn("allowInputTruncation", json.dumps(post[2]))` and `assertIs(..., False)`.
  - Add a save/load or Fuzzing case with `setAllowInputTruncationCol` and `setRetryThrottledRequests(true)`.
  - Add a unit test for the Synapse filter.
  - Optionally parameterize the notebook test over the branches it does not reach.

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

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 7
- **Status**: Open
- **What changed**: pending
- **Why**: pending
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

- Management PUT, GET and management polling now have finite exclusion tests
  with retry opt-in both off and on. Existing management-budget coverage also
  runs both configurations.
- A deterministic injected clock proves only throttled work/waits are refunded:
  earlier 300 ms and final 100 ms remain charged, leaving exactly 600 ms.
- Both true and false truncation options are tested on GA and preview versions.
- Numeric, zero, huge, past-date, future-date, missing and malformed Retry-After
  cases cover the parser/policy and public POST/GET paths.
- Poll mode is tested with a bound truncation column absent from its input.
- Spark job cancellation now covers throttled waits with both interruptOnCancel
  settings, alongside the retained thread-interruption checks.
- Boolean identity and exact schema diagnostics replace weaker assertions.
  Column options are covered through copy/save/load and fuzzing persistence.
  Pure Synapse notebook exclusion is tested without constructing the live
  provisioning suite. Actual guide and notebook cells are executed by tests.
- Validation exposed one reflection-based response-close test still using the
  old private exchange signature. It now supplies explicit retry eligibility;
  its complete 38-test RecoverySuite passes.

See the stage-attempt-2 lens-1 consolidated validation update for all results.
These driver resolutions await independent stage-attempt-3 review.
