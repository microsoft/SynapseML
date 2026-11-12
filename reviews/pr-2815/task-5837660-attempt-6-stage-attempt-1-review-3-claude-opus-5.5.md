---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 3
timestamp: "2026-11-10T19:20:07Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "7fdd923cd573499fb517eaa966eb973e9ce408431935ef2dee02a7307e94124f"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 32219 (char 32218)
Invalid CLI event on line 178: Expecting ',' delimiter: line 1 column 3994 (char 3993)

## Original Response

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Retry and refund loop.** In `ContentUnderstandingBudget.scala`, `withThrottlingRetries` (L16–32) records `elapsed` when it starts, and resets `started` only after a throttled wait (L24–27). Exceptions from an attempt leave without a refund. `pause` (L37–58) checks for cancellation every 50 ms, restores the interrupt flag, and handles `Long.MaxValue` without overflow. The budget is created per operation in `withClient` (`ContentUnderstandingProtocol.scala` around L414–420), so no thread shares it.
- [x] **Retry gate (`ContentUnderstandingProtocol.scala` L338–356).** Retries require `retryThrottledRequests && !managementGet`. GET is always eligible. POST is eligible only when no `Operation-Location` header is present. PUT is excluded by the method check. Management GETs pass `managementGet` at L622 (`analyzer.isDefined`) and L717. The `createAnalyzer` PUT is at L765. Each attempt builds a new request with `data.toHTTPCore` (L363).
- [x] **Uncertain submissions are not replayed.** A 429 with any `Operation-Location` header, including an empty or invalid one, becomes `Unknown` through `uncertainSubmission` (L519–527) and `decodeSubmission`/`safeSubmission` (L529–563). Before headers arrive the status code is 0, so it is never treated as 429. The stub repeats its last reply (`ContentUnderstandingStub.scala` L84–86), so the single-POST assertions in `ContentUnderstandingThrottlingSuite.scala` L60–75 would catch a replay.
- [x] **429 with an unreadable or oversized body is a definitive rejection.** The status code is captured before the body is read (Protocol around L377–380). The `ResponseTooLarge` and `IOException` branches (around L386–409) keep `code=429` and `location=None`. `ContentUnderstandingThrottlingSuite.scala` L77–88 covers this.
- [x] **Retry-After parsing.** `ContentUnderstandingResponseParsing.scala` L89–96 accepts only ASCII digits, at most 19 of them, clamped to `Long.MaxValue`. HTTP dates are parsed inside `Try`, and past dates give 0 (Protocol L280–287). Neither path throws inside the retry callback.
- [x] **Cancellation during unlimited waits.** `checkInterrupted` (Protocol L271–278) checks both the thread interrupt flag and `AsyncClient.isTaskCancelled`. `AsyncClient` passes the Spark `TaskContext` to its futures and converts `InterruptedException` (`core\...\io\http\Clients.scala` around L50–91). With `concurrentTimeout` unset, the async wait is `Duration.Inf` (`HTTPTransformer.scala` L111–113), and CU rejects a configured `concurrentTimeout` (`ContentUnderstanding.scala` L114–115).
- [x] **`allowInputTruncation`.** The trait is at `ContentUnderstandingParams.scala` L138–147 and has no default, so it is omitted unless set. `getValueOpt` turns a null cell into `None` (`CognitiveServiceBase.scala` L102–107). `validateRow` validates only defined column values (`ContentUnderstanding.scala` L272–277). The schema check rejects non-Boolean columns (L243–254, L168–177). `analyzeUrl` writes the lowercase `toString` (L317–323). Poll mode ignores the option through `activeParams` (L83–91). `ContentUnderstandingOptionsSuite.scala` covers unset, true, false, clear, null rows and a wrong column type.
- [x] **Throttling flag default.** `retryThrottledRequests` defaults to false (`ContentUnderstandingParams.scala` L173–176). The CU Python companion copies JVM-set values and defaults for ordinary params and keeps service params on the JVM only (`ContentUnderstandingParams.scala` L238–254).
- [x] **Removing the Python inspection overrides.** The remaining helpers are in `core\src\main\python\synapse\ml\core\schema\Utils.py` L150–242. Generated service-param setters and getters (`Wrappable.scala` L205–229, L308–330) do not use the removed methods, and ServiceParams get no Python default (L182–190). A search of every `src\main\python` file and of Python emitted from Scala found only non-service uses:
  - Python-only params in `LangchainTransform.py` L264–266.
  - The ordinary `postProcessing` param in `OpenAIPromptPythonOverrides.scala` L45 and L115.
  - `feedDict` in `CNTKModel.py` L23–41.
- [x] **Python test isolation.** `Handler` is defined inside `setUp` (`test_ContentUnderstanding.py` L39–135), so patched `do_GET` and `completed_body` changes cannot leak between tests. The MLflow test restores the tracking URI and disables autolog in `finally` (L220–296). The copy, save and load candidates are built before the loop clears anything (L175).
- [x] **Notebook recovery logic.** The resumable filter (ipynb L286–288) only sees validated handles, because `safeSubmission` drops handles on non-accepted statuses and invalid locations. Snapshots use `errorifexists` (L245, L306). The `from_json` schema and span slicing are consistent with `codePoint` encoding.
- [x] **Test coverage gap.** `retryThrottledRequests` appears in tests only in the throttling suite and the Python options test, so no management case runs with the flag on (Issue 2).
- [ ] **Not executed.** I ran no builds or tests, under the read-only contract. I relied on the validation the driver reported.
- [ ] **Not verifiable offline.** Whether live CU or its front door always sends `Retry-After` on 429 is a service-contract question.

## Issues

### Issue 1: Opted-in unlimited 429 retries have no minimum delay, backoff growth or jitter
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 280–287 (`retryDelay` fallback) and 351–355 (throttling delay). Related: `ContentUnderstandingBudget.scala` 16–32 (loop with no cap); `ContentUnderstandingParams.scala` 114–118 (`pollingDelay` may be 0); `docs\...\Content Understanding.md` 118–122 and 367–386.
- **Description**: When the option is on, each throttled attempt waits exactly `retryDelay(raw.retryAfter, settings.pollingDelay)`. That gives a zero or fixed delay in three cases:
  - If `Retry-After` is missing or invalid, the wait is the fixed `pollingDelay`, which the validator allows to be 0.
  - `Retry-After: 0` gives no wait.
  - An HTTP date in the past also gives no wait.

  The loop deliberately has no attempt cap, and the delay never grows or varies, so two things can happen:
  - `setPollingDelay(0)` plus a 429 source that omits `Retry-After`, such as an intermediary, causes a zero-delay request loop that never ends, in every worker.
  - With the default 1 s delay, every worker (partitions × concurrency) retries in step at a fixed rate.

  Other effects:
  - Each POST retry rebuilds the request (`data.toHTTPCore`, L363) and re-uploads the whole base64 body, up to `maxDocumentBytes`. `ContentUnderstandingThrottlingSuite.scala` L40 confirms the body is identical on each send.
  - Nothing is logged while the task is waiting.
  - Long throttling can outlast a static `setAADToken` value or a SAS document URL; the guide says tokens are not refreshed (L121). The resulting 401 then triggers the partition-wide `SharedConfigurationFailure` stop.
- **Risk**: Under sustained throttling without `Retry-After`, the opt-in mode increases load and network transfer instead of backing off. That can prolong or escalate throttling, and the job looks hung. The default path and correctness are not affected.
- **Suggested Fix**: For opted-in throttled retries only, always wait at least any valid `Retry-After`. When `Retry-After` is missing or invalid, and optionally after repeated zero values, apply a floor such as `max(pollingDelay, 1000)` with capped exponential growth and jitter, reset for each request. Optionally add a sanitized, rate-limited throttling log. Add a test with a 429 that has no `Retry-After` and `pollingDelay=0` that asserts a positive wait. In the opt-in documentation section, note the bandwidth cost and the token/SAS lifetime limits.

### Issue 2: No regression test shows that opt-in throttling leaves analyzer management unchanged
- **Severity**: Low
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: Whole suite (no management case). Production paths: `ContentUnderstanding.scala` 146–147, 485–486, 489–491; `ContentUnderstandingProtocol.scala` 343, 352–354, 622, 717, 765.
- **Description**: `settings()` passes `retryThrottledRequests` into the same `Settings` that `createAnalyzer` and `getAnalyzer` use. Management is excluded only by:
  - `managementGet = analyzer.isDefined` (L622) and `managementGet = true` (L717);
  - the method check that leaves out PUT (L352–353).

  The opt-in tests cover only analysis POST and GET. The existing management 429 test (`ContentUnderstandingAdmissionSuite.scala` L308–325) runs with the default `false`.
- **Risk**: A later refactor of `exchange` defaults or of the method check could make management GET 429s retry without limit, ignoring `ManagementGetAttempts` and `maxOperationDuration`, or replay the PUT. That would break the explicit "management unchanged" requirement, and no test would fail.
- **Suggested Fix**: Add stub tests with `setRetryThrottledRequests(true)`:
  1. A PUT 429 makes `createAnalyzer` throw after exactly one PUT.
  2. 429s on the creation-operation GETs and on `getAnalyzer` stop at the same attempt and duration limits as the default path.

  As extra protection, management calls could use `settings().copy(retryThrottledRequests = false)`.

### Issue 3: Guide examples overwrite saved results and handles when rerun
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Content Understanding.md
- **Line(s)**: 96 and 618 (changed examples); the same pattern appears at 68 and 438.
- **Description**: The examples write with `.write.mode("overwrite").parquet(...)`. For a non-partitioned Parquet write, Spark deletes the existing output before it runs the job, and that job is what submits the documents. Rerunning a cell, which is common in notebooks, therefore does two things:
  - It quietly resubmits every row.
  - It first deletes the earlier snapshot, including `Running` and `Unknown` rows whose handles the guide says to resume with poll mode (L441–449, L508–515).

  If the rerun then fails, both the old handles and the new results are lost. The notebook in this same diff uses `errorifexists` for this reason (ipynb L232, L245, L306), so the guide and notebook disagree.
- **Risk**: Accepted operations become orphaned and submissions are billed twice, in a documented flow that is easy to trigger.
- **Suggested Fix**: Use Spark's default `errorifexists` mode (`.write.parquet(path)` or `.mode("errorifexists")`) with a separate output path for each run. State in the text that a rerun should poll the saved handles rather than overwrite them.

### Issue 4: Notebook configuration cell fails when the model-deployments variable is set but empty
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Quickstart - Semantic Extraction with Content Understanding.ipynb
- **Line(s)**: 67–71 (configuration cell) and 212–213 (`setModelDeployments`)
- **Description**: `json.loads(os.environ.get("CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS", "{}"))` uses the `"{}"` default only when the variable is absent. If it is present but empty or whitespace, `json.loads` raises `JSONDecodeError` before the cell's own required-value check at L70–71. This is common when an environment template exports every variable name. The other variables use `or` fallbacks, so this one is inconsistent. JSON that is not an object but is truthy, such as `"name"` or `1`, reaches `setModelDeployments` and fails later with an unclear py4j error.
- **Risk**: The quickstart's first cell fails with a confusing error, especially in local or CI validation runs. There is no data or service impact.
- **Suggested Fix**: Read the value as `(os.environ.get("CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS") or "").strip() or "{}"` before parsing. Then raise a clear `ValueError` unless the result is a dict of string keys to string values.

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
