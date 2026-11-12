---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 4
round: 6
timestamp: "2026-11-10T21:58:58Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "91b4f74fb57bb31bbf37bf23053d1da4822e5b7a75235b3134eb845b6c3d688b"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-4-review-6-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] The supplied diff is truncated partway through the `ContentUnderstandingProtocol.scala` Settings hunk. I reviewed the remaining changes in the frozen working tree under `cognitive\src`, `core\src` and `docs`, with every search scoped to those paths or to specific files.
- [x] **Retry policy** (`ContentUnderstandingThrottling.scala`):
  - Lines 16-20: `Context` holds the per-task stop flag and the throttle reporter.
  - Lines 24-41: `Backoff` uses base=max(1000,pollingDelay) and cap=max(60000,base). Jitter (37-38) never goes below the base, so the first fallback is exact and base≥cap gives a constant delay. A valid header returns its exact value and resets growth.
  - `current*2` is bounded by Int-range values, so it cannot overflow.
  - Lines 43-73: `retry` returns at 46-47 when the option is off. Otherwise it marks at 52, ticks during the wait at 58-61, catches the stop at 62-63 and refunds only after the wait at 65.
- [x] **Exchange and decode** (`ContentUnderstandingProtocol.scala`):
  - Line 322: retries require `analysis && settings.retryThrottledRequests`.
  - Line 329: the `!analysis && method == "GET"` argument keeps management behavior unchanged.
  - Lines 332-334: the diagnostic is relabeled after a shared stop (Issue 1).
  - Line 446: `raw.diagnostic` takes precedence over the service error.
  - Lines 482-506 (`checkSharedFailure`), 508-524 (`decodeSubmission`: a 429 POST without a location is Rejected) and 581-611 (`pollWithClient`: creation polling counts as management) checked.
  - Lines 650-676 (`rowHandler`): one Context per task, with a single `compareAndSet` shared-stop log.
- [x] **429 classification** (`ContentUnderstandingResponseParsing.scala` 26-44): `throttled` means a GET 429, or a POST 429 with no Operation-Location (the `admissionRejected` case). A POST 429 that returns a handle is never treated as retryable.
- [x] **Budget**: `ContentUnderstandingBudget` adds `mark`/`refundSince` on a single-writer `@volatile` field. `pause(delay, onTick)` runs inside `blocking`, ticks every 50 ms and checks for cancellation.
  - Performance: each tick for a waiting row is one AtomicBoolean read plus one short synchronized clock check, which is negligible.
  - There is no per-document logging.
- [x] **Logging** (`ContentUnderstandingLogging`):
  - `ThrottleReporter` logs the first warning, then at most once per 60 s, using a shared count.
  - Task IDs come from `AsyncClient.currentTaskContext`.
  - Messages contain only method, retryCount, waitMs and task IDs: no URL, header, credential or body. This matches guide lines 511-520.
- [x] **Parameters**:
  - `HasAllowInputTruncation` (`ContentUnderstandingParams.scala` 140-149) has scalar and column setters.
  - `HasRetryThrottledRequests` (151-161) is a separate trait from the limits trait and defaults to false.
  - `ContentUnderstanding.scala`: the Boolean column type is validated, and `analyzeUrl` omits the query value when unset or null and sends `false` when false.
- [x] **Scala tests**:
  - `ContentUnderstandingThrottlingSuite` (29-279) and `ContentUnderstandingThrottlingPolicySuite`; line 23 asserts that the 60000 cap step is jittered.
  - `ContentUnderstandingOptionsSuite` (24-62).
- [x] **Python tests** (`test_ContentUnderstanding.py`):
  - 172-226: options survive copy and save/load.
  - 229-309: MLflow autologging excludes credentials while execution still works.
  - 311 onward: cancellation, including a throttled case.
  - 1049 onward: generic inspection does not read JVM service scalars.
- [x] **Codegen**:
  - `Wrappable.scala` 469-473: stubs skip `isSet`.
  - `PyCodegenSuite.scala` 190-191 and `ContentUnderstandingSuite.scala` 642 assert there is no `def isSet(`.
  - A search of `core\src\main` and `cognitive\src\main` found no remaining `isSet`, `isDefined`, `getOrDefault` or `extractParamMap` override (Issue 4).
- [x] **Docs**:
  - CU guide lines 56-58, 72-73, 100-101, 282-290, 326-350, 375-404, 436-458 and 504-530.
  - `docs\Use with MLFlow\Autologging.md` 12-24 correctly separates parameter logging from credential-bearing model artifacts.
  - Notebook:
    - Configuration and endpoint-origin validation.
    - `createAnalyzer(allowReplace=False)`.
    - An `errorifexists` submit write, read back before `show`.
    - Recovery checks the origin before `poller.transform` and before the write.
- [x] Independence: I did not open, list or search `reviews\pr-2815`, any review run directory, or any other reviewer's output. No sibling feedback was encountered.
- [ ] Tests, builds, codegen and Black were not run: this review is read-only. Validation claims come only from the driver's summary.
- [ ] Live Content Understanding behavior (endpoint, credentials, packaged artifacts) was not validated; it is out of scope for this offline review.

## Issues

### Issue 1: Default-mode POST 429 is relabeled as SharedConfigurationFailure after a sibling shared stop
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 322-334 (relabel at 332-334); `SharedStopError` 311-312; decode precedence at 446
- **Description**: Line 322 only allows the retry loop when `settings.retryThrottledRequests` is on. The relabel at line 332 does not check that setting; it tests only `analysis && raw.throttled(method) && context.stopSubmission(method)`.
  - **Trigger**: the default `retryThrottledRequests=false` with concurrency greater than 1. A POST is already in flight when a sibling row sets the per-task `stopped` flag in `rowHandler`. The service then returns a definitive 429 without Operation-Location.
  - **Result**: the row gets `SharedStopError`. Because `decode` prefers `raw.diagnostic` (line 446), the row reports `SharedConfigurationFailure` ("...stopped new submissions...") with httpStatus 429, and the service's throttling error is discarded.
  - **Why this is wrong**: no retry existed to stop. The request was sent and rejected for throttling.
  - **Docs contradicted**:
    - Guide lines 378-379: the default "preserv[es] the behavior described above".
    - Guide lines 289-290: only *unsent* rows get the stop result (Rejected, HTTP 0).
  - **Untested**: the only `stopped.set(true)` test uses GET (ThrottlingSuite around line 266), and the sibling-stop test (around line 241) runs only with the option on.
- **Risk**: Default-mode diagnostics are misattributed. Operators may look for a configuration problem on a row that was only throttled, and the service's 429 code and message are lost. Retry selection is not affected, because the status stays `Rejected`.
- **Suggested Fix**: Add the setting to the relabel condition: `analysis && settings.retryThrottledRequests && raw.throttled(method) && context.stopSubmission(method)`. Then add a protocol test with default settings, a stopped Context, and a POST that returns 429 without Operation-Location. It should assert that the original 429 error is kept, with no `SharedConfigurationFailure` code and status `Rejected`.

### Issue 2: The `pollingDelay` parameter doc misstates when fallback delays are exact
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 118 (secondary: 108)
- **Description**: The doc says "the first fallback and delays of at least 60000 are exact."
  - When pollingDelay is below 60000, the base is below the 60000 cap, so the cap step is jittered to [54000, 60000] (`ContentUnderstandingThrottling.scala` 37-38). `ContentUnderstandingThrottlingPolicySuite.scala` line 23 asserts exactly this (`delays.last >= 54000L`).
  - Only a pollingDelay of at least 60000 gives a constant delay. The guide states this correctly at lines 397-399.
  - This text also appears in the generated Python docs and `explainParam` output.
  - Secondary: the `maxPollAttempts` doc (line 108) dropped "including transient failures". It no longer says that non-429 transient GET failures, and 429s when the option is off, still use up attempts.
- **Risk**: The API reference disagrees with the guide and the tests. Users tuning backoff may expect 60-second steps to be exact.
- **Suggested Fix**: For `pollingDelay`: "...with a 1000 ms floor when Retry-After is unusable. Up to 10% downward jitter never drops below that starting delay, so the first fallback is exact and a pollingDelay of at least 60000 gives a constant fallback." For `maxPollAttempts`: "Maximum polling GET attempts, including transient-failure retries but excluding opted-in analysis 429 retries."

### Issue 3: `Context.stopSubmission` is named like a command but only checks state
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottling.scala
- **Line(s)**: 19 (call sites at 54, 59 and 66; `ContentUnderstandingProtocol.scala` 332)
- **Description**: `def stopSubmission(method: String): Boolean` reads like an action, but it only returns `method == "POST" && stopped.get()`. Call sites such as `if (context.stopSubmission(method)) throw SubmissionStopped` look as if they trigger a stop. Nearby code names checks as questions: `throttled`, `retryable`, `successful`, `admissionRejected`.
- **Risk**: Readers may misread the stop flow, or a future change may treat it as a mutator.
- **Suggested Fix**: Rename it to `submissionStopped(method)` or `isSubmissionStopped(method)`. Optionally rename `stopped` to `submissionsStopped`. This is a mechanical rename of a package-private method.

### Issue 4: Stale Wrappable comment describes a removed runtime `isSet` override
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
- **Line(s)**: 471-473
- **Description**: The comment "The runtime override retains PySpark's inherited string-or-Param boolean signature." explains why stubs skip `isSet`. But this change removed the shared Python inspection overrides:
  - A search of `core\src\main` and `cognitive\src\main` finds no `def isSet` (or `isDefined`, `getOrDefault`, `extractParamMap`) override in any Python mixin or `pyAdditionalMethods`.
  - `PyCodegenSuite.scala` 190-191 and `ContentUnderstandingSuite.scala` 642 now assert that generated code contains no `def isSet(`.
  - The guard is now purely defensive, and the comment describes code that no longer exists.
- **Risk**: Maintainers may look for, or re-add, a runtime `isSet` override. That would bring back the inspection exposure this change removes.
- **Suggested Fix**: Reword it, for example: "Never stub or override isSet: generic inspection stays on PySpark's inherited Params.isSet (string-or-Param signature) and Python parameter maps."

### Issue 5: Guide says the examples use `errorifexists`, but the handle-persistence snippet relies on the default mode
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Content Understanding.md
- **Line(s)**: 56-58; 330; 349
- **Description**: Lines 56-57 say "The examples use `errorifexists` so rerunning a cell cannot silently overwrite saved results or recovery handles."
  - Every other write sets `.mode("errorifexists")` explicitly (lines 72, 100, 458, 648 and 677), and so does the notebook's submit cell.
  - The submit/poll snippet, which is the recovery-handle case that sentence names, uses `submitted.write.parquet(handles_path)` and `polled.write.parquet(results_path)`.
  - Behavior is the same, because Spark's default save mode is ErrorIfExists. This is a consistency fix only, not a defect.
- **Risk**: Readers adapting the recovery snippet may not realize the overwrite protection is deliberate. The snippet also disagrees with the sentence above it and with the notebook.
- **Suggested Fix**: Use `.write.mode("errorifexists").parquet(...)` at lines 330 and 349.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver decision.
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver decision.
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver decision.
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver decision.
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver decision.
- **How verified**: pending

## Driver Resolution Update (supersedes the pending entries above)

- Issue 1: default-mode POST 429s keep their own diagnostics. Opted-in suppressed
  retries preserve read failures under `innererror`; new protocol tests cover it.
- Issue 2: corrected fallback wording and restored transient-failure accounting
  in maxPollAttempts. The duration exception is explicit in the validation error.
- Issue 3: renamed the predicate `submissionStopped` and encapsulated the atomic
  flag behind `stop()`/`isStopped`, updating all internal callers and tests.
- Issue 4: replaced the stale Wrappable comment with the inherited Python Params
  inspection contract; no runtime behavior was changed by this comment fix.
- Issue 5: both guide persistence snippets now explicitly set `errorifexists`,
  matching their existing default behavior and the notebook.
- Validation and stage collection are recorded in the lens-1 driver section.
  Original findings and verdict remain unchanged.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, freshly resolved from the current task catalog",
    "stageBehavior": "Six concurrent independent lenses; human explicitly requested a fix/review loop until clean",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head; most volume is tests/docs. Localized CU options/throttling/budget changes plus shared Python parameter-inspection changes.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, partition shared-stop, fallback backoff and refund accounting interact with existing asynchronous state machines.",
  "rationale": "Six complementary lenses cover behavioral, architectural, concurrency, correctness, testing and documentation concerns. Supported CLI update and serialization probe passed; each owning runner must still validate transport and identity. Failed or contaminated results cannot count as clean.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: get into a fix review loop until everything is clean and give me a report when you are done. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 4,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The synthetic CLI transport probe does not establish full review transport success. Offline checks do not establish live CU compatibility or approval of matching packaged artifacts."
}
