---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 4
round: 5
timestamp: "2026-11-10T21:51:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9e68a77e400b6a6ef13513bb4637510936347405de9c44e9e4998f2ebbfc826d"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-4-review-5-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the new throttling control flow:
  - `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottling.scala`:
    - `Context` (16-20) and `Backoff` (24-41).
    - `retry` (43-72): pre-wait stop check at 54, `onTick` stop/`waiting` at 58-61, refund at 65, post-wait check at 66.
  - `ContentUnderstandingProtocol.scala`:
    - `exchange` (314-335), including the relabel at 332-334.
    - `decode` (446), `decodeSubmission` (508-524) and `rowHandler` (657-680).
- [x] Checked policy unit coverage in `ContentUnderstandingThrottlingPolicySuite.scala`:
  - Fallback backoff (16-26): the first fallback is exact, jitter stays within bounds, and the delay is constant when base ≥ cap.
  - Valid Retry-After is exact (28-43): 0, 120, `Long.MaxValue`, and past and future HTTP dates, plus the reset after a valid header.
  - Refunds keep earlier charged time in both modes (45-67).
  - Management PUT/GET are neither replayed nor extended (69-101).
  - Reporter cadence, tested only by calling the reporter directly (103-120).
- [x] Checked integration coverage in `ContentUnderstandingThrottlingSuite.scala`:
  - Retry-After longer than the budget (29) and untouched poll budgets (47).
  - Exact waits and fallback reset per exchange (62, 218).
  - Sanitized sparse logs with `partitionId=0` on an async worker (90-109).
  - No replay of uncertain submissions (111) and recovery from an unreadable 429 body (127).
  - Ordinary limits preserved (140) and cancellation during waits (155).
  - Non-429 failures unchanged (196).
  - Shared-stop sibling (241-260) and stopped partition still polling GETs (262-275).
- [x] Checked `allowInputTruncation` coverage:
  - `ContentUnderstandingOptionsSuite.scala` 24/44/62: unset omits the option; explicit true/false on GA and preview; a null row omits it; non-boolean columns are rejected; poll mode ignores the column.
  - Python copy/save/load with the retry opt-in defaulting to false: `test_ContentUnderstanding.py` 172-228.
- [x] Checked Python coverage for the inspection/MLflow change in `test_ContentUnderstanding.py`:
  - MLflow autolog (229-310).
  - Cancellation, including a throttled scenario (311-440).
  - Generic inspection and getters (1049-1172, 1278, 1342).
- [x] Checked docs and notebook execution tests:
  - Guide examples write once, display adds no requests, and a rerun raises `AnalysisException` with no new requests (492-557).
  - Notebook cell contract (559-594).
  - Configuration is validated before any service call (596-671).
  - Execute and recovery, including endpoint-origin mismatch rejection and an unused invalid model mapping (672-805).
- [x] Traced async task-context propagation:
  - `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala` 50-91.
  - `HTTPTransformer.scala` 108-115: concurrency > 1 runs handlers inside `AsyncClient.withTaskContext`.
- [x] Traced the notebook exclusion:
  - `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilitiesSuite.scala` 19-28.
  - `SynapseTests.scala` 49-68.
  - `SharedNotebookE2ETestUtilities.scala` 89-103.
- [ ] Tests, builds and codegen were not run, because only read-only tools were allowed. I considered the supplied results but did not reproduce them: affected CU suites 17/17; Python 96 tests and 248 subtests with 7 live tests skipped; style and codegen.
- [ ] Live Content Understanding compatibility was not assessed. Every reviewed test uses loopback stubs.
- [x] Independence was preserved. I did not access `reviews\pr-2815`, session-state or sibling outputs, and kept searches scoped to `cognitive\src`, `core\src` and `docs`.

## Issues

### Issue 1: The shared-stop sibling test cannot detect loss of the in-wait submission stop
- **Severity**: Medium
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: 241-260 (production: ContentUnderstandingThrottling.scala 54, 58-61, 66; ContentUnderstandingProtocol.scala 332-334)
- **Description**: This is a test gap that a mutation would expose. The test's name claims the stop happens "while its throttle wait is active", but the test cannot observe that.
  - Setup: it serves a 429 with `Retry-After: 60`, then a 404 `AnalyzerNotFound`, at concurrency 2.
  - Assertions: it checks only the final statuses, the `SharedConfigurationFailure` text, and `requests.size == 2`.
  - Three paths give identical observable results:
    - The stop lands before the retry decision (line 54).
    - `onTick` aborts the wait (lines 58-61).
    - The `onTick` stop check is missing: the full 60 s wait runs, line 66 returns the 429, and `exchange` relabels it.
  - Nothing distinguishes these paths: there is no elapsed-time bound and no latch, and TestBase has no default timeout.
  - No other test reaches `SubmissionStopped` through `onTick`. The only other pre-stopped context (262-275) is a GET, which must continue polling.
- **Risk**: Valid Retry-After values are honored exactly and without a cap. If the in-wait check regressed, partitions would sleep for the full Retry-After, possibly hours, while CI stays green. This breaks the requirement that a shared stop halts rejected POST retries "including waits".
- **Suggested Fix**:
  - Add a deterministic unit test that reuses the fake-clock budget pattern from PolicySuite 45-67.
    - Override `pause` so it sets `context.stopped`, calls `onTick()`, then sets a `reached` flag.
    - Use `retry(true, …, "POST", context)` with an attempt that always returns 429.
    - Assert one attempt, the 429 returned, `!reached`, and the refunded `remainingMs`.
  - In the integration test, assert that the run finishes well under 60 s. Also delay the 404 reply until the 429 has been sent, so the in-wait path is the one exercised.

### Issue 2: Reporter ticks during a wait, and per-partition sharing, are never exercised through `retry`/`rowHandler`
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottling.scala
- **Line(s)**: 16-20, 56-60 (tests: ThrottlingPolicySuite 103-120; ThrottlingSuite 90-109; Protocol 659)
- **Description**: The reporter's cadence is unit-tested only by calling `ThrottleReporter.retrying` and `waiting` directly.
  - The only integration log test uses one row and `Retry-After: 0`, and asserts a single warning with `retryCount=1`.
  - Two regressions would leave every test green:
    - Deleting `context.reporter.waiting(...)` (line 60).
    - Creating a fresh `Context` per row or per exchange, for example by falling back to the `exchange`/`execute` default `new Context`. That loses the single reporter per partition created at `rowHandler` line 659.
- **Risk**: The documented guarantees could regress silently and cause either warning floods per row or silence during long waits. The guide (lines 511-519) promises "at most once per minute per partition, shared across rows, submissions and polls", and "long waits check cadence".
- **Suggested Fix**:
  - Unit test:
    - Subclass `Context` with `override val reporter = new ThrottleReporter(() => now)`.
    - Make the budget's `pause` call `onTick`, advance the clock past 60 s, and call `onTick` again.
    - Assert two warnings during a single wait.
  - Integration test, choosing either of these:
    - Two rows in one partition, each throttled once with `Retry-After: 0`, asserting exactly one warning.
    - Analyze mode with a POST 429 followed by a GET 429.

### Issue 3: The Synapse notebook-exclusion assertion is tautological
- **Severity**: Low
- **File**: core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilitiesSuite.scala
- **Line(s)**: 26 (SynapseTests.scala 50, 67-68; SharedNotebookE2ETestUtilities.scala 89-103)
- **Description**: The test passes a literal file name that contains the exclusion key, so it can never fail.
  - `new File("SemanticExtractionwithContentUnderstanding.py")` contains the key exactly.
  - `includeNotebook` does a substring match on the absolute path, so it returns false regardless of the real notebook's name.
  - Real Synapse names come from `generateNotebooks`, which strips `/`, spaces, `-` and `,` from the docs-relative path. The Databricks half of the test (20-25) checks the real file; the Synapse half does not.
- **Risk**: If the notebook is renamed (for example, a change in capitalization), it silently rejoins unattended Synapse runs that need user-supplied CU resources.
- **Suggested Fix**: Factor the name transform out of `generateNotebooks` into a helper. Apply it to the actual `.ipynb` from `NotebookFiles` and assert that `includeNotebook` returns false for the result.

### Issue 4: The throttled-POST shared-stop relabel is not gated on the opt-in and drops read-failure diagnostics; neither is tested
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 332-334 (decode 446; rowHandler 670-677; ConcurrencySuite 354-379; guide 377-379)
- **Description**: This combines a reproducible diagnostic loss with a default-mode behavior whose intent is unclear.
  - Not gated on the opt-in (intent uncertain):
    - `exchange` relabels any stopped-partition analysis POST 429 without a location as `SharedConfigurationFailure`, regardless of `retryThrottledRequests`.
    - With the default false and concurrency ≥2, a racing sibling's stop therefore changes a throttled row's error code, and the outcome depends on timing.
    - The guide (377-379) says the default preserves prior behavior.
  - Read-failure diagnostics are lost:
    - `raw.copy(diagnostic = …)` replaces any `ResponseBodyReadFailed` or `ResponseTooLarge` diagnostic, since `decode` prefers `raw.diagnostic`.
    - That contradicts the contract asserted at ConcurrencySuite 369-370, where the read-failure code is preserved, and at 377, where it is nested alongside `SharedConfigurationFailure`. `rowHandler` (672-677) nests such causes as `innererror`.
  - Test coverage: the only relabel test (ThrottlingSuite 241-260) has retry enabled and a readable body.
- **Risk**: The impact is diagnostic only; status and httpStatus are unchanged. Error codes vary between reruns, and transport/read evidence is lost.
- **Suggested Fix**:
  - Decide the intended scope. Either gate the relabel on `settings.retryThrottledRequests`, or document that it applies in both modes.
  - Nest any existing transport-class diagnostic as `innererror`, as `rowHandler` does.
  - Add deterministic tests that pre-set `context.stopped` and call `execute`, as at ThrottlingSuite 262-275:
    - one test per mode, with retry off and on;
    - each covering a POST 429 with a readable body and with a truncated body.

### Issue 5: Notebook tests never send a non-empty `modelDeployments` mapping through analysis
- **Severity**: Low
- **File**: cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py
- **Line(s)**: 654-661, 724, 794-804 (notebook .ipynb lines 34, 258-259)
- **Description**: The notebook's documented `modelDeployments` behavior is not verified.
  - What the notebook says and does:
    - It documents the mapping as "sent as `modelDeployments` with analysis submissions… does not change analyzer creation".
    - It sets the mapping on the analyzer at lines 258-259.
  - What the tests check:
    - `{"model": "deployment"}` only runs through the create-analyzer cell, which asserts HTTP methods (`["PUT"]*4`) but never inspects the request bodies.
    - The executed path uses `"{}"`. Its final assertions never check `modelDeployments` in the POST body or its absence from the PUT body.
    - Lines 443-459 cover the stage itself, not the notebook's wiring.
- **Risk**: Nothing verifies the requirement that the mapping is an analysis-request field, not analyzer configuration. Moving it into the analyzer definition, or dropping it, would pass.
- **Suggested Fix**: Add a subtest of the execution test with `'{"model": "deployment"}'`:
  - Assert that `submissions[0][2]["modelDeployments"]` equals the mapping.
  - Assert `"modelDeployments" not in creation`.
  - Assert that the `"{}"` case omits the field.

### Issue 6: The shared-stop task-context assertion still accepts `taskContext=unavailable` on async workers
- **Severity**: Low
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingSubmissionSuite.scala
- **Line(s)**: 345-349 (production: ContentUnderstandingLogging.scala `sharedStop`/`taskDescription`)
- **Description**: The diff changes `sharedStop` from `TaskContext.get()` to `AsyncClient.currentTaskContext`.
  - At concurrency 2, `AsyncHTTPClient` runs the handler inside `withTaskContext` (HTTPTransformer 108-115, Clients 83-91). The warning should therefore always contain `partitionId=0`.
  - Line 348 still accepts `taskContext=unavailable`. Only the throttle warning (ThrottlingSuite 101) pins task IDs on an async worker.
- **Risk**: Reverting `sharedStop` to use `TaskContext.get()` would drop task IDs from concurrent shared-stop warnings, and no test would fail.
- **Suggested Fix**: For both concurrency values, require `stageId=`, `partitionId=0` and `taskAttemptId=`, and assert that `taskContext=unavailable` is absent.

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

## Driver Resolution Update (supersedes the pending entries above)

- Issue 1: added a fake-clock retry test that sets the stop inside `pause`,
  invokes the tick, and asserts that control never reaches the following
  statement, only one POST occurs, and precisely the throttled time is refunded.
  This directly detects removal of in-wait abort without a wall-clock timeout.
  The existing public sibling integration remains as complementary coverage.
- Issue 2: added a fake-clock heartbeat test through `retry`, plus public
  transformer cases sharing one warning across two rows and across POST/GET.
- Issue 3: extracted the actual generated-name transformation and applied it to
  the real notebook discovered by `NotebookFiles` in the Synapse exclusion test.
- Issue 4: guarded relabeling by the opt-in and preserved nested read diagnostics.
  New deterministic protocol cases cover both modes and all three body cases.
- Issue 5: the actual notebook execution/recovery helper now runs as separate
  empty/nonempty deployment-mapping tests. Assertions inspect POST and PUT
  bodies, including omission for the empty mapping.
- Issue 6: both synchronous and asynchronous shared-stop log tests require
  stage, partition and task-attempt IDs and reject unavailable task context.
- Validation and stage collection are recorded in the lens-1 driver section.
  These updates do not overwrite the original independent findings.

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
