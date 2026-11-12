---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 2
round: 1
timestamp: "2026-11-10T19:54:14Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "f2c52167f7f29f9a0c70b22e33656004cf2bc7973a63d536e9b26c0be25bb6c3"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 33298 (char 33297)
Invalid CLI event on line 394: Expecting ',' delimiter: line 1 column 6370 (char 6369)

## Original Response

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-2-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Budget refund and cancellation.** Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingBudget.scala` lines 13-58.
  - `withThrottlingRetries` captures `elapsed` once, before the first attempt.
  - After each throttling wait, it resets the start time to `now - elapsed`. This gives back only the time spent on the 429 attempt and its wait.
  - With the option off (the default), `attempt` runs exactly once, so existing behavior is unchanged.
  - `started` is `@volatile`. The refund runs only after the attempt and the wait finish, so no in-flight DNS or deadline reader sees a half-updated value.
  - `pause` checks `checkInterrupted()` every 50 ms, which handles both Spark task cancellation and thread interrupts.
- [x] **Which 429s are retried.** Read `ContentUnderstandingProtocol.scala` lines 338-356.
  - A response is retried only if it is a 429 and either a GET, or a POST whose `Operation-Location` is absent (line 378 checks for the header at all, so an empty or invalid handle blocks the retry).
  - The analyzer-creation PUT (line 765) is excluded by its method.
  - Management GETs are excluded by `managementGet = analyzer.isDefined` (line 622) and `managementGet = true` (line 717).
  - There are no management POSTs. The only request builders are in `ContentUnderstanding.scala` at lines 333, 335, 445 and 481.
- [x] **What counts as a definitive rejection.** Read `exchangeBeforeDeadline` (lines 358-411).
  - Status code, `Operation-Location` and `Retry-After` are captured before the body is read. So a 429 with complete headers still counts as definitive when the body is unreadable (`ResponseBodyReadFailed`) or too large (`ResponseTooLarge`).
  - A transport failure with no status (`code == 0`) is never replayed.
  - Each operation uses its own HTTP client with automatic retries disabled (the `rowHandler` comment).
- [x] **Poll and duration budgets.** Read `pollWithClient`/`execute` (lines 603-665) and the management paths (lines 712-800).
  - Throttled GETs are retried inside `exchange`, so the `remaining` counter for `maxPollAttempts` is not decremented.
  - Non-429 retries, the `delay >= remainingMs` cutoff and management `ManagementGetAttempts` are unchanged.
  - `Retry-After` parsing (lines 280-287) clamps numeric overflow and supports HTTP dates.
- [x] **Throttling tests.** Read `ContentUnderstandingThrottlingSuite.scala` lines 28-125. It covers:
  - `Retry-After` longer than the operation budget;
  - poll attempts and duration being excluded;
  - a 429 that carries a handle not being replayed;
  - a 429 whose body cannot be read;
  - ordinary limits still applying;
  - interruption during the wait for both POST and GET.
- [x] **`allowInputTruncation` implementation.** Read `ContentUnderstanding.scala` lines 85-92, 168-197 and 317-323, and `ContentUnderstandingParams.scala` lines 138-147.
  - The parameter is used outside poll mode only.
  - Column type is checked as `BooleanType`, and scalar Booleans are validated.
  - Null row values are omitted via `getValueOpt`, and the query value is `true` or `false` via `value.toString`.
  - There is no version allowlist.
- [x] **`allowInputTruncation` tests and docs.**
  - `test_ContentUnderstanding.py` lines 170-219 and `ContentUnderstandingOptionsSuite` cover: explicit `false`, cleared means omitted, per-row column values true/false/null, and survival through copy/save/load.
  - The guide (`Content Understanding.md` lines 187 and 200-218) says GA omits it by default and forwards it without an allowlist.
- [x] **Python inspection overrides.** Read `core/src/main/python/synapse/ml/core/schema/Utils.py` lines 150-241.
  - The `isSet`, `hasDefault`, `getOrDefault` and `extractParamMap` overrides are removed. The remaining redaction and transfer helpers do not expose JVM-held values.
  - Tests at `test_ContentUnderstanding.py` lines 876-1000 assert that no JVM value is looked up, and that typed getters, copy, save/load and clear still work. Values set through generic `set` remain visible, which matches the human scope.
  - `openai/test_OpenAIResponseSchema.py` lines 87-91 were updated consistently.
  - An earlier grep of the cognitive Python tests found no remaining callers that depend on the removed overrides.
- [x] **Guide examples write once, then read.** The URL and binary examples write once and read the saved Parquet (`Content Understanding.md` lines 68-69 and 96-97). Display uses the saved `results` (lines 240-249), and retry selection reads the saved data (lines 428-439).
- [x] **Notebook.** Read `Quickstart - Semantic Extraction with Content Understanding.ipynb`.
  - The key is entered with `getpass` and never printed.
  - Each run gets a unique analyzer ID (lines 66 and 75) and uses `allowReplace=False` (line 214).
  - It neither deletes analyzers nor changes shared resource defaults (line 395).
  - Snapshots are written once and then read back.
  - The opt-in throttling behavior is described accurately (line 264).
  - It is excluded from automated runners: `SynapseTests.scala` line 50, `DatabricksUtilities.scala` lines 256-269, and Fabric runs only an include-list.
- [x] **Spec conformance.** Checked against the human requirements:
  - Throttling retry is opt-in and defaults to false (Params line 175).
  - Management and non-429 budgets are unchanged.
  - Waits are cancellable and uncertain POSTs are never replayed.
  - No BasicAsyncReply rewrite, global throttling or provisioning was introduced.
- [x] **No tests run by me.** I did not run any builds or tests. The build and test results come from the driver's supplied evidence: JDK 11/Scala 2.12.17/Spark 3.5.0, 66 core and 182 CU Scala tests, and 66 Python tests with 70 subtests. That evidence also reports 42 CI-helper failures because the host has no Bash/WSL, and the Black Jupyter extra could not be installed.
- [ ] **Live CU compatibility is not checked.** It cannot be verified offline. This covers the preview `allowInputTruncation` contract and whether the service accepts the custom analyzer definition (`models`/defaults) under the target API version. No live run was performed, and this is not counted as a defect.

## Issues

### Issue 1: Opt-in unlimited 429 recovery has no minimum delay or backoff, so it can retry in a tight loop
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; ContentUnderstandingBudget.scala; ContentUnderstandingParams.scala
- **Line(s)**: Protocol 280-287 and 351-355; Budget 16-32 and 37-39; Params 114-118
- **Description**:
  - When the option is on, the wait between retries is `retryDelay(raw.retryAfter, settings.pollingDelay)`.
  - `pollingDelay` may be set to 0 (`ParamValidators.gtEq(0)`). A missing or invalid `Retry-After` then gives a 0 ms wait. So do `Retry-After: 0` and a past HTTP date (`max(0, t - now)`).
  - `pause(0)` only checks for cancellation and returns at once. The loop in `withThrottlingRetries` has no attempt or time cap by design, so it re-sends the POST or GET immediately, without limit.
- **Risk**:
  - Each throttled row can resend its full request continuously, including up to 32 MiB of binary input before base64 encoding.
  - This multiplies by `concurrency` and by the number of partitions. It makes throttling on the shared CU resource worse and wastes executor CPU and network.
  - The task never finishes unless it is cancelled.
  - It only happens with the option on plus a zero fallback or a zero `Retry-After`, so the impact is limited.
- **Suggested Fix**:
  - When the option is on and `Retry-After` is missing or invalid, enforce a minimum wait (for example 1 s), or use bounded exponential backoff with jitter for consecutive 429s. Keep honoring positive `Retry-After` values.
  - Alternatively, reject `pollingDelay == 0` when `retryThrottledRequests` is true, or document the interaction.
  - Add a test with `pollingDelay(0)` and no `Retry-After` that asserts the retries are spaced out.

### Issue 2: The `pollingDelay` description leaves out its role as the fallback wait for opt-in 429 retries
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala; docs/Explore Algorithms/AI Services/Content Understanding.md
- **Line(s)**: Params 114-118; guide table line 479 (the prose at lines 381-382 is correct)
- **Description**:
  - The parameter description, which also becomes the generated Python/Scala docstring, says `pollingDelay` applies only to operation polls, the first poll after submission, and management GET retries.
  - The guide's setter table says the same.
  - Neither mentions that `exchange` (Protocol line 354) now also uses `pollingDelay` as the wait between opted-in analysis 429 retries when `Retry-After` is missing or invalid.
- **Risk**: Users tuning polling, for example setting `pollingDelay(0)` for fast status checks, cannot tell from the docstring that they are also setting the throttling wait. This leads directly to Issue 1. The parameter text also disagrees with the guide prose.
- **Suggested Fix**: Add to both descriptions: "and between opted-in analysis HTTP 429 retries when Retry-After is absent or invalid". Optionally note that 0 means immediate retries.

### Issue 3: No regression test shows that analyzer management ignores `retryThrottledRequests`
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottlingSuite.scala
- **Line(s)**: 28-125 (no management case); production exclusions at ContentUnderstandingProtocol.scala lines 343, 352-353, 622, 717 and 765
- **Description**:
  - The requirement that management behavior stays unchanged depends on two separate mechanisms. The method check excludes the creation PUT. The `managementGet` flag excludes the management polling GET and the `getAnalyzer` GET.
  - Every test that sets `retryThrottledRequests` covers analysis only (grep of the CU test directory).
  - The existing management tests run with the default `false`, so they would not notice if the option started affecting management calls.
- **Risk**: A future refactor could drop the `managementGet` argument or widen the method check. Under opt-in, `createAnalyzer`/`getAnalyzer` would then retry 429s indefinitely, and no test would fail. That would break the stated requirement.
- **Suggested Fix**: Add a ThrottlingSuite case with `setRetryThrottledRequests(true)` that asserts:
  - a 429 on the creation PUT fails after exactly one request;
  - a 429 on the management polling GET and on `getAnalyzer` keeps the existing `ManagementGetAttempts`/budget behavior, with a bounded request count.

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

## Driver Rerun Outcome - 2026-11-10

**BLOCKED: zero of six invocations accepted as formal signoff.**

The user explicitly requested this rerun. All six Medium lenses executed
concurrently with runtime reviewer `claude-opus-5.5` and requested reasoning
`max`, using review attempt 6, stage attempt 2. All six CLI processes exited zero,
but every owning runner exited one with `persisted: true`, `outcome: failed`.
Every failed artifact retains the original response; raw events, diagnostics and
receipts remain in the corresponding session run directory. Stage-attempt-1
artifacts are also retained.

The same JSON failure recurred. Each stream has two invalid records, and each
parse-error location is adjacent to masked authorization text. No event stream
was repaired, masking disabled, or failed result promoted to accepted evidence.
Another unchanged rerun is not planned; the CLI transport problem must be
addressed before relying on this harness for signoff.

The frozen diff SHA-256 is
`a66994a33b498bb209657fa26f0227f82761c2059257335fb6224a7616ba7f69`.
All 17 reviewed source files matched the pre-dispatch hash manifest after
collection. No source changes, commits, pushes, remote comments or live CU calls
were made during this rerun.

### Recovered feedback: open, pending triage

The reports contain **6 medium and 24 low findings**, including duplicates;
none is high or critical. These are reviewer assessments, not independently
confirmed defects or valid formal verdicts. The previously corrected OpenAI
inspection assertion and unattended-notebook selection were not re-reported.
Their absence is not a substitute for accepted independent signoff.

| Lens | Medium | Low | Main feedback |
|---|---:|---:|---|
| 1: Broad sweep | 0 | 3 | Zero-delay retries, pollingDelay documentation, management exclusion tests |
| 2: Architecture | 1 | 5 | Explicit retry eligibility, budget responsibilities, trait grouping, pollingDelay, notebook links and test organization |
| 3: Robustness | 1 | 5 | Retry pacing, recovery handles after endpoint mismatch, overwrite behavior, configuration parsing, shared thread-pool blocking and management tests |
| 4: Correctness | 0 | 2 | Empty optional model-deployments JSON and retry-delay behavior/documentation |
| 5: Coverage | 2 | 5 | Management exclusion, exact budget refund, explicit GA truncation, Retry-After fallback/date cases, poll-mode ignored options, cancellation and assertion/persistence coverage |
| 6: Polish | 2 | 4 | Throttling diagnostics, MLflow parameter-versus-model logging scope, retry pacing, lazy/saved result wording, API grouping and notebook polish |

Important details preserved for the next triage:

- **MLflow wording (L6 I2):** the reviewer distinguishes excluding credentials
  from generic parameter logging from saving model artifacts with
  `log_models=True`. The existing test deliberately uses `log_models=False`.
  The request is to clarify the documentation, not sanitize runtime credentials.
- **Recovery handles (L3 I2):** polling saved handles with a mismatched endpoint
  can produce local InvalidInput rows with null handles in the next snapshot.
  Original handles remain in older snapshots, but the documented latest-snapshot
  recovery flow may stop retrying those rows.
- **Notebook routes (L2 I5):** absolute GitHub master links may not work before
  merge or follow the correct versioned documentation route.
- **Thread-pool occupancy (L3 I5):** long synchronous throttling waits can occupy
  the shared execution pool. The reviewer describes this as extending an
  existing pattern, not a newly introduced pool.
- **Coverage findings (L5):** tests should distinguish refunding only throttled
  time from resetting an already partially consumed budget, and explicitly
  exercise management calls while the retry option is enabled.

All findings in all six original reports remain available and open for triage.
No retry-policy changes, new timeouts, architectural refactors, additional test
changes or documentation edits were made in response to this review-only rerun.
Prior validation results and environment limitations remain as recorded in the
stage-attempt-1 driver outcomes; they were not relabeled as all-green.

## Driver resolution update before stage attempt 3

The original response and failed transport outcome above remain unchanged.
The following supersedes the earlier open-for-triage status, not the reviewer verdict.

- Missing/invalid Retry-After now uses adaptive fallback with a positive floor,
  bounded exponential growth and downward jitter in
  `ContentUnderstandingThrottling.scala`. The user explicitly chose to preserve
  valid Retry-After exactly, including zero and past dates; those intentionally
  permit immediate retries. No execution timeout or retry limit was added.
- The guide and pollingDelay parameter description explain the fallback role,
  delay cap, exact-header behavior and refund boundary.
- Management exclusions are explicit at analysis call sites. New policy tests
  cover management PUT, GET and management polling with opt-in both on and off.

Verification is recorded in the consolidated validation update below. These are
driver resolutions awaiting the requested independent stage-attempt-3 review,
not an assertion that the earlier failed review produced signoff.

### Consolidated validation after fixes

- Aggregate production/test compilation, Scala main/test style and cognitive
  code generation passed.
- Shared Scala regressions: 66 passed. Notebook-exclusion regression: 1 passed.
- Broad CU run: 188 of 189 passed. The remaining reflection test used the old
  private `exchange` signature; it was updated to supply retry eligibility.
  The complete RecoverySuite then passed all 38 tests. All 189 distinct CU
  cases are covered across these runs; the initial run itself was not green.
- Python: 96 passed, 241 subtests passed, 7 live CU tests skipped. This includes
  real MLflow autologging, actual guide/notebook cells, copy/save/load, HTTP,
  OpenAI and Spark cancellation regressions.
- Black: 219 Python files unchanged. Seven notebook cells passed formatting
  and syntax checks; notebook schema/empty outputs, sidebar JS syntax and
  `git diff --check` passed.
- Runtime: Python 3.11.18, Spark 3.5.0, Scala 2.12.17, JDK 11.0.33+10,
  MLflow 2.21.3. Tests used local classes/generated Python, not release packages.

The earlier broader CI-helper run still has 42 Bash/WSL environment failures.
Black's Jupyter-extra installation was TLS-blocked; the actual plain-Python
cells were formatted and checked directly. No full-repository or live gate is
claimed. Live validation still requires endpoint, secure credentials and
matching packaged Python/JVM artifacts from one committed PR head.

Relevant commands: aggregate `compile`, `Test/compile`, `scalastyle`,
`Test/scalastyle`, `cognitive/codegen`; nonforked/nonparallel
`core/testOnly *PyCodegenSuite *SimpleHTTPTransformerSuite *ValidateComplexParamSerializer`,
the interactive-CU selector in `DatabricksUtilitiesSuite`,
`cognitive/testOnly *ContentUnderstanding*Suite`, then
`cognitive/testOnly *ContentUnderstandingRecoverySuite`.
Python used the retained local-Spark harness against `test_ContentUnderstanding`,
`test_ServiceParamPythonBridge`, `test_ContentUnderstandingE2E`,
`test_http_package`, `test_OpenAIResponseSchema` and `test_OpenAIPromptParams`.

The user-requested next review uses the frozen 26-file source diff SHA-256
`48d111d93f005b4eccbba64e99914e47a404919e477fcd607926f394c42bc072`.
Review feedback is excluded. No commit, push, ADO edit or live CU call was made.
