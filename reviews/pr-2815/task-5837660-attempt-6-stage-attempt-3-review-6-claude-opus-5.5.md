---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 3
round: 6
timestamp: "2026-11-10T20:59:40Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "f2312d76f4228ffb8eb7e974824d6c7c183ed903e3e14725994cec06ed848acc"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 42248 (char 42247)

## Original Response

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-3-review-6-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Throttling retry loop and time accounting.** In `ContentUnderstandingThrottling.scala` lines 33-58, `mark()` is taken before each attempt. On a retryable 429, the code computes the wait, sleeps with `pause`, then calls `refundSince(mark)`. In `ContentUnderstandingBudget.scala` lines 14-23, the refund adds only the interval since that mark to `started`. So only the throttled attempt and its wait are excluded, and earlier time stays charged. Each throttled exchange counts as a single `maxPollAttempts` iteration (`ContentUnderstandingProtocol.scala` lines 604-622).
- [x] **Retry eligibility.**
  - `ContentUnderstandingProtocol.scala` lines 327-346: POST is retried only on 429 with no `Operation-Location`; GET is retried on 429.
  - Poll uses `analysisRetry = analyzer.isEmpty` (lines 612-613), and the analysis POST uses `analysisRetry = true` (line 638). Management paths keep the default `false`.
  - The flag defaults to false (`ContentUnderstandingParams.scala` lines 151-157 and 175) and is wired into `Settings` (`ContentUnderstanding.scala` lines 146-147).
- [x] **Cancellation and pool notification.**
  - `pause` checks `checkInterrupted` (which uses `AsyncClient.isTaskCancelled`) before sleeping and every 50 ms (`ContentUnderstandingBudget.scala` lines 28-50).
  - The new `blocking` wrappers (Budget line 28, Protocol line 343) are nested inside the shared `handle`, which already uses `blocking` (`HTTPClients.scala` lines 460-472). Nested `blocking` is a no-op, so they are harmless, and the shared HTTP code is unchanged.
- [x] **allowInputTruncation.**
  - `HasAllowInputTruncation` is defined at `ContentUnderstandingParams.scala` lines 139-148.
  - In `ContentUnderstanding.scala`: Boolean type and validation at lines 171-172 and 189; the URL query value is `value.toString` (lines 317-323), so an explicit `false` is sent; the param is listed in `activeParams` at line 89, so poll mode excludes it.
  - Because generic `getValueOpt` returns `Option(null)`, an unset or null value is omitted.
- [x] **Python inspection removal.**
  - `Utils.py` lines 63-242 no longer override `isSet`, `hasDefault`, `getOrDefault` or `extractParamMap`.
  - Typed getters still go through `_service_param_scalar_to_python` (`Wrappable.scala` line 324).
  - `explainParam` redaction is kept (lines 156-180).
  - The guide's text on parameter logging versus credential-bearing model artifacts, and `Autologging.md` lines 12-18, match this behavior.
- [x] **Logging content.** `ContentUnderstandingLogging.scala` lines 25-30 emit only the method, count, wait and task fields. The test at `ContentUnderstandingThrottlingSuite.scala` lines 81-97 asserts counts 1/2/4 and that no secret or URL appears.
- [x] **Documentation and notebook.**
  - Read the guide end to end: throttling (lines 375-400), bounds and logging (lines 465-560), management and notebook link (lines 578-655).
  - Read all notebook cells. The sidebar entries (`sidebars.js` lines 38-39) and the relative links match sibling docs (e.g. `Distributed Training.md` line 75).
  - The notebook is excluded from E2E runs: `SynapseTests.scala` `ExcludedNotebooks` matches the generated name (`SharedNotebookE2ETestUtilities.scala` lines 96-103), `DatabricksUtilities.scala` lines 256-258 filters it, and Fabric uses an include-list (`FabricNotebookTests.scala` lines 151-156).
- [ ] **Builds and tests not run by this reviewer.** The plan was read-only. The supplied validation evidence was inspected but not reproduced.

## Issues

### Issue 1: Fallback backoff never jitters the first retry, and never jitters at all when pollingDelay ≥ 60000
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala (also docs/Explore Algorithms/AI Services/Content Understanding.md, ContentUnderstandingThrottlingPolicySuite.scala)
- **Line(s)**: Throttling.scala 15-16, 27-28; guide 392-394 and 494; PolicySuite 18 and 24-25
- **Description**: The lower bound is `lower = max(base, current - current/10)`. On the first fallback, `current == base`, so `nextLong(base, base + 1)` always returns exactly `base`, with no jitter. When `pollingDelay >= 60000`, `base == cap`, so every fallback delay is exactly `cap`. The tests lock this in (`delay(None) == 1000L`, and ten exact `120000L` values). The guide says the backoff has "up to 10% downward jitter" and the table says it has "jitter". Neither mentions that the first retry, or every retry for large `pollingDelay`, is deterministic. The human requirement asks for "exponential fallback with jitter".
- **Risk**: If many rows or partitions receive a 429 without a usable `Retry-After` at the same moment (for example, at job start or behind a gateway that drops the header), they all retry exactly `base` ms later. That synchronized burst is likely to be throttled again. The later 10% one-sided spread breaks up that synchronization slowly. The performance impact is limited to missing or invalid headers.
- **Suggested Fix**: Clamp the jitter range to the 1 s floor and the cap instead of to `base`. For example, draw from `[max(1000, current - current/10), min(cap, current + current/10)]`, which gives a spread at every step, including the default first retry and `base == cap`. Update the PolicySuite assertions to check ranges, and change the guide (line 392) and table (line 494) to describe the actual jitter.

### Issue 2: Throttling warnings report `taskContext=unavailable` whenever concurrency > 1, although the owning task is available
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingLogging.scala
- **Line(s)**: 25-30 (duplicated from 37-39); related: ContentUnderstandingProtocol.scala 146 and 270; Clients.scala 50-72 and 83-92; HTTPTransformer.scala 108-115; guide 502-507
- **Description**: `throttled` uses `TaskContext.get()`. With `concurrency > 1`, CU runs on `AsyncHTTPClient`, which executes handlers on `ExecutionContext.global` threads. On those threads `TaskContext.get()` is null; only `AsyncClient.Owner` holds the owning task. CU already uses `AsyncClient.currentTaskContext` for cancellation and deadlines (Protocol lines 146 and 270), but the new warning does not. The context-formatting code is also copied verbatim from `sharedStop`.
- **Risk**: Throttling is most likely at higher concurrency; the guide itself advises reducing `setConcurrency`. In that case almost every throttling warning lacks stage, partition and task-attempt IDs, so operators cannot match waits to Spark tasks. This undercuts the documented "available task IDs" diagnostic.
- **Suggested Fix**: Add a private helper that formats task fields from `AsyncClient.currentTaskContext` (it is `private[ml]`, so CU can access it). Use the helper in both `throttled` and `sharedStop`. Update guide lines 502-504, which say async workers report no context, and add a `setConcurrency(2)` log assertion that checks for `partitionId=`.

### Issue 3: Power-of-two log cadence leaves throttling waits silent for hours, with no outcome summary
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala
- **Line(s)**: 11, 45-47; guide 506-508
- **Description**: Warnings are emitted only when `count` is a power of two. In unlimited mode with the 60 s fallback cap, or a steady service `Retry-After`, the gap between warnings doubles each time. Warnings 128→256 are about 2 hours apart, and 512→1024 about 8.5 hours apart. Nothing is logged when throttling ends, so the total retry count and total wait for a request that eventually succeeds are never recorded.
- **Risk**: During a long quota exhaustion, a task that is waiting looks the same as a hung task for hours. After recovery, operators cannot tell how much time throttling consumed. This gap appears only with the new opt-in unlimited mode.
- **Suggested Fix**: Keep the sparse design, but bound the silent interval. Also log when a fixed interval (for example 5 minutes) has passed since this exchange's last warning. Optionally log one sanitized INFO/WARN summary on exit (`retries=N totalWaitMs=W`). Document the cadence at guide lines 506-508. This stays within the human's "sparse count/wait diagnostics" requirement.

### Issue 4: Notebook says the model-deployment mapping is "for analyzer creation", but it is only sent with analysis submissions
- **Severity**: Low
- **File**: docs/Explore Algorithms/AI Services/Quickstart - Semantic Extraction with Content Understanding.ipynb
- **Line(s)**: 34 (settings table), 191-207 and 258-259 (create-analyzer cell); related: ContentUnderstanding.scala 306-307 and 481-483
- **Description**: The settings table describes `CONTENT_UNDERSTANDING_MODEL_DEPLOYMENTS` as a mapping "for analyzer creation". The cell applies it with `analyzer.setModelDeployments(...)`. That parameter is documented as a per-request option (`ContentUnderstandingParams.scala` lines 68-69) and is serialized only into the analyze POST body (`ContentUnderstanding.scala` lines 306-307). `createAnalyzer` sends only `definitionJson` in the PUT (lines 481-483), and the notebook's definition has no `models` entry. The mapping therefore has no effect on creation and is used later by `submitter = analyzer.copy({})`.
- **Risk**: A user whose resource has no defaults may set this variable to fix creation or model resolution. Creation will behave the same as without it, and the user may misread why later analysis succeeds or fails. The documentation is inaccurate, but runtime behavior is correct.
- **Suggested Fix**: Reword the table entry, for example: "Optional JSON object mapping model names to deployment names, sent with each analysis submission as `modelDeployments`; it does not change analyzer creation or resource defaults. Blank uses resource defaults; recovery does not read this setting." Optionally say that the mapping is validated in the creation cell so it fails before any billable call.

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
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, resolved from the current task model catalog",
    "stageBehavior": "Six concurrent independent lenses; human requested review after the fixes",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head. Most volume is tests and documentation; production changes are localized to CU request options, throttling and time accounting, and shared Python parameter inspection.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, fallback-only backoff, elapsed-budget refunds, credential inspection and durable notebook recovery interact with existing state machines.",
  "rationale": "Six complementary lenses cover the localized production changes and cross-connector inspection implications. The user explicitly requested another review after fixing the recovered findings. Retain all earlier failed evidence; a transport failure cannot count as signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: when the fixes are in please start review after this. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 3,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The CLI JSON transport failure may recur. Offline evidence does not establish live CU compatibility or approval of matching packaged Python/JVM artifacts."
}

## Driver resolution update before stage attempt 4

Original failed transport remains failed; these are fixes to recovered findings.

- L6I1: documented the approved deterministic first fallback and base-at-cap
  cases rather than silently changing the selected fallback policy.
- L6I2: task IDs come from AsyncClient.currentTaskContext; concurrency-two logs
  now assert the actual owning partition ID.
- L6I3: minute-rate-limited partition reporting includes long-wait heartbeats
  and cumulative retry counts. Per-document completion logs were not added,
  avoiding the batch log-flood issue. This deliberate policy is documented.
- L6I4: notebook configuration now accurately says modelDeployments is sent
  with analysis submissions, not analyzer creation or resource-default updates.

Reporter cadence/heartbeat and async-log regressions passed with the complete
17-test throttling/policy rerun. Notebook schema, all seven cells, empty outputs,
diff hygiene, and sidebar syntax passed. See lens 1's consolidated driver update.
