---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 3
round: 3
timestamp: "2026-11-10T21:05:34Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "e98f9f0d82be97923148424014395517487b16baeb1495d0936768a85c7247e8"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 42247 (char 42246)
Invalid CLI event on line 421: Expecting ',' delimiter: line 1 column 7323 (char 7322)

## Original Response

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-3-review-3-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Retry predicate**, `ContentUnderstandingProtocol.scala` 326–346. Only HTTP 429 is retried, and only for an analysis GET or for a POST with no `Operation-Location` (`admissionRejected`).
  - `analysisRetry` is set only for analyze/submit POSTs (line 638) and analysis polls (lines 612–613, `analyzer.isEmpty`). Management PUT/GET (`getAnalyzerResponse` ~703) keeps the default `false`.
  - `ContentUnderstandingThrottlingSuite.scala` 101–128 shows these are never replayed: a contradictory or invalid `Operation-Location`, 5xx, 408 or a disconnect.
  - The same lines show a definitive 429 with an unreadable body is retried.
- [x] **Budget refund**, `ContentUnderstandingBudget.scala` 14–47. `refundSince` moves `started` forward by the throttled interval only.
  - `started ≤ now` holds, so elapsed time is never negative. There is one writer per budget (the request thread), so the `@volatile` non-atomic `+=` is safe.
  - `pause(Long.MaxValue)` cannot overflow (`delay - elapsed`) and checks cancellation every 50 ms.
  - The PolicySuite refund test (~44–60) confirms earlier time stays charged and the final attempt is charged.
- [x] **Backoff arithmetic**, `ContentUnderstandingThrottling.scala` 14–31. `base ≤ Int.MaxValue`, `cap * 2` fits in a Long, and the `nextLong(lower, current + 1)` bounds are always valid. A valid header resets `next`, and there is a new `Backoff` per `exchange`.
- [x] **Cancellation.** `checkInterrupted` (Protocol 269–276) uses `AsyncClient.isTaskCancelled` (`Clients.scala` 53–57), plus the thread interrupt flag.
  - Thread interruption is covered by `ThrottlingSuite` 151–190.
  - Spark job-group cancellation is exercised by `test_ContentUnderstanding.py` 311–418; see the limitation in Issue 5.
- [x] **Unbounded async wait.** `concurrentTimeout` is rejected, and `AsyncClient` awaits futures with an infinite wait (`Clients.scala` 82–91). Unlimited throttling therefore cannot cause a Spark-side timeout, task retry or POST replay.
- [x] **`blocking` placement.** It appears only in CU code (`Budget.pause`, `exchange`), and the shared `Clients.scala` HTTP implementation is unchanged. On Spark task threads it is a no-op, and the calls are sequential, not nested.
- [x] **`allowInputTruncation`:**
  - Main stage: `ContentUnderstanding.scala` 86–89, 168–190 and 317–322. Trait: `ContentUnderstandingParams.scala` (`HasAllowInputTruncation`).
  - `ContentUnderstandingOptionsSuite.scala` covers: unset → omitted on GA and preview; explicit false → `allowInputTruncation=false`, never in the body; null column rows omitted; non-boolean column rejected; poll mode ignores the column.
  - Python round-trips explicit `False` through copy and persistence (`test_ContentUnderstanding.py` 172–230).
- [x] **Python inspection removal.** `Utils.py` transfers values to and from the JVM through `_paramMap` directly, so removing the generic overrides keeps typed getters, copy and persistence working.
  - The real MLflow autolog test (~250–310) shows credentials are absent from logged params.
  - `docs/Use with MLFlow/Autologging.md` 12–23 correctly separates parameter logging from credential-bearing model artifacts and generic `set`.
- [x] **Guide snippets** (`Content Understanding.md` 56–100) write with `errorifexists` and re-read Parquet. `test_ContentUnderstanding.py` 467–531 shows exactly 2 POSTs, no requests during extraction/display, and an `AnalysisException` on rerun with no new requests.
- [x] **Notebook recovery** (`test_ContentUnderstanding.py` 532–720):
  - Invalid run IDs and mappings are rejected before any call.
  - An origin mismatch is rejected before polling, and no replacement snapshot is written.
  - Recovery works with `invalid-but-unused` model deployments, and there is a single POST overall.
- [x] **Harness and sidebar.** Databricks and Synapse exclusions are correct (the Synapse name is sanitized), Fabric uses an allowlist, and the `website/sidebars.js` 38–39 entries follow the existing pattern.
- [x] **Throttle log content** (`ContentUnderstandingLogging.scala` 25–30; `ThrottlingSuite` 79–98) is limited to the method, retry count, wait and task IDs. It excludes the URL, key and body.
- [ ] **Execution:** no builds or tests were run, because the plan is read-only. I inspected the driver-supplied validation evidence (188/189 plus a 38/38 rerun, Python 96/241, Black clean) but did not reproduce it.

## Issues

### Issue 1: Fallback backoff has no jitter on the first retry, and none at all when pollingDelay ≥ 60000
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala (also ContentUnderstandingThrottlingPolicySuite.scala, Content Understanding.md)
- **Line(s)**: 14–31 (specifically 25–28); PolicySuite 16–26; guide 391–394
- **Description**: `lower = max(base, current - current/10)` followed by `nextLong(lower, current + 1)`.
  - On the first fallback, `current == base`, so the delay is always exactly `base` (1000 ms by default).
  - If `base == cap` (pollingDelay ≥ 60000), every fallback delay is exactly `cap`, with no jitter ever.
  - The tests lock this in: `delay(None) == 1000L`, and a constant `120000L`.
- **Risk**: The human required "exponential fallback with jitter". When many rows, partitions or executors get 429s with no usable `Retry-After` at the same moment (for example, gateway-generated 429s), they all retry in lockstep exactly 1 s later. That re-creates the burst. With a large pollingDelay they stay synchronized indefinitely.
- **Suggested Fix**: Jitter around the current step, clamped to the floor and cap. For example, `lower = max(MinimumFallbackMs, current - current/10)` and `upper = min(cap, current + current/10)`. This gives [1000, 1100] at the first step, [54000, 60000] at the 60 s cap, and [108000, 120000] when `base == cap`. Update the PolicySuite assertions and the "downward jitter" wording in the guide. The ThrottlingSuite `>= 1000` assertion stays valid.

### Issue 2: Throttle warnings report `taskContext=unavailable` for async rows even though the owning task is known
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingLogging.scala
- **Line(s)**: 25–30 (related: Protocol 146; Clients.scala 53–54, 82–91; guide 502–508)
- **Description**: `throttled` reads `TaskContext.get()`. That is null on the `ExecutionContext.global` workers used when `concurrency > 1`. `AsyncClient.currentTaskContext` (`private[ml]`, reachable from this package) returns the owning task, which `withTaskContext` propagates. The protocol already uses it at line 146. The guide's claim that async workers "lack Spark task context" is therefore inaccurate.
- **Risk**: Concurrency > 1 is the usual high-throughput setup. In that setup, throttle warnings cannot be tied to a stage, partition or task attempt, which makes sustained-throttling incidents harder to diagnose. There is no correctness impact.
- **Suggested Fix**: Use `AsyncClient.currentTaskContext` in `throttled` (and optionally in `sharedStop`). Add a concurrency-2 log assertion and correct the guide sentence.

### Issue 3: Unlimited POST throttle retries ignore the partition shared-stop flag
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; ContentUnderstandingThrottling.scala
- **Line(s)**: Protocol 666–694 (`stopped` checked only at 674, before `execute`), 638; Throttling 33–58
- **Description**: With concurrency > 1, a sibling row can set `stopped` while another row is in the opt-in 429 loop. Typical triggers are AnalyzerNotFound, DeploymentNotFound and scalar-auth failures.
  - Every loop iteration after a definitive 429 is a new admission attempt. The loop never checks `stopped`, so that row keeps POSTing with no count or time limit.
  - The partition has already logged "new submissions stopped in this partition".
- **Risk**: After a known-fatal configuration failure, up to `concurrency - 1` rows keep loading an already-throttled service. They keep the Spark task alive until throttling clears, which contradicts the documented shared-stop behavior. Duplicates are not possible, because every replayed POST was definitively rejected.
- **Suggested Fix**: Pass a stop predicate (for example `() => stopped.get()`) from `rowHandler` through `execute`/`exchange` into `retry`. Check it before each POST retry only, so GET polls of accepted handles continue. When it is set, return the last definitive 429 as `Rejected` with the `SharedConfigurationFailure` diagnostic. Add a concurrency-2 test: row A gets 404 AnalyzerNotFound, row B gets repeated `429 Retry-After: 0`, and B must stop POSTing.

### Issue 4: Throttle warnings are sparse per exchange, not per task, so sustained throttling can flood logs
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: Throttling 39–56 (new `Backoff` and `loop(1L)` per call; log at 45–47); Protocol 611–613
- **Description**: The power-of-two counter restarts with every `exchange`. Each row's POST and each separate poll GET starts at count 1, so every throttled exchange logs at least one WARN. In analyze mode under sustained GET throttling, that is roughly one warning per poll per row. `ThrottlingSuite` 79–98 only checks sparsity within a single exchange.
- **Risk**: Large batches (for example 100k documents with tens of polls each) can produce hundreds of thousands to millions of WARN lines during exactly the incident this feature targets. That costs log storage and ingestion and can rotate out other diagnostics.
- **Suggested Fix**: Aggregate per task or executor. For example, share a per-`rowHandler` `AtomicLong` across rows and polls and log at powers of two of the cumulative count. Alternatively, rate-limit to one summary line (cumulative count, maximum wait) per N seconds. Add a multi-row/multi-poll test with bounded warnings.

### Issue 5: The Python throttled-cancellation subtest cannot detect a throttle wait that ignores cancellation
- **Severity**: Low
- **File**: cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py
- **Line(s)**: 330–333 (`retry_after="60"`), 390–407
- **Description**: The "throttled" scenario uses `Retry-After: 60`, but the "no further requests" window after cancellation is only 0.5 + 0.75 s. The driver-side "cancel" error comes from DAGScheduler job cancellation, whatever the executors do. So with `interruptOnCancel=False`, a regression in which the throttle wait ignores `TaskContext.isInterrupted` would still pass. An example is replacing `budget.pause` with `Thread.sleep` in `retry`. The Scala test (`ThrottlingSuite` 151–190) covers only thread interruption. The current code probably works, because it shares `pause` with the polling scenario, which is a meaningful check.
- **Risk**: The "support Spark cancellation" requirement for throttle waits has no regression guard. Leaked executor-side waits and late requests would go unnoticed.
- **Suggested Fix**: Use a short wait, such as `Retry-After: 0`/`1`, or no header so the 1 s fallback applies. Make the observation window longer than that wait (≥ 2.5 s). Alternatively, add a Scala test that marks the owner `TaskContext` interrupted during a throttled `pause` and checks for prompt termination with no further requests.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

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

- L3I1: documented deterministic first fallback and base-at-cap cases while
  retaining the user-approved floor/downward-jitter behavior.
- L3I2: async diagnostics resolve the owning Spark TaskContext; the public
  concurrency-two log regression requires a real partition identifier.
- L3I3: partition Context reaches every exchange. Shared-stop interrupts
  rejected POST retry waits and returns the last rejection with a shared-stop
  diagnostic. Accepted GET polling ignores this submission-only stop signal.
- L3I4: a synchronized partition reporter replaces per-exchange warning bursts,
  emits first/at-most-once-per-minute warnings, and supports long-wait ticks.

Throttling and policy suites passed 17/17, including shared-stop, accepted GET,
reporter cadence and heartbeat cases. The complete CU case coverage is 194
across the broad run and this corrected rerun; compile/style/codegen passed.
See lens 1's driver update for remaining validation and formal transport status.
