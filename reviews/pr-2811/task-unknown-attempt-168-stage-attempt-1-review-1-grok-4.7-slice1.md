---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 168
stage_attempt: 1
round: 1
timestamp: "2026-11-06T12:07:09Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "77a2aafe10abdfffb92389c64ab0000afe77db7bea94917f038e1a8b0ee2c9c8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2811/task-unknown-attempt-168-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Slice 1/24 assigned frozen diff read in full (lines 1–150, 151–300, 301–450, 451–600, 601–750, 751–900, 901–938); before/after coverage is a new-file addition for all four paths.
- [x] Reviewed `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` (scope lifecycle, submit rejection, await, abort, drain, iterator) against the cancellation guide in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`.
- [x] Reviewed `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala` and confirmed `getKillReason()` is the same public `TaskContext` accessor already used by `core/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/HTTPSinkV2.scala`.
- [x] Reviewed `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala` and `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` for classloader, submission-race, drain, shared-monitor, and kill-reason assertions.
- [x] Checked cross-file contracts in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala` (`sendCancellableRequestsWithContext`, `withRequest`) and `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala` (per-partition iterator plus task-completion close).
- [ ] Tests were not executed in this read-only review.

## Issues

### Issue 1: Expired concurrent-timeout wait drops a task kill
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala
- **Line(s)**: 120-129
- **Description**: `await` calls `checkTask()` and then, when `future.get` times out with `remaining <= 0`, throws `TimeoutException` without checking again. The `ExecutionException` branch immediately below does recheck. A task kill or scope cancellation that becomes visible after that pre-wait check and before the throw is reported as a concurrent timeout. For a still-positive remaining wait the recursive call rechecks, so this gap is only the already-expired branch (including the allowed 1ns timeout, where that branch is the normal final poll).
- **Risk**: Spark treats `TimeoutException` as a task failure and can retry the partition, resending provider calls after an intentional cancel. `TaskKilledException` would preserve the recorded kill reason and avoid that retry classification. The window is the gap after line 121’s `checkTask()` during the zero-wait `get`, so it is timing-dependent rather than the common cancel path.
- **Suggested Fix**: Call `checkTask()` before throwing the timeout exception so a recorded kill or scope cancellation wins, matching the `ExecutionException` path. Keep the existing timeout exception only when that recheck does not throw.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver audit and resolution

The original response above is preserved. All 938 assigned frozen diff lines
were read. Five searches, however, omitted the explicit product-subdirectory
paths required for this attempt. They used Scala-only patterns or the exact
`build.sbt` filename and did not reveal prior feedback, but they fail the
recorded stricter scope contract. This is not a qualifying independent result.

### Confirmed iterator diagnostic race; corrected

The driver independently reproduced the narrowed zero-wait case through
`sendCancellableRequestsWithContext`. A controlled task context publishes its
kill immediately after the initial interruption read, returning the value
observed before the kill. The shared monitor is held at a bounded latch, and a
blocked request is awaited with the supported one-nanosecond timeout. The
regression therefore exercises the real expired `future.get` path rather than
replacing a future, executor, or production cancellation method.

Before the fix, on Temurin Java 8:

```text
core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyCancellableRequests -- -z "expired HTTP waits"
FAILED: 1/1; expected TaskKilledException, received TimeoutException.
```

`Scope.await` now calls `checkTask()` immediately before reporting an expired
result wait. The ordinary timeout exception remains unchanged when cancellation
is not observed. The regression checks the recorded kill reason and the caller's
unchanged interrupt flag; cleanup releases the monitor and worker and joins the
worker in `finally`. The guide describes this additional cancellation check.

This confirms an iterator-level diagnostic race, not the report's claimed extra
Spark retry. Spark 3.5.0's
[`Executor.TaskRunner.run`](https://github.com/apache/spark/blob/v3.5.0/core/src/main/scala/org/apache/spark/executor/Executor.scala)
classifies a `NonFatal` failure as `TaskKilled` when the task has a recorded kill
reason. The earlier claim of a missed positive wait of up to 100 ms was also
incorrect: positive-wait timeouts recurse through `checkTask`. This narrower
zero-wait finding supplied the distinct boundary now covered by the regression.

The complete after-fix validation and port replay results are recorded below
when finished. This source fix consumes attempt 168; none of its results
qualifies the corrected frozen patch.

### Primary after-fix validation

On Temurin Java 8:

```text
core/compile
core/Test/compile
cognitive/compile
cognitive/Test/compile
PASSED.

core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyCancellableRequests
PASSED: 13/13, including the new expired-wait regression.

core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyBoundedRequests -- -z "minimum concurrent timeout"
PASSED: 2/2; buffered results and ordinary pending-work timeouts retain their behavior.

cognitive/testOnly com.microsoft.azure.synapse.ml.services.ai.AIPromptCancellationSuite com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite com.microsoft.azure.synapse.ml.services.ai.AIPromptAttachmentSuite -- -z "cancellation" -z "CancellationException"
PASSED: 11/11 across three suites.

core/scalastyle
core/Test/scalastyle
cognitive/scalastyle
cognitive/Test/scalastyle
PASSED: zero errors and warnings.
```

No selected tests were skipped. These local results are not current-head CI.

### Completed port compatibility replay

The two changed Scala files matched the preceding frozen source in both isolated
port checkouts before replay, and matched the corrected source after validation.
On Java 17, both Spark 4.0.1/Scala 2.13.16 and Spark 4.1.1/Scala 2.13.17 passed
the exact compile, test and style commands listed above: four compile gates,
13/13 cancellation tests, 2/2 minimum-timeout controls, 11/11 cognitive
cancellation tests, and four style gates with zero errors or warnings.
No tests were skipped. These are focused compatibility replays, not full port CI.