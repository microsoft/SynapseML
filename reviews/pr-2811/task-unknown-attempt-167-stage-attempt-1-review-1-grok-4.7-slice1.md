---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 167
stage_attempt: 1
round: 1
timestamp: "2026-11-06T11:29:50Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "753030fa273a6e46ed48640bf5440a53ca05323dc435b38f0dd34bb47601a17b"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2811/task-unknown-attempt-167-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Slice 1/24 covered from the assigned frozen diff, lines 1-881, in bounded reads of at most 150 lines. All four files are additions; before is empty and after is the full diff.
- [x] `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` reviewed for check, submit, await, cancel, close, and fill, including kill-reason and classloader behavior.
- [x] `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala`, and `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` reviewed against that lifecycle.
- [x] Cross-file contracts checked in `HTTPClients.scala` (`sendCancellableRequestsWithContext`, `withRequest`, bounded retry checks), `HTTPTransformer.scala` (`transformRequests`), the unified prompting cancellation and kill-reason sections, and `OpenAIAttachmentUtils.scala` use of `TaskContextUtils.getKillReason`.
- [ ] Tests were not executed; this pass is read-only and does not treat claimed prior runs as new proof.

## Issues

### Issue 1: Pool shutdown can surface as RejectedExecutionException
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala
- **Line(s)**: 102-113, 138, 227-232
- **Description**: `submit` calls `checkTask()` and then `pool.submit` with no lock held across both steps. `cancel` sets `cancelled` and calls `pool.shutdownNow()` on the monitor thread. The fixed pool uses an unbounded queue, so `submit` rejects only after that shutdown. If the caller is descheduled after `checkTask()` returns, `pool.submit` throws `RejectedExecutionException`. `interruptOnCancel=false` does not interrupt the caller, and neither `submit` nor `fill` maps that rejection back to `TaskKilledException` or `CancellationException`. A cancellation during `input.next()` is still caught by the next `checkTask()`; the hole is the check-then-submit gap, which `fill` hits while prefetching.
- **Risk**: A killed task can fail as a normal task error. Spark may retry it and resend HTTP requests the cancellation was supposed to stop, and the recorded kill reason is lost. Driver close during the same gap becomes `RejectedExecutionException` instead of `CancellationException`.
- **Suggested Fix**: Catch `RejectedExecutionException` in `submit`, call `checkTask()`, and if the scope is cancelled or the pool is shut down throw that cancellation exception rather than the rejection. Add a regression that shuts the scope down between the check and `submit`.

### Issue 2: Expired wait reports timeout without rechecking cancellation
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala
- **Line(s)**: 116-125
- **Description**: `await` calls `checkTask()` before `future.get`, but the `remaining <= 0` timeout branch throws `TimeoutException` immediately. The `ExecutionException` branch calls `checkTask()` before propagating the worker failure. A task-only kill during that final wait, up to `PollNanos` (100ms), is therefore reported as a timeout.
- **Risk**: A cancellation that overlaps timeout expiry becomes a retriable timeout instead of `TaskKilledException` with Spark's recorded reason. The guide treats those as different outcomes, and only the timeout is documented as a retryable task failure.
- **Suggested Fix**: Call `checkTask()` before throwing the timeout, matching the `ExecutionException` path, and cover a kill that arrives during the final poll with a regression.

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

## Driver audit and adjudication

The original response above is preserved. The bounded-view audit confirmed all
881 assigned diff lines were read. However, the checkout-wide
`**/*.{scala,java,md}` search returned the path of
`reviews/pr-2811/task-unknown-attempt-165-stage-attempt-1-review-1-claude-opus-5.5-slice1.md`.
That violates the explicit prior-feedback search exclusion. This result is not
an independent qualifying review, regardless of the findings below.

### Issue 1: Confirmed diagnostic race; corrected

The driver reproduced a `RejectedExecutionException` from the production
`sendCancellableRequestsWithContext` iterator when task-only cancellation shut
down its pool during worker construction. The new regression pauses that
construction with an inheritable-thread-local callback and uses a second watch
on the shared monitor to observe completion of the first scope's shutdown.
It does not replace the executor, access private fields, or issue HTTP requests.

`Scope.submit` now catches only `RejectedExecutionException`, rechecks the task
and scope cancellation state, and otherwise rethrows the original rejection.
The regression checks the recorded `TaskKilledException` reason, zero handler
calls, and the caller's unchanged interrupt flag. All iterator/context cleanup
runs in `finally`. The unified prompting guide documents this cancellation race.

The broader claim that Spark necessarily reports this as an ordinary retryable
task failure is not established. Spark 3.5.0's
[`Executor.TaskRunner.run`](https://github.com/apache/spark/blob/v3.5.0/core/src/main/scala/org/apache/spark/executor/Executor.scala)
classifies `InterruptedException` or any `NonFatal` failure as `TaskKilled`
when the task has a recorded kill reason. The demonstrated defect and fix concern
the exception emitted by the HTTP iterator, not a proven additional Spark retry.

Before the fix, on Temurin Java 8:

```text
core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyCancellableRequests -- -z "worker submission"
FAILED: 1/1; expected TaskKilledException, received RejectedExecutionException.
```

After the fix, on the same baseline:

```text
core/compile
core/Test/compile
cognitive/compile
cognitive/Test/compile
PASSED.

core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyCancellableRequests
PASSED: 12/12.

core/testOnly com.microsoft.azure.synapse.ml.io.split1.VerifyBoundedRequests -- -z "minimum concurrent timeout"
PASSED: 2/2.

cognitive/testOnly com.microsoft.azure.synapse.ml.services.ai.AIPromptCancellationSuite com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite com.microsoft.azure.synapse.ml.services.ai.AIPromptAttachmentSuite -- -z "cancellation" -z "CancellationException"
PASSED: 11/11 across three suites.

core/scalastyle
core/Test/scalastyle
cognitive/scalastyle
cognitive/Test/scalastyle
PASSED: zero errors and warnings.
```

No selected tests were skipped. Port compatibility validation is recorded
separately after completion; these local results are not current-head CI.

### Issue 2: Not accepted as reported

The reported final-wait control flow is incorrect. In `Scope.await`, a positive
`waitNanos` implies `remaining > 0`. Its `TimeoutException` takes the `None` path
and recurses through `checkTask()` before another wait or timeout decision.
The branch that throws the concurrent-timeout exception has `remaining <= 0`,
so its `future.get` uses a zero wait, not a final wait of up to 100 ms.

There is still an ordinary race after the last cancellation observation, but
this does not establish the reported missed positive-wait poll. The claim that
a recorded Spark task kill becomes a retryable task failure is also contradicted
by the executor's recorded-kill fallback described above. No timeout behavior
was changed. Both existing minimum-timeout controls passed: a buffered result
remains available, while pending work times out and is drained.

This submission fix consumes attempt 167; none of this attempt's results
qualifies the corrected frozen patch.

### Completed port compatibility replay

The two changed Scala files matched the preceding frozen source in both isolated
port checkouts before replay, and matched the corrected source after validation.
On Java 17, both Spark 4.0.1/Scala 2.13.16 and Spark 4.1.1/Scala 2.13.17 passed
the exact compile, test and style commands listed above: four compile gates,
12/12 cancellation tests, 2/2 minimum-timeout controls, 11/11 cognitive
cancellation tests, and four style gates with zero errors or warnings.
No tests were skipped. These are focused compatibility replays, not full port CI.