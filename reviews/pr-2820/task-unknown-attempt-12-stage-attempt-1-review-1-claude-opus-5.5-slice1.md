---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 1
timestamp: "2026-11-09T05:24:30Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bb7f1bd7fa249aca5e3a3c493a3accac1f6666ceea8213103e951dcefa3b934f"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-12-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 606 lines of the assigned frozen diff in the required ranges: [1,150], [151,300], [301,450], [451,600] and [601,606].
- [x] Read the required guidance files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. The target is master. `build.sbt` gives Spark 3.5.0 and Scala 2.12.17, and `environment.yml` gives pyspark 3.5.0.
- [x] Checked that `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (lines 1–408) matches the diff. Traced `interruptedRead` (28–31), `checkCancellation` (37–40), `isTimeout` (42–50), `isInterruption` (52–56), `isInterruptedRead` (58–77), `readAttachment` (79–90) and the `prepareFile` read block (299–305).
- [x] Checked how the caller handles these errors in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `createMessagesUDF` (about lines 485–501). Its `Try` only catches non-fatal errors, so `InterruptedException` escapes the UDF. It rethrows `TaskKilledException`, and every other error becomes a row-local `e.getMessage`.
- [x] Compared the old and new read helpers: `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` and `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`. In the old `using`, a close failure replaced the original read failure, and could even replace a fatal error. `Using.resource` keeps the original and attaches the close failure as a suppressed exception. A close failure after a successful read still propagates in both versions. The switch is justified.
- [x] Confirmed the old behaviour from the removed diff lines. A plain `InterruptedIOException`, with neither the thread nor the Spark task interrupted, became `AttachmentReadException` and stayed row-local.
- [x] Checked fatal-error handling. `LinkageError` and `VirtualMachineError` are rethrown unchanged. `Using`'s choice of which exception to keep favours fatal errors over `InterruptedException`, and both over ordinary errors.
- [x] Checked sanitization. Errors that escape have fixed messages and no cause, and row-local errors expose only the file name.
- [x] Checked API, Params and dependencies. `OpenAIAttachmentUtils` is `private[openai]`, and no public signature or Param changed. The test-only classes from httpclient 4.5.13 (via httpmime) and httpclient5 5.1.3 are already in `build.sbt`.
- [x] Reviewed the test fixtures and suite in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` and `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`. Cleanup goes through `Using`, the fixture filesystem is injected through Hadoop's filesystem cache, loop-generated test names are unique, and an unknown-scenario guard catches mistyped file names. The timeout fixtures only wrap timeouts in a generic `IOException` (fixture line 95).
- [x] Noted the caller's local evidence: compile, Test/compile, 69 tests with 0 skips, and scalastyle for production and test code, on JDK 11. I did not run any of it in this read-only review.
- [ ] Hadoop S3A source not opened: it is outside the review roots and the read profile. The S3A trigger in Issue 1 relies on known upstream connector behaviour. The classification flaw itself is confirmed from the diff.

## Issues

### Issue 1: Storage timeouts wrapped in a plain `InterruptedIOException` are treated as thread interruptions
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala (test gap in cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala)
- **Line(s)**: 52–56 (especially 54), 64–73 (line 68 returns before the timeout rule at 70–71 is reached), 82. Fixture gap at lines 70–95 of the test file.
- **Description**: `isInterruption` treats any `InterruptedIOException` as an interruption unless its class or a superclass has a name ending in `TimeoutException`. `inspect` stops at that exception (line 68) without looking at its causes. The rule "a timeout owns its cause classification" (lines 70–71) only works when the timeout is the outer exception. The reverse case is a plain `InterruptedIOException` whose cause holds the timeout, and the code calls that an interruption.

  Storage connectors produce exactly this shape. Hadoop S3A (the 3.3.x line used with Spark 3.5 and AWS SDK v1) converts errors in `S3AUtils.translateInterruptedException`. Only `SocketTimeoutException` and inner causes named `*.ConnectTimeoutException` become timeout types. Any other inner `InterruptedIOException` becomes `new InterruptedIOException(message)`, with the SDK exception as its cause. A common example is the relocated `ConnectionPoolTimeoutException` behind "Timeout waiting for connection from pool". The PR's name-based relocation check never sees that class, because the outer plain `InterruptedIOException` short-circuits first.
- **Risk**: In a task that was not cancelled, line 82 calls `interruptedRead()`. That sets the executor thread's interrupt flag even though nothing interrupted it, and throws `InterruptedException`. `OpenAIPrompt.createMessagesUDF` cannot catch it, so the task attempt fails. The job then aborts after `spark.task.maxFailures` attempts, or immediately in local mode.

  Before this PR, the same failure became the sanitized row-local error `Unable to read attachment '<name>'`. That breaks the PR's stated goal of keeping ordinary timeouts sanitized and row-local, on a widely used connector. Connection-pool exhaustion usually hits many rows at once, so a short-lived, per-row problem becomes stage retries or a failed job. The existing relocated and pool-timeout fixtures only wrap timeouts in a generic `IOException` (fixture line 95), so the tests cannot catch this.
- **Suggested Fix**: Only treat a non-timeout `InterruptedIOException` as an interruption when no timeout sits beneath it.
  - In `inspect`, when the current exception is a plain `InterruptedIOException`, walk its cause chain with the same cycle-safe identity set.
  - If a timeout appears before any `InterruptedException` or `ClosedByInterruptException`, classify the failure as an ordinary timeout.
  - Keep the cause-less `interrupted-io.png` case as an interruption.

  Add fixtures for both sides:
  - Add a plain `InterruptedIOException` caused by `IOException(RelocatedHttpClient.ConnectionPoolTimeoutException)` to `TimeoutNames`. It should be ordinary in the unit loop and row-local in the end-to-end loop.
  - Add a plain `InterruptedIOException` caused by `InterruptedException`, which should still be an interruption.

  Optional extra safety: when `TaskContext.get() != null`, only treat an ambiguous plain `InterruptedIOException` as an interruption if the task is cancelled or the thread is still interrupted. Spark marks the TaskContext before it interrupts the task thread, so real Spark cancellation still propagates.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Plain interrupted-IO failures now inspect their cause chain for a timeout before being classified as thread interruption. The cycle-safe traversal stops at a definite `InterruptedException` or `ClosedByInterruptException`; cause-less interrupted IO still propagates. The existing graph traversal continues to inspect independent suppressed cleanup failures.
- **Why**: Primary inspection of Hadoop 3.3.4 `S3AUtils.translateInterruptedException` confirmed the connector shape described in the review. The added translated pool-timeout fixture independently reproduced the regression through public `prepareFile`: it threw `InterruptedException` where the existing row-local attachment error was required.
- **How verified**: The new public-path regression failed before the production fix and passed afterward. `cognitive/compile`, `cognitive/Test/compile`, all 73 `OpenAIPromptMultimodalRequestSuite` cases (zero skips), `cognitive/scalastyle` and `cognitive/Test/scalastyle` passed. New cases verify sanitized row-local pool timeouts through both Chat Completions and Responses, plus translated definite interruptions after storage clears the thread flag. Existing timeout, cleanup, cyclic-graph, task cancellation and fatal-error cases remain passing.