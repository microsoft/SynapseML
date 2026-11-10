---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 2
round: 3
timestamp: "2026-11-09T23:37:07Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9ecf7587680141b7f115a9db098fdc63ae181d6ee618417405df8091d00039fc"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-19-stage-attempt-2-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 682 lines of the assigned frozen diff in the required ranges: [1,150], [151,300], [301,450], [451,600] and [601,682]. The diff covers `OpenAIAttachmentUtils.scala`, the new `AttachmentReadFailureFileSystem.scala` and `OpenAIPromptMultimodalRequestSuite.scala`.
- [x] Read the guidance and version files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml`.
- [x] Re-checked lines 25–104 of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`. The read path runs in this order:
  - pre-read check (94)
  - interruption-graph guard (96)
  - thread-flag guard (97)
  - task-cancel guard (98)
  - `CancellationException` mapped to `CancelledAttachmentReadException` (99)
  - sanitized fallback (100)
  - post-read check (102)

  `prepareFile` now reads through `Using.resource(...open(path))(IOUtils.toByteArray)`.
- [x] Compared the new read with the removed path in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` and `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`.
  - Both use the same `new Configuration()` and byte copy.
  - In the old `using`, a `close()` failure replaced the read failure.
  - `Using.resource` keeps the read failure, adds the close failure as suppressed, and puts an `InterruptedException` ahead of non-fatal errors. Interruption during cleanup is now visible (`read-close.png`, `close-interrupted-io.png`).
- [x] Checked how `createMessagesUDF` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` handles errors:
  - It rethrows `TaskKilledException`.
  - `InterruptedException` is not caught by `Try`, so it escapes.
  - Other non-fatal errors become the sanitized row-local message.
- [x] Traced every fixture case in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` through `isInterruptedRead`, `isTimeoutFailure` and `isTimeout`: `TimeoutNames`, `ClearedInterruptNames`, `InterruptedNames`, `ordinary.png`, `flagged.png` and `cancelled.png`. Each result matches the assertions in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` at lines 411–442, 554–572 and 574–592.
- [x] Checked error-graph failure modes:
  - The identity-set guards and `@tailrec` traversal stop on cyclic cause/suppressed graphs without stack overflow (444–462). `cyclicInterruption` limits `getCause` reads.
  - The separate `visited` and `timeoutCauses` sets mean each node is expanded at most twice.
  - Fatal errors skip the traversal and propagate as the same object (LinkageError test, 464–474).
  - Thread-flag checks before and after IO are tested (476–494).
- [x] Checked concurrency and test cleanup:
  - The cancellation tests (496–552) put a timeout on every wait.
  - A closer releases the gate, cancels the job group and calls `shutdownNow` on the submitting executor; the `Active` entry is removed in `finally`.
  - The row-local Spark tests assert exactly one open (568), so task retries cannot hide failures.
- [x] Checked compatibility:
  - No new dependencies: httpclient 4/5 are already on the classpath per `build.sbt`, and `scala.util.Using` is already used by `OpenAIPrompt`.
  - The fixture compiles on JDK 8 because it uses the test-local `RelocatedHttpClient.HttpTimeoutException`.
  - No public JVM signature or serialized Param changes; every touched member is `private[openai]`.
- [ ] Not checked: I did not run any tests (read-only review). The JDK 8 and JDK 11 compile, Test/compile, 89-test and scalastyle results come from the caller and were not re-run.
- [ ] Not counted as findings:
  - HTTP(S) URL attachment reads still cannot be interrupted. This predates the PR and is out of scope.
  - No test targets the pre-IO `taskCancelled` path (line 94 → 39). The same `checkCancellation()` runs after the read in the `interrupt=false, read=true` cancellation tests.

## Issues

### Issue 1: Timeouts thrown as plain `InterruptedIOException` are treated as interruptions
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 49–54 (rule at 52) and 95–101 (catch order). Related fixture case: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` lines 184–186.
- **Description**:
  - `isInterruption` treats any `InterruptedIOException` as an interruption unless its cause chain contains a class whose name ends in `TimeoutException`.
  - `isInterruptedRead` is the first catch guard (line 96), so this check runs before the thread-flag, task-cancel and sanitized fallback guards. It applies even when neither the thread nor the Spark task was interrupted.
  - Some clients report timeouts as a plain `InterruptedIOException` with no timeout-named cause:
    - OkHttp `callTimeout` throws `InterruptedIOException("timeout")`, typically caused by `IOException("Canceled")` or `SocketException("Socket closed")`.
    - Okio `Timeout.throwIfReached` throws `InterruptedIOException("deadline reached")`.
  - At the merge base, these errors fell through to `case NonFatal(_) => AttachmentReadException`, a row-local error. Now `interruptedRead()` sets the interrupt flag and throws `InterruptedException`.
  - The fixture's `interrupted-io.png` case (184–186) encodes this rule. It and every other plain `InterruptedIOException` case run only on the test thread, with no `TaskContext` (suite 411–442 and 574–592). No test inside a Spark task covers a timeout thrown this way.
- **Risk**:
  - With a connector that reports timeouts this way, one slow attachment fails the whole Spark task instead of producing a row error.
  - The escaping `InterruptedException` is reported as an ordinary task failure because no kill reason is set. Spark retries the task, which can re-send model requests already completed for other rows in the partition. A timeout that keeps recurring aborts the job.
  - The interrupt flag set by `interruptedRead()` also makes task cleanup and completion listeners run with the thread interrupted.
  - This conflicts with the PR's goal of keeping ordinary timeouts sanitized and row-local.
  - Likelihood is low. ABFS, WASB, S3A, GCS and HDFS throw timeout exception types or keep a timeout-named cause, which the cause walk already handles.
- **Suggested Fix**:
  - Inside a Spark task, treat a plain `InterruptedIOException` as an interruption only if the thread flag is set or the task is marked cancelled. Keep the current rule when there is no `TaskContext`. Spark's `Task.kill` calls `markInterrupted` before `taskThread.interrupt()`, so real Spark cancellation is still detected even after a connector clears the flag.
  - Sketch: `case io: InterruptedIOException => !isTimeoutFailure(io) && (TaskContext.get() == null || taskCancelled || Thread.currentThread().isInterrupted)`.
  - This leaves every existing scenario unchanged. The cleared-flag cases run without a `TaskContext`, and the in-task cases use typed timeouts or `InterruptedException`.
  - Add one row-local Spark test that throws `InterruptedIOException("timeout")` caused by a `SocketException`, and assert the sanitized error and a single open.
  - If the current rule is intentional, at least document in the `isInterruption` comment and the PR description that these timeouts fail the task rather than the row.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Accepted
- **What changed**: The PR description now explicitly documents the ambiguity, task failure and possible retries/repeated sends. Its row-local timeout guarantee is restricted to recognizable timeout types or causes. No product behavior changed.
- **Why**: The limitation is real. Presented with both policies and their tradeoffs, the user chose: "Preserve interruption semantics and explicitly document the timeout limitation." The existing source comment already states that plain `InterruptedIOException` signals interruption in Spark. Guessing from exception messages or requiring an observable task/thread signal could swallow a real interruption after a connector clears the flag. This is an explicitly accepted limitation, not a false finding or a claimed fix.
- **How verified**: Exact source-patch and production-file hashes still match the 89-case, zero-skip JDK 8 qualification. The template-preserving description update was published and read back with unchanged PR head, base and draft status. The original reviewer verdict remains `issues_found`; the review ledger records `qualified_with_user_accepted_limitation`, not a clean rebuttal. No new test execution is claimed for this documentation-only resolution.