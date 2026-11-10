---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 2
timestamp: "2026-11-09T08:09:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2fc2cef1768a704a7a2550ee7c9b6ffb9e63ebb9c556f523e9a882d4d517e305"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-16-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 666 lines of the assigned frozen diff in the required chunks: [1,150], [151,300], [301,450], [451,600] and [601,666]. Re-read [301,450], [451,600] and [601,666] to confirm the four suite hunk headers and the interrupted-name lists (diff lines 456-460 and 647-650).
- [x] Read the guidance files: AGENTS.md, .github/skills/synapseml-branches/SKILL.md, .github/skills/synapseml-branches/references/branch-spark3p5.md, .github/skills/code-review/SKILL.md, and .github/skills/synapseml-local-setup/SKILL.md (JDK 11). Took versions from build.sbt and environment.yml: Spark 3.5.0 and Scala 2.12.17.
- [x] Reviewed the production design in cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala, lines 1-110 and 296-330:
  - cancellation checks before and after the read (lines 94 and 102)
  - order of the catch cases (lines 96-100)
  - fatal errors are checked only at the top level (line 71), so `LinkageError` still propagates unchanged
  - the cause/suppressed walk is tail-recursive and cycle-safe (lines 56-91)
  - the `Using.resource` read (lines 315-319)
- [x] Checked the cross-file contract in cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala:
  - The UDF's `Try` only catches non-fatal errors, so `InterruptedException` escapes it.
  - `TaskKilledException` is rethrown.
  - The file already imports `scala.util.Using` in main code.
- [x] Checked the replaced shared helper in core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala and core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala:
  - `using` closes the stream in `finally`, so a close failure hides the original read failure.
  - That makes the inline `Using.resource` justified, and the `read-close.png` test locks the choice in.
  - Fixing the shared helper instead would be outside this PR's scope.
- [x] Checked style limits:
  - build.sbt points `Test / scalastyleConfig` at scalastyle-test-config.xml, which caps files at 800 lines and lines at 120 characters; production code uses scalastyle-config.xml.
  - The suite ends at line 800.
  - The fixture's `seek` override is exactly 120 characters.
  - The new file has the license header.
- [x] Checked compatibility:
  - No public JVM signature, serialized Param, codegen or dependency changes; the object is `private[openai]`.
  - The test-only use of httpclient 4/5 classes and JDK 11 `java.net.http` matches the master toolchain.
- [x] Read the whole new fixture, cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala (lines 1-204): scenario dispatch by file name, the `unexpected` guard, `TimeoutNames`, and the cyclic exception-graph builder.
- [ ] I did not run any tests; this review is read-only. The caller reported compile, Test/compile, 89 tests with 0 skips, and production and test scalastyle all passing on JDK 11, Spark 3.5.0 and Scala 2.12.17. I have noted that result but not reproduced it.

## Issues

### Issue 1: Inferring interruption from the exception graph overrides Spark's own cancellation signal inside tasks
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 28-31, 42-54, 70-91, 96-98
- **Description**:
  - **Order of checks:** `readAttachment` first infers interruption from the exception graph (`isInterruptedRead`, line 96). Only after that does it check the two reliable signals: the thread's interrupt flag (line 97) and `TaskContext.isInterrupted` (line 98).
  - **What counts as an interruption:** `isInterruption` (line 52) treats any `InterruptedIOException` as a real interruption unless a class in its cause chain has a name ending in `TimeoutException` (the name check is at lines 42-48).
  - **Flag is set on the current thread:** `interruptedRead()` (lines 29-30) then sets the current thread's interrupt flag.
  - **Why the inference adds little in production:** the production path is the OpenAIPrompt UDF running inside a Spark task. Spark's `Task.kill` calls `markInterrupted` on the task context before interrupting the thread. So line 98 already catches every Spark cancellation, even when the storage layer clears the thread's flag. Apart from a top-level `InterruptedException` or `ClosedByInterruptException` (which the base branch already handled), line 96 only changes the outcome when neither signal is set — exactly when nothing has cancelled the task.
  - **Example from outside this repo:** some clients don't name their timeouts `*TimeoutException`. Okio's default `AsyncTimeout` error, which OkHttp uses for call timeouts, is a plain `InterruptedIOException("timeout")`.
  - **Example inside this repo:** suppressed exceptions are always checked (line 86). So an `InterruptedIOException` attached to a timeout's cause counts as an interruption of the current thread, even though the code deliberately ignores the timeout's own `InterruptedException` cause. In the fixture's `otherTimeout` (lines 102-118), `concurrent-timeout.png` is treated as ordinary but `concurrent-timeout-close-interrupted.png` is treated as interrupted.
- **Risk**:
  - For storage clients that report timeouts as a plain `InterruptedIOException`, the PR's promise that ordinary timeouts stay row-local does not hold.
  - One slow attachment then raises an `InterruptedException` that escapes the UDF's `Try`, and the task thread's interrupt flag gets set even though nothing interrupted it.
  - Spark records the task as FAILED rather than KILLED and retries it. A timeout that keeps recurring can fail the whole job; on the base branch it would have been a row-local error.
  - Exposure depends on the storage connector. The common ones (ABFS, S3A, GCS, HDFS) report timeouts as `SocketTimeoutException` or other classes named `*TimeoutException`.
- **Suggested Fix**:
  - Rely on the Spark signal whenever it exists. Keep the base branch's unconditional handling of a top-level `InterruptedException` or `ClosedByInterruptException`.
  - Run the new exception-graph inference only when there is no task context or the task was cancelled: `case error if (TaskContext.get() == null || taskCancelled) && isInterruptedRead(error) => interruptedRead()`.
  - This keeps every scenario in the diff working: the cleared-flag and cyclic-graph tests run without a task context, the Spark cancellation tests have the task marked cancelled, and the in-task row-local tests are all timeouts.
  - If interrupts that don't come from Spark must also propagate inside tasks, instead add a comment next to `isInterruption` explaining the `*TimeoutException` naming assumption and this trade-off.

### Issue 2: Interrupted scenario lists are maintained by hand in the suite, while timeout scenarios are defined in the fixture
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala (with cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala)
- **Line(s)**: suite 418-422 and 580-583 (compare with 430 and 560); fixture 30-35, 65-189, 141
- **Description**:
  - The fixture defines `TimeoutNames` in one place (lines 30-35), and the suite reuses it twice (lines 430 and 560).
  - The interrupted scenarios are instead an inline 15-name `interruptedNames` list (lines 418-422), plus a second, hand-copied 11-name list for the UDF-level test (lines 580-583).
  - Which names belong in that second list is determined by the fixture: they are the scenarios that go through `throwWithClearedInterrupt`. The suite repeats that knowledge by hand.
  - Scenario behavior is already keyed by file-name string in many places: `cleanupFailure`, `timeoutWithCleanup`, `storageTimeout`, `otherTimeout`, `wrappedFailure`, the `ordinaryFailure` allow-list (line 141), and the `open` dispatch chain (lines 158-189).
- **Risk**:
  - Adding a scenario that clears the thread flag means editing three or four places in step. Forgetting the second suite list silently loses end-to-end coverage.
  - The existing safeguards don't catch that. The `fs.opened` check (suite line 443) and the `unexpected` guard (fixture line 141; asserted at suite line 50) only flag names that the suite lists but the fixture doesn't know. They don't flag fixture scenarios that no test runs.
- **Suggested Fix**:
  - Next to `TimeoutNames`, define `ClearedInterruptNames` (the 11 names) and `InterruptedNames = Seq("interrupted.png", "plain-interrupted.png", "flagged.png", "read-close.png") ++ ClearedInterruptNames`.
  - Use them at suite lines 418 and 580.
  - Optionally, drive both the dispatch and the lists from a single scenario table, so a new scenario can't be only partly registered.

### Issue 3: The multimodal request suite is now exactly at the 800-line limit for test files
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala
- **Line(s)**: 1-800 (cancellation tests at 411-599; helper at 42-53)
- **Description**:
  - build.sbt points `Test / scalastyleConfig` at scalastyle-test-config.xml, whose file-length check allows at most 800 lines and fails the build above that (lines 3-4).
  - The PR's four hunks to this suite (`@@ -6,11 +6,10 @@`, `@@ -19,53 +18,15 @@`, `@@ -78,6 +39,19 @@`, `@@ -435,71 +409,191 @@`) add 94 lines net, taking it from 706 lines to exactly 800.
  - The PR already moved the fixture into its own file. But about 190 lines of storage-failure and cancellation tests remain in a suite that is otherwise about building multimodal requests.
- **Risk**:
  - Any test added to this suite later will fail `Test/scalastyle`, forcing an unrelated PR to reorganize these cancellation tests.
  - Request-building tests and storage-cancellation tests stay mixed in one file.
- **Suggested Fix**:
  - Move the attachment read-failure and cancellation tests (lines 411-599) and `withAttachmentFailureFileSystem` (lines 42-53) into their own `TestBase` suite next to `AttachmentReadFailureFileSystem.scala`, for example `OpenAIAttachmentReadFailureSuite`.
  - Share `withEchoServer`, `prompt` and `imageBytes` through a small test helper, or copy them with minimal duplication.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Documented the existing policy next to `isInterruption`: plain `InterruptedIOException` remains an interrupt signal inside Spark; typed timeout causes disambiguate storage timeouts. One blank line became a comment, with no executable or line-number changes.
- **Why**: Adopted the suggested documentation alternative. Restricting exception signals to Spark cancellation would discard genuine interrupts from other sources after a connector clears the thread flag. An untyped timeout is indistinguishable from interruption without an additional reliable signal; message-text heuristics are not introduced.
- **How verified**: All 89 existing multimodal request tests passed with 0 skips, including typed timeouts, translated timeout causes, independent cleanup interruption, cleared flags, and Spark cancellation. Production/test compilation and both scalastyle gates passed.

### Issue 2
- **Status**: Addressed
- **What changed**: Defined `ClearedInterruptNames` and `InterruptedNames` beside `TimeoutNames` in the fixture, and reused them in both suite locations.
- **Why**: One shared list now selects all cleared-flag scenarios for both direct reads and the public message-building path, avoiding a hand-copied second list.
- **How verified**: `sbt "set cognitive / Test / fork := false" "cognitive/compile" "cognitive/Test/compile" "cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite" "cognitive/scalastyle" "cognitive/Test/scalastyle"` passed: 89 tests, 0 skipped, both compilation gates and both style gates. The registered test names match the preceding 89-test qualification exactly.

### Issue 3
- **Status**: Addressed
- **What changed**: The shared scenario lists reduce the suite from 800 to 793 lines without deleting tests or compacting unrelated code.
- **Why**: This removes the immediate file-length pressure while keeping public multimodal behavior tests with their existing HTTP and prompt helpers. A separate suite and duplicated or newly abstracted helpers are unnecessary for this change.
- **How verified**: Confirmed the 793-line suite and successful test scalastyle; all 89 test cases remain present and pass.