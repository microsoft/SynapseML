---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-11-09T12:45:34Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "1328a8c2247cac71925cf191e4861da1d96f5babf39d9327db53541774e0f2f2"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-18-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 681 lines of the assigned frozen diff in exactly the required chunks: [1,150], [151,300], [301,450], [451,600] and [601,681]. Every chunk came back complete. The checked-out sources match the diff:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (lines 1–226)
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` (ends at line 793)
- [x] **Guidance and versions.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml`. This is an ordinary fix and correctly targets `master`.
- [x] **Read and error handling (`OpenAIAttachmentUtils.scala`, lines 33–104).**
  - `checkCancellation()` runs before the read (line 94) and after it (line 102).
  - The catch order (lines 96–100) is:
    1. an interruption anywhere in the exception's cause or suppressed chain;
    2. the thread's interrupt flag is still set;
    3. Spark has marked the task as cancelled;
    4. a storage `CancellationException`, which stays a row-level error;
    5. any other failure, which becomes the generic `AttachmentReadException`.
  - Fatal errors other than `InterruptedException`, such as `LinkageError`, match none of these cases and are rethrown unchanged.
  - Fatal `InterruptedException` still goes through `interruptedRead()`, as it did before this PR.
- [x] **Chain walking stops on cycles.**
  - `isTimeout` (lines 42–48) checks the class name and its superclass names. This handles renamed copies of library classes, anonymous subclasses, `java.util.concurrent.TimeoutException`, `HttpTimeoutException`, and the Apache HttpClient 4 and 5 timeout classes.
  - `isTimeoutFailure` (lines 56–68) stops when it reaches an interrupt exception.
  - `isInterruptedRead` (lines 70–91) uses a tail-recursive work list. It keeps a separate set of already-visited exceptions for each of its two modes: one checks for interruptions, the other skips the causes of a timeout. Each exception is expanded at most once per mode, so the walk always ends, even on circular chains.
  - Causes of a timeout are not classified, but suppressed exceptions attached to them still are.
- [x] **New read path (lines 316–318) compared with the code it replaces** (`BinaryFileReader.readSingleFileBytes` in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` and `using` in `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`):
  - Unchanged: the filesystem is still resolved with `new Configuration()`, the bytes are still read with `IOUtils.toByteArray`, and the stream is still closed once on success.
  - Changed: `Using.resource` keeps the read failure as the main error and attaches any close failure as suppressed. A more serious close failure, such as an interrupt or fatal error, becomes the main error instead. The old `finally` let a close failure hide the read failure.
  - This change is needed for the PR's goal; it is not unrelated refactoring.
- [x] **Caller contract (`OpenAIPrompt.scala`, `createMessagesUDF`, about lines 485–500; not changed by this PR).** The `Try` there catches only non-fatal errors and explicitly rethrows `TaskKilledException`. As a result:
  - `InterruptedException` and `TaskKilledException` stop the task.
  - `AttachmentReadException` and `CancelledAttachmentReadException` stay errors on the affected row, as the doc comment at line 25 says.
  - Spark marks the task as cancelled before it interrupts the thread, and reports either error as a killed task.
- [x] **No sensitive data leaks.** The errors that stop a task use fixed messages and carry no cause. Errors kept on the row show only the file's base name, through the existing `AttachmentReadException`. No path, URI, credential or storage message is exposed.
- [x] **Test scenarios match the code.** I traced every name in `TimeoutNames`, `ClearedInterruptNames` and `InterruptedNames` (`AttachmentReadFailureFileSystem.scala`, lines 30–43) through the production logic, and each gives the expected result. Covered cases include:
  - renamed and anonymous timeout classes;
  - a plain `InterruptedIOException` wrapping a timeout versus wrapping a real interruption;
  - a failed close interrupted under a timeout, whether nested or shared;
  - a timeout whose cause chain loops;
  - `read-close` (an `InterruptedException` while reading plus an `IOException` on close).

  `cyclicInterruption` limits how often `getCause` can be called; production calls it once. Injecting the test filesystem works because Hadoop's filesystem cache ignores the `Configuration`.
- [x] **Scope.**
  - No signatures, Params or serialization changed.
  - No dependency, pipeline, workflow or release changes.
  - No RDD code, native AIPrompt or optional recovery.
  - The test-only `HttpTimeoutException` needs JDK 11 or later, which matches `.github/skills/synapseml-local-setup/SKILL.md` and the CI default.
  - The style limits in `scalastyle-test-config.xml` hold: the test suite has 793 lines (limit 800), and the test helper `open` spans lines 167–213 (47 lines, limit 50).
- [x] **Considered and rejected as findings.**
  - A plain `InterruptedIOException` with no timeout in its chain counts as an interruption. This is intentional, follows Hadoop's convention, and errs toward failing the task rather than hiding a cancellation.
  - An interruption is checked before Spark task cancellation. Both end up reported as a killed task.
  - Interruptions attached to a timeout as suppressed exceptions stop the task. This is a deliberate rule and it is tested.
  - The interrupt flag that the code sets again cannot carry over to later tasks, because the thread pool clears it between tasks.
- [x] **Not executed by me.** I ran no tests, builds or commands. The passing compile, `Test/compile`, 89 tests with 0 skips, scalastyle checks and cycle-guard mutation run are evidence supplied by the caller. I did not open any review directories or other reviewers' artifacts.