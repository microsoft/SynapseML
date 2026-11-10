---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 1
timestamp: "2026-11-10T03:43:55Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a26dfe96a7a2b9c582fdc1a5eebd691af7b5fd1a5ed897ba7535c4f878c9c0a7"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-20-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 696 lines of the assigned frozen diff, using exactly the ranges [1,150], [151,300], [301,450], [451,600] and [601,696]. The diff covers 3 files. The checkout copies of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` and `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` match the diff.
- [x] **Guidance and target:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. `build.sbt` sets Spark 3.5.0 and Scala 2.12.17; `environment.yml` pins pyspark 3.5.0. The target branch is `master`.
- [x] **Inlined read matches the old helper:** `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` (`readSingleFileBytes`) already used `new Configuration()` by default and called `path.getFileSystem(conf).open(path)`. Filesystem and configuration resolution are therefore unchanged.
- [x] **Close-failure handling is the only behavior change:** In `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`, `using` let a `close()` failure replace the read failure. `Using.resource` instead keeps the main failure and attaches the cleanup failure as suppressed (or ranks it first if it is more severe). The new classifier needs that. Results for successful reads and for close-only failures are unchanged.
- [x] **`readAttachment` order of checks:**
  - It checks for cancellation before reading.
  - On failure it checks, in order: interruption anywhere in the exception graph, the thread interrupt flag, `TaskContext.isInterrupted()`, `CancellationException`, then ordinary failure.
  - It checks for cancellation again after reading.
- [x] **Fatal errors and sanitization:** `LinkageError`, `VirtualMachineError`, `ThreadDeath` and `ControlThrowable` are rethrown unchanged. Only a top-level `InterruptedException` is treated as an interrupted read. Every replacement exception is created without a cause or path, so sanitization holds.
- [x] **Classifier logic:**
  - It tracks visited nodes in two identity sets, so each node is visited at most twice and cause/suppressed cycles end.
  - All recursive helpers are tail-recursive.
  - Along a cause chain, whichever of timeout or interruption comes first decides the result.
  - Suppressed cleanup failures are always checked again.
- [x] **Fixture scenarios traced by hand:** I traced all 16 `TimeoutNames`, all 11 `ClearedInterruptNames`, the 4 other interrupted names, `ordinary.png` and `cancelled.png`. Every expected result holds. This includes `timeout-shared-close-interrupted.png`, which needs the separate sets, and `timeout-cause-cycle.png`.
- [x] **Caller contract:** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `createMessagesUDF` wraps the call in `Try`, which does not catch `InterruptedException`. It rethrows `TaskKilledException` and turns `AttachmentReadException` subclasses into per-row errors. Callers see no new exception types.
- [x] **Compatibility:** No public JVM signatures, Params or dependencies changed. `IOUtils` and Hadoop `Configuration` were already used by core and `OpenAIPrompt`, and `scala.util.Using` was already used in main code.
- [x] **Spark test assumptions:**
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` runs `local[*]` with no task retries. Executor threads therefore share the test's cached Hadoop `FileSystem`, which the `fs.opened == Vector(name)` assertions confirm.
  - In the before-IO case, only `fs.opened.isEmpty` would catch a missing pre-read check, because the kill reason matches either way. This agrees with the caller's evidence.
  - The single-partition Dataset avoids entering the gate more than once.
  - Cleanup releases the gate, cancels the job group and waits for the executor on every path.
  - The 8 parameterized test names are unique.
- [x] **JDK 8 and style:** The code is JDK 8 compatible: the custom `HttpTimeoutException` avoids `java.net.http`, and `IdentityHashMap`, `newSetFromMap` and `getSuppressed` are older APIs. `NullChecker` in `scalastyle-config.xml` and `scalastyle-test-config.xml` allows explicit null checks. The test suite is 799 lines, within the 800-line `FileLengthChecker` limit. That leaves 1 line of room, which does not block this change. I did not run the caller-provided compile, test or style checks.
- [x] **Reviewed and accepted as stated design choices:**
  - A plain `InterruptedIOException` with no recognizable timeout type or cause is now treated as an interruption and fails the task. Before this change it produced a per-row error. Typed timeouts from ABFS, WASB, S3A and HTTP paths still stay per-row.
  - The fixed reason "Attachment read canceled" replacing Spark's kill reason is pre-existing behavior. Spark's `getKillReason` is `private[spark]`, so it cannot be read here.