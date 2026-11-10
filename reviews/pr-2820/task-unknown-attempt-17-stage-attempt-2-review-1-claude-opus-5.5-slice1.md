---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 2
round: 1
timestamp: "2026-11-09T10:49:47Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "db0b54121e366cca2e626f7f9bd53bdfdf11ea7351cc181cfa42c83bbd5b74af"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-17-stage-attempt-2-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read every line of the assigned frozen diff in the required chunks: [1,150], [151,300], [301,450], [451,600] and [601,667]. That covers 667 of 667 lines across 3 files. I also read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml`.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, lines 93–104: the error handling keeps the pre-PR priority order, and the new graph walk runs first.
  - Fatal errors other than `InterruptedException` (`LinkageError`, `VirtualMachineError`, `ControlThrowable`) match no case and propagate unchanged.
  - Interruption calls the cause-free `interruptedRead()`.
  - Cancellation seen only through `TaskContext` throws `TaskKilledException`.
  - A storage `CancellationException` becomes `CancelledAttachmentReadException`.
  - Other non-fatal errors become `AttachmentReadException(fileName)`.
  - Cancellation is checked both before and after the read.
- [x] Same file, lines 42–91: the classifier handles cycles and deep graphs safely.
  - It uses identity sets and a worklist marked `@tailrec`.
  - Each node is expanded at most once when classified and once when unclassified, which matches the 2048-node cyclic fixture.
  - Typed timeouts are recognized by a class or superclass name ending in `TimeoutException`. This follows the same approach as Hadoop S3A and also covers relocated and anonymous subclasses.
  - Causes owned by a timeout are not classified, but their suppressed cleanup failures still are.
- [x] Same file, lines 313–321: compared the new `Using.resource(path.getFileSystem(new Configuration()).open(path))(IOUtils.toByteArray)` with the code it replaces in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` and `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`.
  - Both use the same default `Configuration` and read the bytes the same way.
  - If `close()` fails after a successful read, the read still fails.
  - If both the read and `close()` fail, both errors are now kept, with `InterruptedException` ranked above an ordinary `IOException`. Before, the close error masked the read error.
  - `scala.util.Using` is already used in main code (`OpenAIPrompt.scala`), so this adds no dependency.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, lines 485–501: confirmed how the caller handles these errors.
  - `Try` catches only non-fatal errors, so `InterruptedException` escapes.
  - Line 497 rethrows `TaskKilledException`.
  - Line 498 turns every other failure into a row-local `(null, e.getMessage)`.
  - None of the new or changed exceptions carries a cause or storage text, so no paths or credentials can leak.
- [x] Traced every scenario in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` through `isInterruptedRead` and `isTimeoutFailure`:
  - 16 timeout scenarios
  - 4 interruption scenarios plus 11 cleared-interrupt scenarios
  - the cancelled, ordinary, read-plus-close and cyclic-graph cases

  Each one matches its assertion in `OpenAIPromptMultimodalRequestSuite.scala` (about lines 409–592). The fixture throws with the thread interrupt flag cleared, so the tests prove classification does not rely on that flag.
- [x] Checked test isolation:
  - The suite helper (lines 42–53) injects the fixture through `fs.attachmentfailure.impl` and Hadoop's filesystem cache. The cache key is scheme, authority and user, so production's fresh `Configuration` finds the fixture.
  - The helper closes the filesystem afterwards and asserts that no unknown scenario names were opened.
  - `Test / parallelExecution := false` protects the shared `Active` gate map.
  - Every gate wait has a 30-second limit, so a failed test cannot hang the suite.
- [x] Checked scope and compatibility:
  - The object stays `private[openai]`.
  - There are no changes to public JVM signatures, Params, serialized shapes, dependencies, release files or the pipeline.
  - No native AIPrompt or optional recovery code was added.
  - The test-only classes (`java.net.http.HttpTimeoutException` and the Apache HttpClient 4 and 5 timeout classes) are on the test classpath.
  - CI unit tests run on JDK 11.
- [x] Two observations that are not defects:
  - A plain `InterruptedIOException` with no typed timeout cause is deliberately treated as an interruption, as documented at line 49. A client that raises untyped timeouts would therefore fail the task instead of just the row. The mainstream Hadoop connectors I considered (ABFS, WASB, S3A, GCS, HDFS) all use typed timeout exceptions.
  - No unit test covers the case where only `TaskContext` shows cancellation before the read (line 94). The same `checkCancellation()` path is tested after the read by the Spark case with `interruptOnCancel=false`.
- [ ] I did not run any tests because this review is read-only. Build and test status comes from the caller: compile, Test/compile, 89 tests with 0 skips, and both scalastyle checks, on JDK 11, Spark 3.5.0 and Scala 2.12.17.