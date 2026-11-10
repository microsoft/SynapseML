---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 2
timestamp: "2026-11-09T21:53:31Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3cbea834e72397ccd5f2f5124c0a74a3f8371d1d9038feab4651015428de98f5"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-19-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read all 682 lines of the assigned frozen diff in the five required ranges: [1,150], [151,300], [301,450], [451,600] and [601,682].
  - The diff covers three files:
    - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
    - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (new)
    - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
  - I opened each file at HEAD by exact path, and its content matches the diff.
- [x] **Guidance applied:**
  - `AGENTS.md`
  - `.github/skills/synapseml-branches/SKILL.md`
  - `.github/skills/synapseml-branches/references/branch-spark3p5.md`: the target is master, so shared code must work on the oldest JDK.
  - `.github/skills/code-review/SKILL.md`
  - Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0, black 22.3.0).
- [x] **Public surface unchanged:**
  - `OpenAIAttachmentUtils` is still `private[openai]`.
  - The `readAttachment(fileName)(read: => Array[Byte])` signature is unchanged (`OpenAIAttachmentUtils.scala` L93).
  - The new helpers at L33–91 are all private to the object.
  - The only change to the exception classes is a scaladoc comment at L25.
  - There are no changes to Params, `Wrappable`/codegen, dependencies, `pipeline.yaml`, workflows or release tooling.
- [x] **Replacing the shared read helper is justified:**
  - The old path used `BinaryFileReader.readSingleFileBytes` (`core/.../io/binary/BinaryFileReader.scala` L27–33). That calls `StreamUtilities.using` (`core/.../core/env/StreamUtilities.scala` L26–34), where a failure in `close()` replaces the original read failure.
  - The new inline `Using.resource(...)(IOUtils.toByteArray)` (L315–319) keeps the same default `Configuration` and read behavior. It also keeps both failures, attaching the second one as suppressed.
  - The shared core helper and its other callers are untouched.
  - Cognitive main code already uses `scala.util.Using` (`OpenAIPrompt.scala` L30), so this is not a new pattern or dependency.
- [x] **Contract with the caller holds:**
  - `OpenAIPrompt.createMessagesUDF` (~L485–501) wraps the call in `Try`, which catches only non-fatal errors, so `InterruptedException` passes through.
  - It rethrows `TaskKilledException` and turns every other failure into a row-level error message.
  - That matches the four outcomes `readAttachment` produces:
    - a sanitized `InterruptedException`, with the thread's interrupt flag set again
    - `TaskKilledException`
    - `CancelledAttachmentReadException`
    - `AttachmentReadException`
  - Data URLs skip `readAttachment`, which fits the storage-only scope.
- [x] **Error classification is consistent:**
  - The catch order (L95–101) keeps the existing priority: interruption, then the thread flag, then task cancellation, then storage `CancellationException`, then ordinary failures.
  - Fatal errors are only checked at the top level (L71), so errors such as `LinkageError` pass through unchanged.
  - The exception-graph walks are tail-recursive and track visited exceptions by identity. They cannot overflow the stack or loop on cycles.
  - The separate `visited` and `timeoutCauses` sets (L74–86) handle one exception appearing twice: first as the cause of a timeout, then as an independent suppressed cleanup failure. It is still classified the second time, which I traced with the `timeout-shared-close-interrupted.png` scenario.
  - The timeout check matches any class or superclass whose name ends in `TimeoutException` (L42–48). A comment explains it, and it handles relocated and anonymous subclasses, which I traced with `relocated-pool-timeout.png`.
- [x] **JDK 8 compatibility:**
  - The main code uses only JDK 7 or older APIs: `IdentityHashMap`, `Collections.newSetFromMap` and `Throwable.getSuppressed`.
  - The test fixture now uses its own `RelocatedHttpClient.HttpTimeoutException` (L45–49) instead of the JDK 11-only type.
  - The HttpClient 4 and httpclient5 types used in tests come from dependencies already in `build.sbt`.
- [x] **Test structure follows repository conventions:**
  - The fixture now has its own file. It has a license header, and `AttachmentReadFailureFileSystem` (L217–227) is public with a no-argument constructor, as Hadoop requires.
  - This keeps the suite at about 793 lines, under the 800-line limit.
  - `withAttachmentFailureFileSystem` (suite L42–53) puts the filesystem setup in one place. It closes the filesystem with `Using.resource` and fails the test on any unknown scenario name.
  - Cleanup for the cancellation test (~L511–519) uses an `AutoCloseable`, so it runs even when an assertion fails.
  - Interrupt flags are cleared in `finally` blocks so they do not leak into other tests.
  - The tests run offline with no credentials.
- [ ] **Builds and tests not run:** this review was read-only. I did not re-run or independently check the provided JDK 11 and JDK 8 results (compile, Test/compile, 89 tests with 0 skipped, scalastyle).