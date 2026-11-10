---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 1
timestamp: "2026-11-09T07:23:45Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "72870f737f4935932a5521461b3d1c24e70661416d9f6a1445a5dfb41890251b"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-16-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Inputs read.** I read all 666 lines of the assigned frozen diff in exactly the required ranges: [1,150], [151,300], [301,450], [451,600], [601,666]. It covers three files:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (new)
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`

  I also read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml` (Spark 3.5.0, Scala 2.12.17). I used only exact-path views and opened no other review artifacts.
- [x] **Control flow** (`OpenAIAttachmentUtils.scala`):
  - `readAttachment` (L93–104) calls `checkCancellation` (L37–40) before and after the read.
  - If the thread's interrupt flag is set, it throws `InterruptedException("Attachment read interrupted")` and re-sets the flag (L28–31).
  - If `TaskContext.isInterrupted`, it throws `TaskKilledException("Attachment read canceled")`.
  - Catch order: interruption anywhere in the exception graph, then the thread flag, then task cancellation, then `CancellationException`, then a generic `AttachmentReadException`.
  - Fatal throwables other than `InterruptedException` match no case and are rethrown unchanged (L71). The `LinkageError` test checks this.
  - Every replacement exception has a fixed message and no cause, so no path, URL or secret can leak.
- [x] **Classifier** (`isTimeout` L42–48, `isInterruption` L50–54, `isTimeoutFailure` L56–68, `isInterruptedRead` L70–91):
  - Two identity sets limit each node to at most two visits, so cyclic graphs terminate.
  - All recursion is `@tailrec`, so the 2048-node fixture cannot overflow the stack.
  - Causes owned by a timeout are not classified, but their suppressed cleanup failures still are.
  - A translated plain `InterruptedIOException` whose cause chain contains a timeout-named type stays row-local.
  - An `InterruptedException` or `ClosedByInterruptException` that appears before any timeout in the cause chain stops timeout attribution.
  - `isTimeout` walks superclasses, so anonymous subclasses (`relocated-pool-timeout.png`) are recognised.
- [x] **Read path.** `OpenAIAttachmentUtils.scala` L315–319 matches the old path in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` L27–33: same `new Configuration()`, `getFileSystem`, `open` and commons-io `IOUtils.toByteArray`, and the cached filesystem is not closed.
  - Only failure precedence changes. In `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala` L26–34, a `close()` exception replaced a read `InterruptedException`.
  - `Using.resource` keeps the `InterruptedException` as the thrown exception (`read-close.png`). It attaches a cleanup `InterruptedIOException` as suppressed under an ordinary read failure (`close-interrupted-io.png`).
  - commons-io and `scala.util.Using` are already used in main code, so this adds no dependency.
- [x] **Consumer contract** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`):
  - In `createMessagesUDF` (L485–500), `Try` rethrows `TaskKilledException` (L497).
  - `InterruptedException` is not `NonFatal`, so it escapes `Try`. Other failures become a row-local `e.getMessage`.
  - The call chain `createMessagesForRow` (L582) → `buildContentParts` (L602) → `wrapFileToMessagesList` (L634) → `prepareFile` (L610) has no intermediate catch.
- [x] **Fixture and test trace.** I traced every scenario in `AttachmentReadFailureFileSystem.scala` through the classifier:
  - The 15 interrupted names produce `InterruptedException`.
  - `ordinary.png` plus the 16 `TimeoutNames` produce a plain `AttachmentReadException`.
  - `cancelled.png` produces `CancelledAttachmentReadException`.

  These match the suite assertions at L411–446, L558–576 and L578–599.
  - The `opened` and `unexpected` checks catch scenario-name typos.
  - Closing via `Using.resource` removes the filesystem-cache injection (L42–53).
  - Gates are removed in `finally` (L554).
  - The Spark cancellation tests (L500–556) cover both APIs with interrupt off/read failed, interrupt off/read succeeded, and interrupt on/read failed. They check the kill reason, that no interrupt flag is left at task completion, and that no HTTP request body was sent.
- [x] **Spec and API conformance.**
  - `OpenAIAttachmentUtils` is `private[openai]` and the `readAttachment` signature is unchanged.
  - There are no changes to Params, codegen, public JVM signatures, dependencies, pipeline, release, native AIPrompt or recovery.
  - Test-only `java.net.http.HttpTimeoutException` and the httpclient4/httpclient5 `ConnectTimeoutException` classes resolve on master's Java 11 CI and the existing classpath.
- [x] **Change since qualification.** The only new line is the doc comment at `OpenAIAttachmentUtils.scala` L25.
  - It is accurate: a storage `CancellationException` with no thread interruption or task cancellation becomes a sanitized row-local error.
  - It is ASCII and about 113 characters, under the 120-character `FileLineLengthChecker` limit. `scalastyle-config.xml` has no Scaladoc rule.
  - The null comparisons at L46 and L61 are allowed by scalastyle 1.0.0 `NullChecker` defaults (`project/plugins.sbt`).
  - The suite file ends at exactly line 800, which equals the test `maxFileLength`. It passes, but there is no room for more lines.
- [x] **Considered, not raised.**
  - **Okio-style timeouts:** a timeout reported only as a plain `InterruptedIOException`, with no timeout-named type in its cause chain (as in Okio/OkHttp call timeouts), would count as interruption and fail the task instead of staying row-local. This follows the JDK contract for `InterruptedIOException` and the PR's deliberate handling of connectors that clear the interrupt flag. I found no storage path in this repository that produces it (connector sources are out of scope and were not inspected), so it is not a supported finding.
  - **Interruption from another thread:** an interruption nested from another thread is deliberately treated as interruption (`wrapped-interrupted.png`). At worst this causes a Spark task retry.
- [ ] **Not executed:** build, tests and scalastyle, because this is a read-only review. I relied on the caller's evidence for the tree before the comment change (compile, Test/compile, 89 tests with 0 skips, production and test scalastyle on JDK 11 / Spark 3.5.0 / Scala 2.12.17) and did not reproduce it.