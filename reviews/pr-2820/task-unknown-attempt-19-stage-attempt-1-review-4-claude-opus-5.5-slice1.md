---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 4
timestamp: "2026-11-10T02:54:34Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "55ffe4f38d59ba0c771b21c77a789a1d2f241edf51053584ca1c300c5e22a82f"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-19-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read every line of the assigned frozen diff in the required ranges [1, 150], [151, 300], [301, 450], [451, 600] and [601, 682]. No read was truncated. I also read the guidance files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. I read `build.sbt` (Spark 3.5.0, Scala 2.12.17, httpclient5 5.1.3, httpmime 4.5.13) and `environment.yml`.
- [x] **`readAttachment` control flow** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` L93–104):
  - `checkCancellation()` (L37–40) runs before the `try`, so its `InterruptedException` or `TaskKilledException` is never converted into a row error.
  - The catch cases run in this order: interruption anywhere in the exception graph, then the thread flag, then Spark task cancellation, then top-level `CancellationException`, then any other non-fatal error.
  - The post-read `checkCancellation()` runs only after the read succeeds, and the content is returned unchanged.
  - Every Throwable reaches the guarded first case, but fatal errors only take the top-level `isInterruption` check (L71). `LinkageError`, `VirtualMachineError` and `ControlThrowable` therefore match no case and are rethrown unchanged. Only a top-level `InterruptedException` is converted.
- [x] **Exception-graph traversal** (`isInterruptedRead`, L70–91):
  - Visited nodes are tracked per (exception, classify flag) in two identity sets. Each node's children depend only on that node, so tracking per pair loses nothing, and each node is expanded at most twice.
  - The traversal is tail-recursive, so cycles terminate and the 2049-node graph in the test cannot overflow the stack.
  - A timeout's cause chain is not classified; suppressed exceptions are always classified.
  - `isTimeoutFailure` (L56–68) has its own cycle guard and stops at `InterruptedException` or `ClosedByInterruptException`.
  - `isTimeout` (L42–48) checks the class name suffix up the superclass chain, so anonymous and relocated subclasses are recognized.
- [x] **Read and close behaviour.** I compared `Using.resource` with the old `StreamUtilities.using` in `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`.
  - The old helper let a close failure replace the read failure.
  - Now the read failure stays primary and the close failure is attached as suppressed. A close failure ranks higher only if it is a `VirtualMachineError`, `LinkageError` or `InterruptedException`.
  - A failure from a successful read's close still propagates directly, as before.
  - `open` is still evaluated outside the resource `try`, as before.
  - The old `BinaryFileReader.readSingleFileBytes` (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala`) also defaulted to `new Configuration()`, so Hadoop configuration handling is unchanged.
  - `scala.util.Using` is already used by main code in `OpenAIPrompt.scala` L29.
- [x] **How `OpenAIPrompt` handles these exceptions.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `createMessagesUDF` wraps the call in `Try`.
  - The sanitized `InterruptedException` is fatal to `Try`, so it escapes.
  - `TaskKilledException` is explicitly rethrown.
  - `AttachmentReadException` and `CancelledAttachmentReadException` become row errors with only the sanitized `getMessage`. This matches the new doc comment at L25 and the row-local test assertions.
- [x] **Every fixture scenario traced.** I followed each name in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` through `open` (L168–215) and the classifier:
  - All 16 `TimeoutNames` end as ordinary errors.
  - All 11 `ClearedInterruptNames` end as interruptions.
  - `interrupted`, `plain-interrupted`, `flagged` and `read-close` end as interruptions. For `read-close`, `Using` keeps the `InterruptedException` primary over the `IOException`.
  - `ordinary` ends as an ordinary error, and `cancelled` ends as `CancelledAttachmentReadException`.
  - Each name reaches the intended branch (by-name `orElse` chain, `translated-` prefix, `-close-interrupted.png` suffix). The `unexpected` guard catches any name that falls through by mistake.
- [x] **Cyclic and fatal tests** (`OpenAIPromptMultimodalRequestSuite.scala` L444–474):
  - `(1 to 2048)` builds 2049 nodes, closed into a cycle by `last.addSuppressed(root)`.
  - `last` keeps an uninitialized cause, so `initCause(interruption)` is legal.
  - `cyclicInterruption().getCause` is called once, within its limit of 2, because of the cycle guard in `isTimeoutFailure`.
  - The `LinkageError` instance is rethrown by identity, and the thread interrupt flag stays clear.
- [x] **Spark cancellation test sequencing** (suite L496–552):
  - Failure and completion listeners are registered, and `gate.context` is set, before `entered` is counted down.
  - Spark marks the task context as interrupted before it interrupts the thread.
  - With `interrupt=false`, the gate is released only after the context is marked. Both the failed read and the successful read then end in `TaskKilledException` with no interrupt flag during completion listeners; the successful read reaches it through the post-read check.
  - With `interrupt=true`, the latch throws, which produces the sanitized `InterruptedException`. The cause-chain walk is lazy and safe when no failure was recorded.
  - The `AutoCloseable` cleanup releases the gate, cancels the job group and shuts down the executor.
  - `group` is removed from `Active` in a `finally` block.
- [x] **Fixture wiring.** `URI.create("attachmentfailure:///").resolve(name)` gives `attachmentfailure:/name` (null authority). Hadoop's filesystem cache key (scheme, empty authority, user) therefore matches the fixture registered by `withAttachmentFailureFileSystem` (suite L42–53), so production's fresh `Configuration` resolves to the fixture. `Using.resource` closes it, which evicts it from the cache between tests.
- [x] **JDK 8, API surface and scope.** Neither changed file uses a JDK 9+ API; the `java.net.http` type was replaced by the test-local `RelocatedHttpClient.HttpTimeoutException`. The httpclient 4 and 5 timeout types come from dependencies already in `build.sbt`. `readAttachment` is still `private[openai]`, and no public JVM signatures, Params, dependencies or release files change. No native AIPrompt or recovery code was added. The test registrations add up to 89, which matches the caller-reported count.
- [x] **Considered but not reported:**
  - A plain `InterruptedIOException` with no timeout cause, such as an untyped deadline from Okio, is classified as an interruption.
  - Interruption detected deep in the exception graph takes precedence over a Spark cancellation that did not interrupt the thread.

  Both follow from the documented typed-timeout design (comments at L43, L49, L57). Both extend the existing ordering, where a top-level interruption already took precedence. I found no in-repo or mainstream-connector trigger (ABFS, S3A, GCS, or Hadoop's HTTP filesystem). Neither meets the bar for a concrete finding.
- [ ] **Tests not executed here.** This was a read-only review. I relied on the caller's local evidence (compile, Test/compile, 89 tests with 0 skips, both scalastyle gates, on JDK 8 and JDK 11) and did not check remote CI.