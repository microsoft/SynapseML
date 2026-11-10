---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 4
timestamp: "2026-11-10T09:08:48Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "1de006fe17c6c33f291568676ec11246bdc6f5269923e17a86493a7a7dc10cb4"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-20-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read all 696 lines of the assigned frozen diff in the five required chunks: [1,150], [151,300], [301,450], [451,600] and [601,696]. The diff covers `OpenAIAttachmentUtils.scala`, the new `AttachmentReadFailureFileSystem.scala` and `OpenAIPromptMultimodalRequestSuite.scala`. The current source matches the diff in the regions I spot-checked.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, the master reference `branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml`. In `scalastyle-test-config.xml`:
  - The new test fixture is 234 lines.
  - The suite is about 799 lines, so it is within the 800-line limit but right at it.
- [x] **Read control flow (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, lines 93–104):**
  - The check before storage IO runs outside the `try`.
  - The catch cases run in this order: hidden interruption, then thread flag, then task cancelled, then `CancellationException`, then ordinary failure. Every case returns `Nothing`, so the types check.
  - The check after IO runs only when the read succeeds.
  - Fatal errors go only through `isInterruption` (line 71), which is true only for `InterruptedException`. Other fatal errors, such as `LinkageError`, match no case and propagate unchanged.
- [x] **Exception-graph walk (lines 56–91):**
  - It always terminates. Two identity sets allow at most 2N visits, and every recursive helper (`timeoutClass`, both `inspect` functions) is tail-recursive.
  - The two sets let a node first reached as a cause owned by a timeout be classified again if another path reaches it as an ordinary node. I traced `timeout-shared-close-interrupted.png` to confirm this.
  - The rule `classify && !isTimeout(current)` agrees with `isTimeoutFailure`'s nearest-wins stop at `InterruptedException`/`ClosedByInterruptException`. I traced the translated-pool, translated-concurrent, nested-interrupted and timeout-cause-cycle cases.
- [x] **Timeout detection (`isTimeout`, lines 42–48):** Matching on the class-name suffix while walking up superclasses correctly handles:
  - the anonymous `ConnectionPoolTimeoutException {}` subclass;
  - relocated test classes;
  - `ConnectTimeoutException` from both HttpClient 4 and HttpClient 5;
  - `java.util.concurrent.TimeoutException`.

  The fixture's local `HttpTimeoutException` avoids `java.net.http`, so it stays compatible with JDK 8.
- [x] **Cleanup data flow (`prepareFile`, lines 315–319):** The old `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala` `using` (lines 26–34) let a `close()` failure replace the read failure. The new `Using.resource` keeps the original failure and attaches the close failure as suppressed, and it promotes `InterruptedException` or fatal errors over ordinary ones. I traced the `read-close.png` and `close-*` cases.
  - Behaviour is otherwise the same as `BinaryFileReader.readSingleFileBytes`: it still uses `new Configuration()` and `IOUtils.toByteArray`.
  - A null stream or an invalid `HPath` still becomes the sanitized `AttachmentReadException`.
- [x] **How `OpenAIPrompt` consumes these errors (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `createMessagesUDF`):**
  - `Try` catches only non-fatal errors, so the sanitized `InterruptedException` escapes the UDF.
  - `TaskKilledException` is rethrown explicitly.
  - `AttachmentReadException` and `CancelledAttachmentReadException` stay row-local as `(null, message)`, which matches the new doc comment.
- [x] **Scenario coverage:** I traced all 33 test-fixture scenarios (15 interrupted, 16 timeout, ordinary and cancelled) through `AttachmentReadFailures.open` and the production classification. Every expected-result list matches. The `unexpected` guard catches any scenario name that no branch handles. In the cyclic-graph tests, the 2049-node graph is classified as ordinary, and the cyclic interruption calls `getCause` once, within its bound of 2.
- [x] **Task-level tests (4 combinations × 2 APIs):**
  - With `interruptOnCancel=false`, the tests assert the reason "Attachment read canceled", no interrupt flag during completion, and no HTTP request body.
  - The successful-read case exercises the check after IO.
  - The before-IO case uses a single-partition Dataset `mapPartitions` (not RDD) and asserts storage was never opened.
  - The `interrupt=true` case asserts the sanitized `InterruptedException` appears in the failure's cause chain.
  - The `AutoCloseable` cleanup releases the gate, cancels the job group, shuts down the executor, and the gate entry is removed in `finally`.
- [x] **Public API:** The `readAttachment` and `prepareFile` signatures are unchanged and only private helpers were added. `BinaryFileReader` is untouched, and no Params, dependencies or pipeline files changed.
- [x] **Accepted design choice:** A plain `InterruptedIOException` with no typed timeout evidence is treated as an interruption: it sets the thread flag and fails the task, which Spark may retry. I confirmed this is implemented as stated (lines 49–54 and 96). The review plan accepts this behaviour, so it is not counted as an issue.
- [ ] **Not run by me:** The compile, Test/compile, 91-test and scalastyle results are the caller's reported evidence; this review was read-only. I accessed no live services or cloud resources.