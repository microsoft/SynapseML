---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 3
timestamp: "2026-11-09T11:38:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "08430826d442e5b6f4f964ca9d51ea75279df2a7325724651ac5eb5d91e4355b"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-17-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Frozen diff read in full.** I read all 667 lines of the assigned frozen diff in the five required chunks: [1,150], [151,300], [301,450], [451,600] and [601,667]. The current `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (lines 1–104 and 302–318) matches the diff's after-image.
- [x] **Guidance and versions.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml`.
  - Versions: Spark 3.5.0 and Scala 2.12.17.
  - Both test-only timeout classes come from dependencies cognitive already has: httpclient 4 (through httpmime) and httpclient5. The PR changes no dependency manifest.
- [x] **Fatal errors are unchanged.** In `isInterruptedRead` (lines 69–90), a fatal error is only checked for a direct `InterruptedException`, with no allocation. `OutOfMemoryError`, `StackOverflowError`, `LinkageError` and `ControlThrowable` fall through every `NonFatal` case and propagate unchanged.
- [x] **The exception-graph walk terminates and is stack-safe.**
  - The non-fatal walk is a tail-recursive worklist with two identity sets, so it visits each node at most once per mode, even with cause or suppressed cycles.
  - `isTimeoutFailure` keeps its own visited set and stops at `InterruptedException` and `ClosedByInterruptException`.
  - `Throwable.addSuppressed` rejects null, so null nodes never enter the worklist.
- [x] **Every fixture scenario reaches its asserted outcome.** I traced each scenario in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (`TimeoutNames`, `ClearedInterruptNames`, `InterruptedNames`, `cancelled.png`, `ordinary.png`, `read-close.png`, and the 2048-node cyclic graphs) through the catch order at lines 92–104. Cases I checked specifically:
  - "translated" plain `InterruptedIOException` with a typed timeout cause;
  - shared and nested suppressed cleanup on a timeout;
  - an anonymous subclass of a relocated timeout class, matched through the superclass-name walk;
  - `TimeoutException` and `HttpTimeoutException` with an `InterruptedException` cause, plus suppressed interruption.
- [x] **Resource handling in `prepareFile`.** I compared `Using.resource(...)` (lines 313–318) with the old `BinaryFileReader.readSingleFileBytes` / `StreamUtilities.using`:
  - It uses the same `new Configuration()` and the same cached FileSystem, which is not closed.
  - Failures to resolve or open the path still happen inside `readAttachment`, and a null stream becomes a sanitized row error.
  - Read and close failures are now both kept. Preferential suppression keeps fatal errors and `InterruptedException` as the primary error, and self-suppression falls through to `finally throw`.
  - This fixes the old case where a close failure hid an interruption (`read-close.png`).
- [x] **Spark cancellation semantics.** This is reasoning from Spark 3.5 behavior; Spark sources are outside the scope roots and I did not inspect them.
  - `Task.kill` marks the `TaskContext` before it interrupts the thread. So storage layers that clear the interrupt flag are still caught by `taskCancelled` or by the `checkCancellation()` after the read.
  - With `interruptOnCancel=false`, both failed and successful reads end in `TaskKilledException`.
  - `InterruptedException`, raw or wrapped, is reported as KILLED when a kill reason exists and does not stop the executor. The executor clears the self-set interrupt flag when the task finishes.
  - When the code runs on the driver, `TaskContext.get()` is null and the check handles that safely.
- [x] **UDF contract and sanitization.** In `createMessagesUDF` in `OpenAIPrompt.scala`, `Try` keeps only non-fatal errors row-local. `TaskKilledException` and `InterruptedException` escape. Row-local messages contain only the attachment file name.
- [x] **Test robustness.**
  - Gates use bounded 30-second waits.
  - Cleanup releases gates, cancels the job group and shuts down the executor, and `Active.remove` runs in a `finally` block.
  - `completed.await` happens before `taskFailure` is read.
  - The injected FileSystem is closed from the cache, and `unexpected` is asserted empty.
- [x] **Residual risk accepted as a documented trade-off, not a defect.** A plain `InterruptedIOException` timeout with no typed cause (for example, OkHttp/Okio "timeout") is treated as an interruption, so it fails the task instead of producing a row-local error. This is the stated policy at line 49 and follows Hadoop FileSystem conventions. From known connector behavior (not inspected here), ABFS, WASB, S3A, GCS and HDFS all report timeouts as typed `*TimeoutException` exceptions.
- [x] **Build compatibility.** `java.net.http.HttpTimeoutException` in the test fixture needs JDK 11 or later. Neither `pipeline.yaml` nor `templates/sbt_cache.yml` shows a JDK 8 compile stage, and the local-setup baseline is JDK 11.
- [x] **No contract changes.** `OpenAIAttachmentUtils` is `private[openai]`. The PR changes no public JVM signature, serialized Param, dependency or release setting, and adds no native AIPrompt or optional recovery code.
- [ ] **Tests not executed by this reviewer.** The review contract is read-only. The caller's evidence (compile, Test/compile, 89 tests with 0 skips, and both scalastyle gates) is context only, not something I verified.