---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 2
timestamp: "2026-11-09T11:06:35Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "05be2e155f05121d5cfadbdd824b1f7c814640079a90bfc710ec69114837deeb"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-17-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 667 lines of the assigned frozen diff in the required order: [1,150], [151,300], [301,450], [451,600], [601,667]. It changes three files:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (new)
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`

  I checked each one against the current source.
- [x] **Guidance and versions.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the target is `master`) and `.github/skills/code-review/SKILL.md`. Versions: Spark 3.5.0 and Scala 2.12.17 from `build.sbt`; pyspark 3.5.0 from `environment.yml`.
- [x] **Replacing `BinaryFileReader` doesn't change how files are read.**
  - The old helper (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala:27-33`) already used `new Configuration()` by default. The new inline read at `OpenAIAttachmentUtils.scala:315-319` keeps the same Hadoop config, filesystem cache use and byte handling.
  - Skipping the shared helper is justified, and a comment explains why. `StreamUtilities.using` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala:26-34`) closes the stream in `finally`, so an error from `close()` replaces the original read error. `Using.resource` keeps both errors.
  - Fixing the shared core helper instead would also change `readFromPaths`, which is outside this PR's scope.
- [x] **The exceptions match what `OpenAIPrompt` expects.**
  - `createMessagesUDF` (`OpenAIPrompt.scala:485-501`) wraps each row in `Try`. That lets `InterruptedException` escape, rethrows `TaskKilledException`, and turns every other error into a row-level error.
  - `readAttachment` (`OpenAIAttachmentUtils.scala:93-104`) produces exactly those cases:
    - an `InterruptedException`, with the thread's interrupt flag set again;
    - a `TaskKilledException`;
    - `AttachmentReadException` or `CancelledAttachmentReadException` with a cleaned message and no cause.
  - Fatal errors pass through the guarded `catch` unchanged, as the same object. The new doc comment at line 25 is accurate.
  - Every non-data-URL read goes through `prepareFile` and then `readAttachment` (`OpenAIPrompt.scala:610-614` and `634-655`). No read path skips this check.
- [x] **No public API or build changes.** All new helpers (`OpenAIAttachmentUtils.scala:33-91`) are `private`. The `private[openai]` signatures of `readAttachment` and `prepareFile` are unchanged. Nothing changes in public JVM signatures, Params, stages, codegen, dependencies, `pipeline.yaml`, workflows or release tooling.
- [x] **The design is consistent.**
  - Thread interrupt, then task kill, then storage cancellation, then ordinary failure: `checkCancellation` (lines 37-40) and the `catch` (lines 96-100) check in the same order.
  - The error-chain walks track visited exceptions by identity and use `@tailrec` loops, so loops in the chain or very long chains can't hang or overflow the stack. The 2048-node test chain (`AttachmentReadFailureFileSystem.scala:55-64`) covers both.
  - Timeouts are recognised by class names ending in `TimeoutException` (lines 42-48), and a comment explains why. No JDK interruption type matches that suffix. `InterruptedByTimeoutException` matches it and is correctly treated as a timeout.
- [x] **Test structure.**
  - The fixture moved to its own file (212 lines, licence header present). The suite is now about 793 lines, under the code-review skill's 800-line limit.
  - Filesystem setup is in one helper, `withAttachmentFailureFileSystem` (suite lines 42-53). It checks the filesystem type, closes the filesystem with `Using.resource` so the cached instance is dropped, and fails on unknown scenario names.
  - Each name in the three scenario lists (fixture lines 30-43) is handled by exactly one branch, and every branch appears in a list.
  - Interrupt flags and test gates are reset in `finally` blocks. Cleanup runs through `Using.resource`, so a failing cleanup check doesn't hide the original test failure.
  - Coverage runs from the helper through `createMessagesForRow` to `transform` in a real Spark job, as `AGENTS.md` asks.
- [x] **Portability.**
  - Only test code uses `java.net.http.HttpTimeoutException`, which needs JDK 11. The local-setup skill requires JDK 11, I saw no JDK override in the `pipeline.yaml` jobs or `templates/sbt_cache.yml` I read, and the caller tested on JDK 11.
  - Production code uses JDK 8 APIs plus `scala.util.Using`. `Using` already compiles in this module (`OpenAIPrompt.scala:30` imports it) and is in the Scala 2.13 standard library on the port branches.
  - Both HTTP-client timeout classes come from dependencies already in `build.sbt`.
- [x] **Considered but not raised:**
  - **Fixed kill reason.** The PR always throws `TaskKilledException("Attachment read canceled")`. Keeping Spark's own kill reason would need internal `TaskContext` APIs, and this message is the existing behaviour that the tests check.
  - **Where the error-classification logic lives.** It stays private in the attachment utility because there is only one caller. Moving it into core would be out of scope.
  - **Missing blank line before the comment at `OpenAIAttachmentUtils.scala:49`.** It's cosmetic, passes scalastyle and was left out on purpose so line numbers don't shift.
- [ ] **Not run by me.** This review was read-only, so I ran no builds or tests. I relied on the caller's local results: compile, Test/compile, 89 tests with 0 skipped, and scalastyle for main and test code, all on JDK 11, Spark 3.5.0 and Scala 2.12.17. I didn't look at remote CI.