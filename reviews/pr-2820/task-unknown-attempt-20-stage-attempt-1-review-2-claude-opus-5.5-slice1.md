---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 2
timestamp: "2026-11-10T04:24:46Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "fd1c692c28c57937677772292eec319ec7798f805f22572151a5a60efe0f9e97"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-20-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 696 lines of the assigned frozen diff in the required chunks: [1–150], [151–300], [301–450], [451–600] and [601–696]. The diff touches three files:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala` (new)
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`

  I spot-checked the checked-out files and they match the diff.
- [x] **Guidance and versions:** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (master target) and `.github/skills/code-review/SKILL.md`.
  - Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
  - New code uses only APIs available in JDK 8 (`IdentityHashMap`, `Collections.newSetFromMap`, `InterruptedIOException`).
  - The test fixture defines its own `HttpTimeoutException` instead of using `java.net.http`, so master's oldest-JDK build still works.
- [x] **Consumer contract:** I checked `createMessagesUDF` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`.
  - Its `Try` lets a fatal `InterruptedException` escape.
  - It rethrows `TaskKilledException`.
  - It turns other non-fatal errors, including `AttachmentReadException` and `CancelledAttachmentReadException`, into row-local `(null, message)` results.

  This confirms the new Scaladoc on `CancelledAttachmentReadException` (`OpenAIAttachmentUtils.scala` L25) is accurate.
- [x] **Replaced read helper:** I compared the new code with the helper it replaces.
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` (`readSingleFileBytes`) also uses a fresh `new Configuration()`.
  - In `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`, `using` closes the stream in `finally`, so a close failure hides the original read failure.
  - The inline `Using.resource(...)(IOUtils.toByteArray)` read (`OpenAIAttachmentUtils.scala` L315–319) resolves the filesystem the same way. It keeps the original failure and attaches the close failure as suppressed.
  - The public core helper is left unchanged, which is in scope. `scala.util.Using` is already used in `OpenAIPrompt.scala` main code, so no new dependency is added.
- [x] **Classifier design (`OpenAIAttachmentUtils.scala` L33–104):**
  - `readAttachment` is still the single place where attachment read failures are classified, and its `private[openai]` signature is unchanged. All helpers are private.
  - Every exception-graph walk uses `@tailrec` and identity-set cycle guards. A node can be revisited at most once more, in the second mode.
  - Fatal errors are classified only at the top level, so a nested interruption cannot replace them.
  - The ordering of checks in `checkCancellation()` matches the ordering of the catch guards.
  - I traced these fixture cases by hand and each lands in its declared name list: `translated-pool-timeout`, `translated-concurrent-timeout`, `nested-interrupted-timeout`, `timeout-shared-close-interrupted`, and `read-close` (which relies on `Using` preferring the `InterruptedException` over the close failure).
- [x] **Repository conventions:**
  - No public JVM signature or Params changes.
  - No new dependencies: httpclient5 5.1.3 is declared in `build.sbt`, and httpclient 4.x comes in through the declared httpmime 4.5.13.
  - No RDD use in new code; the test uses the Dataset API (`spark.range(...).mapPartitions`).
  - The new file has a license header, and there are no wildcard imports.
  - File sizes are within the code-review limits: suite 799 lines, production 422, fixture 234.
  - Test cleanup uses `Using.resource`, as the code-review skill recommends.
- [x] **Test structure:** Moving the fixture into its own file is sound. Four things together stop test scenarios and their names from silently drifting apart: the shared `Location`, the `withAttachmentFailureFileSystem` helper, the `unexpected` guard, and the assertion that the set of `opened` names matches exactly. Injecting the fixture through Hadoop's filesystem cache is documented, and the filesystem is closed after each test.

  I considered three possible concerns and rejected them:
  - The identity-set construction is repeated three times, but each use is local and trivial.
  - Every scenario runs once per API mode, which matches the suite's existing convention.
  - Scenarios are selected by string name, but the checks above guard against mistakes.
- [x] **Plain `InterruptedIOException` treated as interruption:** This is an explicit contract, documented in the code comment at `OpenAIAttachmentUtils.scala` L49, and it matches the JDK's `InterruptedIOException` contract. I treated it as a deliberate product decision, not a departure from established patterns.
- [ ] **Not run:** I did not run any tests or builds; this was a read-only review. I did not reproduce the compile, test and scalastyle results you supplied.
- [ ] **Docs not checked:** I did not inspect website or notebook docs. The read profile forbids discovery tools, and no doc paths were in the source inventory.