---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 2
timestamp: "2026-11-09T04:35:32Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "19850a0ea11b7d23a355d4ab163f951267189aa792b8a3f95d1d07afdc516584"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-11-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 597 lines of the assigned frozen diff, using exactly the required ranges [1, 150], [151, 300], [301, 450] and [451, 597]. Every read returned complete content.
- [x] Read the guidance files by exact path: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, and the linked `.github/skills/scala-code/SKILL.md`. The target is `master`. `build.sbt` gives Spark 3.5.0 and Scala 2.12.17; `environment.yml` pins pyspark 3.5.0.
- [x] Checked the helper the PR replaces:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala:27-33` already used `new Configuration()` by default, so Hadoop configuration behavior is unchanged.
  - In `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala:26-34`, an exception from `close()` replaces the read's own failure.
  - So switching to `Using.resource` is required for correctness. Issue 1 is only about the missing explanation.
- [x] Read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` to check how it handles attachment errors:
  - `createMessagesUDF` (~485-501) lets `InterruptedException` escape (it is not `NonFatal`) and rethrows `TaskKilledException`.
  - Every other failure becomes a row-local error built from `getMessage`.
  - `wrapFileToMessagesList` reads the file before choosing the API.
  - The PR's split between errors that escape and errors that stay on the row matches this.
- [x] Compatibility and conventions:
  - `OpenAIAttachmentUtils` is still `private[openai]`, and the `readAttachment`/`prepareFile` signatures are unchanged.
  - No changes to Params, serialization, build, dependencies, codegen or Python, and no RDD use.
  - Main code already uses `scala.util.Using` (in `OpenAIPrompt.scala`).
  - The test-only imports (httpclient 4 via httpmime, httpclient5 5.1.3) are dependencies already declared in `build.sbt`.
- [x] Reviewed the error-classification design:
  - The new helpers are private and focused.
  - The exception-graph walk tracks visited nodes by identity and is `@tailrec`, so cycles and deep chains are safe; the 2048-node cyclic-graph tests cover this.
  - Fatal errors other than `InterruptedException` propagate unchanged.
  - The rule that treats class names ending in `TimeoutException` as timeouts is documented as handling relocated SDK classes.
  - The `checkCancellation()` calls before and after the read keep cancellation checks in one place.
- [x] Reviewed the test structure:
  - The fixture now lives in its own file, named after its public filesystem class.
  - The gate test's cleanup uses `Using.resource` and runs release, then cancel, then shutdown.
  - Thread interrupt flags are cleared in `finally`.
  - The suite stays under the 800-line limit (about 794 lines).
- [x] The caller's validation results (compile, Test/compile, 69 tests with 0 skips, and production and test scalastyle, on JDK 11 / Spark 3.5.0 / Scala 2.12.17) are taken as provided evidence only. I did not run any commands or tests.

## Issues

### Issue 1: The inline read bypasses the shared `BinaryFileReader` helper without saying why
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 301-304 (compare core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala:27-33 and core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala:26-34)
- **Description**: The PR replaces `BinaryFileReader.readSingleFileBytes` with an inline `Using.resource(path.getFileSystem(new Configuration()).open(path))(IOUtils.toByteArray)`. The change is correct:
  - The shared helper reads through `StreamUtilities.using`. There, an exception from `close()` (run in `finally`) replaces the read's own failure, even an `InterruptedException`.
  - With the old helper, the `read-close.png` scenario would end up as a row-local `AttachmentReadException`.
  - `Using.resource` keeps the read failure and attaches the close failure as suppressed, which `isInterruptedRead` depends on.

  However, nothing at the call site explains this. The PR does explain its other non-obvious choices, such as the timeout naming rule and the walk over suppressed exceptions.
- **Risk**: The repo's Scala guidance asks contributors to reuse existing helpers. Someone tidying up could switch back to `BinaryFileReader` and bring back interruptions hidden by cleanup failures. Only one synthetic fixture case (`read-close.png`) guards against that, and it could easily be mistaken for an over-specified test.
- **Suggested Fix**: Add a one-line comment above the call, for example: `// Not BinaryFileReader: StreamUtilities.using lets close() failures replace the read failure; Using.resource keeps it and suppresses the close failure.` Fixing the shared helper could be tracked separately; it is out of scope here.

### Issue 2: The new scaladoc relies on an undefined "legacy APIs" term
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 25
- **Description**: The phrase "legacy APIs keep this sanitized error row-local" implies there is also a non-legacy consumer that treats this exception differently. On master there is not:
  - The only consumer in the call path is `OpenAIPrompt.createMessagesUDF` (`OpenAIPrompt.scala` ~485-501). It handles this exception exactly like `AttachmentReadException`.
  - Both `chat_completions` and `responses` are current APIs.
  - In the production sources I read, "legacy" otherwise appears only in the `dropPrompt` description ("legacy models", ~line 176), which means something unrelated.

  The comment also describes how one consumer behaves rather than what the exception type signals.
- **Risk**: Maintainers may search for a non-legacy path that doesn't exist, or confuse this with legacy models. It also ties a production type's contract to a native-AIPrompt design that isn't on master and that this PR deliberately leaves out.
- **Suggested Fix**: Describe what the exception signals, for example: `/** Storage-level cancellation (CancellationException), distinct from thread interruption and Spark task cancellation; OpenAIPrompt reports it as a sanitized row-local error. */`

### Issue 3: The string-keyed failure fixture can lose timeout coverage without any test failing
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala (used by cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala)
- **Line(s)**: Fixture 30-33 (`TimeoutNames`), 63-96 (`cleanupFailure`, `storageTimeout`, `wrappedFailure`), 98-144 (`open`; catch-all branch 141-143). Suite ~416-418, ~426, ~556, ~576-577.
- **Description**: More than 20 scenarios are selected by file name and spread across five places: three `Option` helpers, inline `if (id == ...)` checks, and the stream-building branches. The expected outcomes are kept in separate hand-maintained lists: `TimeoutNames` in the fixture and two different interruption lists in the suite.
  - Any name that matches no scenario falls into the catch-all `IOException` branch. That branch also serves `ordinary.png` and the gate IDs.
  - Its result, a sanitized `AttachmentReadException`, is identical to a correctly classified timeout.
  - So a typo or rename in `TimeoutNames`, or in a case label, still passes while only testing the ordinary path. Drift in the interruption names or `cancelled.png` would fail loudly; drift in the timeout names would not.
  - The names are also misleading. `relocated-*`, `nested-*` and `httpclient5-timeout.png` are silently wrapped by `wrappedFailure`. `timeout-close-interrupted.png` fails when the file is opened, unlike the `close-*` scenarios, which fail when the stream closes.
- **Risk**: The tests behind the PR's promise that ordinary timeouts stay row-local could stop covering timeouts without any failure. Adding a scenario safely also needs matching edits in two files.
- **Suggested Fix**:
  - Keep the file-name keys (Hadoop only passes the path), but make a single scenario table the source of truth, for example a `Map[String, Scenario]` holding each scenario's expected classification and how to build its failure.
  - Derive `TimeoutNames` and the interruption lists from that table, and have `open` read from it.
  - Restrict the catch-all branch to `ordinary.png` and gate IDs. Record any other ID in a queue the tests assert is empty; throwing would not work, because thrown errors get sanitized.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Added a concise call-site comment explaining that `Using` preserves cleanup failures needed for interruption classification, unlike `BinaryFileReader`.
- **Why**: Records the correctness reason for intentionally not using the shared helper without expanding this PR into a shared-resource-helper change.
- **How verified**: Inspected the comment against the read/cleanup contract. The exact command `sbt "set cognitive / Test / fork := false" cognitive/compile cognitive/Test/compile "cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite" cognitive/scalastyle cognitive/Test/scalastyle` passed: 69 tests, zero skips, both compilation and style gates.

### Issue 2
- **Status**: Addressed
- **What changed**: The scaladoc now names storage `CancellationException` and the `OpenAIPrompt` row-local behavior instead of undefined "legacy APIs".
- **Why**: Makes the standalone exception contract understandable without depending on the separate native AIPrompt contribution.
- **How verified**: Inspected the comment against `OpenAIPrompt` handling; the same 69-test validation command above passed.

### Issue 3
- **Status**: Addressed
- **What changed**: The ordinary fallback records unknown scenario IDs in a per-filesystem queue, and the filesystem test helper asserts that queue is empty after each test body. Only `ordinary.png`, `flagged.png`, and registered gate IDs may use that fallback without a failure. The small fallback helper keeps cyclomatic complexity within the existing limit.
- **Why**: Unknown names can no longer silently pass as sanitized ordinary IO. This addresses the concrete false-green risk without a larger scenario-table refactor or changing the production error contract. No production failure is claimed.
- **How verified**: All 69 multimodal cases passed with the new unknown-ID assertion enabled, with zero skips, successful compile/Test compile, and both scalastyle gates. The initial validation passed all tests but failed the existing complexity gate; after extracting the fallback helper, the complete command above passed.