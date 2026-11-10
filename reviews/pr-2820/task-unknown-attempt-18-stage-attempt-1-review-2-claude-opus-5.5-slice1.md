---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 2
timestamp: "2026-11-09T20:22:58Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3d747bce8f2ddcad62433bf3b8465b55fbe3a9f8a692ad1219959029ef065df3"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2820/task-unknown-attempt-18-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff fully read.** Read every line of the assigned frozen diff (681 lines) in the frozen ranges [1–150], [151–300], [301–450], [451–600], [601–681]. It covers three files:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
  - the new `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- [x] **Repository guidance applied.**
  - Read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/scala-code/SKILL.md` and `.github/skills/synapseml-local-setup/SKILL.md`.
  - The master reference says: "Shared helpers must use APIs supported by the oldest JDK that actually builds them."
  - Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17). `environment.yml` has no JDK pin.
- [x] **Public surface is unchanged.**
  - `readAttachment` stays `private[openai]` and every new helper is `private`.
  - The signature of `prepareFile`, the Params and their serialized shapes are unchanged.
  - There are no dependency, pipeline, release, native AIPrompt or optional-recovery changes.
- [x] **Matches the UDF contract.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `createMessagesUDF` lets `InterruptedException` escape `Try`, rethrows `TaskKilledException`, and turns other failures into row errors.
  - The new catch order in `OpenAIAttachmentUtils.scala` (lines 33–104) matches that contract: classified interruption, then thread flag, then task cancellation, then `CancellationException`, then generic `AttachmentReadException`.
  - The sanitized exceptions carry no cause or path.
- [x] **The inline read is a justified local deviation.**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/binary/BinaryFileReader.scala` lines 27–33 do `StreamUtilities.using(fs.open(path))(IOUtils.toByteArray).get` with a default `new Configuration()`.
  - `OpenAIAttachmentUtils.scala` lines 316–320 copy that exactly. The only change is that `scala.util.Using` keeps the original failure and attaches the close failure as suppressed.
  - The change is documented in a comment. Leaving the shared core helper alone avoids an out-of-scope refactor.
- [x] **Classifier traced against every fixture scenario.**
  - Scenarios covered: 16 `TimeoutNames`, 11 `ClearedInterruptNames`, the `InterruptedNames` set, `cancelled.png`, `ordinary.png`, cyclic graphs with and without causes, and a fatal `LinkageError`. All test expectations match the code.
  - Fatal errors (`!NonFatal`) are checked only at the top level and rethrown unchanged.
  - Both traversals terminate because of the identity sets: each node is visited at most once per set.
  - Matching timeout classes by name suffix follows how Hadoop connectors recognize relocated timeout classes.
- [x] **JDK used by CI.**
  - `templates/update_cli.yml` lines 7–11: `JavaToolInstaller@0`, `versionSpec: '8'`, `PreInstalled`.
  - `pipeline.yaml` UnitTests job (job at line 807, steps at lines 953–979) applies that template, then runs `tools/ci/sbt_retry.sh setup` and root `sbt testOnly`.
  - `build.sbt`: `setupTask` (lines 409–416) compiles `Compile` and `Test` for every project, including `cognitive`. `root` aggregates `cognitive` (lines 389–397). There is no `javaHome` or `-release` override.
- [x] **No other newer-JDK APIs.** Scanned the changed suite (current file lines 1–739) and the fixture for APIs newer than JDK 8. The only one is `java.net.http.HttpTimeoutException`.
- [ ] **No builds or tests run by this reviewer** (read-only contract). The caller's evidence (compile, Test/compile, 89 tests with 0 skips, scalastyle) used JDK 11 only, so it does not cover the JDK 8 stage.

## Issues

### Issue 1: The test fixture uses a JDK 11-only API, but master CI compiles tests with JDK 8
- **Severity**: High
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala
- **Line(s)**: 130 (inside `otherTimeout`, lines 124–139); the scenario names are on lines 35 and 40
- **Description**: `otherTimeout` creates `new java.net.http.HttpTimeoutException(...)` for `http-timeout.png` and `http-timeout-close-interrupted.png`. The `java.net.http` package exists only in JDK 11 and later.
  - On master, `templates/update_cli.yml` pins JDK 8. The UnitTests job applies that template before `tools/ci/sbt_retry.sh setup`.
  - `setupTask` compiles `Test` for every project, including `cognitive`. Root-level `testOnly` and codegen tasks (which depend on `(Test / compile)` in `project/CodegenPlugin.scala`) also compile these sources.
  - Under JDK 8, scalac stops with `object http is not a member of package java.net`.
  - This breaks the master branch reference's oldest-JDK rule. Local setup defaults to JDK 11, and the supplied evidence used only JDK 11, so the problem was never exercised.
- **Risk**: The cognitive test sources won't compile in the JDK 8 pipeline stages.
  - Every UnitTests matrix leg would fail at "Setup repo", including unrelated packages such as lightgbm and vw.
  - Other jobs that use this template and compile test sources would fail too.
  - Full CI would be blocked for this PR, and for master if it merges. The PR's new scenarios would never run in CI.
- **Suggested Fix**: Replace the class with a JDK 8-available `IOException` timeout type that is not an `InterruptedIOException`. Either option works:
  - `java.nio.channels.InterruptedByTimeoutException` (JDK 7+). Its name ends in `TimeoutException`, so `isTimeout` still matches. Its no-arg constructor leaves the cause unset, so `initCause` still works.
  - A test-local relocated type next to `RelocatedHttpClient`, for example `class HttpTimeoutException extends IOException("Synthetic private HTTP timeout")`.

  Optionally rename the two scenarios. Then run `cognitive/Test/compile` (or `sbt setup`) on JDK 8, or check the JVM version in the ADO UnitTests log. Don't change the CI JDK pin in this PR; AGENTS.md requires approval for pipeline changes.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Added a test-local `RelocatedHttpClient.HttpTimeoutException` extending `IOException` and used it instead of `java.net.http.HttpTimeoutException`. Production code, scenario names, cause and suppressed-exception assertions, and the complete 89-test suite are unchanged.
- **Why**: The original fixture did not compile on the JDK 8 used by master CI. A relocated timeout fixture exercises the same timeout-name classification without requiring JDK 11 or changing CI configuration.
- **How verified**: On JDK 8, the unchanged fixture first failed compilation with `object http is not a member of package java.net`; the updated fixture compiled successfully. On that same JDK, `cognitive/compile`, `cognitive/Test/compile`, `cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite`, `cognitive/scalastyle`, and `cognitive/Test/scalastyle` passed (tests run in-process with `set cognitive / Test / fork := false`): 89 tests, 0 failures, 0 errors, 0 skips. Qualification 151 confirms all prior test names, including the four HTTP-timeout public-path cases, and unchanged production source.