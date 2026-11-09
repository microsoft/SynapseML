---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 2
timestamp: "2026-11-07T12:10:30Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "148bdec4a0ad7954e862f08fcb54ee9609282aa680895aeb38ec1ac125a9f8d2"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-7-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 214 lines of the assigned frozen diff in the required ranges, [1,150] and [151,214]. I compared the before and after versions of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`. The files in the checkout match the after version in the diff.
- [x] **Guidance and scope.** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the PR targets master), `.github/skills/code-review/SKILL.md` and `.github/skills/scala-code/SKILL.md`. I read versions from `build.sbt` (Scala 2.12.17, Spark 3.5.0) and from `environment.yml`. The PR does not change public signatures, serialized Params, dependency pins, workflows or `pipeline.yaml`.
- [x] **Production locking is complete.** In `GlobalParams.scala`, all five public methods (L20–38) use the same lock. The two private maps (L13–14) are only used inside those methods, so every access is protected.
  - `getParam` reads both maps while holding that one lock, so the two-map lookup is atomic.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (L43–52) only reaches the maps through `getParam`.
  - While the lock is held, the only outside code that runs is key `hashCode`/`equals`. It never takes another lock, so there is no lock-ordering deadlock risk.
  - Locking each call separately is the right scope for a change that must not alter behaviour.
- [x] **Fits the repo's existing pattern.** It matches how `SharedSingleton` uses `synchronized` in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SharedVariable.scala` (L40–48).
  - One shared lock is a better fit than separate concurrent maps, because both maps must be read together.
  - Keeping `mutable.Map` keeps `Some(null)` working the same way on Scala 2.12 (master) and Scala 2.13 (the Spark ports). The new null test (`VerifyGlobalParams.scala` L191–197) locks that behaviour in.
  - The plain `synchronized` locks the `GlobalParams` object itself, not the private map that is also named `GlobalParams`. All five methods use it the same way.
- [x] **Callers.** `HasOpenAISharedParams` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` registers a param every time a stage is built or copied, possibly from py4j threads. `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` makes one `GlobalParams` call per set, get or reset. Both become thread-safe without any API change. No caller does a check-then-set that would need a wider lock.
- [x] **Test design.** The separate-JVM probe (`VerifyGlobalParams.scala` L19–95, L208–254) is justified:
  - Registrations can never be removed, and a corrupted map could hang the JVM. Running them in the shared JVM would break the suite's own no-leak rule (L104–105).
  - Temporary files go in `TestBase.tmpDir`, which `afterAll` deletes (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`).
  - The launcher only uses Java 8 APIs. When the app class loader is not a `URLClassLoader`, it falls back to `java.class.path`, which follows the oldest-JDK and class-loader guidance in `branch-spark3p5.md`.
  - Child output goes to a file, so a full pipe cannot block the child.
  - The child is cleaned up within a time limit before the interrupt flag is restored, and termination is asserted before `exitValue()` is read.
- [x] **Resource handling.** Using `scala.util.Using.resource` instead of the repo's `StreamUtilities.using` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala` L26–34) is the better choice here:
  - The repo helper wraps results in `Try`, and an exception from `close()` can hide the original one.
  - `Using.resource` keeps the original exception, attaches later ones as suppressed, and gives priority to `InterruptedException`. The interrupt-restoring cleanup relies on this.
  - The nested `Using` also closes the file stream if the `JarOutputStream` constructor fails.
  - That `Using` compiles on Scala 2.12 through the existing transitive `scala-collection-compat` comes from the evidence you supplied. I did not check it myself because it is outside my read scope.
- [x] **Conventions.**
  - Imports are explicit (no wildcards) and grouped like nearby files.
  - The null test avoids a `null` literal by using `Option.empty[String].orNull`.
  - Lines are within 120 characters and the file is about 255 lines.
  - The suite extends `TestBase`.
  - Tests cover the normal case, invalid arguments (L199–206) and existing null behaviour.
  - The three new comments (L208, L218, L232) are accurate and explain choices that are not obvious from the code.
- [x] **Considered and not raised:**
  - Locking on the public `GlobalParams` object instead of a private lock object: no other lock is ever taken while it is held.
  - Mixing `==` and `===`, and using `require` to check results inside the probe: cosmetic only.
  - The probe's `main` being included in the published tests jar (`Test / publishArtifact := true`): it has no side effects unless someone runs it.
  - Defaults being shared across the whole JVM, and registrations growing with every new stage: both existed before this PR and are out of scope.
- [ ] **No builds or tests run.** This was a read-only review. I relied on your evidence: the current `core/Test/scalastyle` pass, and an earlier 12/12 `VerifyGlobalParams` pass on the same executable code with Java 8 on master and Java 17 on both active Spark ports. CI on the final commit is still pending, as you noted.