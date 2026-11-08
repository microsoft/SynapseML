---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:34:52Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "7e31a7b407e4e283d728a56880f8b9b51d3e68b1a6bf3376fefa3baf0e5c2985"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2806/task-unknown-attempt-1-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full (both files, all hunks) and compared it with `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`.
- [x] Confirmed the five public methods keep their signatures and that both private maps are touched only inside those `synchronized` blocks. `HasGlobalParams.transferGlobalParamsToParamMap` reads through `getParam`.
- [x] Checked master toolchain in `build.sbt` (`scalaVersion` 2.12.17, Spark 3.5.0, `core` inherits `ThisBuild / scalaVersion`) and Spark 4 guidance in `.github/skills/synapseml-branches/references/branch-spark4-common.md` (Scala 2.13).
- [x] Confirmed there is no `scala.util.Using` backport under `core/src/main/scala/scala/util/Using.scala` or `core/src/test/scala/scala/util/Using.scala`, and no dependency in `build.sbt` provides that class.
- [x] Reviewed the probe for command injection, secret handling, and cleanup: `ProcessBuilder` is an argument list, `mode` is a fixed `Seq`, and the child is destroyed in `finally`. No production deserialization or signature change.
- [ ] Did not execute tests. This was a read-only review. Spark 4 passes do not prove master compilation.

## Issues

### Issue 1: Concurrency suite does not compile on master Scala 2.12
- **Severity**: High
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 17, 181
- **Description**: The new test imports `scala.util.Using` and calls `Using.resource` to write the classpath JAR. That type was added in the Scala 2.13 standard library. Master pins Scala 2.12.17 in `build.sbt`, and `core` does not override it. There is no in-repo backport. `core/Test/compile` therefore fails on the unresolved symbol, so the whole `VerifyGlobalParams` file, including the existing tests and the new null-preservation test, cannot be compiled or run on the target branch. Spark 4 trees use Scala 2.13, where this import exists; those passes do not cover master.
- **Risk**: The synchronization fix is not guarded on the branch this PR targets, and master CI cannot compile this suite. A reported current-head `VerifyGlobalParams` pass is not explained by the pinned master toolchain.
- **Suggested Fix**: Remove `import scala.util.Using`. Close the JAR with Scala 2.12 `try`/`finally` (do not bump master Scala to import the port toolchain):

```scala
val jarOut = new JarOutputStream(new FileOutputStream(classpath), manifest)
try {
  ()
} finally {
  jarOut.close()
}
```

The production `synchronized` wrappers should stay as they are.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not actionable; disproved by target-classpath evidence
- **What changed**: No source or dependency change.
- **Why**: The existing transitive scala-collection-compat artifact provides `scala.util.Using` on Scala 2.12.
- **How verified**: See the retained Driver disposition below for the resolved JAR contents, successful master compilation and real Windows Java 8 test execution.

## Driver disposition

Issue 1 is **not reproduced and is refuted by the resolved target classpath**.
The original review above is preserved unchanged.

On the unchanged PR head, Temurin Java 8 and Scala 2.12.17 completed
`core/Test/compile` successfully, then
`core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams` passed
all 11 tests with no skips, including both isolated child-JVM concurrency tests.
`show core/Test/dependencyClasspath` includes the existing transitive artifact
`scala-collection-compat_2.12-2.7.0.jar`. Inspection of that JAR confirms it
contains `scala/util/Using.class`, `scala/util/Using$.class`, and its resource
management implementation. The import does not depend on the Scala 2.13
standard library or an in-repository backport.

No product, test, dependency, or toolchain change is needed for this finding.
This rebuttal does not rewrite the review's `ISSUES_FOUND` verdict into a clean
result. The clean-only review gate remains pending a fresh qualified pass with
the actual dependency provenance supplied as caller evidence.