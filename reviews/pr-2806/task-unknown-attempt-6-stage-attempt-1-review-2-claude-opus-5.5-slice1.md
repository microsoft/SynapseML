---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 2
timestamp: "2026-11-07T10:48:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f5c5580503eeaf5aa5e0ff1a108bf3b514d55509e5c987681a25b9ff858877fc"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-6-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read every line of the assigned frozen diff in exactly two reads, lines 1–150 and 151–211. That covers all four hunks: `GlobalParams.scala` at `@@ -17,23 +17,23 @@`, and `VerifyGlobalParams.scala` at `@@ -8,6 +8,92 @@`, `@@ -82,10 +168,13 @@` and `@@ -98,4 +187,66 @@`.
- [x] The after-state in the diff matches the current files exactly:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 12–39.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, lines 11–252.

  The before-state has the same five methods without `synchronized`.
- [x] All five methods use one lock. Inside `object GlobalParams`, a bare `synchronized` locks the object itself, not the private map that is also named `GlobalParams` (line 14). `getParam` (lines 32–34) reads both maps under that one lock. Nothing runs under the lock except key `hashCode`/`equals`.
- [x] Method names, type parameters and return types are unchanged. Public JVM signatures and serialized Params are preserved, as AGENTS.md requires.
- [x] Existing behaviour is preserved:
  - The maps are still `mutable.Map`, so stored null values are kept. The new test at lines 191–197 checks this.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 41–53) is unchanged.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` only makes single set/get/reset calls, so it doesn't need several calls to happen as one locked step.
- [x] The tests follow repository conventions:
  - The suite extends `TestBase`.
  - `tmpDir` is a per-suite temporary directory that `afterAll` deletes (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`).
  - Loading the probe object has no side effects. It creates threads and writes to `GlobalParams` only inside `main`, after `require` checks the arguments. So the in-process argument test (lines 199–206) can't leak threads or state.
  - The probe class name comes from `getClass.getName`, so a rename can't break it. The expected count is shared through `private[param] val TotalEntries`.
- [x] `scalastyle-test-config.xml` turns on the null check, which explains `Option.empty[String].orNull` at line 192. That fits `.github/skills/scala-code/SKILL.md`, which prefers refactoring over disabling style rules. It also sets 120-character lines, 50-line methods and capitalized object-field names. `Workers`, `Entries`, `Rounds` and `TotalEntries` follow the naming rule, and the import grouping matches `TestBase.scala`.
- [x] Build and branch checks:
  - `build.sbt` uses Scala 2.12.17 and Spark 3.5.0, with `Test / parallelExecution := false` and `Test / publishArtifact := true`.
  - The PR changes no dependencies, workflows or pipeline.
  - It targets `master`, as `.github/skills/synapseml-branches/references/branch-spark3p5.md` expects.
  - The process and class-loader APIs the launcher uses exist on both Java 8 and Java 17. It also falls back to `java.class.path` when the class loaders don't expose their paths.
- [x] I considered these and did not report them:
  - **`scala.util.Using`:** On Scala 2.12 it comes from `scala-collection-compat`, which `build.sbt` doesn't declare; another library pulls it in. `TestBase.scala` already relies on the same kind of indirect library. Scala 2.13 port branches have it built in. It also handles exceptions better than `StreamUtilities.using` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`), which returns `Try` and only catches `Exception`.
  - **Locking on the object:** This is normal Scala practice.
- [ ] I did not run builds, tests or style checks; this was a read-only review. I relied on the validation results you provided: Java 8 core compile and Test compile passed, VerifyGlobalParams passed 12/12, both style checks passed, and the Java 17 ports passed.
- [ ] I did not search the whole repository, for example for other `GlobalParams` callers or other uses of `scala.util.Using`. The read rules for this review allowed only viewing files by exact path.

## Issues

### Issue 1: The child-JVM tests rely on three non-obvious choices that aren't explained
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 208–251, especially 217–223 and 229–243; the probe entry point is lines 19–95
- **Description**: The production change is small and consistent. The test, however, builds a classpath by hand, writes a JAR that contains only a manifest, starts a child JVM, and handles forced shutdown and interrupts, with no comments. Three choices matter and aren't obvious:
  1. **Why a separate JVM (lines 208–209 and 19–95):** The probe registers 64,000 params, and `GlobalParams` has no way to remove them (`GlobalParams.scala`, lines 12–39). Without the fix, the run can corrupt or hang the shared maps. A child process keeps that away from other suites in the same test JVM and can be killed after a timeout.
  2. **Why a manifest-only JAR (lines 217–223):** The full test classpath can exceed the Windows command-line length limit, and Java 8's `java` launcher can't read arguments from a file (`@argfile`).
  3. **Why cleanup is ordered this way (lines 229–243):** `Using.resource` with a one-off `AutoCloseable` kills the child and waits for it before the outer catch restores the interrupt flag. If that order is reversed, `process.waitFor(10, SECONDS)` fails immediately. The bounded wait is skipped and the restored interrupt is lost.

  The file already has a comment explaining its in-process isolation rule (lines 104–105). The new, more complex mechanism has none, which conflicts with the file's own practice and the repository rule to comment code that needs clarification.
- **Risk**: Natural-looking simplifications would compile and could still pass on Linux:
  - Passing a joined classpath with `-cp`.
  - Running the probe in the same JVM.
  - Flattening the cleanup into a plain `try/catch/finally`.

  These could break Windows runs, hang or pollute the shared test JVM if the bug comes back, or leave a killed child holding `tmpDir` files when `afterAll` deletes them. The impact is limited to test maintenance; production code and the API are unaffected.
- **Suggested Fix**:
  - Add a short comment above line 208 covering points 1 and 2.
  - Add one line near lines 229–231 saying cleanup must finish before the interrupt flag is restored.
  - Optionally, add a one-line Scaladoc to `GlobalParamsConcurrencyProbe` saying it is the child-JVM entry point, or move the launch-and-wait code into a private helper.

  No production change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Added three short comments at the child-JVM test loop, manifest construction and cleanup boundary.
- **Why**: Explain isolation of permanent registrations and possible hangs, the Windows/Java 8 classpath constraint, and why bounded cleanup must finish before interrupt restoration.
- **How verified**: Removing exactly the three added comment lines reproduces the previously tested source byte-for-byte; production and executable test code are unchanged. `core/Test/scalastyle` passed for the current file. Behavioral tests were not rerun for this comment-only change. The matching executable source had already passed `core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams` with 12/12 tests and zero skips, both compiles and both style gates on standalone and integrated master, and on both active integrated Spark ports. The same three comments were inherited into the dependent snapshots.