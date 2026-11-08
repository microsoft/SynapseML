---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:57:58Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "d1c2967a9dcbb57faa4988e7c99685fc7c641b89e92e4a9cd87df08ea0f19bf4"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2806/task-unknown-attempt-2-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full (147 lines, both before and after, two files only).
- [x] Read `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`. Every access to `ParamToKeyMap` and the private value map is inside one `synchronized` block on the `GlobalParams` object, including the two-map `getParam` lookup. Public signatures and null-capable `mutable.Map` semantics are unchanged. `HasGlobalParams.transferGlobalParamsToParamMap` only reads through `getParam`.
- [x] Read `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, including `GlobalParamsConcurrencyProbe` and the null and isolated-JVM tests.
- [x] Read `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`. `tmpDir` is a unique temp directory deleted in `afterAll`. `customNormalize` treats Windows as a supported test platform, and `LinuxOnly` is opt-in. These new tests are not tagged `LinuxOnly`.
- [x] Read `build.sbt` (Spark 3.5.0, Scala 2.12.17), `environment.yml`, `AGENTS.md`, and the master branch and code-review skills. No signature, serialization, RDD, or dependency-pin change in this diff.
- [x] Caller-provided validation, not re-executed here: baseline child-JVM probes failed and passed after the fix; current-head `VerifyGlobalParams` 11/11 on Temurin Java 8 / Scala 2.12.17 and on Azure, with no skips. Those runs were not identified as Windows. Transitive `scala-collection-compat` supplying `scala.util.Using` was taken as a caller fact.
- [ ] Did not execute tests, open dependency caches, or read sibling review artifacts (read-only contract).

## Issues

### Issue 1: Child-JVM launcher always executes `bin/java`, which is not the Windows executable
- **Severity**: Medium
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 186-188
- **Description**: The probe is started with `new File(java.home, "bin/java")`. On Windows JDKs, including Temurin 8, that path is not a file; the executable is `bin\java.exe`. `ProcessBuilder` passes this absolute path to `CreateProcess`. Because the path contains a directory, Windows does not append `.exe`, so `start()` throws `IOException` before either concurrency mode runs. Linux `bin/java` is unaffected, which is consistent with the reported Azure pass. The tests are not marked `LinuxOnly`, and `TestBase` already special-cases Windows, so this is a supported-platform failure of the new regression coverage.
- **Risk**: Windows runs fail both isolated-JVM tests without exercising registration or value synchronization. A green Linux run can hide that the new proof does not run on a platform this repo already supports.
- **Suggested Fix**: Resolve the launcher by existence, and keep the rest of the command unchanged. Prefer `bin/java.exe` when that file exists, otherwise `bin/java`. Do not shell out. After that, run these two tests on Windows; absolute `file:` manifest `Class-Path` entries are outside the JAR spec and still need a real Windows launch to confirm they resolve.

### Issue 2: Probe cleanup asserts inside `finally` and can drop the timeout result
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 190-195
- **Description**: `process.waitFor(45, SECONDS)` is in `try`, and the only forced-termination check is `assert(process.waitFor(10, SECONDS), ...)` inside `finally`. If the 45-second wait is interrupted, or the process is still alive and the 10-second wait fails, the `finally` assertion throws before `completed` is asserted and before the redirected log is included. That also suppresses the original interrupt or timeout outcome.
- **Risk**: A hung or slow probe fails with "did not terminate" and no probe output, or an interrupt is lost. The test still fails, but the concurrency failure that the log was meant to capture is hidden. Cleanup should not be the assertion path.
- **Suggested Fix**: In `finally`, only `destroyForcibly()` and wait, without asserting. After cleanup, read the log once and assert termination, the 45-second result, exit code 0, and `GLOBAL_PARAMS_CONCURRENCY_OK`, each with that output. Restore the interrupt status if `waitFor` throws `InterruptedException`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not actionable; disproved by real Windows execution
- **What changed**: No executable-path change.
- **Why**: The exact extensionless executable argument already launches successfully through `ProcessBuilder` on the tested Windows JDKs.
- **How verified**: See the retained Driver resolution below for Windows 11 / Java 8 reports proving both isolated-JVM modes actually executed without skips.

### Issue 2
- **Status**: Fixed
- **What changed**: Cleanup uses `Using.resource`; interruption is restored and termination assertions run after loading the redirected output.
- **Why**: Cleanup failures must not hide the original wait failure, interrupt status or diagnostic output.
- **How verified**: See the retained Driver resolution below for causal timeout/interruption fault injection and passing compilation, full suite and style gates.

## Driver resolution

The original review above and its `issues_found` verdict are retained.

**Issue 1 is refuted by the existing unmodified-source Windows run.** The
reported 11/11 local results were produced on Windows 11 with Temurin Java
8u504, not only on Linux. The test report records `os.name=Windows 11`, zero
skips, and successful execution of both isolated child-JVM modes. Those tests
used the exact extensionless `bin/java` argument and manifest classpath in the
reviewed patch. No executable-path change is warranted by this finding.

**Issue 2 is addressed in the pending patch.** Process cleanup now uses
`Using.resource`, which retains the original wait failure and suppresses a
secondary cleanup exception. Interruption is restored before rethrowing.
Termination assertions run after reading the redirected output, so a failed
forced termination includes the probe log rather than replacing it with a
message from an assertion inside `finally`.

Fault injection using method bodies extracted from the original and corrected
source reproduced missing termination output, lost interrupt status, and a
cleanup interruption replacing the original interruption. All three corrected
cases passed; the double-interruption case retained the original exception
with the cleanup exception suppressed.

On Windows 11 with Temurin Java 8 and Scala 2.12.17, the pending patch passed
`sbt core/compile core/Test/compile
"core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams"
core/scalastyle core/Test/scalastyle`: 11 tests passed, including both child
JVMs, with no failures or skips and no style errors or warnings.

Attempt 2 ends because reviewed source changed. These dispositions do not
convert its original findings verdict into a clean gauntlet or replace the
next complete pre-commit pass and current-head CI.