---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 3
timestamp: "2026-11-07T12:07:05Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e6d368cd98a8f34776949d97fb8d00c2f08482e3d55a7f2eb9c06ac806e2b293"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2808/task-unknown-attempt-4-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 100 lines of the assigned frozen diff in one view (lines 1–100), covering the old and new versions of both files. The production hunk is `@@ -70,9 +70,22 @@` (+16/−3) and the test hunk is `@@ -11,11 +11,64 @@` (+54/−1); the line counts match the hunk headers. The current source matches the new version: `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala` lines 72–89, and `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyResponseBodyInspection.scala` lines 14 and 19–70.
- [x] **Every outcome of `convertAndClose` checked** (`HTTPClients.scala` lines 72–89):
  - Conversion and close both succeed: the converted value is returned.
  - Conversion succeeds but close fails: `failure` is `None`, so line 85 rethrows the close error. This matches the old behaviour.
  - Conversion fails and close succeeds: `finally` runs, then line 79 rethrows the original error.
  - Conversion fails and close fails with a different error: line 84 attaches the close error as suppressed, and the original error stays primary.
  - Conversion fails and close throws the same exception object: the `ne` check stops the `IllegalArgumentException` that `addSuppressed` throws when an exception is added to itself, so the original is not replaced.
  - `closeError` can never be null (`throw null` produces an NPE), so `addSuppressed` cannot throw an NPE here.
- [x] **Fatal errors and concurrency.** The `case …: Throwable` catches record the error and rethrow the same object, so nothing is swallowed and `finally` still closes the response. A cleanup error after a failed conversion is attached as suppressed, the same as Java try-with-resources. This is not a regression: the old code never called close on the failure path. `failure` is a local variable with no closure capture, and `HandlingUtils` gains no shared state. Each concurrent `AsyncHTTPClient` call owns its own response, and the patch adds no locks, sleeps or timing assumptions.
- [x] **Where conversion can fail.** In `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPSchema.scala`:
  - The `EntityData` constructor (lines 45–58) swallows only `SocketException`. Other `IOException`s, runtime errors and fatal errors from `IOUtils.toByteArray` (line 48) propagate.
  - The `HTTPResponseData` constructor (lines 94–99) reads the status line and locale at lines 97–98.
  - The patch now closes the response for all of these failures.
- [x] **Callers in `HTTPClients.scala`.**
  - `advanced` (lines 412–439) still logs and returns `null` through `case e: Exception`, now with the response closed.
  - `basic` (lines 444–449) still throws the same primary exception.
  - The signature `convertAndClose(CloseableHttpResponse): HTTPResponseData` is unchanged, and no Params or serialized shapes are touched.
  - Skipping `req.releaseConnection()` (lines 432 and 447) when conversion fails is existing behaviour and outside this patch.
- [x] **The tests are real regression tests** (`VerifyResponseBodyInspection.scala`):
  - Lines 19–33 assert one close call and that the thrown error is the original read error. This fails on the old code, which never reached close.
  - Lines 35–56 assert `getSuppressed` equals `Seq(cleanupFailure)` for a different close error and is empty for the same object. Without the `ne` check, an `IllegalArgumentException` would escape and `intercept[IOException]` would fail.
  - Lines 58–70 set `Locale.ROOT` (line 67) so conversion really succeeds. Otherwise an NPE would be the primary error and the `IOException` intercept would fail.
  - All fixtures are deterministic and in-memory, with no network or Spark. `Array.toSeq` equality works on both Scala 2.12 and 2.13.
- [x] **Possible edge cases I ruled out:**
  - A null response is not reachable from `client.execute` or the retry helpers.
  - Exceptions with suppression disabled, or shared exception objects, behave the same as try-with-resources, and nothing in the HTTP path above produces them.
  - A close error whose cause is the original error forms a cycle; the JDK's stack-trace printing handles cycles.
  - Running out of memory while recording the failure (`Some(error)`) cannot be handled reliably by any code.
  - Losing a successfully converted body when close fails is existing behaviour that this PR intentionally keeps.
- [x] **Guidance and style.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml`.
  - The code-review skill's resource-management rule (cleanup must run even when an exception is thrown) is met.
  - Added lines are at most about 110 characters, and the only import change is a named `AtomicInteger` import.
  - The patch changes no dependencies, pipeline, workflow or Python code.
- [ ] **Tests and style checks not run by me.** This review was read-only. I relied on the caller's evidence: an Azure run with VerifyResponseBodyInspection 6/6 passing, plus the reported local Java 8 compilation and core style checks.