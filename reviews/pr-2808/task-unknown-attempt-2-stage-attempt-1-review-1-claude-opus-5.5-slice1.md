---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:42:08Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "db6efde3c0054dc438aafd2e68b742c1849e2b3a0fd040b9013d404b5e986de7"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2808/task-unknown-attempt-2-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 98 lines of the assigned frozen diff in one bounded view (lines 1–98), including the before and after versions of both hunks. Each after-version matches the checkout exactly:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala`, lines 70–91
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyResponseBodyInspection.scala`, lines 11–72
- [x] **Guidance read:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Master context comes from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (`pyspark==3.5.0`). The code-review skill's resource-management item is met: cleanup uses `try`/`finally`.
- [x] **Behavior of `HandlingUtils.convertAndClose`** (`HTTPClients.scala`, lines 72–89), checked against the old code (`val rData…; response.close(); rData`) and the pending spec:
  1. Conversion succeeds and close succeeds: the converted value is returned and close runs once. Unchanged.
  2. Conversion succeeds and close fails: `failure` is `None`, so the close error is rethrown. This matches the old behavior.
  3. Conversion fails and close succeeds: close now runs, then the original error is rethrown as the same object. This is the intended fix.
  4. Conversion fails and close fails with a different error: the close error is added as suppressed to the conversion error, which stays primary.
  5. Close rethrows the conversion error object itself: the `ne` guard skips `addSuppressed`, which would otherwise throw "Self-suppression not permitted". The error is never added as suppressed to itself.
  - `failure` is a method-local variable with no closure capture or thread sharing. On success, `Option.empty` does not allocate. The `addSuppressed` call is only reached with a non-null, non-identical throwable.
- [x] **Conversion contract** (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPSchema.scala`):
  - `EntityData` (lines 45–58) swallows only `SocketException`, so the tests' plain `IOException` reaches the caller.
  - In the `HTTPResponseData` constructor (lines 94–99), the entity is read before `getLocale.toString`. So the failure fixtures, which have no locale, fail on the body read first. The success control has no entity and sets `Locale.ROOT`, so its conversion really completes; otherwise it would throw an NPE and `intercept[IOException]` would fail.
- [x] **Tests** (`VerifyResponseBodyInspection.scala`, lines 19–32, 34–54 and 56–68):
  - `eq` identity checks rule out wrapped exceptions.
  - Each loop iteration creates new exception objects, so suppressed lists cannot leak between iterations.
  - In the JDK's `InputStream.read(byte[],int,int)`, the first `read()` happens outside its internal catch, so the synthetic failure reaches the caller unchanged.
  - The first two tests would fail against the old code (no close, no suppression). The third keeps existing behavior covered.
  - `AtomicBoolean` is still used by the existing preview test.
- [x] **Effect on callers:** `advanced` and `basic` in `HTTPClients.scala` still skip `req.releaseConnection()` after a conversion failure. However, closing an HttpClient 4.x response already releases its leased connection, and repeat calls do nothing (this comes from the library's documented behaviour; I did not inspect the library source). So the leak is fixed at this shared function, and callers that also close are unaffected.
- [x] **Compatibility and scope:** The `private[ml] def convertAndClose(response: CloseableHttpResponse): HTTPResponseData` signature is unchanged. There are no changes to Params, serialization, dependencies, workflows or release files, and no unrelated refactoring. The code also works on Scala 2.13 / Java 8 (`addSuppressed` and `ne`, and `Array.toSeq` equality in the test).
- [x] **Style:** I checked against `scalastyle-config.xml`. No rule forbids typed `Throwable` catches, a local `var`, or a one-line `if`. The code uses `Option` rather than `null`, so `NullChecker` passes. Method length and cyclomatic complexity are well under the limits, and every changed line is 120 characters or less.
- [x] **Security:** There are no credentials, endpoints or network use in the tests. Suppressed close errors only add HTTP-cleanup diagnostics to errors that were already logged or rethrown. The change removes a possible leak of pooled connections and sockets when conversion fails.
- [x] **Considered and not raised:**
  - A fatal close error (an `Error`) is suppressed under a non-fatal conversion error. This matches Java try-with-resources and the stated spec that conversion failures stay primary.
  - A close failure after a successful conversion still discards the converted data. This is existing behavior that the spec explicitly keeps.
- [ ] **Not run:** Tests and style checks were not executed in this read-only review. Execution evidence (6/6 tests, style gates, port snapshots) was provided by the caller and not reproduced.
- [ ] **Not searched:** Callers across the repository were not enumerated because the required read profile forbids searches. I checked the callers in `HTTPClients.scala` directly. The function name, the unchanged signature and the fact that repeat closes do nothing mean callers are unlikely to depend on the response staying open after a failure.