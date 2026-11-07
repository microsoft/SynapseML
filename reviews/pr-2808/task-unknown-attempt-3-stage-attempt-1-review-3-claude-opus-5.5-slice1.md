---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 3
timestamp: "2026-11-06T15:32:49Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "abd1649ad6cecd0a17ae9c9a401663585e08a929e150fd7361bc77d4b09621a2"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2808/task-unknown-attempt-3-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 100 lines of the assigned frozen diff (range 1–100). The hunk line counts match: `HTTPClients.scala` is −70,9/+70,22 and `VerifyResponseBodyInspection.scala` is −11,11/+11,64. The new side matches current source for `convertAndClose` (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala` lines 72–89) and for the import and three tests (`core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyResponseBodyInspection.scala` lines 14 and 19–70).
- [x] **All five outcomes of `convertAndClose` behave as intended:**
  - **Conversion and close both succeed:** the data is returned and close runs once.
  - **Conversion succeeds, close fails:** `failure` is `None`, so the close error propagates, as it did before this change.
  - **Conversion fails, close succeeds:** the original error is rethrown unchanged, with no wrapping and the stack trace preserved.
  - **Both fail with different errors:** the `finally` block completes normally and the conversion error propagates with the close error attached as suppressed.
  - **Both fail with the same error object:** the `ne` guard stops `IllegalArgumentException("Self-suppression not permitted")` from replacing the real error.
- [x] **`addSuppressed` risks:**
  - `closeError` can never be null, because a type pattern doesn't match null and the JVM never delivers a null throwable.
  - `addSuppressed` is `final`, so no subclass can override it.
  - An exception created with suppression disabled just ignores the call instead of failing.
  - `failure` is a local variable on one thread and isn't captured by a closure, so there is no concurrency exposure.
- [x] **Serious JVM errors:** catching `Throwable` only records the error and rethrows it. An `OutOfMemoryError` or `StackOverflowError` from conversion still reaches callers and isn't caught by `advanced`'s `case e: Exception`. If close throws a serious error while conversion failed with an ordinary one, the close error is suppressed. That matches Java try-with-resources and the PR's stated contract, so I didn't raise it.
- [x] **Constructor contract in `HTTPSchema.scala`** (`core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPSchema.scala`):
  - `HTTPResponseData(response)` evaluates in this order: headers, then entity, then status line, then `getLocale.toString`.
  - The entity step is `EntityData(e)`, which uses `IOUtils.toByteArray` and swallows only `SocketException`.
  - So the injected `IOException` is the first and only conversion failure.
  - `Locale.ROOT` prevents a null-locale NPE in the success-path test.
  - Based on httpcore 4.4 behaviour, `setLocale` clears the cached status line, which is rebuilt from the stored code and reason.
- [x] **Callers in the same file:** `HandlingUtils.advanced` and `HandlingUtils.basic` pass the response straight in and never use it after a failure. The main exception's type and identity are unchanged, so `advanced` still logs and returns `null`, and `basic` still propagates. Skipping `req.releaseConnection()` after a failure is existing behaviour. Based on HttpClient 4.x connection-holder behaviour (library source isn't in this checkout), closing the response already releases the connection, so a later release call does nothing.
- [x] **Tests:**
  - They are deterministic, with no threads, network or Spark.
  - Each loop iteration creates a new `failure`, so suppressed lists can't leak between iterations.
  - The first `read()` failure comes out of `IOUtils.toByteArray` unwrapped.
  - `Array.toSeq` equality works on Scala 2.12 and 2.13.
  - On the old code, tests 1 and 2 would fail (close count 0, and no suppressed error for the different-error case). Test 3 checks that existing behaviour is kept.
- [x] **Compatibility and style:**
  - The `private[ml]` signature is unchanged, with no Param or serialization changes.
  - `addSuppressed` and `getSuppressed` exist since Java 7, so they work on Java 8.
  - The longest added line is about 110 characters, under the 120 limit.
  - The local `var` follows nearby code (`responseBodyForInspection`, `sendWithFabricAuthRetries`).
  - Guidance read: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Versions came from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml`.
- [ ] **Tests not run:** this review was read-only. The compile, 6/6 suite and style results are evidence supplied by the caller.
- [ ] **Callers in other modules not listed:** searching was prohibited, so callers such as the cognitive async polling code weren't enumerated. Any such caller would only see an extra close call and suppressed cleanup errors, because the main exception is unchanged.