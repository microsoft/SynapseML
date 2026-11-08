---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-11-06T14:38:02Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4fa62e063601110804eda34754e7bda8c1a530e868fc5f0ad6dd36279528daac"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-3-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 159 lines of the assigned frozen diff in two views (lines 1–80 and 81–159). This covers the before and after of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` (hunk at line 17) and both hunks of `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` (hunks at lines 8 and 98→156).
- [x] **Checkout matches the diff.** The current files in the checkout are identical to the diff's post-image: `GlobalParams.scala` lines 17–39 and `VerifyGlobalParams.scala` lines 11–68 and 159–210.
- [x] **Every access to the shared maps is locked.** The two private maps (`GlobalParams.scala` lines 13–14) are only read or written inside the five methods that are now `synchronized` (lines 20–38). `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–51) only reaches them through `getParam`. A bare `synchronized` locks the `GlobalParams` object itself, not the private map field that shares its name.
- [x] **No new deadlock risk.** While the lock is held, the code only does `mutable.HashMap` operations plus hashing and equality on Spark `Param` (its `hashCode`/`equals` are `final`) and on case-object `GlobalKey`s. It never calls back into other code or takes another lock. `set(...)` in `HasGlobalParams` (line 49) runs after the lock is released.
- [x] **Callers don't hold other locks or call this per row.** I checked the callers to judge contention and deadlock risk:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` line 139 calls `registerParam` from a trait initializer.
  - The same file calls `transferGlobalParamsToParamMap()` once per driver-side `transform` (lines 764–768).
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` (e.g. lines 12–22) only wraps set/get/reset.

  Contention is negligible.
- [x] **Behaviour and API are unchanged.**
  - Method signatures and return types are the same, the method descriptors stay binary-compatible, and serialized Params are untouched.
  - Keeping `mutable.Map` under a lock (rather than switching to `ConcurrentHashMap`, which rejects null values) preserves both the null-is-present behaviour and the combined two-map lookup in `getParam`. The new test at `VerifyGlobalParams.scala` lines 160–166 pins the null behaviour.
  - The diff contains no dependency, workflow, pipeline, or release changes.
- [x] **The concurrency probe is sound** (`VerifyGlobalParams.scala` lines 19–67):
  - Each worker in each round gets its own key range, `(round*8 + worker)*2000`, with a maximum of 63,999, so there is no Int overflow.
  - A latch starts the 8 tasks together on an 8-thread pool.
  - Every set, get, register and reset is checked with `require`.
  - The success marker is printed only after all futures finish. Any failure exits non-zero, and a hung worker hits the parent's bounded wait.
- [x] **Cleanup and resource handling are correct** (lines 180–208):
  - The nested `Using.resource` closes the file stream even if creating the `JarOutputStream` fails.
  - The `AutoCloseable` wrapper force-stops and reaps the child process on every path.
  - An interrupt is restored before the exception is rethrown, and `Using` keeps the original interrupt while suppressing a second one during cleanup.
  - The assertions run in the order terminated → completed → `exitValue()`, so `exitValue()` is never called on a live process.
  - Temporary files go in a per-suite `tmpDir` (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` lines 160–164), which `afterAll` deletes (lines 194–199).
- [x] **Branch and version fit.**
  - `build.sbt` sets Spark 3.5.0 (line 31) and Scala 2.12.17 (line 34); the shared `settings` block has `Test / parallelExecution := false` and no `Test / fork`.
  - `environment.yml` pins Python 3.11.8 and pyspark 3.5.0.
  - The master branch guide (`.github/skills/synapseml-branches/references/branch-spark3p5.md`) requires APIs that work on the oldest JDK. The test uses only Java 8 process APIs, and finds the classpath both from the `URLClassLoader` chain (Java 8) and from `java.class.path` (Java 9+).
- [x] **Security.**
  - The child process gets an argument array (no shell), and every input comes from the JVM's own properties and class loaders.
  - There are no secrets, network calls or deserialization.
  - Temp files sit in a directory created with `Files.createTempDirectory`.
- [x] **Concerns I considered and rejected:**
  - Locking on the public `GlobalParams` object: no other code synchronizes on it, and nothing else is ever locked while it is held.
  - `transferGlobalParamsToParamMap` reading params one at a time rather than as a single snapshot: this behaviour already existed and is out of scope.
  - `ParamToKeyMap` growing with every new instance id: already existed and out of scope.
  - The child's stdin pipe is never closed explicitly: the handles are few and are freed when the process exits or is garbage-collected, with no functional effect.
- [ ] **Not independently verified (read-only review).** I did not run any build, style check or test. Three claims rest on caller evidence because dependency caches and validation artifacts were off limits:
  - `scala.util.Using` is available on Scala 2.12 through the transitive `scala-collection-compat` jar. It is used only in test sources and is not declared in `build.sbt`.
  - The probes failed on the old code and pass after the fix.
  - 11/11 tests passed.