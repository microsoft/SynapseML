---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 1
timestamp: "2026-11-07T11:36:53Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ce6b6e769feeb9e5aa3e25f32deb9b1eb21bc862784f5f4877b1ed91eaf68236"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-7-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage:** I read every line of the assigned frozen diff in the two required reads (lines 1–150 and 151–214). That covered the one hunk in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and all three hunks in `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, before and after. The checked-out files match the diff's post-image.
- [x] **Lock correctness (`GlobalParams.scala` L20–38):**
  - All five methods that access the two `private` maps (L13–14) now use `synchronized`.
  - The unqualified `synchronized` locks the `GlobalParams` object itself, not the private map `val` that shares its name.
  - Because both maps share one lock, `getParam` (L33) reads both maps atomically, even while other threads register, set or reset values.
  - No other code can reach the maps, and `untypedKey` has no side effects.
- [x] **No deadlock risk:** While the lock is held, the code only:
  - runs Spark `Param`'s final `hashCode`/`equals`, which compare parent and name strings;
  - runs `GlobalKey` hash and equality checks.

  Key objects are initialized while arguments are evaluated, before the lock is taken. The locked code does no I/O, calls no callbacks and takes no other locks. Re-entering the lock on the same thread is safe.
- [x] **Compatibility and scope:**
  - Signatures, type parameters and return types are unchanged; `synchronized[T]` returns the body's `Unit` or `Option[T]`.
  - Static forwarders and binary signatures are unchanged.
  - `HasGlobalParams` (L41–53) is untouched.
  - There are no changes to Params serialization, dependency pins, workflows, `pipeline.yaml` or release tooling. This matches `AGENTS.md` and `.github/skills/synapseml-branches/references/branch-spark3p5.md`.
- [x] **Caller impact:**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` registers params in the `HasOpenAISharedParams` and `HasOpenAITextParams` trait initializers. These run on every construction and every `defaultCopy`, so unsynchronized `mutable.HashMap` writes could really happen concurrently.
  - `OpenAIServicesBase.warnIfV1ApiVersionConfigured` calls `getParam`.
  - `OpenAIDefaults.scala` sets, gets and resets global values.
  - The lock is held for only a few hash lookups, which is negligible next to service calls.
- [x] **Behavior unchanged:**
  - A null value stays present (`Some(null)`) until reset. The new test at L191–197 locks this in and would catch a switch to a concurrent map that rejects nulls.
  - The new assertions at L171 and L176–177 match existing behavior.
  - The unchanged per-test resets prevent keys leaking into other suites in the shared JVM.
- [x] **Probe logic (L19–95):**
  - Each key's index, `(round*Workers+worker)*Entries+index`, is unique across 0–63,999.
  - All tasks are submitted before the latch releases them, because `Range.map` is strict.
  - After each round, the probe confirms that all earlier registrations and resets are still in place. Each worker also checks lookups across both maps.
  - `TotalEntries` (8×2000×4 = 64,000) matches the final check range.
  - The argument `require` (L61–62) fails before `args.head`, before the executor is created and before any global write. The in-process negative test (L199–206) therefore leaves no state or threads behind.
- [x] **Child-JVM harness (L208–254):**
  - **Manifest:** `Manifest-Version` is set; Java 8's `Attributes.writeMain` would otherwise silently drop `Class-Path`. `Class-Path` uses absolute `file:` URIs from `toASCIIString`, which the JDK's Class-Path check accepts. `Manifest.write` handles the 72-byte line wrapping.
  - **Classpath order:** test class-loader paths come before `java.class.path`.
  - **Process I/O:** output goes to a file, so a full pipe cannot block the child. Arguments are passed as an array, so there is no shell injection.
  - **Cleanup:** the closer passed to `Using.resource` force-kills the child and waits a bounded time after success, timeout or interruption. The interrupt flag is restored only after that cleanup.
  - **Assertions:** they run in the order `terminated`, `completed`, `exitValue()`, so `exitValue()` cannot throw `IllegalThreadStateException`.
  - **Temp files:** they go in the suite's own private `tmpDir`, which `TestBase.afterAll` deletes (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`).
- [x] **JDK and Scala portability:**
  - The test uses only Java 8 APIs: `Process.isAlive`, `destroyForcibly` and `waitFor(long, TimeUnit)`, plus NIO `Files`.
  - Walking the `URLClassLoader` chain and falling back to `java.class.path` covers Java 8 app loaders, sbt's layered loaders and Java 9+ loaders, as the master branch reference requires.
  - The `Iterator.iterate` and `takeWhile` combination never calls `getParent` on null.
  - `build.sbt` pins Scala 2.12.17 and Spark 3.5.0. On Scala 2.12, `scala.util.Using` comes from scala-collection-compat, a transitive dependency that you reported is on the classpath. If it were missing, compilation would fail visibly.
- [x] **Style and comments:**
  - License headers are present and imports are explicit. The explicit `java.util.jar.Manifest` import intentionally hides Predef's `Manifest` alias, which the file doesn't otherwise use.
  - There is no RDD use, and resources are closed with `Using.resource`.
  - The longest lines I checked are ≤120 characters.
  - The three new comment lines (L208, L218, L232) don't affect behavior and are accurate.
- [x] **Considered but not raised (existing behavior, or out of scope for this PR):**
  - `ParamToKeyMap` grows without bound because each random-UID instance registers new entries.
  - `HasGlobalParams.transferGlobalParamsToParamMap` reads several params without one consistent snapshot; changing that would change semantics.
  - The lock is the publicly reachable `GlobalParams` object. This is a common Scala pattern, and none of the callers I reviewed lock on it.
- [ ] **Not run:** I did not run any tests, compiles or scalastyle checks; this review was read-only. Runtime results come from your validation evidence. Pending-head CI is not part of this verdict.