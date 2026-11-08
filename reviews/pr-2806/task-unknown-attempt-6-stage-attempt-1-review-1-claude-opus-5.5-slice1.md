---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 1
timestamp: "2026-11-07T10:18:54Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "0631b006a7237b9eb5bc2a608e40a357db7d3569399d22e076e264dcce86075c"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-6-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Complete diff coverage:** I read all 211 lines of the assigned frozen diff using exactly two views: lines 1–150 and 151–211. The before and after sides match the current files:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 17–39.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, lines 8–99, 168–180 and 187–252.
  - No other files change.
- [x] **Production correctness** (`GlobalParams.scala`, lines 20–38):
  - All five methods that touch the private `ParamToKeyMap` and `GlobalParams` maps now lock the same object (the `GlobalParams` singleton).
  - `getParam` reads both maps while holding that one lock.
  - Nothing else can reach the private maps. `HasGlobalParams.transferGlobalParamsToParamMap` (line 48) goes through `getParam`.
  - While locked, the code only casts keys and calls hash/equality methods that take no other locks. Spark's `Param.hashCode` and `equals` are final and pure.
  - So the code never takes another lock while holding this one, which rules out deadlock. An exception also releases the lock.
- [x] **Compatibility and spec:**
  - Method names, type parameters, parameter lists, return types and erasures are unchanged. Static forwarders are unchanged.
  - There are no changes to `Param`, serialized parameters, dependencies, workflows or the pipeline.
  - Single-threaded behavior is the same, including returning `Some(null)` when a null value was set.
- [x] **Callers:**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` makes only single set/get/reset calls, and each one is now atomic.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`, line 114, registers `deploymentName` every time a stage is constructed. That is the concurrent write path this PR fixes.
  - The PR does not add or claim atomicity across multiple keys.
- [x] **Probe logic** (`VerifyGlobalParams.scala`, lines 19–95):
  - Each worker uses its own key range, `(round*8+worker)*2000` up to index 63999.
  - `Range.map` is strict, so all tasks are submitted before the start latch opens.
  - Waiting on each task's result guarantees the after-round checks see the workers' writes. Those checks cover every completed round, not just the latest.
  - A failed `require` in a worker makes the child exit nonzero.
  - `shutdownNow` runs in `finally`.
  - Arguments are validated before any thread pool or global state is touched. So the in-process rejection test (lines 199–206) leaves the parent JVM's `GlobalParams` unchanged.
- [x] **Child-JVM harness** (lines 208–251):
  - Waits are bounded: 45 s, plus 10 s for cleanup. Cleanup owns the process immediately after `start()`.
  - On timeout the child is force-killed, and the thread's interrupt status is restored.
  - Output goes to a file, so the child cannot stall on a full pipe.
  - Assertions are ordered so `exitValue()` is never called on a live process. The success-marker check uses `contains`, so extra JVM startup messages don't break it.
  - It uses only Java 8 APIs. Classpath discovery handles Java 8 `URLClassLoader` chains and Java 9+ `java.class.path`, as the class-loader note in `.github/skills/synapseml-branches/references/branch-spark3p5.md` requires.
  - The classpath JAR and log go in the suite's private temporary `tmpDir`, which `TestBase.afterAll` deletes.
  - The process gets a fixed argument list with no shell, so there is nothing to inject into.
- [x] **Style and guidance:** I checked the change against `AGENTS.md`, `.github/skills/code-review/SKILL.md` and `scalastyle-test-config.xml`.
  - Lines are 120 characters or fewer, and object field names are capitalized as configured.
  - The only `null` is a `!= null` comparison, which the null check normally permits.
  - There is no `while` or `return`, and `main` is under 50 lines.
  - Imports follow the same order as `TestBase.scala`.
  - There is no RDD use and there are no secrets. Versions come from `build.sbt` and `environment.yml`; the target is master.
- [x] **Considered but not raised:**
  - The lock is the public `GlobalParams` singleton. A private lock object would be optional hardening; nothing in the code I reviewed locks it from outside.
  - The child's stdin pipe is never closed explicitly. The JDK closes it when the child exits on Unix, and when the object is garbage-collected on Windows. Impact is negligible.
  - On Scala 2.12, `scala.util.Using` comes from scala-collection-compat, which is pulled in indirectly rather than declared. If it disappeared, test compilation would fail visibly rather than silently.
  - The `verified=` count in the success marker is the constant `TotalEntries`, not a live counter. The marker is printed only after every check has passed.
- [ ] **Build, test and style runs:** not performed, because this review is read-only. The compile, VerifyGlobalParams 12/12, both style gates, and the probes failing before the fix and passing after it are caller-provided evidence, not my own runs.