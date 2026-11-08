---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 5
timestamp: "2026-11-06T17:11:02Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "75dd266dd319586ec30228eb43e6562361b3d38f6b43e2e698295d084eb45a18"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-4-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 171 lines of the assigned frozen diff (lines 1–90, then 91–171), before and after, for both files:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`: five method bodies are now wrapped in `synchronized`.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`: new imports, the `GlobalParamsConcurrencyProbe` object, and four new tests.
- [x] `GlobalParams.scala` lines 1–53:
  - All five public methods (lines 20–38) lock the same monitor, the `GlobalParams` object itself. A bare `synchronized` means `this.synchronized`, not the private map that is also named `GlobalParams`.
  - Method signatures and return types are unchanged.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–52) is unchanged and calls `getParam` once for each param that is not set.
- [x] `VerifyGlobalParams.scala` lines 1–223, matched to the diff:
  - probe: lines 19–70
  - per-test reset: lines 79–95
  - eight existing tests: lines 97–161
  - null-value test: lines 163–169
  - bad-argument test: lines 171–178
  - two child-JVM tests: lines 180–222
  
  That makes 12 tests, matching the caller's 12/12.
- [x] Probe ordering and setup are correct:
  - Argument checking (lines 27–28) runs before the thread pool is created (line 30) and before global state changes (line 31).
  - The bad-argument test covers missing, unknown and extra arguments and checks both the exception type and the message.
  - `Range.map` (line 35) is eager, so all eight tasks are queued before the start latch opens (line 62).
  - Each worker gets its own key range (line 39), so a failed `require` can only mean map state was lost or corrupted.
- [x] The null-value test (lines 163–169) locks in the current `Some(null)` behaviour. It would catch a switch to a concurrent map that rejects nulls.
- [x] Child-JVM process handling:
  - Output goes to a file (line 200), so a full pipe buffer cannot stall the child.
  - The `Using`-managed closer kills the process and waits for it (lines 203–210).
  - On interrupt, the thread's interrupt flag is restored (lines 211–215).
  - The assertion order (lines 217–220) never calls `exitValue()` on a running process.
  - `TestBase.tmpDir` is created on first use and deleted in `afterAll`.
- [x] Build and isolation:
  - `build.sbt` uses Scala 2.12.17 and Spark 3.5.0.
  - The shared settings (lines 279–318) set `Test / parallelExecution := false`.
  - No `Test / fork` setting appears in what I read (`build.sbt` lines 1–420 and `project/CodegenPlugin.scala`).
  
  Either way, all suites share one test JVM. `ParamToKeyMap` has no way to remove entries, so running the probe's 64,000 registrations in a separate JVM is correct. I also read `environment.yml`; the diff changes no dependencies.
- [x] `scalastyle-test-config.xml` turns on `NullChecker`, which explains `Option.empty[String].orNull` (line 164). The new code fits the 120-column line limit and 50-line method limit.
- [x] Production code mixes these operations:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala` sets and resets global values (e.g. lines 12–22).
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` line 114 registers `deploymentName` every time a `HasOpenAISharedParams` stage is constructed.
- [x] Read the guidance files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, and `.github/skills/code-review/SKILL.md`. The target is `master`. The diff changes no workflow, pinned version or public signature.
- [x] `scala.util.Using` (line 17) is not in the Scala 2.12 standard library. The caller reports that a transitive scala-collection-compat jar on the core test classpath provides it. I can't confirm this with view-only access. If the jar were missing, compilation would fail; the tests would not quietly get weaker. Not raised as an issue.
- [ ] I did not run any tests or builds (read-only review). All pass/fail results mentioned here come from the caller.

## Issues

### Issue 1: Each worker checks only its own entries mid-run; nothing is re-checked after the join, and both modes print the same success marker
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 40–58, 62–65, 220
- **Description**:
  - Each worker checks only the entries it just wrote (lines 45–47 and 51–57), while the other seven workers are still changing the same map.
  - After the join (line 63), the main thread re-checks nothing.
  - In registration mode the map grows to 64,000 entries and resizes in rounds 1 and 3 (Scala 2.12 thresholds 24,576 and 49,152). Each resize relinks every earlier entry, but params from earlier rounds are never checked again.
  - So if a regression loses an entry after its owner checked it, nothing notices. The same goes for an entry that a racing removal brings back in values mode.
  - The parent test accepts the same `GLOBAL_PARAMS_CONCURRENCY_OK` marker (lines 65, 220) for both modes. It cannot tell which workload ran or how many entries were checked.
- **Risk**: These regression tests are weaker than they look. If `synchronized` were removed from only `registerParam` or only `resetGlobalParam`, the tests could still pass some of the time. A dispatch bug that ran the same workload in both tests would also pass.
- **Suggested Fix**:
  1. After each round's join, have the main thread re-check through the public API:
     - Registration mode: every param from rounds 0..r still resolves to `"shared"`. Rebuild each param from its parent uid, or return the params from the `Callable`.
     - Values mode: every `WorkerKey` from that round is gone.
  2. Print a mode-specific marker with the checked count, for example `GLOBAL_PARAMS_CONCURRENCY_OK mode=registration verified=64000`.
  3. Assert that exact string in the parent test.

### Issue 2: `getParam`'s value lookup never runs while `setGlobalParam`/`resetGlobalParam` change the value map
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala (production code: core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala)
- **Line(s)**: VerifyGlobalParams.scala 31, 40–47, 48–57; GlobalParams.scala 33, 43–52
- **Description**:
  - `getParam` reads two maps under one lock (GlobalParams.scala line 33).
  - Registration mode writes the value map once, before any task starts (line 31), so `GlobalParams.get` only ever reads a map that isn't changing.
  - Values mode never calls `registerParam` or `getParam`.
  - Production code does all three at once: stage construction registers params (`OpenAI.scala` line 114), `transferGlobalParamsToParamMap` looks them up (GlobalParams.scala lines 43–52), and `OpenAIDefaults.set*`/`reset*` change values.
  - Separately, no test checks that `getParam` returns `None` for a registered param whose key was never set, or was reset. This gap predates the PR but is cheap to close here.
- **Risk**: Suppose a later refactor keeps only the `ParamToKeyMap` read under the lock and moves `GlobalParams.get` outside it. All 12 tests would still pass, and the HashMap race would come back for lookups that overlap a default change. A transform could then silently miss a configured default, such as the deployment name. The PR synchronizes both registration and lookup, but nothing tests the lookup side under this interleaving.
- **Suggested Fix**:
  1. Add a mixed workload, either as a third child-JVM mode or as an extension of values mode:
     - Register one `Param[Int]` per `WorkerKey`.
     - While workers set and reset their keys, assert `getParam(param).contains(value)` after each set and `getParam(param).isEmpty` after each reset.
     - Keep some keys across rounds so the value map keeps resizing during lookups.
  2. Add a single-threaded unit test: register, then check `getParam` is `None`; set, then check `Some`; reset, then check `None`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: After each round joins, the probe checks every registration from all completed rounds, or checks that all completed value keys remain absent. The success marker includes the mode and total of 64,000 verified entries; the parent requires that exact marker.
- **Why**: Later map changes must not invalidate an earlier successful check, and the parent must distinguish the two workloads.
- **How verified**: The standalone pending patch passed `core/compile`, `core/Test/compile`, `core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams` (12/12, no skips), `core/scalastyle`, and `core/Test/scalastyle` on Java 8. Both isolated JVM modes completed. Production is unchanged. This source correction ends pass 4; its earlier clean stages are not carried forward.

### Issue 2
- **Status**: Resolved
- **What changed**: Values-mode workers now register a `Param[Int]` for each key and check `getParam` before setting, after setting, and after resetting while other workers mutate the maps. The existing sequential registration test also checks the unset and reset states.
- **Why**: The regression fixture now exercises the parameter-to-value lookup while the value map changes, not just direct global lookups and registration against a fixed value.
- **How verified**: The standalone and integrated main on Java 8, and both isolated Spark port snapshots on Java 17, each passed the same 12/12 suite, compilation, and both core style gates without skips. The corrected test file is identical across all four trees; production and test source blobs were verified unchanged after each run.