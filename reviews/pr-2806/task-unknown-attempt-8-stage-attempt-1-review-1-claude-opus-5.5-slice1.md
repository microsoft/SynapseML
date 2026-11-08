---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 1
timestamp: "2026-11-07T14:32:48Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "dfeeb53138263b5a793ef6b951f9e9adfb54a05dc09a07c538643f59618e25b7"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read every line of the assigned frozen diff in exactly two reads, [1,150] and [151,261]. I reviewed the before and after versions of both files. The current `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` match the post-image.
- [x] **Guidance and versions read.** I read these files directly by path: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the master target) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Scala 2.12.17, Spark 3.5.0) and `environment.yml`. I read the imported `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` to check how `tmpDir` is created and cleaned up.
- [x] **Locking covers every access path.** In `GlobalParams.scala` (lines 20–38), all five methods that touch the two private maps now run inside one shared lock (`this.synchronized` on the `GlobalParams` object). `untypedKey` only casts its argument. The inner `GlobalParams` map field does not change what `synchronized` refers to. `getParam` reads both maps under that one lock, so the two reads are consistent.
- [x] **No deadlock risk.** While the lock is held, the only outside code that runs is the hashing and equality logic of the keys and of Spark `Param`, whose `hashCode`/`equals` are `final`. `HasGlobalParams.transferGlobalParamsToParamMap` calls `set` after the lock is released, so the lock is never held while calling into other code.
- [x] **Compatibility and existing behavior preserved.** All five method signatures and declared return types are unchanged. No serialized Param shape, dependency, workflow, pipeline or release file is changed. The new test "global values preserve a present null until reset" locks in the existing behavior where a stored `null` reads back as `Some(null)`. That rules out a silent switch to `ConcurrentHashMap`, which does not accept null values.
- [x] **`awaitProbe` traced for each outcome**, using how `scala.util.Using.resource` behaves:
  - Normal completion returns (true, true).
  - Timeout force-kills the child, waits at most 10 s, and returns (false, terminated).
  - Interrupt: `waitFor` throws and clears the interrupt flag, the cleanup wait still runs to completion, the flag is set again after cleanup, and the original `InterruptedException` is rethrown. An interrupt during cleanup is attached to that exception as a suppressed error.
- [x] **`startProbe` is safe.** It builds an argument array without a shell and takes the launcher from `java.home`. The classpath list comes from file-protocol class loader URLs plus `java.class.path`, made canonical and de-duplicated. Directory entries get a trailing slash from `toURI`. The classpath JAR goes in the per-suite temp directory, which `TestBase.afterAll` deletes. Output goes to a file, so the child cannot block on a full pipe. Only Java 8 APIs are used.
- [x] **Concurrency probe logic is sound.** Workers use non-overlapping keys, computed as `(round*Workers+worker)*Entries`. Spark `Param` compares equal by (parent, name), so freshly created params can verify earlier registrations. 8 × 2000 × 4 = 64,000, which matches `TotalEntries`. Failures exit non-zero and the log is included in the assertion message. A hang is caught by the 45 s wait plus forced kill. The in-process test with bad arguments fails at `require` before any global state changes or threads start.
- [x] **Blocked-child tests cover the claimed cases.** They assert:
  - the timeout result is reported as not completed;
  - the child is terminated;
  - the interrupt exception propagates and the flag is restored;
  - the child is dead afterwards;
  - the whole test takes under 15 s.

  The `finally` block always clears the test thread's interrupt flag and kills any remaining child with a bounded wait.
- [x] **Style limits spot-checked.** The longest added lines are about 114 characters (limit 120). License headers are present. The test file is about 300 lines (limit 800). The probes live in test sources and add nothing to main scope.
- [ ] Tests not run by me: this review was read-only. The reported 14/14 passes (Java 8 on master, Java 17 on the port snapshots) and the result that `scala.util.Using` resolves through the existing transitive `scala-collection-compat` on Scala 2.12 come only from the caller's evidence.
- [ ] Not shown: that the concurrency probes fail when `synchronized` is removed. The current-pass evidence only shows that the cleanup and interrupt tests fail when their logic is removed. A race on an unsynchronized `HashMap` is caught only probabilistically. This is a note, not a defect.