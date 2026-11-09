---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:34:51Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "cff0a308c573facfe24b1492de807e00e6f2d0746e5b7225504d5c709a9fa991"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-1-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 147 lines of the assigned frozen diff in one bounded read (1–147), including the before and after sides of both hunks. The current files `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` match the diff's after-state.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Scala 2.12.17, Spark 3.5.0) and `environment.yml`. The PR targets master.
- [x] **Locking is correct:** In `GlobalParams.scala` L20–38, all five public methods now wrap their bodies in `synchronized`. Inside the object, a private map is also named `GlobalParams`, but the bare `synchronized` still locks the `GlobalParams` object itself, not that map. So all five methods share one lock.
  - Both private maps (L13–14) are only used inside these methods. `getParam` reads both maps under a single lock acquisition.
  - The locked code only calls `hashCode`/`equals` on keys and on Spark `Param` objects, which are final and side-effect free. Nothing inside the lock takes another lock, so there is no deadlock path.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (L43–52) reaches the shared state only through `getParam`.
- [x] **No compatibility or scope change:** Method names, type parameters, parameter types and return types are unchanged, so JVM signatures and static forwarders are unchanged. No Params, defaults, serialized shapes, dependency pins, workflows, `pipeline.yaml` or release tooling are touched, which complies with `AGENTS.md`.
- [x] **Null handling is unchanged:** A value set to `null` is still returned as `Some(null)` until it is reset. The new test at `VerifyGlobalParams.scala` L160–166 guards this, so a later switch to a map type that rejects `null` would be caught.
- [x] **Child-JVM probe is sound** (`VerifyGlobalParams.scala` L19–67):
  - Each worker and round uses its own key range; the largest index is 63,999, so there is no integer overflow.
  - Each `Param` has a unique parent and name, so no two registrations collide.
  - `SharedKey` is set before the workers start and is never reset.
  - A failed `require` surfaces as an `ExecutionException` and a non-zero exit. A hang is bounded by the 30-second per-task wait and the parent's 45-second watchdog.
  - The success marker prints only after all rounds finish, and `shutdownNow` runs in `finally`.
- [x] **Test launcher is sound** (`VerifyGlobalParams.scala` L168–198):
  - **Classpath:** Neither `build.sbt` nor `project/CodegenPlugin.scala` sets `Test / fork`. The test therefore combines the class-loader URLs (covers in-process sbt) with `java.class.path` (covers forked runs and JDK 9+), then canonicalizes and de-duplicates them.
  - **Manifest jar:** Entries are written as encoded absolute `file:` URIs into a manifest-only jar. This avoids the Windows command-line length limit, and directory entries keep their trailing `/`.
  - **Process launch:** The child is started from an argument list, not a shell, with a constant `mode`, so there is no injection risk.
  - **JDK compatibility:** Every API used exists in JDK 8, as the master branch reference requires.
  - **Cleanup:** Files go to `TestBase.tmpDir`, which `afterAll` deletes, and the log is read only after the child process has ended.
- [x] **Style (checked by reading, not run)** against `scalastyle-test-config.xml`:
  - The longest added line is about 115 characters (limit 120), and `main` is about 41 lines (limit 50).
  - The object fields `Workers`, `Entries` and `Rounds` match `objectFieldRegex`.
  - `null` appears only in a `!= null` comparison.
  - Import grouping follows `TestBase.scala`.
- [x] **Considered and not reported:**
  - The unbounded growth of `ParamToKeyMap` and the non-atomic multi-parameter transfer existed before this PR, and the PR must keep that behaviour.
  - Locking on the `GlobalParams` object rather than a private lock object is idiomatic, and nothing in the reviewed files synchronizes on it from outside.
  - `FileOutputStream` would leak only if the `JarOutputStream` constructor fails on an I/O error inside a private temp directory.
- [ ] **Not verified: `scala.util.Using` on Scala 2.12.** It is not in the Scala 2.12 standard library, and I could not inspect the resolved dependencies under the read-only view profile. I relied on the caller's evidence that the current-head VerifyGlobalParams run passed 11/11. That count matches the diff: 8 existing tests plus 3 new ones.
- [ ] **No builds, tests or scalastyle run:** This was a read-only review. All validation results above come from the caller.