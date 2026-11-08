---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 1
timestamp: "2026-11-06T17:57:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "232250f103437774e3f49ca99f6031e7c0d234dfd39b5ee5bd2f478746f2e10b"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read the full assigned frozen diff in two separate requests: lines [1,150] and [151,211]. That covers all 211 lines. I checked the before and after versions of both files. The hunk line counts match the current checkout. `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` (lines 1–53) and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` (lines 1–252) match the diff's new version exactly.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md` and `.github/skills/scala-code/SKILL.md`. Versions come from `build.sbt` (Scala 2.12.17, Spark 3.5.0, ScalaTest 3.2.14) and `environment.yml`. The PR targets `master`. It changes no dependency pin, workflow, `pipeline.yaml` or release tooling, and adds no RDD code.
- [x] **Production locking (`GlobalParams.scala` lines 13–14, 20–38):** The two `private` `mutable.Map` fields can only be reached through the five public methods. All five now lock on the same `GlobalParams$` object, so all reads and writes are safely visible across threads. `getParam` (line 33) now reads both maps under one lock. Inside `object GlobalParams`, plain `synchronized` refers to the object itself, not the private map field that shares its name.
- [x] **Signatures and behavior unchanged:** Return types and compiled method signatures are the same. In Scala 2.12, `synchronized` compiles to inline lock instructions, so no new public members appear. Behavior is also the same:
  - the last write still wins;
  - a stored null still comes back as `Some(null)`;
  - a reset still returns `None`.

  The new test at `VerifyGlobalParams.scala` lines 191–197 locks in the null behavior.
- [x] **No deadlock path:** While the lock is held, the only outside code that runs is `hashCode`/`equals` on keys. Spark `Param` builds these from `parent` and `name`, and the case-object keys use generated constants or identity. Neither takes another lock or runs class setup code. `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–52) calls `set` only after `getParam` has returned and released the lock.
- [x] **Probe correctness (`VerifyGlobalParams.scala` lines 19–95):**
  - Each round and worker gets its own key range through `offset = (round*Workers+worker)*Entries` (line 73).
  - The after-round check (lines 50–58) covers `[0, (round+1)*16000)`, which is exactly all completed ranges.
  - `TotalEntries` (64,000) matches the success line printed on line 90.
  - New `registrationParam(index)` instances can be used for lookups because `Param` equality uses `parent` and `name`.
  - Argument checking (lines 61–62) runs before the thread pool is created or `SharedKey` is set. So the in-process negative test (lines 199–206) leaves no global state or threads behind.
  - The success line prints only after every `require` passes, so the test cannot pass without doing its checks.
- [x] **Child-JVM harness (lines 208–251):**
  - It starts the process with fixed arguments and no shell. It uses the `java` from `java.home` and builds a manifest Class-Path with `toASCIIString` so spaces are encoded.
  - Output goes to a file, which avoids a blocked-pipe deadlock.
  - `Using.resource` kills and waits for the child on success, timeout or interrupt. The interrupt flag is restored at line 241.
  - `exitValue()` (line 247) runs only after `terminated` and `completed` are asserted.
  - A stuck pool thread inside the child is bounded by the parent's 45-second wait and forced kill.
  - Both nested jar streams are closed (lines 221–223). The temp files live in `TestBase.tmpDir`, which `TestBase.afterAll` deletes.
- [x] **Security:** The change has no deserialization, network, credential or injection surface. `Files.createTempDirectory` creates the temp directory.
- [x] **Style, checked by reading `scalastyle-test-config.xml`:**
  - The longest changed lines are about 114 characters, under the 120 limit.
  - `main` is about 35 lines, under the 50-line method limit.
  - The only `null` literal is the comparison `_ != null`, which the checker allows. `Option.empty[String].orNull` avoids a `scalastyle:off` comment, as the scala-code skill prefers.
  - There are no wildcard imports and no new procedure syntax.
- [x] **Considered and not reported as defects:**
  - Other code could lock on the public `GlobalParams` object. This is normal Scala, and the reviewed files contain no outside code that does it.
  - `transferGlobalParamsToParamMap` still does not read all params as one consistent snapshot. This predates the PR and is out of scope ("without changing global semantics").
  - In `values` mode, the after-round check only confirms that values are gone. Registration persistence is still covered by the per-worker lookups (lines 39–46) and by the full `registration` mode.
  - `ParamToKeyMap` can grow without limit. This also predates the PR.
- [ ] **Tests not run by this reviewer:** This review was read-only. The local results (Java 8, 12/12, style gates, both child-JVM modes, port snapshots), the transitive `scala.util.Using` classpath fact, and the earlier Azure results all come from the caller. CI on the pending head is still pending, and that gate is separate from this verdict.