---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 3
timestamp: "2026-11-10T04:08:38Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "90ec5b0435344f5c76ce23c154e704b0d2233f628aa6996b2d353675abee28de"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff and guidance read:** I read all 455 lines of the assigned frozen diff using exactly `[1,150]`, `[151,300]`, `[301,450]` and `[451,455]`. That covers all five files and every hunk. I also read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. For the master versions, `build.sbt` gives Spark 3.5.0 and Scala 2.12.17, and `environment.yml` gives Python 3.11.8 and pyspark 3.5.0.
- [x] **Session restored on failure** (`core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `withSession`, L278-285):
  - The code picks the session and saves the caller's active session before changing anything.
  - Every step that can throw runs inside `try`. This includes:
    - `PipelineWriter.validateStages`
    - Spark's "already exists" `IOException`
    - `require` failures and `SecurityException` from child stages
    - read errors
  - `finally` restores the caller's session with `fold(clearActiveSession)(setActiveSession)`, the same pattern as Spark's own `withActive`.
  - Nested calls restore in reverse order. Re-entering with the same session changes nothing.
  - `PipelineSerializer.read` takes element `(0)` only after the session is restored (L269).
- [x] **Null and unassigned sessions** (L262, L276): `this(SparkSession)` turns null into `None`.
  - Because `getOrElse` is by-name, the active or default session is looked up on each call. This matches the old `new PipelineSerializer()` path.
  - If no live session exists, `getOrCreate` installs one before the caller's session is saved. The old `MLWriter.sparkSession` lookup had the same side effect.
  - The default session is never written directly.
- [x] **Concurrency:** The active session is stored per thread (an `InheritableThreadLocal`). `typeToSerializer` (L59-60) creates new, immutable serializers on every call, so no shared mutable state is added. Spark 3.5 Pipeline and PipelineModel writers and readers handle child stages one after another on the calling thread, so the assigned session reaches every nested level.
- [x] **Compatibility and saved format:**
  - The public no-argument constructors remain (L259, L273).
  - The signatures of `write`, `read`, `typeToSerializer` and `writeMLWritable` are unchanged.
  - Spark's own Pipeline writer and reader still produce and load the artifacts, so the on-disk format is unchanged.
  - `writer.overwrite()` updates the writer in place. With `overwrite=false` on an existing path, Spark throws an `IOException` before writing anything.
- [x] **Cross-file contracts**, followed by exact import paths:
  - `ComplexParam.save/load` passes its session to `typeToSerializer`.
  - `TransformerParam` (`ComplexParam[Transformer]`) uses `PipelineSerializer`.
  - The fixture's `stages` param (`ComplexParam[Array[PipelineStage]]`) uses `PipelineArraySerializer`.
  - `ComplexParamsWriter.saveImpl` and `ComplexParamsReader.load` pass their own writer or reader session.
  - The older 3-argument overloads are unchanged, and their new doc comments (L66 and L174 of `ComplexParamsSerializer.scala`) are accurate.
- [x] **Tests can tell the sessions apart:**
  - `TestBase.sparkConfiguration` turns on the legacy deserialization flag for the default session and for new sessions. In `VerifyUDFTransformerPersistence` (L46-84), the `allowed=false` cases would fail if the active or default session were used for nested reads.
  - The ConfigKey fixture means the passing cases in `ValidateComplexParamSerializer` (L157-225, L227-272) only pass with the correct session.
- [x] **Test isolation:**
  - Each parameter combination writes to its own artifact name.
  - Each suite gets its own `tmpDir` (created with `Files.createTempDirectory` and deleted in `afterAll`).
  - `try`/`finally` restores both the active and default sessions.
  - `Test / parallelExecution := false` in `build.sbt` limits the risk from temporarily swapping the JVM-wide default session.
  - The modified suite is about 470 lines, under the 800-line style limit.
- [x] **Existing behaviour, not caused by this PR:**
  - When overwriting, Spark's `FileSystemOverwrite` deletes the old artifact first, so a failed overwrite loses it. This is a known Spark TODO and is unchanged.
  - `PipelineSerializer.read` on a pipeline with zero stages fails the same way as before.
  - Spark 3.5's Pipeline reader and writer handle metadata through `SparkContext`.
- [ ] **Not run by me:** I did not execute any builds, tests or style checks (read-only review). The compile, Test/compile, 31-test and scalastyle results are evidence supplied by the caller. Spark 4.x port compatibility is outside this master PR and needs checking on the port branches after the merge.