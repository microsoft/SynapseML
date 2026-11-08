---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 3
timestamp: "2026-11-07T12:43:58Z"
outcome: "clean"
reviewer_model: "grok-4.7"
input_sha256: "4fbfb2a84eb8682ff0c164638c3421841bf6d9480da7db6009ecf48b864f6072"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2806/task-unknown-attempt-7-stage-attempt-1-review-3-grok-4.7-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Assigned frozen diff read in full through the mandated ranges (214 lines). Production change only wraps the five `GlobalParams` methods in `synchronized`. Test change adds the concurrency probe, null preservation, and isolated child-JVM harness.
- [x] `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`: both private maps are touched only inside those methods. `getParam` reads `ParamToKeyMap` and the value map in one critical section. Blocks do not call user code, so `HasGlobalParams.transferGlobalParamsToParamMap` cannot deadlock on this monitor. A stored null stays `Some(null)` because `HashMap.get` uses `Some(value)`, not `Option(value)`. Signatures and serialized shapes are unchanged.
- [x] `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`: worker key ranges are disjoint; argument rejection returns before executor creation or map mutation; registration and value probes run in a child JVM so permanent registrations cannot leak into the suite. Output is file-redirected, avoiding a pipe-buffer deadlock. `Using.resource` force-destroys and bounds the child wait before the interrupt flag is restored. Assertions run only after cleanup and require exit 0 plus the verified-count token.
- [x] `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`: suite `tmpDir` is removed in `afterAll`. `VerifyGlobalParams` resets shared keys in `afterEach`, including when a body fails before its own reset.
- [x] `build.sbt`, `environment.yml`, and `.github/skills/synapseml-branches/references/branch-spark3p5.md`: master is Spark 3.5.0 / Scala 2.12.17. The launcher stays on Java 8 APIs (manifest classpath, no `@argfile`, timed `waitFor`, `destroyForcibly`) and falls back to `java.class.path` when a loader is not a `URLClassLoader`. Caller inspection places `scala.util.Using` on the existing test classpath; this patch adds no dependency.
- [ ] Behavioral tests and style gates were not executed in this read-only pass. Caller-supplied results were treated as external evidence, not reproduced.