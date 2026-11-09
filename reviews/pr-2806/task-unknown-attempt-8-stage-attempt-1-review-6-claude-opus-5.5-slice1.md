---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 6
timestamp: "2026-11-08T02:48:13Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4e27007db3b61e4766381c43a7a36594d69d1655bd5fc130f35ea9c1f0a311e5"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-6-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read all 261 lines of the assigned frozen diff in exactly two reads: lines 1–150 and 151–261. I reviewed the before and after versions of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`. The checked-out files match the patched version in the diff. The test file has 14 tests.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. The target is master. `build.sbt` sets Spark 3.5.0 and Scala 2.12.17, and I also checked `environment.yml`. The patch changes no dependencies, build files, workflows or pipeline settings.
- [x] **Compatibility and behavior:** The five public methods keep the same names, type parameters, parameter types and return types. Only their bodies are wrapped in `synchronized`, locking on the `GlobalParams` object. The map and key types and the serialized Params are unchanged.
  - `getParam` now reads both maps while holding one lock.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (`GlobalParams.scala` lines 42–52) calls `set` outside that lock, so there is no lock-ordering risk.
  - A stored `null` still comes back as `Some(null)`, and the new null test locks in that behavior.
- [x] **Performance:** Each locked section is a single constant-time map operation with no I/O or logging. I checked two callers:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala`: driver-side set, get and reset calls.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`: `HasOpenAISharedParams` registers its parameter once per new object (around line 114).

  I found no per-row lookup in the code I read, so lock contention should be negligible. `ParamToKeyMap` already grows without limit; this patch doesn't change that, and it is out of scope.
- [x] **Diagnostics (child JVM):** If a concurrency test fails, its message includes the child JVM's full stdout and stderr. Each `require` message names the entry that failed. The success marker includes the mode and `verified=64000` (8 workers × 2,000 entries × 4 rounds), which matches what the final `verifyCompletedRound` checks. If a worker fails or times out, `main` throws, so the stack trace lands in the child's log.
- [x] **Timeout and interrupt handling:** I traced `awaitProbe` step by step:
  1. `waitFor` throws `InterruptedException` and clears the thread's interrupt flag.
  2. The `Using` cleanup then calls `destroyForcibly` and waits up to 10 seconds. The flag is clear, so this wait is not cut short.
  3. The exception is rethrown with the flag restored.

  Each blocked-probe test's `finally` block clears the flag and makes a second kill attempt. Its 15-second limit is longer than the worst case (100 ms + 10 s). The code comment describes this ordering correctly.
- [x] **Comments and test names:** The new comments are accurate:
  - The Java launcher only gained `@argfile` support in JDK 9, so Java 8 needs another way to pass a long classpath.
  - Cleanup must finish before the interrupt flag is restored.
  - Running in a separate JVM is justified because there is no way to unregister parameters.

  The test names match what each test asserts.
- [x] **JDK and runtime:**
  - All the `Process` APIs used exist in Java 8.
  - The class-loader walk falls back to `java.class.path` when the Java 9+ app class loader isn't a `URLClassLoader`.
  - Canonical directory paths get a trailing slash from `File.toURI`, which the manifest needs.
  - `java.util.jar.Manifest` handles line wrapping for the long `Class-Path` value.
  - Child output goes to a file, so the child can't stall on a full pipe.
  - Test files go in the suite's `tmpDir`, which `afterAll` deletes.
  - The test contains no secrets and makes no network calls.
- [x] **Considered but not raised:**
  - A Scaladoc note on thread safety is optional: every method visibly takes the lock and both maps are private.
  - The blocked-probe tests don't include child output in their messages. They test cleanup, and a child that fails to start would show full output in the concurrency tests.
  - Both callers of `awaitProbe`, which returns `(Boolean, Boolean)`, unpack the result into correctly named values.
  - The lock is the public object rather than a private one, which lets callers combine several calls atomically. I saw no outside code locking on it in the files I read.
  - The test name "values completes" has a minor grammar slip, but its meaning is clear.
- [ ] **Not run, by design (read-only review):** I ran no builds or tests. I relied on your reported results: 14/14 on Java 8 master, Java 17 on both port snapshots, and the mutation checks. I did not inspect where `scala.util.Using` comes from on Scala 2.12; I took that from your evidence. CI on the new head is still pending until publication.