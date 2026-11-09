---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 6
timestamp: "2026-11-07T09:55:40Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "034474b26be20096cad2be17bcd6227f2a947ecd03d910f7a8ea6331fd5e8bb1"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-6-claude-opus-5.5-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Full diff read.** I read every line of the assigned frozen diff in exactly the two required ranges, [1,150] and [151,211]. I reviewed all four hunks before and after:
  - `GlobalParams.scala`: `@@ -17,23 +17,23 @@`
  - `VerifyGlobalParams.scala`: `@@ -8,6 +8,92 @@`, `@@ -82,10 +168,13 @@` and `@@ -98,4 +187,66 @@`
- [x] **Checkout matches the diff.**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 20-38: all five public methods are now `= synchronized { … }`.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`: the probe is at lines 11-95, the unset/reset checks at lines 171 and 176-177, and the null, argument and child-JVM tests at lines 191-251.
- [x] **Compatibility and semantics.**
  - Public method names, type parameters, parameter types and return types are unchanged. `synchronized` only changes the method body and its monitor, so method descriptors and static forwarders stay the same. No Param shapes change.
  - The backing `mutable.Map` still stores null values and returns `Some(null)`; lines 191-197 pin that behavior.
  - `getParam` now reads both maps under one monitor, and JVM monitors are reentrant.
  - In `HasGlobalParams.transferGlobalParamsToParamMap` (GlobalParams.scala lines 41-52), `set` runs after `getParam` returns, so no outside code runs while the lock is held.
- [x] **Performance.** Each locked section is a single hash lookup or update. All the callers I checked run on the driver:
  - `OpenAIDefaults` setters and getters in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala`
  - `registerParam` calls in the constructors of `HasOpenAISharedParams` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala`) and `HasSubscriptionKey` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala`)

  The per-row methods `HasServiceParams.getValueOpt` and `getValueMap` never call `GlobalParams`, so per-row work on executors gains no lock contention.
- [x] **Test cost and time limits.**
  - Each mode exercises 64,000 entries. The checks after each round add up to 160,000 lookups on one thread.
  - Each worker future waits at most 30 s. The parent waits 45 s, then allows 10 s for the kill.
  - Child output goes to a file, so a full pipe buffer cannot deadlock the test.
  - `tmpDir` is per suite and is deleted in `TestBase.afterAll` (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`).
- [x] **Observability.**
  - Every child-JVM assertion (lines 245-249) includes the combined stdout/stderr log.
  - The success marker records the mode and the entry count.
  - `require` messages name the failing key or index, and ScalaTest's `assert` reports the actual exit value.
  - Argument checking (lines 61-62) happens before the thread pool is created or any global value is written, so the in-process negative test (lines 199-206) has no side effects.
- [x] **Style and repository guidance.**
  - The longest new lines are under 120 characters, imports are explicit, and license headers are intact.
  - There is no RDD usage and no dependency, workflow, pipeline or release change.
  - I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` and `environment.yml`. The target branch is master.
- [x] **Independence.** I used only exact file views. I did not read sibling or prior review artifacts, run directories, session state or dependency caches.
- [ ] **Not run or independently checked.** This review is read-only, so I did not run tests or style gates. I also did not confirm that `scala.util.Using` is on the Scala 2.12 test classpath. For all of these I rely on the validation evidence the caller provided.

## Issues

### Issue 1: The locking rule is undocumented, and the private map shares the object's name, so `GlobalParams.synchronized` locks a different monitor depending on where it is written
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala
- **Line(s)**: 12-14, 20-38
- **Description**: Thread safety now depends on a rule that is written down nowhere: every access to `ParamToKeyMap` and to the private map named `GlobalParams` must hold the object's own monitor (`GlobalParams.this`). The bare `synchronized` calls follow that rule.

  Inside the object, however, the name `GlobalParams` refers to the private `mutable.Map`; lines 21, 25, 29 and 33 rely on that. So `GlobalParams.synchronized { … }` written inside the object would lock the map. The same expression anywhere else, including `HasGlobalParams` in this file, locks the object. No comment explains which lock guards the maps or warns about the name clash.
- **Risk**: A future helper, such as a bulk reset, snapshot or unregister method, would compile cleanly even if it skipped `synchronized` or used `GlobalParams.synchronized` inside the object. Either mistake would quietly bring back the lost-update and resize race this PR fixes. The child-JVM probe only exercises the five existing methods, so it would not catch the regression.
- **Suggested Fix**: Add a one-line comment above lines 13-14, for example `// Both maps are guarded by this object's monitor; access them only inside synchronized.`

  Optionally, use a dedicated private lock (`private val Lock = new Object` with `Lock.synchronized { … }`). That makes the guard explicit and prevents outside code that synchronizes on the public object from stalling registration or lookup. Neither change touches signatures or behavior.

### Issue 2: The child-JVM launcher names a local `java`, hiding the `java` package, and does not explain its unusual setup
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 208-228 (notably 220 and 225)
- **Description**: `val java` (line 225) hides the root `java` package for the rest of the test body. `val classpath` (line 220) is actually the manifest-only JAR `File`, not a classpath. Three deliberate choices have no explanation:
  - **Separate JVM.** `GlobalParams` has no unregister API, and each probe run leaves 64,000 registrations plus the `SharedKey` value behind.
  - **Two classpath sources.** Entries come from both the `URLClassLoader` chain and `java.class.path`. The chain covers the Java 8 application class loader and sbt's layered loaders. The system property is needed because the Java 9+ application class loader is not a `URLClassLoader`.
  - **Manifest-only JAR.** It replaces `-cp` to keep the command line short.
- **Risk**: Any fully qualified `java.…` reference added after line 225 would fail with a misleading "value … is not a member of String" error. Each of these plausible simplifications can pass in one environment and fail in another:
  - Joining `paths` into a `-cp` argument can exceed Windows' 32,767-character command-line limit for a full Spark test classpath.
  - Dropping one classpath source breaks one JDK generation, which is the risk the master branch reference warns about.
  - Running the probe in-process permanently pollutes state shared with the other test suites.
- **Suggested Fix**: Rename the values to `javaExecutable` and `classpathJar`, and add a short comment above line 210 covering the three choices above. This is a test-only change with no production impact.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not actionable for this patch; optional maintainability suggestion
- **What changed**: No source change. Kept the existing private map names and the single object monitor used by all five accessors.
- **Why**: There is no unguarded access or inconsistent monitor in the current implementation. Each accessor explicitly uses the same unqualified `synchronized`, and neither private map escapes. A hypothetical new bulk operation using another monitor would be a separate code change requiring its own review and coverage. Adding a dedicated lock changes an otherwise sufficient design, while the suggested comment does not correct a present behavior or compatibility defect. The naming predates this patch.
- **How verified**: Inspected every access to both private maps and the unchanged `HasGlobalParams` caller. Both concurrency modes passed on the current patch on master and both active port snapshots. An additional isolated negative control removing only the two reader locks was detected by the existing probes in 6/8 executions; the locked control passed 4/4. The original finding and this non-blocking disposition are retained.

### Issue 2
- **Status**: Not actionable for this patch; optional naming and comment suggestions
- **What changed**: No source change.
- **Why**: No fully qualified `java` reference follows the local `java` declaration, so the proposed shadowing failure does not occur. A short local executable-path name and `classpath` for the file passed to `-cp` do not make the current code incorrect. The test name explicitly says the probe runs in an isolated JVM; the implementation explicitly combines loader and system paths and creates a manifest JAR. The suggested comments and longer names are reasonable alternatives, but not missing behavior or a current portability defect. Future changes that remove isolation or one classpath source need their own compatibility validation.
- **How verified**: Reviewed the complete launcher body and confirmed it uses the local name only as the `ProcessBuilder` executable argument. The same source passed both real child-JVM modes under Java 8 on master and Java 17 on both active port snapshots, including the manifest-classpath launch. No unsupported API, direct long classpath argument or in-process registration stress was introduced.