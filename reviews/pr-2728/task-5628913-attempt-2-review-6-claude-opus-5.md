# Code Review — Round 6 of 6 (sequential mode)

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening (performance, observability, documentation, naming clarity)
- **Mode**: sequential
- **Model**: Claude Opus 5 (`claude-opus-5`)
- **Artifact**: `reviews/pr-2728/task-5628913-attempt-2-review-6-claude-opus-5.md`
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

All four findings are Low. Nothing blocks the change: no correctness, safety, or
resource-lifecycle defect was found in the current source. The findings are
observability and clarity polish appropriate to this round.

## Evidence Checklist

- [x] Reviewed the **current** working-tree source, not the prompt diff or earlier rounds:
      `git diff --stat` in the `fabric-test-cleanup-20260918` task checkout shows the five modified files
      (`FabricNotebookTests.scala`, `FabricTestArtifactTrackerSuite.scala`,
      `docs/Reference/Developer Setup.md`, `pipeline.yaml`, `tools/ci/tests/test_pipeline_yaml.py`)
      on top of `1205df21f0`; read all 210 lines of `FabricNotebookTests.scala` and all
      new blocks of the other four files.
- [x] **Lazy setup chain traced end to end.** `preflight` (FabricNotebookTests.scala:46) →
      `storeSetup` (50-53) → `submissions` (182-188) → `futures` (190), with per-test guards at
      72, 104 and 195. `captureFabricSetup` (31-37) is correct because `scala.util.Try` catches
      only `NonFatal`, which excludes `InterruptedException`; the outer `catch` therefore converts
      it to a cached `Failure`, and `getFabricSetup` (39-44) re-arms `Thread.interrupt()` on every
      access. Caching (not re-running) is locked in by `FabricTestArtifactTrackerSuite.scala:157-177`
      (`attempts == 1` across two accesses) — so a maintainer removing the "unreachable-looking"
      catch at line 35 would be caught by an existing test.
- [x] **Fabric-connection contract verified.** `FabricConnection.scala:29-42`
      (`createOperationsConnection`) throws unless `fabricWorkspaceId` is `Some`, and
      `FabricNotebookTests.scala:27` is now the only assignment. Traced every `fabric` touch point —
      58 (tracker lambda, deferred), 67, 109-123, 169, 172-178 — and each is reachable only after
      `ensureFabricPreflight()`. The doc claim "Suite construction does not connect to Fabric"
      (Developer Setup.md:80-81) holds for all three suites.
- [x] **No published test-name regression.** `FabricOperations.scala:507-509` defines
      `getBlobNameFromFilepath` as `filePath.split(File.separatorChar).last`, which is identical to
      the new `notebookFile.getName` at `FabricNotebookTests.scala:193`. Azure DevOps test-history
      continuity for the notebook cases is preserved.
- [x] **Executor lifecycle checked for leaks and double-creation.** `executorStarted` (157) is set
      only after `createNotebookExecutor()` returns (162-164), so a throwing factory leaves it
      `false` and `afterAll` (208-214) does not re-force the lazy val;
      `FabricTestArtifactTrackerSuite.scala:205` proves exactly one `"executor"` call on that path,
      and `:143` asserts `isTerminated` on the success path. Conversely, executor creation implies
      `submissions` completed (the executor is only forced by the first `Future(...)` at line 186),
      so "created but never shut down" is unreachable.
- [x] **Bounded parallelism preserved and deterministically proven.** `MaxConcurrency = 3`
      (FabricNotebookTests.scala:158-159, 216) with a single fixed pool. The latch fixture
      (`FabricTestArtifactTrackerSuite.scala:34, 53-64`) increments `active`/`peak` *before*
      `countDown()`, and no worker can decrement until all three have entered, so
      `peak == MaxConcurrency` (:141) is deterministic rather than timing-dependent.
- [x] **No lazy-val deadlock introduced by submitting inside a lazy initializer.** While the test
      thread holds the instance monitor evaluating `submissions` (182-188), worker threads touch
      only `artifactTracker` (a strict `val`, line 57) and the already-initialized `fabric` lazy val
      (forced during preflight at line 28); `storeId` is passed by value (184-186) rather than
      re-read from `storeArtifactId`. The initializer itself never waits on a worker.
- [x] **Unit-suite cost checked.** `TestBase.scala:152-199` starts Spark only through
      `lazy val spark` (156), which no Fabric suite touches, and `beforeAll`/`afterAll` are trivial;
      running six real suites inside `FabricTestArtifactTrackerSuite.executeSuite` (:21-28) adds no
      Spark or cloud cost. `FabricTestArtifactTracker.cleanup()`
      (FabricTestArtifactTracker.scala:52-67) makes no call when nothing was tracked, so `afterAll`
      is safe after a failed preflight.
- [x] **CI report path and publication scope verified.** `core/target/test-reports` exists in this
      worktree; no `testOptions`/`-u`/`junitxml` override exists in `build.sbt` or `project/`; the
      same hard-coded root is pre-existing at `pipeline.yaml:406`, so the new copy at
      `pipeline.yaml:368-370` and the narrowed `searchFolder` at `pipeline.yaml:425-429` resolve to
      the same place. Narrowing from `**/test-reports/TEST-*.xml` to the staged folder is an
      improvement here: it excludes stale reports that `templates/sbt_cache.yml` could restore.
      Exit-code preservation, the `condition: succeeded()` gate (380) and the always-on collect
      (403-421) were read against `tools/ci/tests/test_pipeline_yaml.py:593-673`.
- [x] **New Python test is executable in CI.** `tools/ci/tests/test_pipeline_yaml.py` already imports
      `os`, `re`, `subprocess` and `pytest` (lines 9-17) used by the new assertions (556, 593-673);
      the `skipif(os.name != "posix")` guard at 593 means the three parametrized bash cases run on
      the Ubuntu agents but were skipped during the Windows validation described in the task.
- [x] **Explicitly out-of-scope items confirmed as designed, not filed as findings:** per-suite
      preflight after the CI gate (per-instance `private lazy val preflight`,
      FabricNotebookTests.scala:46), strict `true`/`false` dry-run parsing (25-26), and the
      untouched ownership/24h/activity/dependency safeguards (`git diff` shows no change to
      `FabricArtifactCleanup`). No public main API, dependency version, or other repository was
      touched.
- [ ] No build, test, scalastyle, Black or cloud execution performed, and no live CI run inspected —
      excluded by this round's constraints. All conclusions are from source reading.

## Issues

### Issue 1: Store and executor setup failures are reported as "Job failed for &lt;notebook&gt;.py" on every notebook test
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 194-205 (force at 197; wrap at 202-203), with 161-165, 182-188, 190
- **Description**: `ensureFabricPreflight()` is called at line 195 *outside* the `try`, so a
  preflight failure propagates unwrapped. But `futures` is forced at line 197 *inside* the `try`,
  and `futures` unwraps the cached `submissions` result (182-188), which also covers store
  allocation (`storeArtifactId` at 184 → `createTrackedStore` at 67) and executor creation
  (161-165). Those two setup failures are therefore caught by `case NonFatal(t)` at 202 and
  rethrown as `RuntimeException(s"Job failed for $notebookName", t)` — once per selected notebook.
  The new fixture tests lock the behavior in: `FabricTestArtifactTrackerSuite.scala:189` and `:204`
  assert `failures.forall(_.throwable.exists(_.getCause eq failure))` for the store and executor
  failures, while `:152` asserts the unwrapped `_.throwable.contains(failure)` for the preflight
  failure. Three setup failures of the same class are now reported in two different shapes.
- **Risk**: a single workspace/capacity failure to allocate the shared store, or a thread-pool
  creation failure, is published to Azure DevOps as N independent "Job failed for
  ExploreAlgorithms….py" failures. Triage begins at the notebook and its Spark Job Definition,
  neither of which was ever created; the real cause is only visible one level down the `getCause`
  chain. Before this change store allocation ran in the constructor, so the same failure aborted
  the suite once with the unwrapped error — this round's split made the reporting less precise.
- **Suggested Fix**: force the submission batch before the `try`, so only genuine job outcomes get
  the "Job failed for" label:
  ```scala
  test(notebookName) {
    ensureFabricPreflight()
    val future = futures(index)._1
    try {
      Await.result(future, notebookTimeout)
    } catch { ... }
  }
  ```
  Then update `FabricTestArtifactTrackerSuite.scala:189` and `:204` to expect the unwrapped
  `failure`, matching the preflight assertion at `:152`.

### Issue 2: `run-metadata.txt` retains a stale `e2e_step=not-started` line after E2E has run
- **Severity**: Low
- **File**: pipeline.yaml (guard gap in tools/ci/tests/test_pipeline_yaml.py)
- **Line(s)**: pipeline.yaml 352-358 (sentinel at 357) and 388-396; test at 655-663
- **Description**: the `Fabric cleanup preflight` step's initial truncating write emits
  `e2e_step=not-started` (line 357). The `E2E` step only appends (`e2e_step=preparing` at 388,
  `e2e_step=running` at 390, `e2e_step=finished` at 395). On a fully successful run the evidence
  file therefore holds both `e2e_step=not-started` and `e2e_step=finished`, with the stale sentinel
  appearing *before* the cleanup phase lines. Every other key in this file is either written once
  or follows a monotonic phase sequence, so `e2e_step` is the only key whose file now contains two
  contradictory values, and nothing in the artifact states a "last value wins" convention. The new
  test asserts `"e2e_step=finished" in metadata` (662) but never asserts the sentinel is absent, so
  the contradiction is unguarded.
- **Risk**: `fabric-e2e-$(System.JobAttempt)` is the primary post-mortem evidence for this job. A
  human scanning `run-metadata.txt`, or any consumer using a first-match parse (`grep -m1
  '^e2e_step='`, `head`, or a naive key/value loader that keeps the first binding), concludes E2E
  never started on a run where it actually ran and passed — the exact misreading this evidence file
  exists to prevent.
- **Suggested Fix**: remove `e2e_step=not-started` from the preflight write (pipeline.yaml:357) and
  emit it from `Collect Fabric E2E evidence` only when it is true, after the existing
  `if [ ! -f ... ]` block (408-415):
  ```bash
  grep -q '^e2e_step=' "$artifact_root/run-metadata.txt" || \
    printf '%s\n' 'e2e_step=not-started' >> "$artifact_root/run-metadata.txt"
  ```
  This preserves the guarantee the new test checks at line 659 (cleanup failure ⇒
  `e2e_step=not-started`) without contradicting a successful run. Add an assertion that
  `e2e_step=not-started` is absent from the metadata in the `cleanup_exit == 0` branch (around
  test line 662).

### Issue 3: `futures` still carries a notebook name that no consumer reads
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 185-190, 197
- **Description**: `submissions` builds `(Future(runNotebook(...)), notebookFile.getName)` pairs
  (186) and `futures` is typed `Array[(Future[String], String)]` (190), but the only consumer reads
  `futures(index)._1` (197). The name used for both the test name and the failure message is
  re-derived from `selectedPythonFiles` at line 193. The pair was meaningful in the previous
  `futures.foreach { case (future, notebookName) => ... }` loop; after the rewrite to index-based
  lookup its second element is dead, and `_1` obscures what is being awaited. `futures` is a public
  member of the suite class, so the dead element is part of its surface.
- **Risk**: clarity and maintenance only. The positional join between `selectedPythonFiles.zipWithIndex`
  (192) and the `submissions` array (185) is now an implicit contract that a name-carrying pair
  would have made explicit and self-checking.
- **Suggested Fix**: drop the tuple —
  `selectedPythonFiles.map(file => Future(runNotebook(file, storeId)))` at 185-187,
  `lazy val futures: Array[Future[String]]` at 190, and `Await.result(futures(index), notebookTimeout)`
  at 197. If the positional contract is worth asserting, `assert(futures.length == selectedPythonFiles.length)`
  is cheaper than carrying the unused string.

### Issue 4: Docs omit that a CI run performs cleanup three times, and describe executor setup as "remote work"
- **Severity**: Low
- **File**: docs/Reference/Developer Setup.md
- **Line(s)**: 76-86 (specifically 80-81 and 85-87)
- **Description**: lines 76-78 present the CI behavior as a single named `Fabric cleanup preflight`
  task gating E2E. Lines 80-86 then attribute the cached preflight to "Direct smoke and notebook
  suite runs". Because `preflight` is a per-instance `private lazy val`
  (FabricNotebookTests.scala:46), the `E2E` step's
  `sbt "testOnly ...FabricSmokeTests ...FabricNotebookTests"` (pipeline.yaml:392) runs cleanup once
  more per suite — three workspace cleanups per CI run. The documentation never states this, and
  "Direct ... runs" reads as a non-CI path. Separately, lines 85-87 state that store creation and
  executor setup failures are cached "so later tests do not repeat remote work"; executor creation
  (FabricNotebookTests.scala:158-159) is a local `Executors.newFixedThreadPool` call, so the
  rationale is wrong for half of what the sentence covers.
- **Risk**: an engineer reading the CI log sees the cleanup banner three times and the E2E step
  absorbing two extra full workspace-inventory passes (plus the per-candidate job/schedule queries
  in `FabricArtifactCleanup`), with no documentation confirming that this is by design. The likely
  reaction is a regression report against an intentional safety property. The "remote work"
  phrasing also mis-states what the executor cache protects.
- **Suggested Fix**: add one sentence after line 81, e.g. "Each suite runs its own preflight so that
  resource creation is protected even when a suite is run on its own; a CI run therefore performs
  cleanup once in the gate and once more per E2E suite." Reword lines 85-87 to
  "Store creation and executor setup failures are also cached, so later tests do not repeat them."

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Resolve the cached submission batch before the notebook-outcome
  try/catch. Store and executor setup errors now retain their original exception;
  only actual notebook outcomes get the notebook failure label.
- **Why**: Infrastructure setup errors must not be attributed to jobs that never ran.
- **How verified**: Updated both actual-suite setup-failure tests to require the
  original exception, and all 43 targeted Scala tests passed on the final source.

### Issue 2
- **Status**: Fixed
- **What changed**: Removed the initial E2E not-started marker. The always-run
  evidence step adds it only when no E2E phase was recorded.
- **Why**: A completed E2E run should not retain a not-started marker.
- **How verified**: The executable Bash regression requires the marker to be absent
  after successful or failed E2E execution, present after cleanup failure, and
  present in a new case where setup failed before either test task ran.

### Issue 3
- **Status**: Fixed without changing the existing member type
- **What changed**: Destructure the future/name pair and use its submitted notebook
  name in the job-failure message.
- **Why**: This removes the unused value and positional accessor while retaining
  the existing public `futures` member's return type.
- **How verified**: Compilation and the actual-suite notebook regressions pass;
  registered test names and bounded parallel execution remain unchanged.

### Issue 4
- **Status**: Fixed
- **What changed**: Documented cleanup once in the CI gate and once per E2E suite,
  including direct suite runs. Reworded setup-failure caching to cover initialization
  and notebook batches rather than calling executor creation remote work.
- **Why**: Operator guidance should state the intentional additional checks and
  distinguish local initialization from remote work.
- **How verified**: Compared the documentation with the pipeline's two E2E suites
  and the per-instance preflight/store/submission result caches.

### Final validation

After these fixes, all 43 targeted Scala tests passed with no skips. All-module
`scalastyle`, `Test/scalastyle`, `compile`, and `Test/compile` passed on JDK 11.
All 86 tests in `tools/ci/tests/test_pipeline_yaml.py` passed, including the new
failed-setup case and the assertions against stale E2E metadata. Black 22.3.0
reported 207 files unchanged. No live Fabric result is claimed for the new
orchestration.
