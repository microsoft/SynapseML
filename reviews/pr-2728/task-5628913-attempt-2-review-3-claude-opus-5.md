# Code Review — Round 3 of 6 (sequential mode)

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: C:\Users\singhrana\Documents\SynapseML\.worktrees\fabric-test-cleanup-20260918\reviews\pr-2728\task-5628913-attempt-2-review-3-claude-opus-5.md
- **Issues Found**: 7
- **Verdict**: ISSUES_FOUND

Severity mix: 0 Critical, 0 High, 2 Medium, 5 Low. All stated requirements are met
except the interrupt/fatal path of the preflight memo (Issue 1). No finding asks to
widen the committed 24-hour repo-owned deletion policy, and none of the findings
block the approved design (named CI preflight after setup/auth; cached lazy guards
for direct suite runs).

## Evidence Checklist
- [x] Enumerated the uncommitted change set with `git --no-pager status --porcelain=v1` and
      `git --no-pager diff --stat` in `.worktrees\fabric-test-cleanup-20260918`: exactly five modified
      files (`FabricNotebookTests.scala`, `FabricTestArtifactTrackerSuite.scala`,
      `docs/Reference/Developer Setup.md`, `pipeline.yaml`, `tools/ci/tests/test_pipeline_yaml.py`),
      387 insertions / 51 deletions, nothing staged.
- [x] Read the full post-change source of
      `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala` (not only the
      hunks) plus its per-file `git diff`, and traced every new lazy guard: `preflight` (L31),
      `ensureFabricPreflight` (L33), `createTrackedStore` (L45), `storeArtifactId` (L79, L136),
      `executorService`/`executorStarted` (L141-L148), `futures` (L166-L172), test body (L175-L184),
      `afterAll` (L187-L195), `shutdownAndCleanup` (L247).
- [x] Confirmed the constructor-does-not-contact-Fabric requirement by reading
      `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/FabricConnection.scala`: `fabric` is a
      `lazy val` (L15, L31) that reads `fabricWorkspaceId` only at first use, so moving
      `fabricWorkspaceId = Some(integrationWorkspaceId)` into `cleanupStaleArtifacts()` (L27) binds the
      workspace before `FabricOperations` is constructed; the trait body now only assigns vars and builds
      `FabricTestArtifactTracker` with a by-name lambda, and `FabricNotebookTests`' constructor runs only
      `discoverNotebooks()` (local generation + file listing).
- [x] Verified bounded concurrency is unchanged: `FabricNotebookTests.MaxConcurrency = 3` and
      `createNotebookExecutor()` still returns `Executors.newFixedThreadPool(MaxConcurrency)`
      (FabricNotebookTests.scala L142-L143, L199); the new fixture test asserts
      `peak.get() == FabricNotebookTests.MaxConcurrency` with 4 notebooks and a 3-permit latch.
- [x] Verified shutdown ordering and failure aggregation survive: `shutdownAndCleanup(shutdown: => Unit,
      cleanup: => Unit)` (L247) takes both arguments **by name**, so `if (executorStarted) shutdownExecutor(...)`
      (L190) is evaluated inside its own `try`, cleanup still runs when shutdown throws, and the
      interrupt flag/suppressed-exception handling (L250-L268) is untouched.
- [x] Verified the failed-preflight teardown does not create a pool: `executorService` is now `lazy`
      and `afterAll` is guarded by `executorStarted`; the new test
      `"Cache notebook preflight failure and never initialize stores, submissions, or an executor"`
      asserts `suite.calls == Vector("cleanup")` (no `store`, no `executor`) across 4 failing tests.
- [x] Empirically disproved the assumption that `Try` caches every preflight failure: disassembled the
      resolved standard library at
      `%USERPROFILE%\AppData\Local\Coursier\cache\...\scala-library\2.12.18\scala-library-2.12.18.jar`
      with `javap -p -c scala.util.control.NonFatal$` — it tests `instanceof VirtualMachineError |
      ThreadDeath | InterruptedException | LinkageError | ControlThrowable` — and `javap -p -c
      scala.util.Try$`, whose `apply` catches only via `NonFatal$.unapply`. (`build.sbt:34` pins
      `scalaVersion := "2.12.17"`; `NonFatal`/`Try` are identical across 2.12.x, and the cached 2.12.18
      artifact was the only local copy available.)
- [x] Checked suite-level parallelism before asserting a cleanup race: `build.sbt:282` sets
      `Test / parallelExecution := false`, so the two in-suite preflights inside
      `sbt "testOnly ...FabricSmokeTests ...FabricNotebookTests"` run sequentially — the concurrency
      claim in Issue 4 is therefore scoped to redundancy and cross-run overlap, not an intra-JVM race.
- [x] Read `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala` and
      `FabricTestArtifactTracker.scala` to evaluate the blast radius of repeated/partial cleanup:
      `deleteAndConfirm` tolerates `PowerBIEntityNotFound`, `confirmAbsent` retries 31×2s, and
      `safeStore` requires `!current.values.exists(ownedJob)` — the last point is why orphaned stores can
      persist (Issue 2). The committed 24-hour `RetentionSeconds`/`expired` policy is unchanged by this diff.
- [x] Read the full `pipeline.yaml` FabricE2E job (L320-L440) rather than the hunks: preflight task
      ordering after `templates/fabric_kv.yml` + `templates/publish.yml`, `condition: succeeded()` on
      E2E, `>>`-only metadata appends in E2E, `always()` collect + publish + artifact steps.
- [x] Validated the hard-coded report path `$(Build.SourcesDirectory)/core/target/test-reports`
      (pipeline.yaml L368, L406) against the real layout: `core\target\test-reports` exists in this
      worktree and in sibling worktrees, and `grep` found no `testOptions`/`test-reports` override in
      `build.sbt` or `project/`. The new `failTaskOnMissingResultsFile: true` gate therefore points at
      the correct directory — not a finding.
- [x] Verified the notebook test-name change is behavior-preserving:
      `FabricOperations.getBlobNameFromFilepath` (FabricOperations.scala L507-L509) is
      `filePath.split(File.separatorChar).last`, equivalent to the new `notebookFile.getName` on the
      Linux agents, so published test IDs in `PublishTestResults` do not shift.
- [x] Read the new Bash-executing pytest and the mock `sbt`/`activate` fakes in
      `tools/ci/tests/test_pipeline_yaml.py`, including the `(0,0) / (17,0) / (0,23)` parametrization and
      the `rm -f` of the cleanup report on the E2E invocation, to determine what the fakes actually prove.
- [ ] No live execution, no sbt run, no Fabric contact, and no re-run of the reported validation
      (40 Scala tests, scalastyle, compile/Test compile, 85 pipeline Python tests, Black 22.3.0) — the
      task explicitly forbids it; findings below are derived from source and disassembled library
      semantics only.

## Issues

### Issue 1: Preflight memo does not capture `InterruptedException`, so a cancelled cleanup is re-run instead of staying cached
- **Severity**: Medium
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 31, 33 (`private lazy val preflight = Try(cleanupStaleArtifacts())`, `ensureFabricPreflight`)
- **Description**: `scala.util.Try.apply` catches only `NonFatal`, and `NonFatal` explicitly excludes
  `InterruptedException`, `VirtualMachineError`, `ThreadDeath`, `LinkageError`, and `ControlThrowable`
  (verified by disassembly of the resolved `scala-library` — see the evidence checklist). When
  `cleanupStaleArtifacts()` is interrupted, the `Try` does not convert it to a `Failure`; the exception
  escapes the lazy-val initializer, so the JVM never marks `preflight` initialized. The next
  `ensureFabricPreflight()` call — from the next registered test, or from `storeArtifactId`/`futures` —
  re-runs the **entire** cleanup pass. This is the one path where the "cleanup failure remains cached"
  requirement does not hold. A subsequent successful re-run can also silently flip the suite from failing
  to passing after an interrupt.
- **Risk**: Cancellation is a realistic trigger in this job: `pipeline.yaml` sets
  `timeoutInMinutes: 120` and `cancelTimeoutInMinutes: 5` for FabricE2E. On cancellation the interrupted
  cleanup is retried per remaining test, each retry performing a full paginated inventory scan plus
  per-candidate `jobs()`/`schedules()` calls and issuing real deletes during teardown — burning the
  5-minute cancel window and mutating the shared workspace while the job is being torn down. The
  interrupt status is also not restored before the exception escapes, so cooperative cancellation
  downstream is lost.
- **Suggested Fix**: Replace the `Try(...)` memo with an explicit total capture and an interrupt-aware
  rethrow, e.g. `private lazy val preflight: Try[Unit] = try Success(cleanupStaleArtifacts()) catch {
  case t: Throwable => Failure(t) }`, and in `ensureFabricPreflight()` re-set
  `Thread.currentThread().interrupt()` before rethrowing a cached `InterruptedException`. This keeps the
  existing exception-identity contract asserted by
  `FabricTestArtifactTrackerSuite` (`failures.forall(_.throwable.contains(failure))`) while making the
  memo total.

### Issue 2: `storeArtifactId` and `futures` are not failure-cached, so a transient failure is retried once per test and can duplicate remote work
- **Severity**: Medium
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 136-139 (`lazy val storeArtifactId`), 166-172 (`lazy val futures`), 179 (`futures(index)._1`), 45 (`createTrackedStore`)
- **Description**: A Scala `lazy val` whose initializer throws is *not* marked initialized, so it is
  recomputed on the next access. `preflight` (L31) is deliberately wrapped in `Try` to avoid exactly
  this; `storeArtifactId` and `futures` are not. Because `futures(index)` is evaluated inside each test
  body (L179) and the suite registers one test per selected notebook (currently 5 in
  `IncludedNotebooks`), a failure inside these initializers is re-attempted once per remaining test.
  Two concrete paths: (a) `createTrackedStore()` → `fabric.createStoreArtifact()` fails transiently
  (429/5xx/poll timeout) — each remaining test issues another store-creation attempt, and any store
  created server-side whose id never reached `trackArtifact` is untracked; (b) `Future(...)` submission
  throws for element *k* after 0..*k*-1 were already submitted (e.g. `RejectedExecutionException`, OOM) —
  `futures` stays uninitialized and the next test resubmits notebooks 0..*k*-1, producing duplicate SJD
  artifacts and duplicate jobs against the same store.
- **Risk**: Workspace resource amplification and orphan accumulation. Untracked stores are reclaimed only
  by the 24-hour cleanup, and `FabricArtifactCleanup.safeStore` refuses to delete a store while **any**
  owned `SparkJobDefinition` exists in the workspace, so orphans can survive many runs. Duplicate job
  submissions double the load on the same lakehouse and inflate the tracker, and the duplicated failures
  surface as N confusing `Job failed for <notebook>` wrappers (L181-L182) that all describe one
  store-creation fault.
- **Suggested Fix**: Memoize the submission stage the same way as `preflight`, e.g.
  `private lazy val submissions: Try[Array[(Future[String], String)]] = Try { ensureFabricPreflight();
  val storeId = storeArtifactId; selectedPythonFiles.map(f => (Future(runNotebook(f, storeId)), f.getName)) }`
  with `futures` reading `submissions.get`, and make `storeArtifactId` a cached `Try` as well. The first
  failure is then reported to every test without re-contacting Fabric, matching the reviewed intent.

### Issue 3: `executorStarted` is a non-volatile `var` read unsynchronized in `afterAll`, risking a skipped executor shutdown
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 141 (declaration), 147 (write inside the `executorService` lazy initializer), 190 (read in `afterAll`)
- **Description**: The write happens inside the lazy-val initializer (monitor-protected in Scala 2.12),
  but the read in `afterAll` takes no lock and the field is not `@volatile`, so the code establishes no
  happens-before edge of its own. Today this is safe only because ScalaTest drives tests and `afterAll`
  on the same thread (`Test / parallelExecution := false` in `build.sbt:282` does not change that, and
  the new `executeSuite` helper in `FabricTestArtifactTrackerSuite` also runs `suite.run(...)` on the
  calling thread). The guard is therefore correct but incidentally correct.
- **Risk**: If `futures`/`executorService` is ever first touched from a helper thread, or the suite gains
  `ParallelTestExecution`, `afterAll` can read a stale `false`, skip `shutdownExecutor`, and leak three
  non-daemon pool threads with in-flight Fabric polling — the exact leak the `shutdownAndCleanup`
  ordering (L247-L268) was introduced to prevent, and one that can keep a forked test JVM alive.
- **Suggested Fix**: Make the flag `@volatile`, or better, remove the flag entirely: hold the pool in a
  `private val executorRef = new AtomicReference[ExecutorService]` assigned inside the lazy initializer
  and use `Option(executorRef.get()).foreach(FabricNotebookTests.shutdownExecutor)` in `afterAll`. That
  keeps the "no pool for failed-preflight teardown" requirement and removes the visibility question.

### Issue 4: The in-suite preflight is memoized per suite instance, so the E2E step re-runs two extra full cleanup passes after the gate task already passed
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala; pipeline.yaml
- **Line(s)**: FabricNotebookTests.scala 24-33 (`cleanupStaleArtifacts` + per-instance `preflight`); pipeline.yaml 361, 383
- **Description**: `preflight` is an instance-level `lazy val`, so each suite performs its own cleanup.
  In CI the dedicated `Fabric cleanup preflight` task already runs `FabricTestCleanup`; the subsequent
  `sbt "testOnly ...FabricSmokeTests ...FabricNotebookTests"` then runs `FabricArtifactCleanup.run` twice
  more in the same JVM. `Test / parallelExecution := false` (build.sbt:282) rules out an intra-JVM race,
  so this is redundancy rather than a race. The redundancy is structural: `cleanupStaleArtifacts()`
  couples a cheap per-instance assignment (`fabricWorkspaceId = Some(integrationWorkspaceId)`, L27) to
  the expensive remote pass, so the remote pass cannot be shared without breaking the second suite's
  `fabric` construction.
- **Risk**: Each extra pass is a paginated inventory scan plus a re-read of the full inventory per
  deletion candidate (`FabricArtifactCleanup.run`), inside a job capped at `timeoutInMinutes: 120`. More
  importantly, a transient inventory/delete failure in pass 2 or 3 now fails **every** smoke and notebook
  test even though the approved gate task already succeeded, converting an infrastructure blip into an
  E2E red. With concurrent pipeline runs, three passes per run also widen the window in which
  `safeStore`'s "no owned job exists" precondition is false, silently retaining stale stores.
- **Suggested Fix**: Keep the approved semantics but split the method: always assign `fabricWorkspaceId`
  per instance, and memoize only the remote `fabric.cleanupTestArtifacts(...)` call once per JVM (an
  `object`-level `lazy val`/`AtomicReference` keyed by workspace id). Alternatively gate the in-suite
  cleanup behind an env flag that CI leaves unset, since CI already has the dedicated preflight task,
  while direct/dev suite runs keep the cached guard.

### Issue 5: `SYNAPSEML_FABRIC_CLEANUP_DRY_RUN` validation is untrimmed and case-sensitive, and its blast radius is now every Fabric suite
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala
- **Line(s)**: 25-26
- **Description**: The validation logic moved verbatim from the old `FabricTestCleanup` test body into
  the shared `cleanupStaleArtifacts()`, so it is pre-existing logic — but its reach changed. `"True"`,
  `"TRUE"`, `"1"`, or `"false "` (a trailing space is easy to introduce through an ADO variable or a
  shell export) now fails `FabricSmokeTests` and every `FabricNotebookTests` test, and the cached
  `IllegalArgumentException` message names neither the suite nor the offending value.
- **Risk**: A one-character environment typo turns the whole Fabric E2E red with a message that reads
  like a product failure, and the cached exception repeats identically across all tests, making triage
  slower than the old single-test failure.
- **Suggested Fix**: Normalize and echo the bad value while keeping the fail-closed behavior:
  `val raw = sys.env.getOrElse("SYNAPSEML_FABRIC_CLEANUP_DRY_RUN", "false").trim`, then
  `require(Set("true", "false")(raw.toLowerCase), s"SYNAPSEML_FABRIC_CLEANUP_DRY_RUN must be true or false, got '$raw'")`.

### Issue 6: The E2E step appends to `run-metadata.txt` without creating `$artifact_root`, relying on the preflight task having run
- **Severity**: Low
- **File**: pipeline.yaml
- **Line(s)**: 385-388 (E2E `set -eo pipefail` + first `>>` append) versus 350-351 (`mkdir -p` only in the preflight step)
- **Description**: The E2E task's first effective statement appends to
  `"$artifact_root/run-metadata.txt"`, a path created solely by the preflight task. `condition:
  succeeded()` reflects job status, not "the previous step ran": if the preflight task is ever skipped
  (a future condition, a template reorder, or a `continueOnError` change) the job status can still be
  succeeded while the directory does not exist, and the append dies under `set -e` with a bare
  redirection error and zero metadata.
- **Risk**: Evidence loss in exactly the scenario the metadata exists to explain, plus a failure message
  that points at a redirection rather than at the missing preflight. The new Bash-executing test never
  exercises this ordering because it always calls `run_step("Fabric cleanup preflight", ...)` before
  `run_step("E2E", ...)`.
- **Suggested Fix**: Add `mkdir -p "$artifact_root"` immediately after `artifact_root=...` in the E2E
  inline script (idempotent, one line), and optionally add a pytest case that runs the E2E script
  standalone against an empty staging directory.

### Issue 7: The new Bash-executing test proves the preflight `cp` but never proves the collect step's `find` copy
- **Severity**: Low
- **File**: tools/ci/tests/test_pipeline_yaml.py
- **Line(s)**: new test `test_fabric_preflight_scripts_preserve_exit_codes_and_evidence` (mock `sbt` body and the final `is_file()` assertion); interacts with pipeline.yaml 417-420, 426-430
- **Description**: The mock `sbt` deletes `TEST-...FabricTestCleanup.xml` and writes
  `TEST-...FabricSmokeTests.xml` on the E2E invocation, which is a good model of the real cleanup-report
  lifetime — but the only filesystem assertion is that `TEST-...FabricTestCleanup.xml` exists in
  staging, and that file is placed there by the preflight step's `cp`, not by the collect step's `find`.
  Nothing executed asserts that the collect step actually copied the smoke report; the collect step's
  pattern is only string-matched elsewhere (`"TEST-com.microsoft.azure.synapse.ml.nbtest.Fabric*.xml" in
  collect["bash"]`).
- **Risk**: A wrong `report_root`, a wrong `-maxdepth`, or a narrowed `-name` pattern would leave the
  test suite green while `PublishTestResults` — now hard-scoped by `searchFolder` and armed with
  `failTaskOnMissingResultsFile: true` — fails or publishes incomplete results on every real run. (The
  current `core/target/test-reports` path was verified correct by hand; the gap is the missing
  regression guard, not a present bug.)
- **Suggested Fix**: In the `cleanup_exit == 0` branch, also assert
  `(artifact_root / "test-reports" / "TEST-com.microsoft.azure.synapse.ml.nbtest.FabricSmokeTests.xml").is_file()`,
  so the executed test covers the collect step's copy as well as the preflight step's.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Added interrupt-aware setup capture and retrieval. Cleanup and
  resource setup retain the original `InterruptedException`; each retrieval restores
  the thread interrupt before rethrowing it.
- **Why**: Cancellation must not restart cleanup. Fatal JVM errors remain uncaught;
  the suggested catch-all `Throwable` implementation would hide those errors.
- **How verified**: Added `Cache interrupted preflight and restore interrupt status
  on each access`, which clears the flag between accesses and asserts one cleanup
  attempt, identical exceptions, and a restored flag. Final validation is recorded below.

### Issue 2
- **Status**: Fixed
- **What changed**: Cached store setup and the initial submission batch as results,
  retaining successful allocations and failures separately from their public lazy getters.
- **Why**: A later test must not repeat remote allocations or an already-started batch.
- **How verified**: Added actual-suite regressions for failed store allocation and
  executor setup. Both assert one attempt across four tests and no notebook work.
  An initial rejection experiment disproved the suggested `Future(...)` failure
  trace on Scala 2.12: its execution-context callback reports rejection rather than
  throwing out of the batch initializer. That rejected-work timeout behavior was
  pre-existing and does not resubmit the batch. The retained regression exercises
  the real synchronous setup failure instead. Final validation is recorded below.

### Issue 3
- **Status**: Fixed
- **What changed**: Marked `executorStarted` volatile.
- **Why**: The current suite executes teardown on the same thread, but a volatile
  flag makes publication explicit without introducing another resource abstraction.
- **How verified**: Existing actual-suite success and failed-preflight regressions
  cover executor termination and the absence of executor allocation, respectively.

### Issue 4
- **Status**: Not a defect; intentional requirement
- **What changed**: Retained the preflight task and per-suite guards.
- **Why**: The user approved both. Direct suite runs must be safe without relying on
  a previous CI process. A global memo or bypass flag would change that contract and
  couple independently configured suites. The additional inventory passes are a
  deliberate safety cost; they do not relax the 24-hour deletion policy.
- **How verified**: The suite regression asserts one cleanup call per instance,
  not one per test. The pipeline runs its explicit gate before both suites.

### Issue 5
- **Status**: Not a defect; existing fail-closed contract
- **What changed**: Kept the exact documented `true`/`false` validation.
- **Why**: The standalone cleanup already rejected these values and therefore
  already blocked CI E2E. Invalid configuration should also block direct runs.
  Normalization is not required, and printing arbitrary environment values is unnecessary.
- **How verified**: Compared the unchanged validation with HEAD and the operator
  documentation. The error names the setting and both accepted values.

### Issue 6
- **Status**: Not a defect in current wiring
- **What changed**: Kept E2E dependent on successful preflight initialization.
- **Why**: Preflight has no skip condition or `continueOnError`; the hypothetical
  future misconfiguration is absent. Allowing standalone E2E metadata initialization
  would weaken the expected ordering rather than enforce it. The `always()` evidence
  step already creates fallback metadata when preparation fails.
- **How verified**: Pipeline regressions check task order, success gating, no
  `continueOnError`, append-only E2E metadata, and evidence fallback.

### Issue 7
- **Status**: Fixed
- **What changed**: Added an assertion for the staged smoke report after running the
  real collect-step script, including its absence when cleanup failure skips E2E.
- **Why**: This proves the collect step copies E2E reports, not only that preflight
  preserved its own report.
- **How verified**: All 13 Fabric pipeline tests passed after this assertion,
  including successful runs, cleanup exit 17, and E2E exit 23. Black 22.3.0 passed.

### Post-fix validation

The final targeted run passed all 43 tests in `FabricTestArtifactTrackerSuite` and
`FabricArtifactNamesSuite`, with no skipped tests. Compilation of the changed
Scala test code and all-module `scalastyle`/`Test/scalastyle` passed. The interrupted
preflight, store-allocation failure, and executor-setup failure regressions passed.
Notebook exception handling now restores interrupts explicitly and wraps only
`NonFatal` errors, so fatal setup errors are not converted into ordinary test failures.
