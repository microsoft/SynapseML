# Review 3 — Edge Cases and Robustness (attempt 2)

Latest driver disposition: the evidence-only M1 is resolved by
`master-confirmation-read-green-v4.log`: 46 passed across two suites, with no
failures, cancellations, ignored, or pending tests. The command retains the
exact CI tracker-suite selector and uses `*FabricArtifactNamesSuite` for the
correct naming suite. Both mixed-in error tests are listed. The unchanged
source's style check passed in v3. A post-run PowerShell log-check typo was
corrected separately; it was not an SBT test failure. Original findings below
are preserved as review history.

- **Task:** `task-spark4-sync-20260921`, attempt 2 | **Round:** 3 (bounded follow-up)
- **Model:** claude-opus-5 | **Branch:** master sync worktree, HEAD `1aff4ce470`
- **Trigger:** current-head High in microsoft/SynapseML#2732 (`discussion_r4066179872`)

## Scope

The uncommitted confirmation-read correction only, not the sync or its history:
`core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala`,
`.../FabricTestArtifactTrackerSuite.scala`, `.../FabricTestArtifactTrackerFailureTests.scala`,
`docs/Reference/Developer Setup.md`. The inherited success-only summary enhancement is
not reopened.

## Verdict

**ISSUES_FOUND — 1 Medium.** The correction is sound; no defect found in its logic, tests,
or docs. The Medium is an evidence gap: the cited green log provably predates the source.

## Verified correct

- `tryDeleteItem` returns `Option[Throwable]` for real DELETE failures only, matching
  `PowerBIEntityNotFound` before `NonFatal` and returning `None`, so a concurrently deleted
  item still flows into `confirmAbsent` — which now sits in the `case None =>` branch,
  outside any recoverable catch, so a read or timeout aborts the loop with no further DELETE.
- One `try`/`catch NonFatal` wraps the whole loop, runs
  `failures.filterNot(_ eq e).foreach(e.addSuppressed)` and rethrows the **same**
  instance: identity and the self-suppression guard both hold.
- `Some(e)` still appends to `failures` and continues, so independent job deletions
  proceed and stores stay gated by `failures.isEmpty && safeStore(...)`. `initial =
  index(client.inventory())` stays outside the `try`, harmless because `failures` is empty.
- `ConfirmationAttempts = 31` with `require(remaining > 1, ...)` yields 31 reads and
  30 pauses, matching the bounded-confirmation test.
- The 16-case matrix (inventory/jobs/schedules/confirmation x prior DELETE failure x
  reused/distinct instance) asserts thrown identity, exact suppressed set, exact attempted
  IDs, and store retention; its read-ordinal arithmetic traces correctly, `jobs`/`schedules`
  do not touch the inventory counter, and the extra `lastJob` candidate is a real control.
- The bounded-confirmation test seeds two jobs and asserts `client.deleted ==
  Vector(staleJob.id)` and `pauses == 30`, failing under the old continue-on-failure path.
- `deleteAndConfirm` has no remaining references, and every claim in the
  `Developer Setup.md` paragraph matches the source, including interrupts staying outside
  `NonFatal` with prior propagation.

## M1 (Medium) — cited green evidence predates the current source

`master-confirmation-read-green-v2.log` reports `FabricTestArtifactTrackerFailureSuite:`
among 3 discovered suites. That class no longer exists: those tests became
`private[nbtest] trait FabricTestArtifactTrackerFailureTests`, mixed into
`FabricTestArtifactTrackerSuite`. The log cannot describe the current tree, and the
"46/46 across 3 suites" shape is stale — the selector now resolves to 2 suites.

`FabricArtifactCleanup.scala` is older than that log and stays covered; only the later
test-file split is unproven. Inspection finds no problem with it: the mix-in is present
so both tests still register, the total stays 46, the trait's imports are all used, and no
`testNames` assertion reflects on `this`. But compile, scalastyle, and the run were not
re-observed on this source. **Action:** rerun `core/Test/scalastyle` and the same
`testOnly` selector, expecting 46 tests over 2 suites; do not cite the existing log until
then.

## Non-blocking

- `Developer Setup.md`: the rewrap leaves a ragged ~39-character line ("suppressed
  exceptions. Reused exception") mid-paragraph — cosmetic. Separately, with
  `previousFailure = false` the `reuseFailure` axis is inert, so the 16 cases cover 12
  distinct behaviours; harmless redundancy given the line budget.

## Evidence and coverage limitations

- `master-confirmation-read-red.log`: old behaviour attempted 3 DELETEs after the
  confirmation read threw, 1 expected. `FabricTestArtifactTrackerSuite.scala` is 790 lines.
- No Gemini version has executed at any point: 3.8/3.7/3.6 returned backend HTTP 400 in
  round 2 and 3.5 failed identically with zero turns before round 5. The three-family gate
  is **unfulfilled**; this is not a full-gauntlet green.
- Azure Pipelines and current-head GitHub review have not run against this change, and the
  ports do not carry it yet, so no port proof is claimed.

## Addendum — CI wiring and the v3 log (follow-up)

Wiring claim verified. `pipeline.yaml:968` names
`com.microsoft.azure.synapse.ml.nbtest.FabricTestArtifactTrackerSuite` explicitly and
`nbtest` is wildcarded nowhere in that file, so a separate `FailureSuite` class would have
run locally but never in the `misc` leg. Converting it to a trait mixed into the
CI-selected suite is the right fix, and `pipeline.yaml` is unmodified so no pipeline
permission is needed. `PipelineTestCoverageSuite` filters on `isConcreteClass`, so the new
trait needs no matrix entry and cannot trip that guard.

**M1 stays open.** `master-confirmation-read-green-v3.log` exists and postdates the 14:03
refactor, and it does prove the wiring: scalastyle 0 errors, and
`FabricTestArtifactTrackerSuite` runs 43 tests rather than its previous 41, so both moved
cases register on the CI-selected suite. But it reports **43 tests across 1 suite**, not
the expected 46 across 2. The selector asked for `...ml.nbtest.FabricArtifactNamesSuite`
while that class is in `...ml.fabric`, so `testOnly` matched nothing and silently dropped
its 3 tests (43 + 3 = 46). `pipeline.yaml:1896` documents this exact hazard: `testOnly`
exits 0 when its filter matches nothing.

**Action:** rerun with `com.microsoft.azure.synapse.ml.fabric.FabricArtifactNamesSuite` and
confirm 46 across 2 suites. CI coverage of that suite is unaffected — it is matched by
`com.microsoft.azure.synapse.ml.fabric.**` at `pipeline.yaml:957`.
