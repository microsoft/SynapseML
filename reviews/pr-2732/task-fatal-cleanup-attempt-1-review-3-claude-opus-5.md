# Review 3 — Edge Cases and Robustness

- **Task:** `task-fatal-cleanup`, attempt 1 | **Round:** 3 | **Model:** claude-opus-5
- **Branch:** master sync worktree, delta relative to HEAD `02272e0a5d`
- **Trigger:** Copilot finding on microsoft/SynapseML#2734, comment 4066689707

## Scope

The three-file uncommitted follow-up only:
`core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTracker.scala`
(one catch clause), `.../FabricTestArtifactTrackerFailureTests.scala` (+33), and
`docs/Reference/Developer Setup.md` (+2). No wider sync or history re-audit.

## Verdict

**ISSUES_FOUND — 1 Low.** The reported bug is genuinely fixed and the regression is a
real one. The Low is the mirror-image information loss the narrowed catch introduces.

## Priority matrix after the change

"Excluded" means anything `NonFatal` rejects: interrupts, `VirtualMachineError`,
`ThreadDeath`, `LinkageError`, `ControlThrowable`.

| body outcome | cleanup outcome | result |
| --- | --- | --- |
| success | success | body value returned |
| success | NonFatal | cleanup error thrown |
| success | excluded | cleanup throwable thrown |
| any failure | NonFatal | body error thrown, cleanup attached as suppressed |
| any failure | excluded | cleanup throwable thrown, **body error discarded** |

The last row is correct on priority — the interrupt or fatal now wins, which is the
point of the fix — but the body error is not merely deprioritised. Because the throw
leaves a `finally`, the in-flight body exception is replaced outright: it is neither
rethrown nor attached, so nothing records it.

## L1 (Low) — body failure vanishes when cleanup throws an excluded throwable

`withArtifact` still assigns `failure = Some(error)` in the body catch, but on the
excluded-throwable path nothing reads it. The single production caller is
`FabricNotebookTests.withTrackedArtifact`, where the body error is the notebook
failure and the cleanup error is an artifact DELETE. A pipeline timeout that
interrupts the DELETE after a notebook has already failed therefore surfaces only
the `InterruptedException`, and the notebook failure that triggered triage is gone.

This is the same class of defect the earlier rounds of this work fixed, and the
repository already has the idiom for it: rethrow the winner with the loser attached
under an `ne` guard, exactly as `FabricArtifactCleanup.run` and this method's own
`NonFatal` branch do. A second catch clause preserves the new priority while keeping
the body error visible, and the 15-case regression asserts only
`thrown eq cleanupFailure`, so it would stay green:

```scala
case cleanupError: Throwable =>
  failure.filterNot(_ eq cleanupError).foreach(cleanupError.addSuppressed)
  throw cleanupError
```

If this is instead accepted as a deliberate trade-off, the new documentation sentence
should say so, because "never suppresses an interrupt or fatal error behind a notebook
failure" does not tell a reader that the notebook failure is then dropped.

## Verified correct

- The narrowed catch is the only production edit. `NonFatal` was already imported and
  used by `cleanup()`, so no import churn and no unused symbol.
- Identity is exact in all 15 combinations: `assert(thrown eq cleanupFailure)` pins the
  same instance rather than the type, which is what the red log disproved.
- The `original ne cleanupError` self-suppression guard on the `NonFatal` path is
  untouched, so a shared instance is still never suppressed into itself.
- Tracking and retry: `artifactIds.remove` sits after `deleteTrackedArtifact`, so any
  throwing cleanup leaves the id tracked. The test proves the retry precisely —
  `attempts == 1` after `withArtifact`, `2` after the first `cleanup()`, and `2` again
  after a second `cleanup()`, so the deque drains and nothing is deleted twice.
- `deleteTrackedArtifact` still treats `PowerBIEntityNotFound` as success, so a
  concurrently deleted artifact is untracked rather than retried.
- Body fatals are still recorded and rethrown by `case error: Throwable`, so a fatal
  body error keeps priority over a `NonFatal` cleanup error.
- Both documentation sentences are accurate: cleanup no longer hides an interrupt or
  fatal, and an unsuccessful per-job deletion does stay tracked.

## Evidence

- `master-fatal-cleanup-red.log`: the new test alone, 1 run, 0 succeeded, 1 failed,
  with `IllegalStateException: job failed was not the same instance as
  java.lang.InterruptedException: cleanup interrupted`. A true red on old behaviour.
- `master-fatal-cleanup-green.log`: `core/compile`, `core/Test/compile`, main and test
  scalastyle each `Found 0 errors`, and 47 of 47 tests passing across
  `FabricTestArtifactTrackerSuite` and `FabricArtifactNamesSuite`, with the new case
  listed by name. 47 is the expected 46 plus one.
- `FabricTestArtifactTrackerFailureTests.scala` is 76 lines and
  `FabricTestArtifactTrackerSuite.scala` is unchanged at 790.

## Non-blocking

Tracking the same artifact id through `withArtifact` twice would leave a duplicate in
the deque, since `ConcurrentLinkedDeque.remove` drops one occurrence. That predates
this delta and is unaffected by it; noted only because retry behaviour was in scope.

## Coverage limitations

Round 1 and the parent's round 2 fallback were both GPT-family and found no concrete
issues. No Gemini version has executed at any point in this task — every attempt has
returned a backend HTTP 400 with zero execution — so the three-family review gate is
**unfulfilled** and this is not a full-gauntlet green. This report covers round 3 only.
Azure Pipelines and current-head GitHub review have not run against this delta, and the
ports do not carry it yet, so no port proof is claimed.

## L1 resolution

The parent accepted L1. A separate excluded-throwable handler now attaches the
earlier body error under an identity guard and immediately rethrows the cleanup
throwable. This preserves fatal priority and diagnostics without wrapping it.
The documentation states that attachment depends on throwable suppression support.

The strengthened matrix failed before this handler was added:
`master-fatal-cleanup-diagnostics-red.log` reports an empty suppressed list instead
of the original `IllegalStateException`. The matrix now checks suppressed errors
as well as identity. A fresh throwable probes suppression support independently,
because `ControlThrowable` support differs across the supported Scala versions.
A separate shared-instance interrupt case guards against self-suppression.
Updated green validation and reviewer verification follow below.

## Round 3 verification of the L1 resolution

Verified against the current worktree; the original verdict and history above stand
unchanged. **L1 is resolved. No new issue.**

- **Catch order is safe.** `case NonFatal(cleanupError)` precedes
  `case cleanupError: Throwable`, so the broad clause cannot shadow the narrow one and
  only excluded throwables reach it. Reversing them would have silently restored the
  original bug.
- **Priority is unchanged; only diagnostics changed.** The excluded path still ends in
  `throw cleanupError`, so the interrupt or fatal still wins and still replaces the body
  error as the propagating exception. The only difference is that the body error is now
  attached instead of vanishing, which is exactly the narrow repair L1 asked for.
- **Identity guard is correct in both directions.**
  `failure.filterNot(_ eq cleanupError)` yields `None` both when the body succeeded and
  when the body threw the same instance, so `addSuppressed` is never called on self and
  cannot raise `IllegalArgumentException`.
- **The probe cannot contaminate the assertion.** `newCleanupFailure()` builds a fresh
  instance, so `thrown.getSuppressed` is read from an object the probe never touched.
  Deriving `expectedSuppressed` from `jobFailure.toSeq` also keeps the body-success rows
  expecting an empty list, and the probe makes the matrix portable across the Scala
  versions that differ on `ControlThrowable` suppression rather than hard-coding either.
- **The same-instance test is a real negative control.** Without the `filterNot` guard
  the tracker would call `addSuppressed` on self and fail with `IllegalArgumentException`
  rather than returning the shared `InterruptedException` with an empty suppressed list.
- **Docs are accurate.** All three sentences hold: the cleanup throwable escapes, the
  earlier notebook failure is attached, and the qualifier "where that throwable permits
  suppression" honestly covers categories that disable suppression instead of
  over-promising.
- **Evidence.** `master-fatal-cleanup-diagnostics-red.log` is a true red for the new
  assertion — `Array() did not equal List(java.lang.IllegalStateException: job failed)`,
  1 run, 0 succeeded, 1 failed. `master-fatal-cleanup-green-v2.log` has completed:
  `core/compile`, `core/Test/compile`, main and test scalastyle each `Found 0 errors`,
  `Run completed`, and 48 of 48 tests passing across both suites, with both new cases
  listed by name. 48 is the expected 47 plus the same-instance test. The log carries no
  `[error]` line and no `*** FAILED ***`; its nine keyword hits are test names and the
  tracker's own expected `println` diagnostics. All three edited files were last written
  before that run, so the log describes this source.

Round 3 verification only. No Gemini version has executed at any point in this task, so
the three-family gate remains **unfulfilled** and this is not a full-gauntlet green.
