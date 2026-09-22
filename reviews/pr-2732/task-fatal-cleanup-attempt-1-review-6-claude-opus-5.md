# Fatal cleanup follow-up, round 6

## Review summary

- Theme: final polish and hardening — performance, observability, docs, naming.
- Model: Claude Opus 5.
- Scope: the frozen three-file fatal-cleanup delta only (55 insertions, 1 deletion).
- Blocking issues: 0. Non-blocking nits: 2.
- Verdict: **CLEAN** for this bounded round.

## Performance

The added `case cleanupError: Throwable` clause sits on a failure-only path inside the
existing `finally` of `withArtifact`. A passing notebook run never enters it, so the
happy path cost is unchanged and no allocation, lock, or extra service call was added.

The regression is cheap. The 15-combination matrix builds two throwables and one tracker
per case and calls only local counters, so it contacts nothing. Measured runtime in
`master-fatal-cleanup-green-v2.log` is 3 seconds 489 milliseconds for all 48 tests across
both suites, with sbt stage totals of 26, 11, 11, 9, and 7 seconds. The four extra
throwable constructions per case are negligible, and `ControlThrowable` carries
`NoStackTrace`, so it does not even fill a trace.

## Observability

Every cleanup failure path still surfaces the failure to a reader:

- Excluded throwable: it escapes and now carries the earlier notebook failure as a
  suppressed entry, so a single stack trace shows both causes.
- `NonFatal` with a failed body: attached as suppressed, printed with the body trace.
- `NonFatal` with a successful body: thrown directly.

`withArtifact` deliberately emits no artifact-scoped `println` on failure, unlike
`cleanup()`, which logs `Artifact cleanup failed for artifact <id>`. This is inherited
and is adequate rather than a gap: a failed delete leaves the id tracked, because
`artifactIds.remove` runs only after a successful delete, so the final `cleanup()` pass
logs that id. On an interrupt or fatal the run terminates by design, which is the
intended signal. No new observability requirement is introduced and none is proposed.

## Documentation and naming

The four added lines in `docs/Reference/Developer Setup.md` are accurate against the
source on all three claims: the escape, the conditional attachment, and the retained
tracking. The qualifier "where that throwable permits suppression" is the honest phrasing
for categories that disable suppression, rather than an over-promise. The wording does
not contradict the neighbouring "Interrupted cleanup preserves the interrupt signal",
which covers final cleanup, because the new text is explicitly scoped to per-job cleanup.
The new lines wrap at 73 to 88 characters, matching the 73 to 88 range of the surrounding
paragraph, and contain no machine-local path.

## Hardening and port readiness

Sizes hold comfortably under `scalastyle-test-config.xml` limits of 800 lines per file,
120 characters per line, and 50 lines per method: the tracker is 72 lines with a longest
line of 101, the trait is 90 lines with a longest line of 108, and `withArtifact` is about
25 lines. `FabricTestArtifactTrackerSuite.scala` stays at 790 of 800, which is why placing
the new cases in the mixed-in trait was the correct choice rather than a style waiver.

`addSuppressed` cannot itself mask the escape: a suppression-disabled throwable makes it a
silent no-op, and `failure` can never hold null because a `throw null` raises a real
`NullPointerException` instance. The anonymous `new ControlThrowable {}` plus the runtime
suppression probe is the version-portable construction, which matters because the ports
build on Scala 2.13, where `ControlThrowable` disables suppression while 2.12 does not.
That behaviour is asserted dynamically instead of hard-coded, so the same source should
hold on the ports — but that remains unproven until the port run executes.

## Non-blocking nits, no action required

Both are cosmetic, and acting on either would mean editing a frozen tree and rerunning a
green suite for no behavioural gain. Recorded for a future touch of these files only.

1. The test names say "fatal", but the matrix covers throwables *excluded by* `NonFatal`,
   and `InterruptedException` and `ControlThrowable` are not fatal in the JVM sense. The
   documentation is more precise here with "an interrupt or fatal error".
2. `cleanupFailures` holds `() => Throwable` factories rather than throwables; the
   per-iteration `newCleanupFailure` name is the accurate one.

## Evidence and limits

- `master-fatal-cleanup-red.log` and `master-fatal-cleanup-diagnostics-red.log` are
  genuine reds for the original masking and for the lost diagnostics respectively.
- `master-fatal-cleanup-green-v2.log` completed: compile, test compile, main and test
  scalastyle each reporting 0 errors, and 48 of 48 tests passing across 2 suites with
  both new cases listed. It contains no `[error]` line and no failed test.
- Rounds 1, 2, 4, and 5 recorded no issues; the round 3 finding is resolved and verified.
- The change touches private test infrastructure only — no public JVM signature,
  generated binding, serialized parameter, or branch runtime setting.
- **No Gemini-family review executed at any point in this task.** Every attempt returned
  a backend HTTP 400 with zero execution, so the independent three-family gate is
  **unfulfilled**. This is not a completed gauntlet.
- Azure validation and current-head GitHub review have not run, and the ports do not
  carry this change yet, so there is no port proof.
