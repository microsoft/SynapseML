# Fatal cleanup follow-up, round 4

## Review summary

- Theme: detailed correctness.
- Model: GPT-6 Astra, direct parent review.
- Scope: the final fatal-cleanup delta, including the round-3 L1 resolution.
- Issues found: 0.
- Verdict: CLEAN for this bounded review.

## Data-flow audit

`FabricTestArtifactTracker.withArtifact` records and rethrows a body failure.
Cleanup succeeds before the tracked id is removed. If cleanup fails, that id
remains available for the final cleanup attempt.

The first recovery clause matches `NonFatal` only. It preserves the existing
priority: an earlier body error wins, otherwise the cleanup error escapes.
The following `Throwable` clause can therefore match only excluded throwables.
It attaches a distinct earlier body failure where suppression is supported and
always throws the same cleanup object. A missing body failure and a shared
throwable instance both produce an empty filtered option, avoiding null or
self-suppression. The catch order is essential and correct.

The new test's fresh suppression probe cannot alter the actual cleanup error.
Each matrix case constructs a new cleanup throwable and tracker. Expected
suppressed entries come from the selected body outcome, not the implementation.
Callback attempts must be 1 after failure, 2 after retry, and still 2 after a
second final cleanup. The separate shared-instance interrupt case checks both
identity and an empty suppressed list.

## Evidence and limits

- The original fatal masking and the initial fix's lost diagnostics each have
  failing regression logs: `master-fatal-cleanup-red.log` and
  `master-fatal-cleanup-diagnostics-red.log`.
- `master-fatal-cleanup-green-v2.log` records compile, test compile,
  production/test Scala style, and 48 passing tests across the tracker and
  naming suites. These logs are local evidence, not repository artifacts.
- This changes private test infrastructure, not public JVM signatures,
  generated bindings, serialized values, or branch runtime settings.
- No Gemini review executed because of the previously established backend
  failure. This report is not a completed three-family gauntlet or Azure CI.
