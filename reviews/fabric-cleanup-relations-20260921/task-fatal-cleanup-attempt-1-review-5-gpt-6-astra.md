# Fatal cleanup follow-up, round 5

## Review summary

- Theme: testing and coverage.
- Model: GPT-6 Astra, parent fallback for the unavailable Gemini slot.
- Scope: the final fatal-cleanup implementation and regression tests.
- Issues found: 0.
- Verdict: CLEAN for this bounded review.

## Requirement-to-test mapping

`FabricTestArtifactTrackerFailureTests` registers two new tests on the existing
CI-selected `FabricTestArtifactTrackerSuite`:

- A 15-combination test covers five categories excluded by `NonFatal` and three
  body outcomes. InterruptedException, InternalError, ThreadDeath, LinkageError,
  and ControlThrowable are crossed with success, ordinary failure, and fatal
  body failure. Assertions check the exact escaping object and the prior body
  error as suppressed where supported.
- A shared-instance interrupt test prevents an IllegalArgumentException from
  self-suppression from replacing the original throwable.

The matrix also verifies that failed per-job deletion remains tracked, retries
once, and does not repeat after successful cleanup. Its independent suppression
probe handles the supported Scala versions without hard-coded version branches.
Existing suite cases continue to cover success return values, body-error
priority over ordinary cleanup errors, missing artifacts, and final cleanup.

The callbacks increment local counters and throw constructed objects. They do
not contact Fabric, exhaust memory, interrupt an actual worker, or depend on
timing. The regression therefore tests the handler boundary directly without
claiming live-service or managed-runtime coverage.

## Evidence and limits

The original code failed the identity assertion. The first correction failed
the strengthened suppressed-error assertion. Both failures were reproduced
before the corresponding fixes. Final master validation passed 48 tests across
two suites with no failures, cancellations, ignored tests, or pending tests,
plus compile, test compile, and production/test Scala style. See locally
retained `master-fatal-cleanup-green-v2.log`.

The Gemini slot did not execute. Earlier backend HTTP 400 failures remain an
unfulfilled independent-family gate, not successful reviews. Port validation
and current-head Azure validation are still separate required evidence.
