# Fatal cleanup follow-up, round 2

## Review summary

- Theme: architecture and patterns.
- Model: GPT-6 Astra, parent review in the unavailable Gemini slot.
- Scope: the three-file fatal-cleanup follow-up to `02272e0a5d`.
- Issues found: 0.
- Verdict: CLEAN for this bounded review, not a full gauntlet pass.

## Evidence

- `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTracker.scala`
  narrows only the cleanup recovery handler to `NonFatal`. This matches bulk
  cleanup and `FabricNotebookTests.shutdownAndCleanup`. The body handler still
  records and immediately rethrows every throwable so ordinary cleanup failures
  cannot mask a fatal body failure.
- No public signature, serialized parameter, runtime setting, dependency, or
  pipeline definition changes. The helper remains private test infrastructure.
- `FabricTestArtifactTrackerFailureTests.scala` follows the existing private
  test-trait structure. `FabricTestArtifactTrackerSuite` already mixes it in, so
  the existing explicit pipeline selector includes the new regression.
- `docs/Reference/Developer Setup.md` documents fatal propagation and retained
  artifact tracking without claiming that later cleanup is guaranteed to run.
- Local compile, production/test Scala style, and the tracker plus naming suites
  passed with 47 tests. Evidence is retained locally in
  `master-fatal-cleanup-green.log`, not published as a repository artifact.

## Model limitation

The Gemini slot was not executed for this follow-up. Earlier session attempts
with the available Gemini models failed at the backend with HTTP 400 before
review execution. This fallback does not satisfy independent Gemini coverage.
