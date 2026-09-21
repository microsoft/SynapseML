# Follow-up round 5: proof and CI selection

**Result:** CLEAN for the bounded correction.
**Reviewer:** GPT-6 Astra, direct fallback, 2026-09-21.

A fresh read-only Gemini 3.8 request also failed with backend HTTP 400 before
reviewing. No Gemini review is claimed.

- The red control recorded three DELETE attempts after a confirmation read
  failed, where one was expected. The assertion measures destructive requests,
  not only returned IDs or exception type.
- The matrix runs 16 parameter combinations across four read sites, with and
  without prior DELETE failure and with distinct/reused exceptions. It asserts
  the escaping instance, exact suppressed errors, exact attempted IDs, and store
  retention. The third job detects unintended continuation. The reused-error
  flag does not create a distinct control-flow behavior when no prior error
  exists; the report does not count 16 separate ScalaTest tests.
- The bounded confirmation case has two eligible jobs, asserting one DELETE
  and 30 pauses. Existing direct-DELETE-failure tests still demonstrate that
  independent jobs are attempted and stores retained.
- The two extracted tracker tests are registered through
  `FabricTestArtifactTrackerFailureTests` on the existing CI-selected suite.
  The concrete suite selector ran 43 tests and explicitly listed both cases.
  The trait is not an additional unscheduled test class.
- `master-confirmation-read-green-v4.log` reports 46 passed across two suites,
  including the three naming tests, with zero failures, canceled, ignored, or
  pending tests. `master-confirmation-read-green-v3.log` proves zero style
  errors on the same source and the exact tracker-suite selection.
- The prior v3 naming-suite FQN was wrong; v4 corrects that selection. The v4
  native SBT run passed; only the subsequent PowerShell log-check invocation
  failed, and a corrected named-parameter check verified the expected output.

The logs are locally retained evidence. No live service, new port execution,
or completed three-family gauntlet is claimed.
