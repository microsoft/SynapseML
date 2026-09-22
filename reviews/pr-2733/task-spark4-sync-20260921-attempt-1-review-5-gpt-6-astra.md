# Round 5: tests and coverage

**Result:** CLEAN (no actionable test-coverage finding).
**Reviewer:** GPT-6 Astra, direct fallback, 2026-09-21.
**Reviewed source tree:** `4605bb9bc797b817626ce5c27ae93f7dcea993a1`.

Gemini 3.5 also failed with backend HTTP 400 before executing a turn, after the
newer Gemini attempts failed. This is not Gemini coverage; the three-family
review gate remains unfulfilled.

- Read the service-bridge assertions: scalar/column binding, conflicting and
  null arguments, invalid setter retention, generic/named setter ordering,
  copy, Java restoration, save/load, real empty-frame transform, and prompt
  scratch-copy atomicity. The mocked `_transform` case proves extra-param
  dispatch only; it is not presented as a service request test.
- Configuration performance is asserted at the gateway boundary: zero JVM
  calls, excluding unrelated Py4J object-release messages. The transfer test
  checks the exact default-transfer count, not a permissive upper bound.
- `TextAnalyticsHeaderSuite` executes public transforms against an ephemeral
  loopback HTTP server: partial/manual batches, per-row credentials, payload
  and row preservation, copy/load, asynchronous submission, and polling.
  `CognitiveServiceBaseSuite` supplies blank/null, mutable-array, map
  sanitization, invalid-type, precedence, and lazy-fallback coverage.
- Cleanup tests exercise the actual runner with an in-memory client: 36 mixed
  invalid relation cases reject the whole inventory without deletion; nested
  valid references remain supported. Six later-read cases cover inventory,
  job history, schedules, and reused/distinct exceptions, retaining stores and
  allowing only the first attempted DELETE.
- The relocated self-suppression regression extends repository `TestBase`;
  the fixture remains offline. Main suite length is 796, with no style waiver.
- Red controls reproduced the original parser bug and both successive
  diagnostics gaps. Final `spark40-cleanup-round4-green.log` records zero
  style errors and 46 passing tests across three suites, with no skips.
- Aggregate compile/test compile/styles/codegen, 30 codegen and 36 cognitive
  tests, 89 pipeline tests, and pinned Black passed. Built core/cognitive/OpenCV
  wheels supplied 54 Python tests plus 160 subtests on Spark 4.0.1/Python
  3.12.11. JUnit reports 214 executions, zero failures/errors/skips; wrapper and
  JVM code-source provenance was asserted.

Final source audit proves only four cleanup test-infrastructure files changed
after the full production/Python validation. Live Azure/Fabric, full remote CI,
and the unscheduled streaming suite are not claimed by this local evidence.
