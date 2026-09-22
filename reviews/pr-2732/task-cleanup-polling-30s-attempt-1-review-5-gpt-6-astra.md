# Polling update, testing and coverage

## Review summary

- Round: 5
- Theme: Testing and coverage
- Mode: sequential, explicit fallback for unavailable Gemini
- Model: gpt-6-astra
- Findings: 0
- Verdict: CLEAN for the polling delta

## Evidence checklist

- [x] The fake client tests the real cleanup loop. It records requested wait
  durations, inventory reads, and DELETE calls rather than reimplementing the
  retry decision.
- [x] Immediate success asserts no sleeping and five inventory reads for a
  child and parent. Delayed success covers the first, middle, and final waits
  for both resources independently. Assertions pin each wait to 30,000 ms.
- [x] Exhaustion asserts ten waits totaling 300,000 ms, 13 total inventory
  reads, one DELETE, and a retained parent. A second job proves that failure
  stops subsequent candidates, not merely the parent.
- [x] A not-found DELETE response still requires bounded confirmation.
  Inventory exceptions and conflicting metadata abort on either the immediate
  confirmation read or the next read, with exact read and wait counts.
- [x] Interruption preserves throwable identity and performs no further
  inventory read. The new foreign-consumer test retains the parent after a
  consumer appears during the child's wait.
- [x] Five moved failure tests preserve their bodies and assertions. The
  existing trait remains mixed into `FabricTestArtifactTrackerSuite`; no new
  suite selector or pipeline wiring is necessary.
- [x] `master-polling-30s-red.log` demonstrates failure with the old 31-read,
  two-second policy after behavior-preserving sleeper instrumentation.
  It is not evidence from an untouched old callback signature.
- [x] `master-polling-30s-green.log` records 50 passing tests across the two
  concrete suites, including the moved tests, with no ignored or canceled
  tests. No test uses real waiting or cloud resources.

The production sleep call is covered through its explicit millisecond
argument, not by a slow wall-clock test. No live Fabric result is claimed.
This fallback does not satisfy the unavailable Gemini-family review gate.
