# Cleanup confirmation polling, round 3

## Review summary

- Theme: edge cases and robustness.
- Model: Claude Opus 5.
- Scope: the bounded four-file polling delta only — `FabricArtifactCleanup.scala`,
  `FabricTestArtifactTrackerSuite.scala`, `FabricTestArtifactTrackerFailureTests.scala`,
  and `docs/Reference/Developer Setup.md` — plus `confirmAbsent` and its call site.
- Issues found: 0 blocking, 0 Low. Two non-blocking observations recorded.
- Verdict: **CLEAN** for this bounded round.

## Bounded counting proof

`confirmAbsent` reads first and pauses only after a positive read, so
`check(ConfirmationAttempts)` with `ConfirmationAttempts = 11` yields **11 reads and 10
pauses** of `ConfirmationDelayMillis = 30000`, a 300000 ms per-item budget. `require`
fires at `remaining == 1`, which is the eleventh read, so the last read is a real chance
to confirm rather than a wasted attempt. The failure message interpolates the constant,
so the text cannot drift from the policy.

Inventory reads per run follow `1 + 2 * (1 + retries + 1) = 2 * retries + 5` for the two
candidates, and every new assertion matches that closed form exactly: 5 reads with zero
pauses, `5 + 2 * retries` in the 1/5/10 case, 13 on exhaustion, and 3 on interrupt. The
budget is re-entered per candidate, so it is genuinely per item and not a wall clock,
which is what the documentation now claims.

## Edge cases verified

- **Immediate confirmation.** With `removeImmediately`, `pauses.isEmpty` holds and reads
  stay at 5, so the fast path never sleeps and is unchanged by the new interval.
- **Boundary at the last usable wait.** In the `retries = 10` case the tenth pause removes
  the item and the eleventh read confirms it, exercising the exact `remaining > 1` edge in
  both directions rather than only the failing side.
- **Fresh budget per item.** The 1/5/10 loop asserts `2 * retries` pauses in total, proving
  the child spends its own budget and the parent then starts a new one.
- **No repeated DELETE while polling.** The in-pause assertions compare `client.deleted`
  against the expected vector on every pause, so a resent DELETE would append a duplicate
  and fail. The property survives the rename from the old test title.
- **Exhaustion.** 10 pauses summing to 300000, 13 reads, `deleted == Vector(staleJob.id)`,
  and the retained store prove the abort leaves the next job and the parent untouched; the
  next candidate never even reaches its own inventory read.
- **Read failure and conflicting IDs during polling.** Failing read 3 or 4 gives exactly
  `failedRead - 3` pauses and stops at that read count. The conflicting variant appends a
  same-id item with a different description, which survives `items.distinct` and therefore
  trips the `Conflicting Fabric inventory IDs` requirement rather than being deduplicated.
- **Interrupt.** The injected `InterruptedException` escapes by identity after exactly one
  pause and three reads, so no further read or deletion follows.
- **Protected consumer arriving mid-poll.** The notebook added during the pause is not an
  owned job, because `ownedJob` requires `SparkJobDefinition`, and it is not a managed
  endpoint, so it blocks the parent through the neighbour rule the test names. This is the
  right edge to add: the safety re-check reads inventory *after* the poll, so widening the
  window from 60 to 300 seconds cannot let a late consumer slip past.

## Abort and safety edges

The `require` failure is an `IllegalArgumentException` and therefore `NonFatal`, so the
outer handler attaches earlier deletion errors excluding the rethrown instance and
rethrows the same object immediately, ending the loop before any further DELETE. An
interrupt or fatal from `pause` bypasses that handler and propagates unchanged. That
asymmetry is deliberate and already documented — "interrupts and fatal errors keep their
existing propagation" — and unlike the tracker case fixed earlier, nothing is lost,
because each DELETE failure is logged when it happens. The handler itself is untouched by
this delta.

## Test-harness robustness

`CleanupClient.run` defaults `pause` to `_ => ()`, so no test can accidentally invoke the
production `Thread.sleep` default; the whole run completes in 4.683 seconds despite a
30-second constant. The five moved tracker and shutdown bodies are byte-identical to the
removed ones and stay on the CI-selected class through the existing mixin, so no pipeline
selector change is needed. The moved interrupt test still clears the thread flag in its
`finally`, and the new polling interrupt test only constructs and throws, so the earlier
registration order of the trait cannot leak an interrupt into later tests.

## Non-blocking observations

1. The production default `millis => Thread.sleep(millis)` is the one line no test
   executes, since asserting it would require a real sleep. The constant itself is pinned
   by the injected assertions on `30000L`, so the residual risk is a one-line forward.
   Accepted, no action suggested.
2. Tolerated inventory lag rises from 60 to 300 seconds while worst-case reads per item
   fall from 31 to 11. The cost is granularity: an item that becomes absent a second after
   DELETE now waits up to 30 seconds. This is the authorized trade-off, recorded only so
   the number is explicit.

## Evidence and limits

- `master-polling-30s-red.log` is a genuine red against the old policy on the new tests:
  `Vector(2000, 2000) did not equal Vector(30000, 30000)` and
  `"... after 31 reads" did not contain "after 11 reads"`.
- `master-polling-30s-green.log` completed: compile, test compile, main and test
  scalastyle each `Found 0 errors`, and 50 of 50 tests passing across 2 suites with no
  skipped, cancelled, ignored, or pending tests, under the exact CI suite selector.
- Sizes stay inside the 800-line and 120-character limits: the suite is 768 lines, the
  mixin 171, and the cleanup source 255, with longest lines of 112, 108, and 117.
- No stale `31`-attempt or two-second wording remains in source or documentation; the only
  matches are historical review records, which correctly describe the state at their time.
- This is private test infrastructure only — no public JVM signature, generated binding,
  serialized parameter, or branch runtime setting changes.
- **No Gemini-family review executed; the slot remains unavailable, so the independent
  three-family gate is unfulfilled and this is not a full gauntlet.** Azure validation and
  port validation are separate evidence that has not run for this delta.
