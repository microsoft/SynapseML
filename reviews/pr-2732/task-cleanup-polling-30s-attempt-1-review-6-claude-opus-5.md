# Cleanup confirmation polling, round 6

## Review summary

- Theme: final polish and hardening — performance, observability, documentation, naming.
- Model: Claude Opus 5.
- Scope: the unchanged four-file polling delta at working head `81ccc549` only.
- Blocking issues: 0. Non-blocking observations: 3.
- Verdict: **CLEAN** for this bounded round.

The delta is byte-identical to the one reviewed in round 3: same four files, same 171
insertions and 110 deletions, and every source timestamp predates that report. Rounds 4
and 5 recorded no findings. This review makes no claim about the separately advanced
master baseline, the unapproved pipeline import, or any future port integration.

## Performance

The change is a net reduction in cloud work. Worst-case confirmation now costs **11 paged
inventory fetches per item instead of 31**, a two-thirds cut in the most expensive
operation in the loop, while tolerated propagation delay rises from 60 to 300 seconds.
The fast path is untouched: an item already absent on the immediate read confirms with
zero waits, which the baseline test pins at five reads and an empty pause list.

One unsuccessful confirmation consumes at most five minutes of requested waiting
plus request time before it aborts the run. Earlier successful confirmations can
each consume that same waiting budget. Total cleanup duration can therefore
exceed five minutes; there is no run-level wall-clock limit.

Two details are right and worth recording. `require` takes its message by name, so the
interpolated failure string is never built during a successful poll. `index` is
re-evaluated on every read, which costs a rebuild of the inventory map but is what makes
the mid-poll conflicting-identifier abort possible; the validation is deliberate rather
than an oversight, and it now runs 11 times instead of 31.

In tests nothing sleeps. `CleanupClient.run` defaults `pause` to `_ => ()`, so the 32
simulated waits across the 1/5/10 loop cost nothing and the suite finishes in 4.683
seconds against a 30-second constant.

## Observability

Every outcome is logged with the artifact identifier: the deletion attempt, a concurrent
not-found, a failed DELETE with its exception class, a confirmed deletion, a retained
candidate, and the closing summary. The exhaustion message names both the identifier and
the read count, and it interpolates the constant, so the text cannot drift from policy.

One characteristic is worth stating plainly because this delta changed its magnitude.
`confirmAbsent` does not receive the `log` function and emits nothing while waiting, so
`FabricOperations.cleanupTestArtifacts`, which takes the `println` default, can now go
**up to five minutes of waiting plus request time silent** between "deleting" and
"confirmed deletion" for one item, where the old policy allowed one minute of
waiting plus request time. This is attributable to
the item named in the preceding line, and always followed by an explicit outcome, so it
is acceptable as it stands. It is recorded as an observation, not a defect.

## Documentation

The replacement paragraph in `docs/Reference/Developer Setup.md` is accurate on all four
claims: an immediate check, up to ten further checks at 30-second waits, a fresh per-item
budget plus request time rather than a wall-clock deadline, and no DELETE resent during
confirmation. Phrasing it as "ten more checks" keeps the prose correct without hardcoding
the attempt total, and the new lines wrap at 75 to 83 characters, matching the surrounding
paragraph. The existing sentence that interrupts and fatal errors keep their existing
propagation still matches the code exactly, so the documented contract remains complete.

No stale description of the old 31-read, two-second policy survives anywhere in source or
documentation. The only remaining matches are historical review records, which correctly
describe the state at the time they were written and should not be rewritten.

## Naming

`ConfirmationDelayMillis` renames the former seconds constant to carry its new unit, and
building it from `TimeUnit.SECONDS.toMillis(30)` keeps the authored interval legible
rather than burying a bare 30000. The `pause` parameter, its `millis` binding, and the
single call site agree on the unit end to end. The three new test titles each describe
what their body actually asserts, including the protected-consumer case, whose mechanism
really is the neighbour rule its name implies.

## Hardening and blast radius

`FabricOperations.cleanupTestArtifacts` is the only caller outside the suite, and it
passes neither `pause` nor `log`, so changing the callback from `() => Unit` to
`Long => Unit` has no call-site impact at all; the object is `private[ml]` test
infrastructure with no public signature, generated wrapper, serialized parameter, or
branch runtime setting involved. `Thread.sleep` and the former `TimeUnit.SECONDS.sleep`
have identical interrupt semantics, so propagation behaviour did not move with the
rewrite. Sizes stay within the 800-line and 120-character limits at 255, 768, and 171
lines with longest lines of 117, 112, and 108.

## Non-blocking observations

None require a change, a rerun, or a delay before commit.

1. Confirmation waiting is silent, as described above. A future touch could thread the
   existing `log` into `confirmAbsent` and emit one line on the first wait.
2. The old title's explicit "without resending DELETE" wording is gone, although the
   property is still asserted by the in-pause comparisons of recorded deletions.
3. The constant is named for attempts while the failure message speaks of reads. Both are
   correct and the message is the clearer of the two.

## Evidence and limits

- `master-polling-30s-red.log` fails the new tests under the old policy with
  `Vector(2000, 2000) did not equal Vector(30000, 30000)` and `"after 31 reads"` not
  containing `"after 11 reads"`.
- `master-polling-30s-green.log` completed: compile, test compile, main and test
  scalastyle each reporting 0 errors, and 50 of 50 tests across 2 suites with nothing
  skipped, cancelled, ignored, or pending, under the exact CI suite selector.
- **No Gemini-family review executed; the slot remains unavailable, so the independent
  three-family gate is unfulfilled and this is not a full gauntlet.** Azure validation and
  port validation have not run for this delta.

## Resolution note from the driving reviewer

The original performance paragraph's run-level wording needed qualification:

> Wall-clock exposure is bounded per item rather than multiplied across the run, because
> the first unconfirmable item throws and ends the loop. A stuck run therefore costs about
> five minutes plus request time once, not once per candidate.

That original text is retained here as review history, not as a current claim.
The performance and observability paragraphs above now state the correct limits.
Only one
*unsuccessful* confirmation can exhaust its budget in a run, because that
failure stops the run. Earlier successful confirmations can each consume
their own ten waits. Total cleanup duration can therefore exceed five minutes,
and request time also extends the silent interval. There is no run-level or
per-item wall-clock deadline. The source, documentation, and child/parent
final-attempt regression consistently implement independent per-item waiting
budgets, so no code change is needed.

New polling reports are first committed under `reviews/pr-2732/`, following
the report-location rule newly added on master. The original findings and
reviewed source remain unchanged.
