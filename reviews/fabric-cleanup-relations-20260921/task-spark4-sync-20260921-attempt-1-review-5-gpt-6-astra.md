# Round 5: tests and coverage

Publication note: this prerequisite-specific directory preserves the separate
port review records. Paths in the original review describe its review-time location.

**Result:** CLEAN (no actionable test-coverage finding).
**Reviewer:** GPT-6 Astra, direct fallback, 2026-09-21.
**Reviewed source tree:** `b4dd50774784a1fd7fca611883c333a5a21458c2`.

All attempted Gemini versions, including the Round 5 Gemini 3.5 agent, failed
with backend HTTP 400 before reviewing. This fallback does not establish the
three-family gate.

- The 36 malformed-relation combinations mix a valid unrelated GUID with each
  invalid dependency shape, exercise inventory and the actual cleanup runner,
  and assert the named-field exception, no deletion, and an unchanged store.
  Existing outer null/empty and nested-valid canonicalization coverage remains.
- Six later-read cases cover inventory, history, and schedules, each with a
  distinct or reused exception. They assert exception identity, exactly the
  prior deletion error suppressed when distinct, only the first job DELETE
  attempted, and store retention.
- The repeated tracker exception test asserts original identity, no
  self-suppression, all attempts, and an empty queue afterward. It was moved
  into a focused `TestBase` suite to keep the main suite under 800 lines,
  without disabling Scala style or weakening assertions.
- The original parser regression was reproduced before the fix on Spark 4.1.
  Master red controls record 1 pass/2 failures for diagnostics, then 3 passes/1
  failure for the expanded metadata test before broadening the guard.
- `master-cleanup-round4-green.log` records zero style errors and 46 passing
  tests across three suites on JDK 11, with no skips. The timing output proves
  the final `TestBase` suite ran. Both port reruns pass the same 46 tests and
  style checks; all four Scala source files are identical.

These are offline fake-client tests, not live Fabric deletion evidence.
Current-head remote review and Azure validation are still separate gates.
