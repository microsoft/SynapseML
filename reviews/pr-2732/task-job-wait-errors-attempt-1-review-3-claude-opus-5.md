# Job-wait error handling, round 3

## Review summary

- Theme: edge cases and robustness.
- Model: Claude Opus 5.
- Scope: `git diff HEAD` at `dd33c240` — the unstaged source and documentation change plus
  the staged review-record renames. No wider history or future integration is in scope.
- Issues found: 0 blocking, 0 Low. Two non-blocking observations.
- Verdict: **CLEAN** for this bounded round.

## Behaviour at each call site

`withFabricJobFailure[T](notebookName)(job: => T): T` is `protected final` on
`HasFabricNotebookTestConnection`, and its body parameter is by name, so the job runs
inside the `try` rather than before it. Both sites now delegate:

- **Smoke** carried the defect. Its `case t: Throwable` wrapped everything, so an
  interrupt or a fatal became a `RuntimeException`. It now restores and rethrows, and the
  guarded expression — `Await.ready(...).value.get` followed by `assert(result.isSuccess)`
  — is unchanged, so wait, monitor, and lifecycle behaviour are untouched.
- **Notebook** already had the correct shape, and the extraction reproduces it exactly,
  including the distinct `submittedNotebookName` binding, so no message regressed.

The `InterruptedException` case is listed first. It is not strictly required for
selection, because `NonFatal` already excludes that type, but it is required for
correctness: an interruptible wait throws with the flag **cleared**, so without the
explicit restore the signal would be lost. The clause earns its place.

No broad `case _: Throwable` remains anywhere in `FabricNotebookTests.scala`; the other
catch clauses in that file are all `InterruptedException`-specific already.

## Exclusion-set coverage

All five categories that `NonFatal` excludes are now covered, which is the property that
matters for this fix: `InterruptedException` through the interrupt test, and
`InternalError`, `ThreadDeath`, `LinkageError`, and `ControlThrowable` through the fatal
test, each asserted to escape by identity and unwrapped.

The ordinary-failure test is the right complement. `AssertionError` is deliberately
included and is genuinely `NonFatal` despite extending `Error`, which mirrors the smoke
site, where a failing `assert` raises a non-fatal ScalaTest exception that must still be
wrapped. The failed-`Future` case covers the notebook site's real path, and the success
case pins that the helper returns the body value rather than collapsing to `Unit`.

## Edge cases verified

- **The interrupt test uses the real primitive.** It pre-interrupts the thread and then
  awaits an uncompleted `Promise` with `Duration.Inf`. That cannot hang: interruptible
  acquisition tests and clears the flag before blocking, so the exception is raised
  immediately and deterministically, with no second thread and no timing window.
- **The restore assertion is a true guard.** Because the await clears the flag on throw,
  `isInterrupted` would be false without the fix, and `isInterrupted` does not itself
  clear the flag, unlike the static form used for the cleanup.
- **Identity is checked at the source.** The test captures the instance the await actually
  threw and compares it to what escapes, rather than comparing to a hand-made exception.
- **No interrupt leaks.** The flag is set inside the `try` and cleared in `finally`, so a
  failure anywhere in the body still leaves the thread clean for the tests that follow —
  which matters because this mixin registers before the rest of the suite.
- **The fixture cannot reach Fabric.** `fabric` is declared `lazy val ... = createConnection()`
  in `FabricConnection.scala`, and the fixture overrides it as a `lazy val` of type
  `Nothing`. Laziness means construction never opens a connection, `Nothing` conforms to
  the declared type, and any accidental access would fail loudly. The helper is proven
  connection-free.

## Review-record changes in this diff

Eighteen historical reports move into `reviews/pr-2732`. Twelve are pure renames. The six
with content each change exactly one line, the self-referential `Artifact` field, so every
finding, hash, decision, and coverage limitation is preserved; that includes both of my
own reports in the set.

The polling round-6 correction is right, and I confirm my original wording was wrong. Only
an *unsuccessful* confirmation ends the run, so earlier successful confirmations can each
spend their own ten waits and total cleanup can exceed five minutes. The replacement text
states that, the observability paragraph now counts request time alongside waiting, and
the superseded sentences are retained as an explicit block quote marked as history rather
than deleted. That removes the contradiction between the body and the resolution note
while keeping the original finding visible.

## Non-blocking observations

1. The helper is unit-tested directly; the two call sites are verified only by
   compilation, because exercising them needs live Fabric. That is the correct boundary
   here, recorded so the evidence is not overstated.
2. The rewritten `Artifact` lines are inconsistent in separator style — some now read
   `reviews/pr-2732/...` and others `reviews\pr-2732\...`. Both are repository-relative
   and neither leaks a machine-local path, so this is cosmetic only.

## Evidence and limits

- `master-job-wait-red.log` ran the name-filtered selection, 13 tests with 11 passing and
  2 failing — exactly the interrupt and fatal guards — reporting
  `RuntimeException: Job failed for test-notebook.py was not the same instance as
  java.lang.InternalError: job VM failure`. The ordinary-failure test passing there is a
  useful control: the extraction preserved that behaviour.
- `master-job-wait-green.log` completed: compile, test compile, main and test scalastyle
  each `Found 0 errors`, and 53 of 53 tests across 2 suites with nothing skipped,
  cancelled, ignored, or pending, under the exact CI suite selector, with all three new
  tests listed.
- Sizes stay well inside the limits: 307, 225, and 768 lines with longest lines of 103,
  113, and 112, and the helper itself is 12 lines. The three documentation lines wrap at
  75, 76, and 28 characters and are accurate on all three claims they make.
- This is private test infrastructure only — no public JVM signature, generated wrapper,
  serialized parameter, or branch runtime setting changes.
- **No Gemini-family review executed; the slot remains unavailable, so the independent
  three-family gate is unfulfilled and this is not a full gauntlet.** The ports do not
  carry this change yet, and Azure validation has not run for it.
