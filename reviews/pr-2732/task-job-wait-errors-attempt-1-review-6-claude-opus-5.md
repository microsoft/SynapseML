# Job-wait error handling, round 6

## Review summary

- Theme: final polish and hardening — performance, observability, docs, naming.
- Model: Claude Opus 5.
- Reviewed base: `dd33c2401ec14558af9afd9aac39c4d87c257e57`.
- Scope: the frozen job-wait delta only — two test sources plus one docs paragraph,
  and the staged relocation of 18 existing review reports.
- Blocking issues: 0. Non-blocking observations: 4.
- Verdict: **CLEAN** for this bounded round.

The delta is unchanged since round 3. All three changed files were last written at
21:48:30 and `master-job-wait-green.log` completed at 21:50:59, so the green run
executed the exact source reviewed here. Rounds 4 and 5 both reported no findings.

## Performance

`withFabricJobFailure` wraps each wait in a `try`/`catch`. On the JVM an untaken
`catch` is an entry in the method's exception table, so the success path costs
nothing at steady state. The only added allocation is one by-name thunk per call,
against waits measured in minutes; it is not measurable.

No timeout, monitor, submission, or upload work moved across the boundary. Smoke
still uses `Await.ready(...).value.get` with `Duration(fabric.timeoutInMillis, MILLISECONDS)`
and notebook still uses `Await.result(future, notebookTimeout)`. Because `job` is
by-name, the operation stays *inside* the `try`, so a synchronous throw from
`fabric.monitorJob(...)` is still handled exactly as before the extraction.

`protected final` blocks overriding and keeps the call monomorphic for a trait that
is mixed into three suites.

The regression cost is near zero. `JobFailureFixture` opens no connection: `fabric`
is a `lazy val` overridden to throw, `preflight` and `storeSetup` are `private lazy val`
and never forced, and the strict `artifactTracker` val is safe because it only closes
over `fabric` inside a lambda that this fixture never invokes. The interrupt test awaits
`Duration.Inf` but cannot hang — interruptible acquisition checks and clears the flag
before parking, so it throws immediately. `master-job-wait-green.log` records the whole
run at 3 seconds 595 milliseconds for 53 tests across two suites.

## Observability and failure messages

The wrapper message `s"Job failed for $notebookName"` is byte-identical to the text both
call sites used before, so existing triage habits and log greps keep working. The cause
chain is preserved and asserted by identity, not by message comparison.

Excluded throwables now escape bare, with no notebook name attached. At the notebook site
that is unchanged: interrupts and fatals already bypassed the wrapper there. At the smoke
site it is new, since the old `case t: Throwable` labelled everything. The lost context is
immaterial: the smoke test is registered as `test("OnePlusOne")` and drives one fixed
notebook, so the suite and test name still identify the scenario, and attaching a wrapper
to an `InterruptedException` or `InternalError` is precisely the defect being repaired.

Nothing is logged-and-swallowed, and no logging statement was added or removed. The three
test names name their category, so a failure report states which behaviour broke.

## Docs

The new paragraph in `docs/Reference/Developer Setup.md` is accurate on all three claims:
the flag is restored, excluded throwables propagate as the same instance, and ordinary
failures keep both the notebook name and the original cause. It sits at the end of the
Fabric behaviour section immediately before `### scalastyle`, and it reuses the vocabulary
of the adjacent cleanup paragraph ("interrupts and fatal errors keep their existing
propagation"), so the document stays internally consistent. Two sentences is proportionate.

## Naming

`withFabricJobFailure` follows the local `withTrackedArtifact` idiom in the same trait, so
it reads consistently. Strictly, `withX` normally means "supply X to the body" whereas this
helper translates failures of the body; a name such as `reportingJobFailure` would be more
literal. Consistency with the surrounding file is a defensible choice, so this is a nit.
The `notebookName` parameter accepts a blob name at the smoke site and `submittedNotebookName`
at the notebook site; both are notebook identifiers and both preserve the prior text.

## Report relocation correctness

Eighteen reports moved into `reviews/pr-2732`. Twelve are pure renames and six changed
exactly one line each, the self-referential `Artifact` field; the diffstat corroborates this
as six files at `2 +-` and twelve at `0`. Findings, verdicts, hashes, resolution history,
and the recorded model-coverage limits are all preserved.

No stale path reference survives. The only remaining mentions of the former directory name
are four occurrences of the branch `fix/fabric-cleanup-relations-20260921`, which are correct
and must stay. References to `reviews/sync-20260921/` inside a moved report remain valid
because that directory still exists. The former directory is now empty and holds no tracked
file, so nothing stale is committed; git does not track directories, so it will not appear in
a fresh checkout.

## Hardening and style headroom

The mixin keeps the new tests on the CI-selected concrete suite: the green log lists exactly
`FabricTestArtifactTrackerSuite` and `FabricArtifactNamesSuite`, with all three job-wait tests
under the former. No `pipeline.yaml` change is needed.

`FabricNotebookTests.scala` is 307 lines with a 103-character longest line;
`FabricTestArtifactTrackerFailureTests.scala` is 225 lines with 113 characters. Against the
800-line and 120-character test limits there is ample headroom, and both scalastyle passes
report 0 errors over 210 files. No waiver is in play.

`new ThreadDeath()` is deprecated for removal on newer JDKs, but master builds on JDK 11 and
the ports on JDK 17, no `-Xfatal-warnings` exists in `build.sbt` or `project/`, and the same
construction already appears in the existing fatal-cleanup matrix in this file. Forward-looking
only; no action now.

## Non-blocking observations

1. "Fatal" is used in the docs and in a test name as shorthand for the whole `NonFatal`
   exclusion set, which also covers `ControlThrowable`. Usage is consistent with the
   pre-existing convention in the same file and document.
2. `withFabricJobFailure` is not a loan-pattern helper despite the `with` prefix.
3. `reviews/pr-2708/README.md` records a relocation with an index table and states that
   relocation is not another review run. `reviews/pr-2732` now holds 29 reports from four
   task streams without such an index. `pr-2728`, `pr-2735`, and `pr-2736` also lack one, so
   no rule is broken, but this directory would benefit most from the same note.
4. The green run compiled one main source and two test sources. The delta touches no main
   source, so that main recompile is incremental state, not a production change; the two test
   sources match the two modified files exactly.

## Evidence and limits

`master-job-wait-red.log` shows 13 tests with 11 passing and 2 failing after a
behaviour-preserving extraction, the ordinary-failure case passing as a control.
`master-job-wait-green.log` is complete: both scalastyle configurations at 0 errors,
53 of 53 tests passing across 2 suites, no skips, `All tests passed.`

The two production call sites are verified by compilation and reading only; exercising them
needs a live Fabric workspace, which is out of scope here. No cloud call, source edit,
staging, commit, or agent dispatch was performed in this round.

No Gemini-family model has executed in any round of this task; the backend returned HTTP 400
with zero turns on every attempt. The three-family review gate is therefore **unfulfilled**,
and this round must not be presented as a completed gauntlet. The ports have not received
this change, so no port validation is claimed.
