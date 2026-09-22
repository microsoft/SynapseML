# Job-wait errors, testing and coverage

- Round: 5
- Model: gpt-6-astra, explicit fallback for unavailable Gemini
- Reviewed base: `dd33c2401ec14558af9afd9aac39c4d87c257e57`
- Findings: 0
- Verdict: CLEAN

The three new tests execute the shared guard called by the real smoke and
notebook wait sites. They cover a successful future, synchronous ordinary
and assertion failures, an unsuccessful future, interruption, and all four
remaining `NonFatal` exclusions. Assertions inspect result, message, cause
identity, thrown-instance identity, and interrupt status.

The interrupt is produced by an actual `Await.result` on an unresolved promise
after setting the thread flag. No artificial sleep or cloud resource is
needed. The test captures the original exception and clears the flag in
`finally`; the guard must restore it after the wait clears it.

`master-job-wait-red.log` records 11 passing and two failing tests after moving
the original smoke catch-all into the testable boundary without changing its
behavior. The ordinary-error case passes as a control. The failures show
wrapping of the actual interrupt and a fatal throwable.
`master-job-wait-green.log` records all 53 tracker/naming tests passing, with
no skips, after the boundary correction.

The tests stay in the existing mix-in and CI-selected concrete suite. They do
not execute live smoke provisioning, upload, or monitoring. Compilation and
source inspection establish the two call sites; no live Fabric claim is made.
Gemini did not run, so this fallback does not satisfy three-family coverage.
