# Job-wait errors, architecture and patterns

## Review summary

- Round: 2
- Model: gpt-6-astra, explicit fallback for unavailable Gemini
- Reviewed base: `dd33c2401ec14558af9afd9aac39c4d87c257e57`
- Findings: 0
- Verdict: CLEAN for this delta

## Evidence

The shared `withFabricJobFailure` boundary belongs on the existing connection
trait used by both smoke and notebook tests. It introduces no connection,
executor, state, or extra job submission. Its by-name body executes once.
The existing notebook handler moves unchanged into that boundary, while the
smoke handler stops wrapping interrupts and fatal errors.

Both real wait sites call the shared boundary. Their `Await.ready`/assert and
`Await.result` bodies, timeouts, tracked-artifact handling, and teardown remain
unchanged. No public SparkML method, parameter format, or generated wrapper
changes. The callback is test infrastructure rather than a production API.

The three regressions register through the existing failure-test trait on the
CI-selected tracker suite. They use failed/completed futures, an actual
interrupted wait, and contained fatal throwables without any Fabric access.
`master-job-wait-green.log` records both compile checks, both Scala style
checks, and 53 passing tracker/naming tests.

Historical reports move into `reviews/pr-2732/`; their filenames, reviewed
revisions, findings, and resolutions remain. Current artifact-location fields
are updated. The corrected polling report labels its old timing statement as
historical feedback instead of leaving a contradictory current claim.

No pipeline, workflow, release metadata, or dependency pins change.
Gemini did not execute; this fallback is not a three-family gauntlet pass.
