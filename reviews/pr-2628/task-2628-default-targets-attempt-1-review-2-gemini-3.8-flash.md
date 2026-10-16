# Default release targets: round 2

## Scope and limitation

Reviewed the uncommitted default-target follow-up to
`2391aa166ab46238307de9edf9650102cbff950e`, targeting `master`.
The requested Gemini 3.8 Flash review failed with HTTP 400 before providing
findings. This file records that failed attempt and the coordinator's direct
architecture review, not an independent Gemini result.

## Direct assessment

No outstanding architecture findings.

- `release_matrix.py` separates the known target catalog from defaults.
  Loading and exporting saved plans still use their explicit selection.
  The pre-change three-target digest has an exact regression assertion.
- Bootstrap, publication and evidence continue to use the approved plan.
  A normal tag-workflow input cannot change a bootstrap plan's selection.
  Preparation has no transient input that disappears when its PR merges.
- The optional policy check is shared with ledger validation. Old, stricter
  policy records remain readable, while selected Spark 4.0 cannot use a
  record claiming the policy was unnecessary.
- Notes render the validated selection rather than duplicating a runtime
  table in shell. The output is written only after approval and evidence
  validation, and existing output is not overwritten.
- Consumer documentation retains Spark 4.0's published version independently.
  Production website checks still require an exact publication lock.
  Historical documentation is unchanged. The operator guide explicitly
  describes the additional source/documentation review for a Spark 4.0 opt-in.

## Evidence

The coordinator observed 863 release tests passing, with the existing opt-in
SBT test skipped, 271 native version-bump/history tests passing, and 36 website
tests passing. These results do not establish hosted CI, consumer-wheel
compatibility or production publication readiness.
