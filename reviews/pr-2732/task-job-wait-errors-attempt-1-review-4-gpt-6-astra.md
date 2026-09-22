# Job-wait errors, detailed correctness

- Round: 4
- Model: gpt-6-astra
- Reviewed base: `dd33c2401ec14558af9afd9aac39c4d87c257e57`
- Findings: 0
- Verdict: CLEAN

Read both changed wait sites and the shared generic boundary. The by-name
operation remains inside `try`, so an exception during monitor creation is
handled as well as one raised during waiting. Success preserves the result.
`InterruptedException` restores the flag before rethrowing the same instance.
`NonFatal` wraps ordinary errors with the same notebook-name message and
original cause. Excluded throwables escape without a wrapper.

Smoke keeps its existing ready/assert behavior; the notebook handler keeps
its submitted notebook name and result wait. No submission, timeout, resource
allocation, or cleanup ordering moved across the exception boundary.
The new fixture cannot resolve Fabric, and its interrupted wait always clears
the test thread's status in `finally`.

The 18 report moves preserve original review content except current artifact
location metadata. The corrected timing claim states a per-item waiting
budget, not a total cleanup deadline. The old wording is explicitly historical.
`master-job-wait-green.log` records successful compilation, both Scala style
checks, and all 53 tracker/naming tests.

No live service or port validation is claimed here. Gemini is unavailable.
