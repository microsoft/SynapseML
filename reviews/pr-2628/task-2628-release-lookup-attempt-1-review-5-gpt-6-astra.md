# Release lookup fix: round 5

- Theme: test quality.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.

The new parameterized test executes the actual lookup and publication Bash
steps extracted from the workflow. Only external curl and GitHub CLI calls
are stubbed. The former CLI lookup is supported to reproduce the pre-fix bug.

Assertions cover the process result, workflow output, generated notes and
publication calls. They prove that errors cannot proceed to generation or
creation, while a confirmed 404 still follows the existing publication path.
The HTTP 200 case verifies that an existing release is left untouched.

All 116 targeted workflow, recovery and public-documentation checks passed
before these review artifacts were added. This is not a live publication test.
