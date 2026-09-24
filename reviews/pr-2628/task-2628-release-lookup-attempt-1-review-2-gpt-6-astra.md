# Release lookup fix: round 2

- Theme: architecture and repository conventions.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.

The fix remains in the existing workflow step and uses curl already available
on the runner. It adds no dependency, helper framework or publication target.
The maintainer authorized this workflow change. Regression coverage extends
the existing executable Bash workflow tests.

Rebasing onto the current target preserved the existing PR's stable patch ID.
The incoming target change is an unrelated website dependency update, not an
additional change authored by this fix.

No outstanding issue found. This is a bounded single-coordinator review.
