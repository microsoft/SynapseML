# `spark3.5`

- Shared Spark port/release branch; only branch-specific compatibility work
  should target it directly.
- Merge `master` into this branch; do not rebase or force-push the shared ref.
- Read versions and dependency pins from this branch's live build files.
- Because `master` may use the same Spark generation, compatibility replay can
  intentionally omit this branch. That does not waive direct PR validation.
- Verify workflows and Azure triggers on `spark3.5` itself and confirm the
  relevant tests actually ran; historical coverage differed from `master`.
