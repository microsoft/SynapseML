# Review evidence for PR #2781

The six qualifying pre-commit reviews were moved without changing their bytes.
Their original public locations remain in commit `6bb12ed7c45f914f40b6c7afc4b546c6ac9b051b`,
an ancestor of this artifact-only update. The original PR head was verified
against the canonical pull-request ref before migration.

## Migration index

| Original path | Current path | SHA-256 |
| --- | --- | --- |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-1-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-1-gpt-6.1-sol.md` | `e252186361e5b2ac5794e0d2ea5668dea4b1bb070e77b3e8b5f9fde6377c3063` |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-2-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-2-gpt-6.1-sol.md` | `8d48a72e00b1a0e83814e6a5cbbe3102cefc2adc4c58d3365210981dda6b6194` |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-3-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-3-gpt-6.1-sol.md` | `6781f10ed98cb3d4159e04cfd79923248c716806ceb5d65e8d5c9ce585d688f3` |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-4-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-4-gpt-6.1-sol.md` | `260dc3bb20489ebe6e5b9c428b6df08939f64ef799ed1b8b32b6de6aded42ab0` |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-5-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-5-gpt-6.1-sol.md` | `7517f2a95296e91ec7c4e3799793bc8433b23d5e21c3264302edef869375d2ee` |
| `reviews/pre-pr-luna-defaults-cache/task-unknown-attempt-2-stage-attempt-1-review-6-gpt-6.1-sol.md` | `reviews/pr-2781/task-unknown-attempt-2-stage-attempt-1-review-6-gpt-6.1-sol.md` | `40adf8250736ad2386c9a153136dfdf5b136fa5b9cda7b8f8b4e842a2ed8da62` |

## Initial publication evidence (historical)

- Reviewed source manifest: `79c381b3b29adf7a289db85004a66f9b17fcd9fa8d7f7d35813021b59078400f`.
- Complete source diff: `3fbf991da0fb5920711f161b35099fd1cae1bdb741191751e201dbfd7aa12ed1`.
- Target integrated: `6cef832de0257ea50e320c5378985aa70c1d4f5b`. Its sole intervening change updated the pinned Java-setup action; all reviewed source and evidence blobs, and the complete source diff, are unchanged.
- Two pre-commit passes were consumed: one dirty first lens and one complete six-lens clean pass. The sixth artifact retains the privacy exclusion and verified resolution for the original dirty response. Its private original and raw transport must not be published.
- Fresh r3 generated wrappers passed two real-Spark bridge tests with nine subtests.
- 39 synthetic managed-Spark integration checks passed using the exact r3 candidate artifacts.
- Local Python was 3.12.3 rather than branch-pinned 3.11.8; exact remote runtime validation remains required.
- This migration changes review artifacts only. Current-head CI, automated review, and the separate final CI-qualified gauntlet remain pending.

## Validated CI correction and pre-commit pass 3

The three favorite-color test fixtures now request full sentences while retaining
their minimum-length assertions. The prior deployment responses were valid short
answers, not provider or serializer errors. All 55 affected live Scala tests
passed, with one unchanged intentional custom-endpoint ignore.

The complete 29-path patch against target
`48b77657f615e25d443d39177e108eae467e3949` has manifest
`d915a3f9e98cc7d59f760f8d38e1c5981e1958474aad1870b4e909a13ee7fc48`
and diff `0ac860af199ca2f91812fe8e81601bfa4ab523e2cebbe38247b9825087eb53bd`.
All five production files remain identical to r3. The upstream website-only
lockfile update is integrated without rewriting published ancestry.

All six independent `gpt-6.1-sol/max` lenses passed:

1. [Broad sweep](task-unknown-attempt-3-stage-attempt-1-review-1-gpt-6.1-sol.md)
2. [Architecture](task-unknown-attempt-3-stage-attempt-1-review-2-gpt-6.1-sol.md)
3. [Robustness](task-unknown-attempt-3-stage-attempt-1-review-3-gpt-6.1-sol.md)
4. [Correctness](task-unknown-attempt-3-stage-attempt-1-review-4-gpt-6.1-sol.md)
5. [Testing](task-unknown-attempt-3-stage-attempt-1-review-5-gpt-6.1-sol.md)
6. [Hardening and driver provenance](task-unknown-attempt-3-stage-attempt-1-review-6-gpt-6.1-sol.md)

The predecessor's Azure artifacts contain 43 CPU and 4 GPU Databricks E2E passes
with zero E2E skips. That partial evidence does not clear its red OpenAI job.
This correction still requires fresh published-head CI and automated review,
followed by the distinct final CI-qualified review. All earlier evidence remains
retained; the migration hashes above still match the original six files.
