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

## Source identity and current evidence

- Reviewed source manifest: `79c381b3b29adf7a289db85004a66f9b17fcd9fa8d7f7d35813021b59078400f`.
- Complete source diff: `3fbf991da0fb5920711f161b35099fd1cae1bdb741191751e201dbfd7aa12ed1`.
- Target integrated: `6cef832de0257ea50e320c5378985aa70c1d4f5b`. Its sole intervening change updated the pinned Java-setup action; all reviewed source and evidence blobs, and the complete source diff, are unchanged.
- Two pre-commit passes were consumed: one dirty first lens and one complete six-lens clean pass. The sixth artifact retains the privacy exclusion and verified resolution for the original dirty response. Its private original and raw transport must not be published.
- Fresh r3 generated wrappers passed two real-Spark bridge tests with nine subtests.
- 39 synthetic managed-Spark integration checks passed using the exact r3 candidate artifacts.
- Local Python was 3.12.3 rather than branch-pinned 3.11.8; exact remote runtime validation remains required.
- This migration changes review artifacts only. Current-head CI, automated review, and the separate final CI-qualified gauntlet remain pending.
