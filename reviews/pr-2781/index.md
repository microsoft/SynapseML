# Review evidence for PR #2781

## Current narrowed scope and pre-commit pass 4

Initial delivery is the Luna model migration only. All newly introduced cache
controls, serializers, wrapper additions, documentation and tests are deferred.
The four cognitive production files and existing Python ServiceParam bridge
are restored exactly to the target. Existing caching behavior is preserved.
The retained production change is conditional advice on the existing Fabric
`ModelNotFound` error; active examples and live-test selections use Luna.

The complete 23-path source patch has manifest
`47226683d9630a89aca905d5ae482c76473a8b5b8b6b74699b56bd75c00c1ba9`
and diff `0dbd7446354374743d4c3f9edbb673de486797e857a0bed28a011cff0ee99c0b`.
All six sequential, independent `gpt-6.1-sol/max` lenses are clean:

1. [Broad sweep](task-unknown-attempt-4-stage-attempt-1-review-1-gpt-6.1-sol.md)
2. [Architecture](task-unknown-attempt-4-stage-attempt-1-review-2-gpt-6.1-sol.md)
3. [Robustness](task-unknown-attempt-4-stage-attempt-1-review-3-gpt-6.1-sol.md)
4. [Correctness](task-unknown-attempt-4-stage-attempt-1-review-4-gpt-6.1-sol.md)
5. [Testing](task-unknown-attempt-4-stage-attempt-1-review-5-gpt-6.1-sol.md)
6. [Hardening and driver provenance](task-unknown-attempt-4-stage-attempt-1-review-6-gpt-6.1-sol.md)

Fresh local compilation, style, 60 offline Scala tests, 55 live Scala tests,
23 installed Python bridge tests plus six subtests, notebook structure checks
and code generation passed. One intentional live `Custom EndPoint` ignore
remains. Local Python 3.12.3 and PySpark's bundled Scala 2.12.18 do not exactly
match target Python 3.11.8 and build Scala 2.12.17.

The full source patch and canonical manifest independently match on newer
website-only target `acd6a8cc5f914a3d443d9666a8c4599d65f067da`.
Latest-target integration, fresh narrowed-head CI and automated review, and the
separate final CI-qualified pass remain required. Four of six pre-commit passes
and zero of two final passes have been consumed.

The cache-inclusive implementation and exact earlier candidate binaries were
preserved separately. Every earlier public review below remains unchanged.
Those earlier tests and reviews are historical evidence, not qualification of
the narrowed source.

## Earlier cache-inclusive review history

The six qualifying pre-commit reviews were moved without changing their bytes.
Their original public locations remain in commit `e7f0a0d20ef7a590be3b4a053c5153f3a436b404`,
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

## Target rebase and historical CI reconciliation (October 23)

The personal feature branch was explicitly authorized to rebase onto current
`master`, `71346cf992d489007d133a1a9f83c325e7694373`. All 29 source/test/example
blobs and the complete frozen source diff remain byte-identical to pass 3.
The patch was independently applied to the new target and its full path, mode,
and blob manifest matched. All twelve original review files are unchanged.
Only this index is updated to repair archival links and record the new context.

| Historical commit | Retained rebased commit |
| --- | --- |
| `6bb12ed7c45f914f40b6c7afc4b546c6ac9b051b` | `e7f0a0d20ef7a590be3b4a053c5153f3a436b404` |
| `edadc4a04a6cf51f9956d1569cf57ff7fe345958` | `f8e06cf94a30cb0b1dadd913fa0d915ff44b3ac3` |
| `32395b928c9e3ca354ea13a1bc2eb42fbe1bff96` | `7116b5df01bd4727262a19fdb9b4a23dbe18194e` |

The first retained commit contains the original pre-PR review paths. The second
retains their byte-preserving migration, and the third retains the fixture fix
and six pass-3 reviews. No review originals were pruned.

Azure build 242072049 tested the historical `32395b9` head:

- OpenAI: 266 passed and two intentional `Custom EndPoint` ignores.
- Databricks notebooks: 43 CPU and 4 GPU passed, with no notebook skips.
  The changed Langchain, OpenAI, multimodal Responses, and Forms notebooks each
  have a named passing result. Cache-option instructions in notebook markdown
  are not evidence of service-side cache-hit or TTL behavior.
- Cognitive Python: 167 passed using actual Python 3.11.8. This does not change
  the historical local bridge's Python 3.12.3 limitation.
- R deep-learning: all four published result records passed; their display
  names contain three distinct names. Coverage publication then failed TLS
  certificate-name verification. No failing R test or product defect was found,
  and neither coverage requirements nor TLS checks were weakened.
- Fabric E2E and InternalCompat were excluded by the pipeline's fork policy;
  they were not executed and are not counted as passes.

The required aggregate failed because coverage publication failed. These are
historical results, not validation of the rebased head. Fresh current-head CI
and automated review are required before the separate final CI-qualified
six-lens review. Budgets remain three of six pre-commit passes and zero of two
final passes. The unchanged r3 feature artifacts retain their earlier evidence;
they are not newly built artifacts for the target's unrelated AutoML additions.
