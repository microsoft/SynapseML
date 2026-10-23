# Review evidence for PR #2781

## Current formatting and Boolean-test correction: pre-commit pass 8

The complete current 29-path patch passed the explicitly authorized
[Low-tier, single-reviewer harness pass](task-unknown-attempt-8-review-0-harness.md)
using `gpt-6.1-sol/max`. This human-selected exception applies only to
precommit attempt 8, not to the distinct final Medium six-lens review.
The complete-input, runtime-model, independence and privacy audit passed.

Source manifest:
`2062293f76a534475c71421ecd28ed1348c5666e12b4baa72a59e6242b6c62be`.
Full diff:
`ce78a5da810b002bd8f336243447a7b4f558cc9edcc8cc17955ab64bf9a37585`.
Target: `acd6a8cc5f914a3d443d9666a8c4599d65f067da`.

- Notebook-aware Black 22.3.0 passes. Only four wraps in two notebook cells
  changed; executable ASTs, other cells, metadata and clean outputs are
  preserved. The exact formatted tutorial passed seven loopback requests.
- Scalar Boolean assertions now distinguish `True` from integer `1` on
  original, copied and restored values. The scalar persistence fixture
  includes a Boolean. Two installed-wrapper tests and six subtests pass.
- Independent integer-corruption probes fail as intended in nine cases:
  four original wrappers, four copies and one restored scalar stage.
  [Original finding and appended resolution](task-unknown-attempt-7-stage-attempt-1-review-5-gpt-6.1-sol.md)
  are retained. No runtime change, artifact rebuild or new live call was needed.

Precommit allowance is eight of eight consumed; final allowance remains
zero of two. **Engineering readiness is not established.** Current-revision
Azure validation and coverage remain pending. The authorized comment trigger
is paused until its protected reviewed-revision resource guard is established;
the coverage-disabled historical manual run is not a substitute. No branch,
pipeline, permission or release policy was changed.

## Historical pending formatting correction and pre-commit pass 7

An explicitly approved additional pass covers the full 29-path source patch.
Pinned notebook-aware Black 22.3.0 changed only four line wraps in code cells
40 and 42 of `OpenAI.ipynb`. The affected executable ASTs, every other cell,
notebook metadata, list-of-string sources and empty outputs were preserved.
The exact five cache tutorial code cells passed seven loopback requests with
native SDK/SynapseML wire parity. Unchanged production binaries and their
earlier validation were reused; no new live requests were needed.

The pending source manifest is
`213d4b7178c74f5c7da2f7c67ba6112630e0a78b4c56e89a127f947685c9f022`;
the complete diff is
`83b4442d184f4e530d67d0abef9097f9de74d18fb0c324f8b7474e4de9cd8dfa`,
against unchanged target `acd6a8cc5f914a3d443d9666a8c4599d65f067da`.

1. [Broad sweep: clean](task-unknown-attempt-7-stage-attempt-1-review-1-gpt-6.1-sol.md)
2. [Architecture: clean](task-unknown-attempt-7-stage-attempt-1-review-2-gpt-6.1-sol.md)
3. [Robustness: clean](task-unknown-attempt-7-stage-attempt-1-review-3-gpt-6.1-sol.md)
4. [Correctness: clean](task-unknown-attempt-7-stage-attempt-1-review-4-gpt-6.1-sol.md)
5. [Testing: confirmed open assertion gap; driver provenance and disposition](task-unknown-attempt-7-stage-attempt-1-review-5-gpt-6.1-sol.md)

The Boolean-preservation test accepts integer `1` wherever it expects `True`.
A readback-only fault-injection experiment confirmed both existing cache
test methods still pass. Baseline wrappers preserve actual Booleans; this is
a test-sensitivity gap, not an observed runtime conversion bug. The proposed
test correction needs a fresh complete source-review pass.

**Blocked:** seven of seven authorized precommit passes are consumed.
Lens 6 was not dispatched; no test fix, commit, push, CI trigger or final
review followed the finding. Final allowance remains zero of two. Required
PR coverage and current-revision CI remain pending. The following sections
preserve historical evidence and do not describe current qualification.

## Historical cache refinement and pre-commit pass 6

The current scope includes the Luna migration, optional request-level
`prompt_cache_options`, and documented cache breakpoints inside input content.
Scalar and row-bound options preserve JSON types, including mixed-type Spark
structs. The provider, rather than a client model/mode/lifetime whitelist,
validates cache policy. The notebook contains executable native SDK and
SynapseML examples, explicit and implicit modes, omission behavior, actual
usage fields, and the limitation that cache hits are not guaranteed.

The complete 29-path source patch published in
`51343aa337a5fdbabdf75ec30f6fd62d2203a0e3` has source manifest
`0294032458aa55909c1208076e07a5594c374ba905d66e0efdbb2cde41920dd6`
and full diff
`b943496ae5dd8fc90860a9e261981d5adef0b5b41aa12ded119c46018eb4a292`,
against target `acd6a8cc5f914a3d443d9666a8c4599d65f067da`.

Pass 5 found a mixed-type Spark struct serialization defect after four clean
lenses. Its [original finding and appended resolution](task-unknown-attempt-5-stage-attempt-1-review-5-gpt-6.1-sol.md)
remain intact. All four public transformer routes failed before the fix;
eight HTTP requests then preserved string and Boolean values after the fix.
The partial pass was invalidated, not combined with later clean lenses.

All six independent, sequential `gpt-6.1-sol/max` lenses in pass 6 are clean:

1. [Broad sweep](task-unknown-attempt-6-stage-attempt-1-review-1-gpt-6.1-sol.md)
2. [Architecture](task-unknown-attempt-6-stage-attempt-1-review-2-gpt-6.1-sol.md)
3. [Robustness](task-unknown-attempt-6-stage-attempt-1-review-3-gpt-6.1-sol.md)
4. [Correctness](task-unknown-attempt-6-stage-attempt-1-review-4-gpt-6.1-sol.md)
5. [Testing](task-unknown-attempt-6-stage-attempt-1-review-5-gpt-6.1-sol.md)
6. [Hardening, aggregate outcome, and driver provenance](task-unknown-attempt-6-stage-attempt-1-review-6-gpt-6.1-sol.md)

Local compilation, Scala styles, selected Python Black checks, code generation,
69 Scala tests, and 25 installed Python tests plus 12 subtests passed. All five executable cache
tutorial cells passed with seven mock and seven live requests; the runnable
PR snippets also produced equivalent native SDK and SynapseML request bodies.
Local Python 3.12.3 and bundled Scala 2.12.18 differ from target Python 3.11.8
and build Scala 2.12.17.

This is **precommit qualification only**. Current-head Azure validation,
required GitHub status, automated feedback disposition, the distinct final
CI-qualified review, and final-SHA reconciliation remain required. The
successful model-only predecessor build does not qualify the cache amendment.
Budgets stand at six of six precommit passes and zero of two final passes.
This index correction is allocated review evidence only; it does not change
the frozen source fingerprint or erase any earlier finding.

### Current source CI disposition

SHA-pinned manual Azure build `242404552` completed **failed** on merge
`def7be6822d7edd59321634ca4811361b3bb1ac4`, whose ordered parents are the target
above and source `51343aa337a5fdbabdf75ec30f6fd62d2203a0e3`.

- Published OpenAI results: 270 passed and two existing `Custom EndPoint`
  ignores. Both cache suites, both new Python cache tests, and the mixed-type
  public-transform regressions passed.
- Cognitive Python: 167 passed. Databricks notebooks: 43 CPU and 4 GPU passed.
  Website samples: 111 passed. These passes do not clear the failed build.
- Python Style Check failed because Black requires line wrapping in two new
  `OpenAI.ipynb` cells. This is an attributable notebook-formatting defect,
  not an infrastructure failure; the selected local Python checks did not
  cover notebook formatting.
- All 54 coverage-publication tasks were skipped. The manual build reason
  does not enable the pipeline's PR coverage condition; queue-time values
  did not override that expression. Coverage is **not qualified**.
- Foundry's optional dependency-cache restores reported TLS errors; its eight
  tests still passed. These errors are retained separately, not used to
  excuse the notebook failure or missing coverage.
- The Azure Pipelines app posted a failed `microsoft.SynapseML` check on the
  exact synthetic merge commit, not on the source-head rollup. This is real
  merge-bound failure evidence, not a passing or missing-everywhere status.

The formatting correction and its required new full source-review pass remain
blocked by the exhausted six-pass precommit budget. No source was changed,
no check was waived, and no final review was started. The artifact-only index
correction addresses the automated review's stale-scope finding separately.

## Historical narrowed scope and pre-commit pass 4

At pass 4, initial delivery was the Luna model migration only. Newly introduced cache
controls, serializers, wrapper additions, documentation and tests were deferred.
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

The branch is rebased onto website-only target
`acd6a8cc5f914a3d443d9666a8c4599d65f067da`. The complete source patch and all
23 canonical path/mode/blob entries remain identical, as do all 18 earlier
review artifacts. The reviewed source is retained in
`7a8a7af559ab5a72b815d1d4f0b3e3f18ec0c1b5`. Fresh narrowed-head CI and
automated review, and the separate final CI-qualified pass remain required.
Four of six pre-commit passes and zero of two final passes have been consumed.

The cache-inclusive implementation and exact earlier candidate binaries were
preserved separately. Every earlier public review below remains unchanged.
Those earlier tests and reviews are historical evidence, not qualification of
the narrowed source.

## Earlier cache-inclusive review history

The six qualifying pre-commit reviews were moved without changing their bytes.
Their original public locations remain in commit `ed31f3148e6e75e90c6b3cd24792c073ea194aa4`,
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

| Historical commit | Intermediate rebased commit |
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

## Narrowed-source rebase and predecessor CI (October 23)

The latest target changed five website files, not AI source, runtime build
configuration, pipeline configuration, or trusted guidance. Rebase retained all
five feature commits without squashing. The following mapping supersedes the
intermediate mapping above; all right-hand commits are retained ancestors.

| Before this rebase | Retained rebased commit |
| --- | --- |
| `e7f0a0d20ef7a590be3b4a053c5153f3a436b404` | `ed31f3148e6e75e90c6b3cd24792c073ea194aa4` |
| `f8e06cf94a30cb0b1dadd913fa0d915ff44b3ac3` | `dc7cad8d84e1ba6024c089c3e50dfef2518796c3` |
| `7116b5df01bd4727262a19fdb9b4a23dbe18194e` | `bcea926fbf4756ada4232e21a24c8508661f4bc0` |
| `d68a73ad98398a4520cc41157e3a3f5f8de6bd9c` | `20b7665e0858c381c29f57e4dee6bb30b04b5db5` |
| `26b51a0d481445089a070211cf2f2530b80989f2` | `7a8a7af559ab5a72b815d1d4f0b3e3f18ec0c1b5` |

Azure build 242356862 tested the **earlier cache-inclusive** `d68a73a` head,
not this narrowed source. Its distinct current-attempt results were:

- OpenAI: 266 passed; two existing `Custom EndPoint` cases intentionally ignored.
- Databricks: 43 CPU and 4 GPU notebook checks passed, with zero notebook skips.
  Named results cover `Langchain.ipynb`, `OpenAI.ipynb`,
  `Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb`, and
  `Quickstart - Understand and Search Forms.ipynb`.
- Cognitive Python: 167 passed. R deep-learning: four result records passed.
- Python deep-learning-nohf: four passed and three pre-existing explicit skips
  (`test_bert_base_cased`, `test_lit_deep_text_model`, `test_mobilenet_v2`).
  Its test execution and test-result publication succeeded, but coverage
  publication failed TLS hostname validation (`RemoteCertificateNameMismatch`)
  on all three automatic task executions. Those executions are not additional
  unique tests. No product fix, test weakening, TLS bypass, or manual retry was
  applied.
- FabricE2E and InternalCompat were excluded by unchanged fork-policy
  conditions, not executed or counted as passes.

The aggregate remains failed. None of these historical results qualifies the
narrowed source; fresh SHA-bound CI and final review remain required.
