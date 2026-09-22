# Round 5 testing and coverage review

## Review summary

| Field | Result |
| --- | --- |
| Target | microsoft/SynapseML#2733, base `spark4.0` |
| Worktree | Isolated port checkout; machine-local path omitted |
| Reviewed HEAD | `96e5ac204b45b1d0c83c8407ca97904c56f7bd94` |
| Merge source / MERGE_HEAD | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Reviewed staged tree | `b7c6bd34f1eeac39dccaab00ca38cd54539772b4` |
| Round / attempt / actual model | 5 / 1 / `gpt-6-astra` |
| Mode | Sequential, explicitly authorized GPT fallback |
| Findings / verdict | **1 Low / ISSUES_FOUND** |
| Artifact | `reviews\pr-2733\task-latest-master-20260922-attempt-1-review-5-gpt-6-astra.md` |

The requester reports Gemini failed before execution and explicitly authorized
this GPT fallback. No unavailable-model probe was retried. Gemini did not
perform this review; this is not full three-family success.

## Snapshot and inspected evidence

HEAD, MERGE_HEAD, and the complete index match the accepted round-4 candidate.
There are no unresolved index entries or unstaged tracked changes.

SHA-256 of raw `git ls-files --stage -z` output:

`0f0f368a6989f2de9991063f4073ad721000902dea0e962a59ee4c1f09d727c0`

SHA-256 of raw `git --no-pager diff --cached --binary --full-index --no-ext-diff --no-textconv HEAD --` output:

`049915dd33a504a837ddf303165abd46e1ad9f52114f21afb0607157a6440611`

Used retained staged-source context and the file inventory in the prior review
artifacts. Narrow follow-up reads covered
`tools\ci\tests\test_e2e_impact.py:269-312` and
`tools\ci\e2e_impact.py:128-161` to verify the finding below.

## Coverage evidence checklist

- [x] `tools\ci\tests\test_e2e_impact.py:163-269` uses actual temporary Git
  repositories, commits, two-parent merges, and shallow clones. Cases cover
  add/modify/delete, a runtime-to-review rename, empty diffs, target advancement,
  depth-one fail-open versus depth-two selection, and symlink/gitlink modes.
  These exercise Git's real output rather than only fabricated path lists.
- [x] `tools\ci\tests\test_e2e_impact.py:349-370` checks exact CLI output for
  PR, scheduled, and manual builds. Lines 372-421 and
  `tools\ci\tests\test_pipeline_yaml.py:296-341` connect the output names to
  the detector step, dependency, Databricks conditions, and five CPU/one GPU
  scheduling shape. These are local CLI/YAML checks, not Azure execution proof.
- [x] `tools\ci\tests\test_e2e_impact.py:382-392` explicitly asserts Fabric's
  Boolean False condition while retaining the common dependency assertion.
  Lines 424-454 discover the four coverage-producing jobs, count 54 uploads,
  compare both `codecov.yaml` thresholds, and reject selector gates on them.
  The disabled-Fabric adaptation does not bypass those checks.
- [x] `GeospatialCoreSuite.scala:168-202` exercises the public writer/reader
  with scalar and column-bound parameters, unique save paths, fake credentials,
  UID/URL/parameter comparisons, output/error schema equality, and the
  retirement exception after loading. Existing nearby tests cover missing
  columns and expected schema types. The unchanged retired transform prevents
  live HTTP, and `TestBase` owns temporary-directory cleanup.
- [x] `VerifyValueIndexer.scala:42-45,76-90` retains the named tests and every
  assertion; only unused duplicate-loop wrappers disappear. Genuine type,
  categorical metadata, round-trip, and inherited fuzzing coverage remain.
  The ignored null case predates this increment. Deleting
  `tools\pytest\run_all_tests.py` does not remove the active pytest dispatch
  in `project\CodegenPlugin.scala:428-448`.
- [x] The changed README's five-CPU/one-GPU and disabled-Fabric claims now match
  the guarded pipeline. Round 4 established that this was the only change
  after local validation. The requester reports 68 Scala tests passing with
  one pre-existing ignored case, all 291 CI helpers, Black 22.3.0, aggregate
  compilation, Test compilation, and Scala style passing on each candidate.
  Those completed runs were not repeated or represented as executions by this
  round.
- [ ] The full-test override has a regression test that fails if the override
  is ignored. The current test does not establish this; see R5-1.

## R5-1: Override assertions pass through an unrelated fail-open path

- Severity: Low.
- File/lines: `tools\ci\tests\test_e2e_impact.py:284-292`.
- Affected scope: the same incoming test in both ports.

`test_forced_or_unknown_full_test_option_runs_everything` supplies only
`BUILD_REASON` and `SYNAPSEML_FULL_TESTS`. If the override guard in
`tools\ci\e2e_impact.py:128-139` is removed, `changed_paths` rejects the missing
`BUILD_SOURCEBRANCH`. The error handler still returns `ALL_SUITES`, satisfying
all five assertions for the wrong reason. The CLI tests use the fixture's
`SYNAPSEML_FULL_TESTS=false`, so they do not cover the forced-override case.

A bounded, memory-only mutation probe extracted this exact test function from
the staged blob and removed only the override guard from an in-memory helper
AST. It wrote no files and invoked no Git commands from the selector.

| Probe | Original helper | Override guard removed in memory |
| --- | --- | --- |
| Existing five override assertions | 5/5 pass | 5/5 pass |
| `fullTests=true`, with `changed_paths` stubbed to return a successfully detected isolated Python-test path | `ALL_SUITES` | Empty selected set |

The override guard itself is currently correct. The gap is that these
tests would miss an ignored full-test override, allowing a valid isolated PR
to skip CPU/GPU E2E despite the explicit override.

Suggested fix: use the existing `make_pr` fixture with a skippable change,
retain its valid merge metadata, and vary `SYNAPSEML_FULL_TESTS`. Assert all
families for the existing forced/unknown values, with a `"false"` control that
actually selects none. A `"False"` control would also cover Azure's Boolean
string normalization.

## Resolution log and limits

R5-1 is **Open**. No source fix was made because this request authorizes review
artifacts only. The relevant helper/test blobs are identical across the two
ports, so the single bounded probe applies to both.

No full suite, broad discovery, remote CI, or later review theme ran. No source
file, index, commit, ref, or resource was changed; the dirty root was untouched.
Only this unstaged round-5 artifact was written in this worktree.

## Follow-up resolution

R5-1 is **Fixed**. The test now uses `make_pr` with an isolated Python-test
change and valid two-parent merge metadata. It first proves that the normal
`false` setting selects no notebook suites, then checks every forced or unknown
override against that same valid repository. Production selector code is unchanged.

A memory-only mutation removing the override guard passed all five old assertions
but fails all five strengthened assertions. With the real guard, all 121 selector
tests pass on both ports and Black 22.3.0 passes. This reinforces validation of the
imported CI contract without changing master-compatible runtime behavior.
