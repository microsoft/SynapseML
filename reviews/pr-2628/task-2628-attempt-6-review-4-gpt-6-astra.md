## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: sequential
- **Model**: gpt-6-astra
- **Artifact**: `reviews/pr-2628/task-2628-attempt-6-review-4-gpt-6-astra.md`
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed only the staged integration for microsoft/SynapseML#2628 against local HEAD `ae45761b61ccffe06f4516b97a24042665212cd1`. The index contained 23 changed files, including four prior review artifacts. SHA-256 of `git diff --cached --binary` was `804bdb35910fbd9d5d5850feac7c7dad9727b84bffac695c472312e409f3d14f`. The worktree matched the index before review. GitHub reports the PR base as `master`.
- [x] Read the supplied round-4 prompt, worktree `AGENTS.md`, branch and release guidance, and round-1 and round-3 findings and resolution records. Reviewed the current staged source rather than treating the prompt's embedded diff as the latest source. The finding below concerns a regression introduced by the new pre-tag guard, not the resolved round-1 producer-fixture error.
- [x] Independently loaded the original `HEAD:scripts/release/release_matrix.py` into an in-memory module. Compared its schema-2 plans with the staged loader for `master`, `spark4.1`, the default two-target selection, and the explicit three-target selection. All four original documents and approval IDs round-trip unchanged, with no DBC obligations. Equivalent schema-4 plans have different approval IDs and exactly one DBC obligation per selected target.
- [x] Followed DBC requirements through `release_ops.py` inventory, action presence, receipt validation, completion and producer-evidence export; `verify_release.py` inventory and public evidence validation; and `release_guard.py notes`. The notes CLI validates producer evidence before comparing fresh anonymous DBC downloads with the recorded hash and size. The focused regressions reject omitted archives, changed public hashes or sizes, incomplete receipts and forged producer facts. Legacy plans retain the no-DBC notes and receipt scope.
- [x] Read the complete native builder and publisher, including committed-source preparation, whitespace-sensitive cell comparison, archive limits and output rejection, staging identity, no-overwrite publication, ambiguous-upload reconciliation and cleanup. Independently checked the supplied current-head synthetic archive against Git source. All 56 notebooks and 1,108 nonempty native commands match the sanitized source. Its source digest matches the provenance record; the archive is 304,258 bytes with SHA-256 `83b5d57d50b19881b6ef972fcc407974ec7d9a2aa18cf0c9beae448d8d642903`. This was a local inspection of the supplied native evidence, not another Databricks API run, notebook execution or publication.
- [x] Checked the Release job and its schema gate. DBC build, retention and public verification precede PyPI and ESRP; the final receipt receives the DBC staging directory. The ordering contract passes. Traced normal primary and port tag callers and the separate bootstrap path. Reproduced the new CLI dependency failure in the real POSIX tag-recovery tests, as detailed below.
- [x] Ran an offline focused selection covering `test_release_dbc.py`, `test_release_dbc_contract.py`, `test_release_guard.py`, `test_release_plan.py`, `test_release_public.py`, `test_verify_release.py`, the saved three-target identity test, the actual producer-receipt test and the public-notes export tests. WSL returned **285 passed and one local Git interoperability failure**: its Linux Git cannot resolve this Windows-created worktree's Git-directory reference. The affected `test_current_committed_notebooks_are_admissible` subsequently passed with native Windows Git. This environment failure is not reported as a source defect.
- [x] A separate offline run of public two-/three-target completion, exact receipt inventory, partial visibility and producer-evidence rejection tests returned **20 passed**. The targeted POSIX tag-recovery command returned **2 failed**, both with the same new missing-module error. Cache writing and Python bytecode generation were disabled. Removed the owned temporary reproduction repositories.
- [x] Kept source and the index unchanged. This report is the only repository file added. No other agents, factories, recursive review workflow, repository commits, live tag pushes or live publishing were used.
- [ ] Did not rerun the full release suite, hosted CI or cloud publication. The supplied 922-pass/one-optional-SBT-skip result predates the latest round-3 changes; the supplied 139-test focused result and older pipeline/docs results do not cover the failing POSIX fixture identified here.

## Issues

### Issue 1: Update the POSIX tag-recovery fixture for the new notebook preflight
- **Severity**: Medium
- **File**: `scripts\release\release_guard.py`
- **Line(s)**: 586-588. Related fixture: `scripts\release\test_release_tag_recovery.py:388-417`; failing assertion: line 583; mandatory CI invocation: `.github\workflows\pr-validation.yml:79`.
- **Description**: The new `push-tags` branch imports `release_dbc` and checks the tagged commit's notebooks. The existing POSIX `release_repo` fixture creates a separate checkout by copying only `release_guard.py`, `release_matrix.py`, `verify_release.py` and `release_config.py`. It does not copy `release_dbc.py`, so the actual workflow script now stops at the new import with `ModuleNotFoundError: No module named 'release_dbc'`. Both parameterizations of `test_rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips` fail. Copying the new module alone is insufficient: the fixture's common, primary and port commits contain no notebooks. Running the real `prepare_notebooks` against each retained fixture checkout independently returned `Release source contains no notebooks`.
- **Risk**: The required Linux release-tooling check is broken by the staged change. The affected module skips on Windows, so the passing focused guard/archive tests do not establish that the actual tag-recovery workflow still passes. The workflow tests currently stop before exercising their tag-recovery assertions.
- **Suggested Fix**: Update `release_repo` to copy the new runtime dependency and commit a minimal admissible Python notebook before creating its common, primary and port commits. Keep the real notebook guard active in these workflow tests rather than bypassing it. Rerun the POSIX tag-recovery module and the release suite after updating the fixture.

Reproduction from the worktree's `scripts\release` directory, using WSL Python and Git:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest \
  test_release_tag_recovery.py::test_rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips \
  -q -p no:cacheprovider

FAILED ...[False]
FAILED ...[True]
release_guard.py:586: from release_dbc import prepare_notebooks
ModuleNotFoundError: No module named 'release_dbc'
2 failed in 2.72s
```

This reproduction uses isolated temporary repositories and mocked GitHub responses. It performs no live tag publication.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending. This review changed no source or tests.
- **Why**: The new CLI preflight needs both its module and committed notebook input in the workflow fixture.
- **How verified**: Both targeted POSIX parameterizations failed at the new import. Independent inspection of their temporary checkouts confirmed the missing module, and the real notebook preparer rejected both checkouts for having no notebooks. The earlier focused passing results do not exercise this fixture.

## Resolution verification

Resolved in `test_release_tag_recovery.py`. The fixture copies `release_dbc.py`
and commits a minimal Python notebook before deriving primary and port commits.
It keeps the real preflight enabled. All **87 POSIX tag-recovery tests passed**
after the change, including both rerun-recovery parameterizations.
