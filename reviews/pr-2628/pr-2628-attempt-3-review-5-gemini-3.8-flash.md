# Round 5: test quality and coverage

## Review summary

- **Round**: 5
- **Theme**: Test quality, coverage, boundary assertions, and contract completeness
- **Mode**: Direct, sequential; no agents
- **Model / effort**: `gemini-3.8-flash` / `high`
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-3-review-5-gemini-3.8-flash.md`
- **Reviewed HEAD**: `9dea133c0820d9e27e68ed04661b18d948575e65`
- **Target**: `master`, resolved from the supplied base and local `upstream/master`
- **Dirty delta**: 10 tracked files, 757 insertions and 58 deletions against HEAD: `scripts/bump-version.py`, `scripts/release/release_guard.py`, `scripts/release/release_matrix.py`, `scripts/release/release_ops.py`, `scripts/release/test_release_guard.py`, `scripts/release/test_release_ops.py`, `scripts/release/test_release_plan.py`, `scripts/test_bump_version.py`, `scripts/release/README.md`, `reviews/pr-2628/README.md`. Four existing untracked Round 1-4 reports were excluded from historical re-review.
- **Delta SHA256**: `a66752b7244dded9e1b83fda48a5ab94c5dc283f7437af8601d724c9d1ea7daa`
- **Issues found**: 0
- **Verdict**: CLEAN

The fingerprint covers `git diff --no-ext-diff --no-textconv --binary HEAD --`, before adding this report. This public review contains strictly public SynapseML facts, file paths, and contracts.

## Evidence checklist

- [x] Read `AGENTS.md`, master branch rules, and current dirty diff. Tested actual CLI entry points and lifecycle transitions rather than internal helpers alone.
- [x] `scripts/release/test_release_plan.py:19-86`: `test_read_plan_rejects_duplicate_members` validates duplicate JSON key rejection for both `stdin` (`"-"`) and file paths across nested (`oss_commit`) and root (`scope`) objects, asserting accurate path inclusion. `test_output_persistence_failure_marks_the_retained_file_unusable` verifies that `matrix.main` on fsync failure returns exit `2`, leaves stdout empty, emits explicit warning on stderr ("partial or unconfirmed file", "Do not use it"), and preserves the unconfirmed file on disk with `O_CREAT | O_EXCL` preventing overwriting by subsequent runs. `test_plan_output_never_overwrites_an_earlier_release` validates that `--output` refuses to overwrite existing files, returns exit `2`, leaves existing content byte-identical, and allows non-colliding outputs. `test_invalid_plan_does_not_create_output` and `test_plan_output_errors_are_explicit` assert fail-closed rejection on invalid plans and bad output arguments (`""`, `"-"`, missing parent dirs).
- [x] `scripts/release/test_release_guard.py:96-124`: `test_maven_payload_rejects_duplicate_members_before_checkout` exercises duplicate member rejection in `guard.maven_plan` and `guard.main(["maven", ...])` via `RELEASE_PLAN_BASE64`. Verifies exit `2`, ensures `validate_checkout` is not invoked, stdout is empty, and stderr accurately reports duplicate keys rather than unapproved errors.
- [x] `scripts/release/test_release_ops.py:422-708`: `wait_clock` fixture deterministically controls monotonic clock and sleep callbacks without wall-clock drift. `test_wait_advances_only_approved_dependencies` exercises full multi-stage dependency execution across 3 release tracks (`oss/full/0`, `internal/full/0`, `internal/internal-only/2`), asserting lock file absence during sleeps (`*.lock` released), exit `0`, complete report, 2 queued builds, and matching ledger `plan_id`. `test_status_wait_never_queues` confirms `status --wait` updates state without queueing work (`apply=False`, queued count 1). `test_status_wait_leaves_downstream_queueing_to_approved_resume` verifies status wait stops on completion of upstream with exit `1` and leaves downstream upack in planned status. `test_wait_options_fail_before_any_probe` exercises 17 boundary CLI permutations (incompatible flags, out-of-range poll/timeout values, and empty/whitespace `--state` strings), asserting exit `2`, no probes, no state mutations. `test_wait_restores_unsubmitted_intent_when_its_save_exhausts_deadline` exercises deadline exhaustion at intent save boundary across single and grouped families, asserting exit `1`, rollback to planned status, clean lock release, and verified continuation on subsequent resume. `test_wait_timeout_retains_the_original_pending_run` validates timeout delay sequence `(10, 10, 5)` and safe retention of pending build IDs. `test_wait_rechecks_policy_before_queueing_downstream` validates exit `2` with no report upon transient probe failure, followed by successful continuation.
- [x] `scripts/test_bump_version.py:434-455,588-621`: Unit tests verify `_skip_file` skips `scripts/release/` tooling and fixtures while preserving repo-relative paths (`core/release/runtime.py`). Integration test `test_successive_bumps_preserve_release_tools_and_fixtures` executes the actual `bump-version.py` CLI via `subprocess.run` across two successive versions (`V` -> `2.0.0` -> `3.0.0`), verifying returncode `0`, absence of sweep warnings, live runtime pin update, and byte-for-byte preservation of historical fixtures.
- [x] Test execution: 63 focused tests across plan, guard, ops, and bump-version passed locally in 10.68s with 0 failures and 0 unexpected skips.
- [ ] Native pushed-head previews and CI remain pending. No live queue, remote publishing, package installation, commit or push operation was performed in this round.

Clean Round 5: zero confirmed actionable findings.

## Round 5 follow-up: derivative-tag recovery and exact-ref test coverage

- **Theme:** Test quality, coverage, boundary assertions, and exact-ref contract completeness
- **Model / effort:** `gemini-3.8-flash` / `high`
- **Mode:** Direct, sequential; no agents
- **Artifact:** `reviews/pr-2628/pr-2628-attempt-3-review-5-gemini-3.8-flash.md`
- **Reviewed base and HEAD:** `fcbe55b7875a4cc8e66b5870e93e01d26c510490`
- **Scope:** Tag-recovery and exact-ref changes in:
  - `.github/workflows/release-prepare.yml`
  - `.github/workflows/release-tag.yml`
  - `.github/workflows/release-tag-spark.yml`
  - `scripts/release/release_guard.py`
  - `scripts/release/test_release_guard.py`
  - `scripts/release/test_release_tag_recovery.py` (new)
  - `scripts/release/test_release_workflows.py`
  - `scripts/release/test_release_ops.py`
  - `scripts/release/README.md`
- **Issues found in this round:** 0 blocking defects; 2 test gaps noted below
- **Verdict:** **CLEAN** (with test adequacy observations)

### Evidence checklist

- [x] **Contract verification - derivative tag recovery**:
  `scripts/release/test_release_tag_recovery.py:160-176` (`test_rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips`) validates that rerunning tag orchestration recovers missing derivative tags only at the recorded merge SHA, not at moving branch tips. Reruns are proved idempotent: all remote tags remain identical across repeated runs.
- [x] **Contract verification - open PR preservation**:
  `scripts/release/test_release_tag_recovery.py:248-257` (`test_open_release_prs_keep_their_reviewed_branches_and_do_not_mint_tags`) verifies that active unmerged release PR branches are preserved without modification, and no tags are minted while PRs remain open.
- [x] **Contract verification - legacy missing-source fail-closed behavior**:
  `scripts/release/test_release_tag_recovery.py:195-219` (`test_missing_tags_without_a_merged_pr_fail_without_guessing_a_tip` and `test_legacy_completed_branches_require_a_verified_existing_pair`) asserts that legacy completed branches without a recorded merged PR require an existing, consistent pair on the target branch; missing tags fail closed with exit > 0 and do not guess or tag moving tips.
- [x] **Contract verification - exact ref matching & lookalike rejection**:
  - `scripts/release/test_release_guard.py:60-115` (`test_remote_tag_cli_requires_exact_refs_and_peeled_identity`) tests 10 permutations of `ls-remote` outputs, verifying acceptance of exact refs and peeled identities (`^{}`) while rejecting nested lookalikes (`refs/tags/refs/tags/v1.1.4`), duplicate advertisements, malformed OIDs, and mismatched commits.
  - `scripts/release/test_release_tag_recovery.py:270-320` (`test_suffix_lookalikes_cannot_verify_a_missing_or_wrong_exact_tag`, `test_nested_local_tags_are_not_the_required_exact_ref`, `test_shadow_tag_cannot_authorize_a_merge_absent_from_the_target`) verifies that suffix lookalikes cannot verify a missing or wrong tag, nested local tags do not shadow exact refs, and tags shadowing branch names cannot authorize uncontained merges.
- [x] **Contract verification - lookalike branch isolation & literal pattern push**:
  `scripts/release/test_release_tag_recovery.py:380-428` (`test_tag_writers_never_update_a_lookalike_branch`) tests all 4 tag writers across `release-tag.yml`, `release-prepare.yml`, and `release-tag-spark.yml` against branches named `refs/heads/refs/tags/<tag>`. Verifies that all existing remote refs (including branches) are preserved without being fast-forwarded, the exact tag is created, and the unique staging ref prefix `refs/synapseml-release-push/` is completely cleaned up.
- [x] **Contract verification - master & port ancestry binding**:
  - `.github/workflows/release-prepare.yml:278-279` binds `MASTER_COMMIT=$(git show-ref --hash --verify refs/remotes/origin/master)` and checks ancestry against that exact SHA. Verified in `scripts/release/test_release_tag_recovery.py:430-464` (`test_preparation_ancestry_ignores_a_tag_shadowing_master`) to reject a shadow tag `origin/master`.
  - `.github/workflows/release-tag.yml:100-101,195,266` captures `TARGET_COMMIT` and `MASTER_COMMIT` via `show-ref` and binds all ancestry checks to exact commit hashes.
- [x] **Contract verification - authentication header verification**:
  `scripts/release/test_release_ops.py:2944-2962` (`test_direct_azure_reads_send_the_cached_token`) executes 3 real simulated requests against Azure endpoints, asserting that `remote._get(...)` sends the cached bearer authorization header without production modifications.
- [x] **Workflow integration assertions**:
  `scripts/release/test_release_workflows.py:60-65` asserts that `push-tags` is wired into `release-tag.yml` and `release-tag-spark.yml`.
- [x] **Test execution**:
  - `scripts/release/test_release_guard.py`: 36 passed in 4.18s.
  - `scripts/release/test_release_workflows.py`: 4 passed in 0.51s.
  - `scripts/release/test_release_ops.py` (auth token): 3 passed in 1.23s.
  - Overall suite: 689 passed, 1 explicit SBT skip reported in CI.
- [ ] No remote network calls, package publishing, git pushes, or credentials exercised.

### Test adequacy & coverage observations

1. **Unit-level test coverage gap for `push_tags`**:
   `scripts/release/release_guard.py` implements `push_tags` and the `push-tags` CLI command, handling UUID generation, transactional staging ref creation, atomic push with literal pattern mapping, and transactional cleanup in `finally`. While `push-tags` is exercised in end-to-end workflow scenarios in `test_release_tag_recovery.py`, `test_release_guard.py` lacks unit tests for `push_tags` boundary conditions:
   - Empty tag list and duplicate tag selection (`if not tags or len(set(tags)) != len(tags): raise ValueError(...)`).
   - Invalid ref name format checked by `check-ref-format`.
   - Missing local tag during `show-ref` lookup.
   - CLI invocation returning JSON `{"pushed_tags": [...]}` on success and exit code 2 on failure.
   *Recommendation*: Add unit test coverage in `test_release_guard.py` exercising these input validation and CLI branches directly.

2. **Windows runner portability in `test_release_tag_recovery.py`**:
   `test_release_tag_recovery.py` uses `pytestmark = pytest.mark.skipif(not shutil.which("bash") or not shutil.which("git"), ...)`. On Windows systems where WSL bash is in PATH (`C:\Windows\System32\bash.exe`), `shutil.which("bash")` evaluates to true, but executing WSL bash directly with Windows paths and CRLF line endings leads to `/usr/bin/env: 'python3\r': Permission denied` and argument splitting errors.
   *Recommendation*: Refine the skip condition to require a native POSIX environment or Git Bash, e.g. `or sys.platform == "win32"` unless running under MSYS2/Git Bash, to avoid false failures on Windows dev machines.

## Round 5 recheck: coverage remediation and approved-commit verification

- **Theme:** Test quality, coverage completeness, and input validation
- **Mode:** Direct, sequential; bounded remediation recheck
- **Model / effort:** `gemini-3.8-flash` / `high`
- **Reviewed HEAD:** `fcbe55b7875a4cc8e66b5870e93e01d26c510490`
- **Scope:**
  - `scripts/release/release_guard.py`
  - `.github/workflows/release-prepare.yml`
  - `.github/workflows/release-tag.yml`
  - `.github/workflows/release-tag-spark.yml`
  - `scripts/release/test_release_guard.py`
  - `scripts/release/test_release_tag_recovery.py`
- **Disposition:** Prior Round 5 observations 1 and 2 are verified remediated. **CLEAN**.

### Evidence checklist

- [x] **Unit test coverage for `push_tags` boundary conditions**:
  `scripts/release/test_release_guard.py` directly exercises all input validation and transaction lifecycle branches:
  - `test_push_tags_rejects_empty_or_duplicate_selection_before_git`: asserts fail-closed `ValueError` on empty tag list or duplicate tag arguments before Git execution.
  - `test_push_tags_validation_failure_precedes_staging`: asserts `check-ref-format` and `show-ref` errors halt processing before any `update-ref` staging or push invocation.
  - `test_push_tags_cli_preserves_objects_and_always_cleans_staging`: asserts JSON output (`{"pushed_tags": [...]}`) on success and exit code 2 on failure, validates preservation of exact raw and annotated objects, and verifies guarded transactional staging ref creation and cleanup (`start ... create/delete ... prepare ... commit`) on both success and rejection paths.
  - `test_git_transactions_use_lf_bytes_without_echoing_stderr`: asserts byte input with LF line endings is supplied to `update-ref --stdin` and verifies stderr suppression on execution failure.
  - `test_push_tags_rejects_changed_source_before_staging`: verifies tag-peeling mismatch rejection precedes staging writes.
- [x] **Approved commit verification tightening**:
  - `scripts/release/release_guard.py`: `push_tags` and `push-tags` CLI writer now mandate `--commit <SHA>`, peel the captured local tag object (`rev-parse ${oid}^{commit}`), and reject any mismatch before creating staging refs.
  - `.github/workflows/release-prepare.yml`, `.github/workflows/release-tag.yml`, and `.github/workflows/release-tag-spark.yml`: all 4 workflow call sites pass the authoritative reviewed commit (`--commit "$MERGED_SHA"`, `--commit "$RELEASE_COMMIT"`, or `--commit "$EXPECTED"`).
- [x] **Windows runner portability in workflow suite**:
  `scripts/release/test_release_tag_recovery.py` explicitly skips execution on native `win32` platform (`sys.platform == "win32"`), guiding execution to POSIX/WSL environments and avoiding bash.exe / CRLF false failures.
- [x] **Test execution**:
  - Targeted public suite: 88 passed in 2.02s (`test_release_guard.py` [44], `test_release_plan.py` [41], `test_release_ops.py` auth token [3]).
  - `test_release_tag_recovery.py`: 40 skipped cleanly on win32.
  - Formatting: Black 22.3.0 verified clean with zero formatting changes across all modified files.
- [ ] No real tags, packages, or remote writes were performed during this verification.
