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
