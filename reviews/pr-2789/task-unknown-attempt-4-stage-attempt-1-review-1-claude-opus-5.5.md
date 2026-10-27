---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 1
timestamp: "2026-10-27T07:22:25Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ad3a3f0fa1181cffaca6e66add5743db0855f6a3837c3fd0a1fe850daa092d83"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-4-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `scripts/bump-version.py`, covering the helpers (205–378), version detection (464–476), docs helpers (498–578), and `main`/`_bump` (581–862). After `_open_root` at 647, every filesystem access uses `root_fd`. This covers detection (467), discovery (686), analysis (693), the manifest check (723), writes (767–769), post-condition checks (783), sweep walking and reads (813–820), and docs working directories (531, 567). No pathname-based access remains after the root is pinned.
- [x] Checked the unsupported-platform path. `_supports_safe_dir_fd()` (209–217) gates `main` at 634–638 through `ap.error`, which exits with code 2 and writes to stderr before any root access. `test_cli_rejects_unsupported_platform` (`scripts/test_bump_version.py`:823) covers both explicit `--from` and auto-detection.
- [x] Checked symlink, hard-link and non-regular-file rejection in `_safe_open_relative` (262–313):
  - each parent opens with `O_DIRECTORY|O_NOFOLLOW`;
  - the final path opens with `O_NOFOLLOW|O_NONBLOCK`;
  - an `S_ISREG` check follows;
  - writes also require `st_nlink == 1`.

  `_walk_files` (337–351) uses `os.fwalk(".", dir_fd=root_fd)` without following symlinks. Its descent guard compares the stat results before and after opening each directory.
- [x] Traced `_run_in_directory` (245–259) against the CPython `_posixsubprocess` child order: `pass_fds` become inheritable, then `preexec_fn` runs `fchdir` and `close`, then `close_fds`, then exec. The caller's cwd is never opened or changed, and no root descriptor survives exec.
- [x] Traced descriptor ownership:
  - `root_fd` closes in the `finally` block at 657–658;
  - the `_root_directory` dup is released at 237–242;
  - the docs directory descriptors close in `finally` blocks (526–529, 574–576);
  - the only gap found is Issue 3.
- [x] Reviewed the new tests in `scripts/test_bump_version.py` at 482–554 and 580–1110. These cover the swap matrix (853), a swap before the initial open, FIFOs, the binary sweep, an unreadable caller cwd, and docs symlink refusal. The docs failure tests (538, 698) replace `subprocess.run` and never trigger a real `preexec_fn` failure (Issue 2). The unsafe-docs CLI test (581) never checks that input files stay unchanged (Issue 1).
- [x] Baseline: the driver reports that `TestSnapshotRegression.test_no_unexpected_unanchored` also fails on unmodified master at the same SHA. I am not attributing it to this diff. I did not read any review artifacts, `reviews/` directories, or sibling outputs.
- [ ] Not executed: pytest and black. This reviewer has read-only tools; the conclusions rest on source inspection plus the supplied validation evidence.

## Issues

### Issue 1: Unsafe docs-directory refusal happens after tracked files are rewritten, and the "rerun" advice fails in the default auto-detect mode
- **Severity**: Medium
- **File**: scripts/bump-version.py (also scripts/test_bump_version.py)
- **Line(s)**: bump-version.py 650–656, 762–775, 836–848, 519–531, 553–555; test_bump_version.py 580–619
- **Description**: `_open_docs_directory` refusals are raised only inside `_run_convert_notebooks`/`_run_docusaurus`. In a real run, those calls come after the write loop (767), the post-condition check, the sweep, and the "Done! N files updated." line (836). No safety check of `website`/`website/docs` runs before `_safe_write_bytes`. Dry-run does catch the problem through `_run_docusaurus` (761), but only when nothing would be written anyway.

  Take a pre-existing `website/docs` symlink. This is a generated, git-ignored directory, so a local symlink there is plausible. The script then:
  1. rewrites `README.md`, `website/docusaurus.config.js` and the other matched files;
  2. exits 1 with "Correct the unsafe path and rerun this script without bypassing its checks."

  The handler never says that version strings were already changed. Rerunning the documented default command (`--to X`) after removing the link fails: `_detect_version` now reads X from the already-updated config, and `_validate_version_bump` exits with "current version is already X." Only two recoveries work: a rerun with an explicit `--from OLD`, which is noisy (every manifest file is flagged and "No contextual matches"), or `git checkout .` first. The CLI regression at 581 exercises this refusal but never asserts that `README.md` is unchanged, so the partial write goes unnoticed.
- **Risk**: The spec calls for "corrective rerun guidance" on safety refusals. Instead, the refusal leaves the repository half-bumped, and the advice it prints leads nowhere in the default mode. In release automation, that means a partly applied bump and a retry that cannot succeed.
- **Suggested Fix**:
  - Move the `website`/`website/docs` checks into a helper, e.g. `_check_docs_directories(root_fd, allow_missing_docs=True)`. When `not args.skip_docs`, call it in `_bump` before the write loop, for both dry-run and real runs. Keep the existing checks after sbt as a second layer of protection.
  - Extend the test at 581 for both linked paths. Add an auto-detect variant with `website/docs` linked. Assert that every input file is byte-identical after the refusal, and that a rerun succeeds once the link is removed.
  - Some refusals can still happen after writes, e.g. if sbt itself creates a symlink. For those, make the message state that version strings were already updated and give a command that works: `--from {old_v} --to {new_v}`, or `git checkout .` followed by a rerun.

### Issue 2: When `fchdir` fails in the child, the OS cause is lost, contrary to the stated acceptance
- **Severity**: Low
- **File**: scripts/bump-version.py (also scripts/test_bump_version.py)
- **Line(s)**: bump-version.py 249–251, 532–534, 568–570; test_bump_version.py 537–554, 698–719
- **Description**: If `os.fchdir(fd)` raises inside `preexec_fn`, CPython's `_posixsubprocess` discards the exception. The parent receives `subprocess.SubprocessError("Exception occurred in preexec_fn.")` with errno 0. Both handlers then print "…failed: Exception occurred in preexec_fn." with no errno, strerror or path. The acceptance criterion says "Both docs tools return failure with the OS cause if launch/cwd setup fails". The tests cited as evidence replace `subprocess.run` with functions that raise `OSError`/`FileNotFoundError` directly, so the real `preexec_fn` failure path is never tested.

  Reaching this failure is uncommon: it needs, for example, search permission removed or an EIO between opening the descriptor and the fork. When it does happen, the diagnostic gives the user nothing to act on.
- **Risk**: The cwd-setup failure message is useless for diagnosis, and the validation evidence claims more coverage than it has.
- **Suggested Fix**:
  - In `enter_directory`, catch `OSError` and write the error, including `e.strerror` and the errno, to fd 2. At `preexec_fn` time, fd 2 is already the captured stderr pipe. Then call `os._exit(126)`. The existing `r.returncode != 0` branches will print the cause from `r.stderr`.
  - Add a regression test that monkeypatches `os.fchdir` to raise `PermissionError(errno.EACCES, …)`; the forked child inherits the patch. Assert that `os.strerror(errno.EACCES)` appears in stderr and that the caller's cwd is unchanged.

### Issue 3: `_safe_open_relative` leaks `final_fd` when `os.fstat` raises
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 304–311
- **Description**: Once the final `os.open` succeeds, `os.fstat(final_fd)` (304) runs with no cleanup protecting `final_fd`. Only the two explicit rejection branches close it (306, 309). If `fstat` raises (for example `EIO`/`ESTALE` on NFS/FUSE, or `EOVERFLOW`), the `OSError` reaches callers that catch it and move on (`_read`, the manifest check, the write loop), and `final_fd` is never closed. This breaks the stated rule that descriptors must "close on success and failure."
- **Risk**: One descriptor leaks per failing file during the analysis and sweep loops. The practical impact in a short-lived CLI is small, but the descriptor-ownership rule is violated.
- **Suggested Fix**: Wrap the checks after the open so every path that does not return the descriptor closes it, e.g. `try: st = os.fstat(final_fd); …checks…; return final_fd` with `except BaseException: os.close(final_fd); raise`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver dispositions after original review

All three findings were confirmed. Seven selected regression cases failed
before the fixes; the expanded focused suite now passes 246 tests with four
snapshot tests deselected.

- Issue 1: Added descriptor-rooted docs preflight before version-file writes,
  retaining helper-level checks. Pre-existing unsafe directories leave input
  bytes unchanged in real and dry runs. Tests verify auto-detect retry after
  removing the symlink. Unsafe paths introduced during generation now report
  the partial bump and explicit `--from OLD --to NEW` recovery.
- Issue 2: Child cwd setup writes the actual OS error to captured stderr and
  exits 126. Both docs helpers are tested with a real harmless child that
  inherits an injected `fchdir` permission failure; parent cwd stays unchanged.
- Issue 3: Final descriptor validation now closes the descriptor on every
  exceptional exit. An injected `fstat` failure verifies closure.

Black 22.3.0 checks for both files and `git diff --check` pass. This attempt
remains an issues-found result; fixes require a new complete review pass.