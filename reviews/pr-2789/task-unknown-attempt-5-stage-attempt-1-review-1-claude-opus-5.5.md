---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 1
timestamp: "2026-10-27T07:41:19Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "323caec7e4f60bc1a81c51e302910523e3d07de337555280f70ed528432656c8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-5-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Unsupported platforms stop at startup.** In `scripts/bump-version.py:649-653`, `ap.error(...)` runs right after `parse_args()` and before the root is resolved, checked or opened, so it exits with code 2. `_supports_safe_dir_fd` (`:209-217`) checks for `os.open` `dir_fd` support, `fwalk`, `fchdir`, `O_NOFOLLOW` and `O_DIRECTORY`. Python only provides `os.fwalk` when `open`/`stat` support `dir_fd` and `scandir`/`stat` support fds, so the later `stat(dir_fd=…)` calls are covered too. `test_cli_rejects_unsupported_platform` (both auto-detect cases) checks exit code 2, the stderr message, unchanged file bytes and no "All done!".
- **Root stays pinned for every step.** `main` opens `root_fd` once (`:662`) with `O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC` and closes it in `finally` (`:673`). Every later filesystem access in `_bump` goes through `root_fd` or a dup of it:
  - version auto-detection: `_detect_version` (`:470`) → `_read` → `_safe_read_bytes`
  - discovery: `_find_files(root_fd=)`
  - analysis reads
  - expected-file checks: `_safe_open_relative`
  - writes: `_safe_write_bytes(r.rel)`
  - post-condition reads
  - sweep: `_walk_files(root_fd)` (`:830`) plus pinned `_read`
  - docs commands: `_root_directory(root, root_fd)`

  I searched for `.exists(`, `.is_dir(`, `read_text`, `read_bytes`, `write_bytes`, `cwd=`, `os.walk` and `os.chdir`. The only remaining path-based calls are the pre-open `root.is_dir()` and `.git` checks (`:656-658`) and the no-root `_read` fallback (`:370`), which only `TestSnapshotRegression` uses.
- [x] **Traversal and file opens refuse links and special files.**
  - `_walk_files` (`:343-357`) uses `os.fwalk(".", dir_fd=root_fd)` without following symlinks. fwalk's lstat/open/same-file check stops it entering symlinked directories, and file symlinks are skipped after a no-follow stat.
  - `_safe_open_relative` (`:266-318`) opens each parent directory with `O_NOFOLLOW|O_DIRECTORY`. It opens the final file with `O_NOFOLLOW|O_NONBLOCK`, so a FIFO cannot block.
  - It then requires a regular file (`:310`) and rejects writes to files with more than one hard link (`:312`), both before `ftruncate` (`:336`).
  - Existing symlink, parent-symlink, hard-link, directory and swap-race rejection is kept, and normal substitutions write the same bytes as before.
- [x] **File descriptors are closed on every path.**
  - `_safe_open_relative` closes parent directory descriptors in `finally` and closes the final descriptor on any validation exception (`except BaseException`).
  - `_safe_read_bytes` hands its descriptor to `fdopen`.
  - `_safe_write_bytes` uses `closefd=False` and closes in `finally`.
  - `_root_directory` (`:236-242`) closes its duplicate.
  - `_check_docs_directories` (`:516-531`) and `_run_docusaurus` (`:559-593`) close the website and docs descriptors on every return or raise, including the dry-run early return.
  - No shared directory offset is read: fwalk reopens `"."` relative to the root descriptor instead of reading from it.
- [x] **Docs commands get the pinned working directory without touching the caller's.** In `_run_in_directory` (`:245-263`), `preexec_fn` runs in the child after the fork and after stdio has been redirected. It calls `fchdir` and then closes the passed descriptor. On `OSError` it writes the cause to fd 2 and exits with 126. Python closes extra descriptors only after `preexec_fn` runs, so the descriptor does not leak into sbt/npm. The parent never changes or opens its own cwd. Both helpers catch launch `OSError`/`SubprocessError` and return failure (`:547`, `:583`).
- [x] **Symlinked website/docs paths are rejected before any write.**
  - The preflight check `_check_docs_directories(root_fd)` (`:686`) runs before scanning and writing, in both real and dry runs.
  - Both helpers check the paths again with no-follow opens.
  - `_open_docs_directory` (`:504-513`) turns every error except "not found" (symlinks, dangling symlinks, not-a-directory) into an `UnsafeRootedPath` that names the path.
  - If an unsafe path appears after the writes, `:878` re-raises with a partial-bump notice and `--from OLD --to NEW` retry advice. No manual sbt/npm commands are printed. `main` adds guidance not to bypass the checks (`:667-671`).
  - I traced the retry: with `--from OLD --to NEW`, the second run finds no remaining matches, both counts are 0 so the post-condition passes, and it reruns the docs steps.
- [x] **Tests.** I reviewed `scripts/test_bump_version.py:480-575` (`TestRunDocusaurus`), `:601-830` (CLI unsafe-docs, retry, late-unsafe, unreadable caller cwd, root swap before open, launch failure) and `:830-1180` (root-swap matrix, FIFO, binary, descriptor closure, symlink, hard-link and swap tests).
  - The root-swap matrix covers 4 stages, each with docs enabled and with `--skip-docs`. At the "open" stage the outside copy has a different config version. Sweep-only `.bin` files differ between the original and outside trees, which proves sweep reads are pinned. The tests also check that the root descriptor is closed and that the docs child's actual cwd is right.
- [x] **Ruled out (not reported).**
  - The path-based `.git` and `is_dir` checks run before the root is pinned. A symlink swap at that point is rejected by `O_NOFOLLOW` at `:662`. Swapping in a real directory only points the script at a directory the swapper already controls, so it does not allow writes outside the repository.
  - `pass_fds=(fd,)` makes the descriptor inheritable in the child, but the child closes it before exec, and every failure path exits before exec. Nothing leaks to sbt/npm.
  - Manual sbt/npm advice is still printed for ordinary tool failures (build or launch errors). The scope allows this, and `test_cli_reports_docs_launch_failure` covers it.
  - The sweep now prints warnings for permission errors and non-regular files that it used to skip silently. That is a change in output, not a defect; undecodable binary files are still skipped silently.
- [x] **Baseline failure is not caused by this diff.** The diff does not change `analyze`, the anchor patterns or `_skip_file`. The new `_find_files` returns the same files as before minus file symlinks, so it cannot add unanchored matches. That fits the stated pre-existing failure in `TestSnapshotRegression.test_no_unexpected_unanchored`.
- [ ] **Not executed by me:** tests, Black and `git diff --check`. I relied on the validation evidence you supplied. Remote CI and Azure validation are also not available, so this round does not establish merge readiness.