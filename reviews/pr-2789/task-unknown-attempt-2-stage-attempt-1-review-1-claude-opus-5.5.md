---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 1
timestamp: "2026-10-27T06:44:08Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6668bfc4f499ecff606afa2573c5a8bf70846651a7980dd4e4b65334651f693e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-2-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `scripts/bump-version.py`: the helper layer (lines ~205-373), `_detect_version` (461), the two docs helpers (495-558), and `main`/`_bump` (561-835).
- [x] Followed every file access inside `_bump`. Each one goes through the pinned `root_fd`, either as an `os.dup` copy or as `dir_fd`:
  - version detection (`_detect_version` → `_read`)
  - file discovery (`_find_files`, line 351)
  - scan reads
  - expected-file check (695)
  - writes (739)
  - post-condition reads (755)
  - sweep walk and reads (785)
  - both docs helpers (504, 536)
  - No path-based `_open_root(root)` fallback can be reached from the CLI. This matches the `len(root_fds) == 1` assertion in the swap test.
- [x] Checked `_safe_open_relative` (259-310):
  - Each parent directory is opened with `O_DIRECTORY|O_NOFOLLOW` relative to the previous fd.
  - The final open uses `O_NOFOLLOW|O_NONBLOCK`, then an `fstat` check that it is a regular file (`S_ISREG`).
  - For writes, files with more than one hard link are rejected before `ftruncate` runs in `_safe_write_bytes` (319-331). No open uses `O_TRUNC`, so a symlink, hard link, FIFO or directory is never truncated.
- [x] Checked that file descriptors are always closed:
  - `main` closes the root fd in `finally`, including on `SystemExit` (626-630).
  - `_root_directory` and `_safe_open_relative` close their copies in `finally`.
  - `website_fd` and `docs_fd` are closed on every path.
  - `_working_directory` (245-256) restores the old cwd and closes `previous_fd` in nested `finally` blocks.
- [x] Checked the unsupported-platform gate (614-618). `ap.error` runs after `parse_args` and before any path resolution or file read, and exits with code 2. `test_cli_rejects_unsupported_platform` (`scripts/test_bump_version.py:673`) covers both `--from` and auto-detect, and checks that files are unchanged and no success message prints.
- [x] Checked how the standard library's `os.fwalk(".", dir_fd=root_fd)` behaves inside `_walk_files` (334-348):
  - The top directory is opened relative to the pinned fd.
  - Symlinked directories are listed but never entered.
  - File symlinks are filtered out by the `follow_symlinks=False` stat.
- [x] Reviewed the new tests in `scripts/test_bump_version.py`:
  - `test_cli_keeps_original_root_after_swap` (703): swaps at scan, analysis and write, with docs on and off. Checks the outside tree is unchanged, subprocesses run in the moved root, and the fd is closed.
  - `test_rejects_symlinked_docs_directories` (517)
  - `test_restores_cwd_when_docs_subprocess_fails` (536)
  - the FIFO, binary-file, hard-link, symlink-swap and validation-error fd tests
- [ ] Not run by me: pytest, Black and `git diff --check`. This was a read-only review, so I relied on the supplied results (232 passed, 4 deselected). The `TestSnapshotRegression.test_no_unexpected_unanchored` failure was reported as already failing on upstream master; I did not reproduce it.
- [ ] Out of scope: sandboxing sbt/npm, and files those tools write under `website/` such as `versioned_docs`. The review plan excludes these.

## Issues

### Issue 1: `_run_convert_notebooks` crashes with a traceback when sbt can't be launched, after files are already rewritten
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 519-520 (compare 546-550 in `_run_docusaurus`; caller at 814-819)
- **Description**: The new `_run_docusaurus` puts `_working_directory(...)` and `subprocess.run(...)` inside `try/except OSError` and returns `False` on failure. `_run_convert_notebooks` runs the same two lines outside its `try/except`, so these errors escape `_run_convert_notebooks`, `_bump` and `main` uncaught:
  - `FileNotFoundError` when `sbt` is not on `PATH`.
  - Errors from the new `os.open(".")` or `os.fchdir` calls in `_working_directory`, for example `EACCES` when the directory the script was started from is not readable.

  The missing-`sbt` traceback already happened before this PR. What's new is the mismatch: the PR now handles this failure for docusaurus but not for convertNotebooks, and the cwd-pinning code adds new ways for it to fail. `test_restores_cwd_when_docs_subprocess_fails` (test file line 536) only tests `_run_docusaurus`.
- **Risk**: A live run without `--skip-docs` on a machine without `sbt` rewrites the version strings first (line 739) and then crashes with a traceback. The recovery instructions in `main` (lines 815-818, "Version strings updated but convertNotebooks failed… run manually") never print. The cwd is still restored and no files outside the repo are touched, so this is a robustness problem, not a security one.
- **Suggested Fix**: Wrap lines 519-520 the same way the docusaurus helper does:
  ```python
  try:
      with _working_directory(fd):
          r = subprocess.run(cmd, cwd=".", capture_output=True, text=True)
  except OSError as e:
      print(f"ERROR: Cannot run sbt convertNotebooks: {e}", file=sys.stderr)
      return False
  ```
  Then run `test_restores_cwd_when_docs_subprocess_fails` against both helpers, checking that each returns `False` and `Path.cwd()` is restored.

### Issue 2: Opening the repo root at startup has no error handling, so a symlink swap just before the open, or an unreadable root, gives a raw traceback
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 620-630
- **Description**: `main` checks the resolved root by pathname (`root.is_dir()` and `(root / ".git").exists()`, lines 621-624). It then calls `_open_root(root)` at line 626 with no error handling. That open uses `O_DIRECTORY|O_NOFOLLOW` and needs read permission, so it raises `OSError` in two cases:
  - `ELOOP`: the root path is replaced by a symlink between the checks and the open.
  - `EACCES`: the root can be traversed but not read. Both pathname checks pass, but the read-only open fails.

  Either way the user gets a traceback instead of the clean `Error:` exit that every other startup check uses. Nothing is read or written, so the failure is safe. However, no test covers this window: the earliest swap in `test_cli_keeps_original_root_after_swap` happens after `_detect_version`, when the root fd is already open. If `O_NOFOLLOW` were later dropped from the root open, no test would notice.
- **Risk**: The race this PR guards against shows up as a crash rather than a clear error. The safe failure for a swap before the root is opened is not locked in by any test.
- **Suggested Fix**:
  ```python
  try:
      root_fd = _open_root(root)
  except OSError as e:
      sys.exit(f"Error: cannot open repository root '{root}' safely: {e}")
  ```
  Add a CLI test that wraps `bump._open_root` so it swaps `fake_repo` for a symlink to an outside tree right before calling the real function. Assert that it exits with the new message, the outside tree is unchanged, and no success message prints.

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

## Driver disposition after this pass

Both findings were confirmed with failing regressions and fixed locally. This
pass ended after round one; a fresh complete pass is required on the changed
content. Original feedback above is preserved and is not clean sign-off.

| Finding | Disposition | Fix and evidence |
|---|---|---|
| 1: sbt launch/cwd error | Fixed locally | The pinned working-directory/subprocess block is now inside the helper's `OSError` handler. Both tool variants restore cwd and return `False`; a public CLI test verifies exit 1, the OS error, and recovery guidance after rewriting versions. |
| 2: initial root-open error | Fixed locally | Startup catches root-open `OSError` and exits with an actionable error. A test swaps the root for a symlink immediately before the real no-follow open, verifies both trees remain unchanged, and rejects success output. |

Before the fixes, the selected regression command produced 3 failures and 1
pass. Afterward, `python3 -m pytest -q scripts/test_bump_version.py -k 'not
TestSnapshotRegression'` passed with **235 passed, 4 deselected**. Black 22.3.0
checks passed for both files individually; `git diff --check` passed.

The effective decision remains Medium, six independent sequential lenses using
`claude-opus-5.5` at maximum reasoning on one frozen patch. This was the second
pre-commit pass; it is retained as finding-resolution evidence only.