---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-10-27T06:59:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "401c9e633493efe68893f08bed57224a69f620eb0688660ec4ffc298910a66af"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-3-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `scripts/bump-version.py`, lines 1–850. This included the new rooted helpers at lines 205–375: `_supports_safe_dir_fd`, `_root_open_flags`, `_open_root`, `_root_directory`, `_working_directory`, `_safe_open_relative`, `_safe_read_bytes`, `_safe_write_bytes`, `_walk_files` and `_find_files`. It also included `_read`, both docs helpers (lines 495–556), `main` (lines 612–633) and `_bump` (lines 636–840).
- [x] Checked every filesystem access after the root is pinned. I searched for `read_bytes`, `write_bytes`, `exists(`, `is_dir(`, `os.walk`, `cwd=` and `open(`. Every post-pin access uses `root_fd`: auto-detection (line 641), discovery, analysis reads, the manifest check, writes (line 742), postcondition reads, the sweep (line 788) and both docs helpers. Only three pathname accesses remain: the pre-pin `root.is_dir()` check (line 621), the `.git` check (line 623) and the `_read` fallback without a root (line 361), which only the tests use.
- [x] Checked the unsupported-platform gate. `ap.error` (lines 614–618) runs after argument parsing and before `resolve()` or any open. It exits with code 2 and the stderr message contains "require dir_fd". This holds for both `--from` and auto-detection, matching `test_cli_rejects_unsupported_platform`.
- [x] Checked descriptor ownership:
  - `_safe_open_relative` closes parent descriptors in `finally` (lines 310–311) and closes rejected non-regular or hard-linked files before raising.
  - The write helper closes in `finally`.
  - `_root_directory` duplicates and closes its descriptor.
  - `_working_directory` restores the working directory and closes the saved descriptor in a nested `finally`.
  - `main` closes `root_fd` on every `sys.exit` path.
- [x] Checked the no-follow behaviour:
  - Parent path parts open with `O_DIRECTORY|O_NOFOLLOW`; the final file opens with `O_NOFOLLOW|O_NONBLOCK`.
  - `fstat` must show a regular file, and writes are refused when `st_nlink > 1`.
  - `fwalk` does not follow symlinks and `lstat` skips symlinked files.
  - FIFOs cannot block.
  - A dangling `website/docs` symlink fails with ENOTDIR or ELOOP, not ENOENT, so the convert step's missing-`docs` allowance cannot let it through.
- [x] Checked that the docs subprocess runs in the pinned directory. `fchdir` plus `cwd="."` makes the child use the pinned directory. The child gets the real path from `getcwd()`, and bash ignores a stale `PWD` value. The pinned descriptors cannot leak because they are close-on-exec and `close_fds` is set.
- [x] Reviewed all new or changed tests in `scripts/test_bump_version.py`, lines 480–553 and 578–960. I looked at assertion strength, whether reused descriptor numbers could break the `fstat` checks, and whether pytest cleans up correctly when the fixture root has been swapped for a symlink.
- [x] Left out, under the stated scope: symlink checks on docusaurus outputs (`versioned_docs`, `versioned_sidebars`, `versions.json`), because external tool writes are explicitly out of scope. Also left out: renaming a real directory into place between the `.git` check and the first open, and the existing partial-write behaviour on write failure.
- [x] Did not count the known failure as caused by this diff. `TestSnapshotRegression.test_no_unexpected_unanchored` is reported as failing on unmodified master, and that test uses the `_find_files`/`_read` path without a root.
- [ ] Tests were not run. This was a read-only review, so I relied on the supplied results (235 passed, plus that one known failure).

## Issues

### Issue 1: Symlinked docs folders are reported as a normal tool failure, and the advice tells the user to run the unprotected commands
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 504–521, 536–550, 817–831
- **Description**: The pre-checks correctly refuse to run sbt or npm when `website` or `website/docs` is a symlink or not a directory (lines 506/509 and 539/540). But the refusal goes through the same `except OSError` used for launch failures, which causes several problems:
  - The convert step prints `ERROR: sbt convertNotebooks failed: [Errno 20] Not a directory: 'docs'`, even though sbt never ran.
  - Only the last path part (`docs`) is named.
  - On Linux the error is ENOTDIR ("Not a directory") because the directory check happens before the no-follow ELOOP check. That is confusing for something `ls website/docs` shows as a directory. macOS reports ELOOP.
  - `_bump` then prints "Fix the build issue, then run manually: sbt convertNotebooks / npm --prefix website exec -- docusaurus docs:version X" (lines 817–831). Those commands follow the symlink, so doing what the message says produces exactly the outside-the-repo output the check exists to block.
  - The docusaurus message also loses master's clear "website/docs/ does not exist after convertNotebooks" wording and now says `No such file or directory: 'docs'`.
- **Risk**: The script's own recovery advice undoes the new symlink check, and the user gets a misleading explanation after version strings have already been written. This is Low because the script itself never writes outside the repo; a person has to act.
- **Suggested Fix**:
  - Report pre-check refusals separately from tool failures, with the full relative path, for example: "website/docs is a symlink or not a real directory in the repository; refusing to run external docs tools".
  - In `_bump`, for that case, tell the user to replace it with a real directory and re-run, instead of printing "Fix the build issue … run manually".
  - Bring back the clear missing-`website/docs` wording.
  - Assert this message, and the absence of the manual-run advice, in a symlink regression test.

### Issue 2: `_working_directory` needs read permission on the caller's current directory, so docs steps now fail from unreadable directories
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 246–256 (open at 248); used at 516 and 546
- **Description**: The current directory is saved by opening `"."` read-only (`O_RDONLY|O_DIRECTORY|O_NOFOLLOW`), which needs read permission on the directory the user ran the script from. Master passed `cwd=str(root)` and never touched the parent's directory. Running from a directory that can be entered but not read (for example, `install -d -m 0311 /tmp/x && cd /tmp/x && python3 <repo>/scripts/bump-version.py --repo-root <repo> --to N`) now writes every version string, then convertNotebooks fails with `Permission denied: '.'` and the run exits 1 half-finished. The failure is only about restoring the caller's directory, which the root pinning does not depend on.
- **Risk**: A regression in an uncommon environment that leaves the bump partly complete, and the manual-run advice from Issue 1 then applies.
- **Suggested Fix**: Open the saved directory with `os.O_PATH` where available; on Linux, `fchdir` accepts `O_PATH` descriptors. If `os.open(".")` fails, fall back to saving `os.getcwd()` and restoring it with `os.chdir()`. Keep `fchdir(fd)` into the pinned directory unchanged. Add a test that runs from a mode-0311 temp directory (skipped when running as root) and checks that the stubbed subprocess runs in the pinned root and the caller's directory is restored.

### Issue 3: The root-swap test never swaps before auto-detection and never checks that the sweep stayed pinned
- **Severity**: Low
- **File**: scripts/test_bump_version.py
- **Line(s)**: 769–849 (stage map 788–792, swap hook 805–812, output assertion 842)
- **Description**: The acceptance criteria require auto-detection and sweep reads to stay pinned, but `test_cli_keeps_original_root_after_swap` would not catch a regression in either one.
  - **Auto-detection:** The "scan" stage wraps `_detect_version` and swaps only after it returns, so detection always reads the original tree. No case swaps between `_open_root` and detection.
  - **Sweep:** In the scan case, the outside tree's `outside.txt` contains `SynapseML v{V}`. A pathname-based sweep would only print a read-only `SWEEP WARNING … outside.txt` line, but the test only checks for `"3 replacements confirmed"` and unchanged outside bytes.

  The current code is correct when read: `scripts/bump-version.py` passes `root_fd` at line 641 and line 788. But switching either call back to pathname access would still pass the suite.
- **Risk**: Two operations the spec requires to be pinned could quietly regress.
- **Suggested Fix**:
  - Add an `"open"` stage that swaps inside `recording_open_root` right after the real open.
  - Give that outside tree a `website/docusaurus.config.js` with a different version, such as `1.0.0`. Pathname-based detection would then bump the wrong version and fail the `3 replacements confirmed` check.
  - In every stage, assert that `"SWEEP WARNING"` and `"outside.txt"` do not appear in the captured output.

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

## Driver disposition after this pass

This pass stopped after round one for the following scoped fixes. Original
findings are preserved; another full pass is required on the changed content.

| Finding | Disposition | Change and evidence |
|---|---|---|
| 1: unsafe manual recovery advice | Fixed locally | Directory safety refusals now raise `UnsafeRootedPath` with the full website-relative path. The CLI reports a corrective rerun instruction, not manual commands that bypass guards. Missing generated docs again have a website/docs-specific message. Both symlink cases are tested through the public CLI and both private tool helpers. |
| 2: caller cwd read permission | Fixed locally | Working-directory selection moved into the child using descriptor-based `fchdir`. The single-threaded standalone CLI never changes or opens the caller cwd. A real harmless subprocess regression runs the complete CLI from a mode-0311 caller directory and verifies both child working directories and the unchanged caller cwd. No pathname-restoration fallback is used. |
| 3: auto-detection and sweep proof | Fixed locally | A new swap stage runs immediately after the initial root open, with a different version in the outside config. Sweep-only files differ between trees; tests reject stale-version warnings, outside filenames, and any read-refusal warning. Actual subprocess cwd is tested rather than inferred from mock arguments. |

Seven new/strengthened safety and permission cases failed before the behavior
fixes. Afterward the focused suite passed with **240 passed, 4 deselected**.
Black 22.3.0 checks passed individually for both files; `git diff --check`
passed. The new child setup is specifically for the existing single-threaded
standalone CLI, not a promise of threaded library or subinterpreter support.