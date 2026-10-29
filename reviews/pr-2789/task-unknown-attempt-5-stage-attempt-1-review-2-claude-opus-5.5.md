---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 2
timestamp: "2026-10-27T07:52:49Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "810cb07461ae75cb10095e4ac0f1b80be45a6c5b99ec9964fc563dd97599db52"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-5-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read all of `scripts/bump-version.py` (lines 1–890) and confirmed that the working tree matches the supplied diff. This covers `_supports_safe_dir_fd`/`_root_open_flags` (209–229), `_root_directory` (236–242), `_run_in_directory` (245–263), `_safe_open_relative` (266–319), `_walk_files`/`_find_files`/`_read` (343–382), the docs helpers (504–593), `main`/`_bump` (649–686), the manifest (738–746), the writes (781–789), the sweep (828–842) and the docs phase (856–883).
- [x] I read the changed parts of `scripts/test_bump_version.py` (476–1174), the section layout (57, 122, 158, 354, 399, 476, 580, 601), and the unchanged `TestSnapshotRegression.live_results` fixture (1601–1631).
- [x] I listed every call to the rooted helpers (`_read(`, `_find_files(`, `_safe_*`, `_detect_version(`, the docs helpers, `_walk_files(`). Every CLI call in `_bump` passes `root_fd` (681, 686, 703, 710, 740, 777–778, 784, 800, 830, 837, 860, 869). Only the tests use the pathname fallback (496–574, 863–1171, 1614, 1619).
- [x] I listed every place that raises or catches `UnsafeRootedPath` (205, 222, 269, 273, 287, 306, 311, 313, 510, 520, 667, 741, 787, 878–879) to check how the error types are split.
- [x] I checked that file descriptors are closed on both success and failure: `main` finally (672–673), `_root_directory` (239–242), the intermediate and final descriptors in `_safe_open_relative` (290, 314–319), `_check_docs_directories` (523–531), the `website_fd` finally in `_run_docusaurus` (586–588), and the child-only close in `enter_directory` (251–255). I found no leaks.
- [x] I confirmed that the descriptor-based `preexec_fn` design never changes the caller's working directory, and that the docs-directory preflight (685–686) runs before any write in both real runs and dry runs.
- [x] I checked the AGENTS.md rules that apply to a standalone Python script (comment policy, Black pin, no RDD/JVM/codegen surface). The Scala, codegen and wrapper rules do not apply here.
- [ ] Commands, tests and Black were not run. This review is read-only. I relied on the supplied evidence (246 passed, 4 deselected; Black 22.3.0 passed per file; `git diff --check` passed).
- [ ] I did not re-check the baseline `TestSnapshotRegression.test_no_unexpected_unanchored` failure, because running commands is not allowed. I treated it as pre-existing and raised no finding against it.

## Issues

### Issue 1: `UnsafeRootedPath` is used for safety refusals and for ordinary I/O and platform errors, which produces misleading diagnostics
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 205–206, 220–225, 286–289, 305–306, 375–382, 504–531, 568–576, 650–653, 667–671, 738–746, 787
- **Description**: The same exception type is raised in four different situations:
  - real policy refusals: a symlink in the path, a non-regular file, a hard link, or a bad relative path;
  - every other `OSError` from the parent and final opens, including ENOENT and EACCES (286–289, 305–306);
  - a missing `website/` directory (519–522);
  - an unsupported platform (220–225).

  Callers then choose their message based on the type, which gives wrong results:
  - `_read` prints the class name. During auto-detection, a missing `website/docusaurus.config.js` therefore prints `WARNING: UnsafeRootedPath: … No such file or directory`.
  - `_open_docs_directory` adds "(symlinks are refused)" to every non-ENOENT error, including EACCES and ENOTDIR (509–513).
  - Running `--from X --to Y` without `--skip-docs` on a tree with no `website/` exits with "Correct the unsafe path and rerun this script without bypassing its checks" (667–671). Nothing is unsafe in that case, and `--skip-docs` is the legitimate option.
  - The manifest check reports an expected file that exists but was refused or is unreadable as "FILE MISSING" (740–746).

  The same "missing website" condition is treated as an unsafe-path error in `_check_docs_directories` but as a normal `return False` in `_run_docusaurus`, which repeats the website→docs open steps inline (568–576). The capability message text is duplicated (222–225 and 650–653). Some exception tuples are redundant: 376–379 list `FileNotFoundError` and `PermissionError` next to `OSError`, and 787 lists `UnsafeRootedPath` next to its base class `ValueError`.
- **Risk**: Operators see security-style errors and the wrong fix for routine missing-file or permission problems. Future handlers cannot tell "refused for safety" apart from "could not access". The duplicated text and the duplicated open steps can drift apart. There is no safety impact.
- **Suggested Fix**:
  - Raise `UnsafeRootedPath` only for policy refusals.
  - After an open fails, classify the cause with `os.stat(part, dir_fd=…, follow_symlinks=False)` rather than errno, because the O_NOFOLLOW errno differs by OS (ELOOP, ENOTDIR, EMLINK). Re-raise any other `OSError` unchanged, and catch `OSError` separately in `main()` with neutral wording.
  - Report a missing `website/` as missing and mention `--skip-docs`.
  - Use one module-level constant, or a dedicated capability exception, for the platform message.
  - Move the website/docs open steps into one helper, for example `_open_website(root_fd, *, require_docs)`, and use it from the preflight, `_run_convert_notebooks` and `_run_docusaurus`.
  - Narrow the exception tuples.

### Issue 2: Pinning depends on every caller passing `root_fd`, and the old unrooted `_read` path is now used only by the snapshot test
- **Severity**: Low
- **File**: scripts/bump-version.py; scripts/test_bump_version.py
- **Line(s)**: bump-version.py 237–238, 277, 360–370, 709–710, 800, 837; test_bump_version.py 1614–1619
- **Description**: Every rooted helper takes `root_fd=None`. When a caller leaves it out, the helper quietly reopens `root` by pathname for that call (238, 277). `_read` also keeps an unrooted branch, `path.read_bytes()` (370), which follows symlinks and blocks on FIFOs. All CLI calls pass `root_fd` today, so pinning depends on caller discipline. The only remaining unrooted `_read(fp)` call is in the unchanged `TestSnapshotRegression` fixture (1619), so that live-repo snapshot checks a read path the CLI no longer uses. Paths are also converted back and forth: `_walk_files` yields root-relative paths, `_find_files` makes them absolute (362), `_bump` makes them relative again (709), and `_read` does so once more (368). As a result, `FileResult.path` holds a pathname that would silently bypass pinning if future code read it directly.
- **Risk**: A single forgotten `root_fd=` brings back the root-swap race for that phase, and no test fails unless it swaps the root. The snapshot coverage no longer matches production behaviour.
- **Suggested Fix**:
  - Make `root_fd` a required keyword-only argument on the rooted helpers. Tests can get one through `bump._root_directory(tmp_path)` or a small fixture.
  - Remove the unrooted branch from `_read`.
  - Pass root-relative paths through scan, read and sweep.
  - Switch the snapshot fixture to the rooted API without changing its assertions.

### Issue 3: The repository check uses the pathname before the root is pinned
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 655–664
- **Description**: `root.is_dir()` and `(root / ".git").exists()` run against the pathname. `_open_root(root)` then pins whatever is at that path. O_NOFOLLOW rejects a symlink swapped in during this window, and `test_cli_rejects_root_swap_before_open` covers that case. A real directory renamed into place, however, gets pinned without ever being checked as a repository.
- **Risk**: The pinned root is not the object that was validated. This is the same check-then-use pattern the PR removes everywhere else. Practical impact is small because it needs write access to the root's parent directory.
- **Suggested Fix**: Keep the pathname checks for friendly messages if wanted, but run the deciding check after `_open_root`: `os.stat(".git", dir_fd=root_fd)`. `O_DIRECTORY` already guarantees the root is a directory. On failure, keep the current "has no .git" message.

### Issue 4: The sweep re-reads scanned files through `_read`, so the same warning is printed twice
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 703–710, 828–842, 375–382
- **Description**: The sweep now calls `_read(..., quiet_binary=True)`, which still prints a warning for every `OSError` or `UnsafeRootedPath`. Any file with an allowed extension that the scan already reported is reported again by the sweep. Examples are a FIFO `pipe.txt` ("Refusing non-regular file") or a file with mode 000. The original sweep skipped these errors silently. `warn_symlinks=False` shows the intent not to repeat scan warnings, but it covers only symlinks, and `quiet_binary=True` covers only decode errors. Re-reading scanned files can never produce a sweep hit: any old version string in a scanned file is either context-anchored, which puts the file in `changed` and excludes it from the sweep, or unanchored, which stops the script before any write.
- **Risk**: Duplicate stderr output from a script meant for unattended releases. Two separate boolean flags (`warn_symlinks`, `quiet_binary`) make the warning rules hard to follow. Every file in the repository is read a second time for no benefit.
- **Suggested Fix**: Record `scanned = {fp.relative_to(root) for fp in files}` and skip those paths in the sweep. Sweep-only files such as `sweep.bin` still exercise the pinned-read check in `test_cli_keeps_original_root_after_swap`. Alternatively, replace the two flags with one rule that suppresses repeated warnings. Assert on stderr in `test_cli_skips_fifo`.

### Issue 5: The new POSIX-only requirement is not documented in the usage text
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 2–16, 597–617, 649–653
- **Description**: Before this PR the script used only portable APIs (`os.walk`, `Path` I/O, `subprocess.run(cwd=…)`), so the version-string bump, for example `--skip-docs`, worked on any platform. It now exits with code 2 unless the platform provides `dir_fd`, `fwalk`, `fchdir`, `O_DIRECTORY` and `O_NOFOLLOW`, which rules out Windows. Neither the module docstring nor the `--help` epilog mentions this requirement.
- **Risk**: Release operators on unsupported hosts only find out at runtime.
- **Suggested Fix**: Add a one-line requirement note (POSIX, e.g. Linux or macOS; use WSL on Windows) to the module docstring and the argparse epilog.

### Issue 6: New tests are placed in the wrong classes and repeat the same helpers
- **Severity**: Low
- **File**: scripts/test_bump_version.py
- **Line(s)**: 476–576, 580–601, 856–922, 1132–1174, 490, 509–515, 720–731, 1008–1019, 994–999, 1104–1108, 560
- **Description**:
  - The suite is organised as numbered unit-test sections followed by "6. Integration Tests — End-to-end with temp directory". The new unit tests for private helpers (`_safe_write_bytes`, `_safe_open_relative`, `_read`; 856–922 and 1132–1174) were added to `TestIntegration`.
  - The convertNotebooks cases were added to `TestRunDocusaurus`.
  - The stub that replaces `subprocess.run` and checks the child's working directory is copied three times (509–515, 720–731, 1008–1019). `recording_open_root` is copied twice (994–999, 1104–1108).
  - The name `test_restores_cwd_when_docs_subprocess_fails` (560) describes the parent changing and restoring its working directory, which this design never does.
- **Risk**: The tests are harder to find and maintain. If only one copy of the working-directory stub is updated, the test proving the child runs in the pinned directory can quietly get weaker. The test name is misleading.
- **Suggested Fix**:
  - Move the helper tests into a dedicated class, for example `TestRootedIO`, next to the unit sections.
  - Rename or split `TestRunDocusaurus` into a docs-commands class.
  - Extract fixtures such as `cwd_probe_run` and `root_fd_recorder`.
  - Rename the test, for example to `test_parent_cwd_unchanged_when_docs_launch_fails`.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver dispositions after original review

This pass stopped at round two. Round one was clean on that earlier revision,
but source changes invalidate it; no rounds are carried into the next pass.
The user explicitly requested continuation and CI after the five-cycle stop.
Five selected regressions failed before the following scoped fixes:

1. Ordinary missing-file and permission errors retain their OS types and causes.
   No-follow/directory-type errors retain safety-specific diagnostics, accounting
   for ELOOP, ENOTDIR, EMLINK and EISDIR across supported POSIX systems. A second
   pathname stat was deliberately not added: a path can change again, and that
   result must not control the safety of the original failed operation. Platform
   failure has its own exception and shared message. Manifest diagnostics now
   distinguish missing files from unreadable/refused files. Missing website
   paths explain the valid `--skip-docs` option. `_open_website` centralizes docs
   preflight and tool checks.
2. Rooted helpers now require `root_fd`; they cannot silently reopen the root
   when a caller forgets the keyword. `_read` has no unrooted branch. Snapshot
   scan/read coverage now uses one retained descriptor. Absolute `FileResult`
   labels remain for existing diagnostics; all reads require the rooted API,
   so relabeling the complete analysis model is unnecessary.
3. The deciding `.git` check now uses the retained descriptor after the root
   opens. A real-directory replacement regression proves a non-repository
   replacement cannot receive writes.
4. The sweep skips already scanned paths. The FIFO regression requires exactly
   one refusal warning; sweep-only root-swap tests still prove pinned reads.
5. The module usage and CLI help now document POSIX APIs and WSL.
6. Renamed the docs test class to cover both commands and the failure test to
   describe unchanged parent cwd. Remaining placement/extraction suggestions
   are nonblocking style preferences: these filesystem-backed tests follow the
   existing integration-test grouping, and scenario-specific child probes have
   different generation and assertion behavior. A generic fixture or moving
   existing tests would add unrelated restructuring, not correctness proof.

Validation: 249 focused tests passed, four snapshot tests deselected; full suite
252 passed with the same independently reproduced upstream snapshot failure.
Black 22.3.0 checks for both files and `git diff --check` pass. The original
issues-found outcome remains preserved; a fresh complete pass is required.