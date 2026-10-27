---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-10-27T06:26:20Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0419c40539174e06ec81ab2dc7dadae7e8fff98d7f64d1c46877ab7a8e5edc05"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-1-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `scripts/bump-version.py`. This covered `_is_within_root` and the capability helpers (204–238), `_safe_open_relative` (241–290), the read and write wrappers (293–311), `_find_files` (314–328), `_read` (331–345), `_detect_version` (433–444), the docs helpers (466–510), and `main`/`_bump` (563–792).
- [x] Followed root-descriptor ownership through the code. `main` opens the root at 579 and closes it in `finally` at 583. `_safe_open_relative` duplicates it at 252, swaps parent descriptors at 265–266, and closes in `finally` at 289–290. `_safe_write_bytes` closes in `finally`, and `_safe_read_bytes` closes through `fdopen`. All of these close on both success and `SystemExit`.
- [x] Checked the unsupported-platform path. `_supports_safe_dir_fd` (216–222) runs before any filesystem access, and `ap.error` (567–571) exits with code 2 and the "require dir_fd" message. Both parameter cases of `test_cli_rejects_unsupported_platform` (`scripts/test_bump_version.py` 631–658) match the spec.
- [x] Checked that symlink, parent-symlink, hard-link and directory rejection still work through `O_NOFOLLOW`/`O_DIRECTORY` per component, `S_ISREG`, and `st_nlink > 1` on write (268–287). Tests at `scripts/test_bump_version.py` 585–630 and 753–794 cover these.
- [x] Checked the scope of `test_cli_keeps_original_root_after_swap` (`scripts/test_bump_version.py` 660–719). It hard-codes `--skip-docs` (703). It swaps only after the first `analyze`/`apply` call (685–692), which is after file enumeration. Its outside tree uses the same relative names as the repo (664–669).
- [x] Compared the sweep before and after the change. The old sweep skipped decode and permission errors silently. The new one routes through `_read`, which prints a warning (744–753 and 344).
- [x] Read `TestRunDocusaurus` (`scripts/test_bump_version.py` 480–512) and the `TestSnapshotRegression` fixture (1222–1270) to see how existing tests use the changed helpers.
- [ ] Test execution: not done. This review was read-only with no command execution, so I relied on the supplied evidence (220 passed, 4 deselected; baseline snapshot failure) and did not re-check it.
- [ ] Prior or sibling review artifacts: not read, as the independence rules require.

## Issues

### Issue 1: Docs-generation subprocesses still run against the unpinned root pathname
- **Severity**: Medium
- **File**: scripts/bump-version.py; scripts/test_bump_version.py
- **Line(s)**: bump-version.py 475, 489–492, 506 (called from `_bump` at 774 and 780); test_bump_version.py 703
- **Description**: By default (without `--skip-docs`), `_bump` runs two tools after the write, postcondition and sweep stages:
  - `sbt convertNotebooks` with `cwd=str(root)`
  - `npm exec -- docusaurus docs:version` with `cwd=str(root / "website")`

  `root_fd` is still open at that point but is not used. `subprocess` resolves `cwd` by pathname and follows symlinks, and `docs_dir.exists()` is also checked by path. Suppose the root pathname is swapped for a symlink after analysis, which is the live finding being fixed. Then sbt loads and runs the swapped directory's build definition and creates or updates files there (`project/`, `target/`, generated `website/docs/`). Docusaurus writes its versioned snapshot output there too. The pinned repo gets no docs, yet the run can still print "All done!".

  There is also a non-race variant. Because `website` and `website/docs` are resolved by path, an in-repo `website` symlink sends docs output outside the root. That is inconsistent with the parent-symlink rejection now applied to file I/O. The new swap regression test forces `--skip-docs`, so none of this is tested.
- **Risk**: In the script's default mode, this breaks the requirement that swapping the root pathname for a symlink after analysis must not change outside files. It also runs a build definition chosen by whoever controls the swap, as the release user.
- **Suggested Fix**: Take the child's working directory from the pinned descriptor:
  - Open `website` relative to `root_fd` with `O_DIRECTORY | O_NOFOLLOW`, and check `docs` with `os.stat(..., dir_fd=..., follow_symlinks=False)`.
  - Run each tool with `subprocess.run` and no path `cwd`, after `os.fchdir(pinned_fd)`. Restore the previous cwd from a saved directory fd in `finally`. Alternatively, use `preexec_fn=lambda: os.fchdir(fd)`; the script is single-threaded.
  - At minimum, fail closed before spawning if `os.lstat(root)` is not a directory with the same `(st_dev, st_ino)` as `os.fstat(root_fd)`.

  Add a docs-enabled variant of the swap test that stubs `subprocess.run` and asserts the child's cwd is the moved root, not the outside tree. Update `TestRunDocusaurus.test_uses_local_npm_binary` to match.

### Issue 2: File enumeration in the scan and the sweep still walks the root pathname
- **Severity**: Low
- **File**: scripts/bump-version.py; scripts/test_bump_version.py
- **Line(s)**: bump-version.py 316, 326, 611, 647, 732, 737; test_bump_version.py 664–669, 685–692
- **Description**: Both `_find_files(root)` and the post-write sweep list files with `os.walk(root)`. `os.walk` follows a symlinked top directory. `_is_within_root` resolves both `fp` and `root` through the same symlink, so it still returns True. Only the per-file opens are pinned.

  If the root is swapped after `_open_root` or auto-detection but before `_find_files` (611), the scan uses the swapped tree's file names:
  - Names missing from the pinned tree are skipped with warnings.
  - Names that exist only in the pinned tree are never analyzed.
  - The sweep looks at the wrong tree as well.

  With an empty swap target, the run reports zero replacements and "Post-condition verified", then prints "All done!" and exits 0 without bumping the pinned repo. No outside file is written, but this contradicts "pinned throughout … analysis … and sweep reads". The manifest check at 647 also uses a path-based `exists()`. The regression test cannot catch this: it swaps only after enumeration, and its outside tree mirrors the repo's relative names.
- **Risk**: Under the targeted race, a partial or no-op bump is reported as success, and the sweep results don't describe the repository that was actually modified.
- **Suggested Fix**:
  - Enumerate in both places with `os.fwalk(".", dir_fd=root_fd)` (default `follow_symlinks=False`).
  - Build `rel` from the relative dirpath it yields.
  - Detect symlinks with `os.stat(name, dir_fd=dirfd, follow_symlinks=False)`.
  - Add `os.fwalk` to `_supports_safe_dir_fd` and drop the path-based `_is_within_root`.

  Add a swap stage right after `_detect_version` returns, using an outside tree whose file names differ from the repo's. Assert that the pinned repo is fully bumped and the outside tree is unchanged.

### Issue 3: The sweep now prints a warning for every non-UTF-8 file
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 331–345 (344), 744–753
- **Description**: The sweep reads every non-symlink file in non-denylisted directories, whatever its extension. On master it skipped undecodable or unreadable files silently. Now it calls `_read`, which prints `WARNING: UnicodeDecodeError: <path>` to stderr for each binary asset (images, archives, model files, and so on), and `WARNING: UnsafeRootedPath` for special files. `_read` already catches everything in the sweep's surrounding `except` tuple, so that handler (748–753) can never run.
- **Risk**: Every successful live run on a repo with binary assets fills the unattended-release log with spurious warnings, which buries the ones that matter, such as "Skipping symlink" and real unreadable-file warnings.
- **Suggested Fix**: In the sweep, call `_safe_read_bytes(root, rel, root_fd=root_fd)` directly and `continue` silently on `(UnsafeRootedPath, OSError, UnicodeDecodeError)`. Alternatively, add a `quiet` flag to `_read`. Add a CLI test with a non-UTF-8 file that asserts the sweep prints no warning for it.

### Issue 4: Every rooted-open failure is relabelled "unsafe" and loses its errno
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 261–264, 278–279, 344, 436–440, 690
- **Description**: `_safe_open_relative` turns every `OSError` from the parent or final open (EACCES, ENOENT, EROFS, EPERM, ELOOP) into `UnsafeRootedPath("Refusing unsafe … path")`. The original errno only survives in the exception chain, which is never printed. As a result:
  - A read-only file or read-only filesystem is reported as `ERROR writing X: Refusing unsafe final path: X`.
  - `_read` prints only the class name, so missing or permission-denied files show up as `WARNING: UnsafeRootedPath`. Its separate `FileNotFoundError`/`PermissionError` cases can no longer be reached for rooted reads.
  - `_detect_version` then reports the config as "not found" even when it exists but was rejected, for example as a symlink, or could not be read.
- **Risk**: Operators can't tell a real symlink or hard-link safety refusal from an ordinary permission, read-only filesystem or missing-file error. Both directions mislead triage during an unattended release.
- **Suggested Fix**:
  - Keep the cause in the message, e.g. `raise UnsafeRootedPath(f"Refusing unsafe final path: {rel} ({e.strerror})") from e`. Alternatively, wrap only `ELOOP`/`EMLINK`/`ENOTDIR` and re-raise other errors unchanged.
  - Print `{e}` in `_read`, not just the class name.
  - Change the `_detect_version` message to "not found or unreadable".

### Issue 5: The non-regular-file check runs after an open that can block, so a FIFO hangs the run
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 268–284
- **Description**: The final path component is opened with `flags | O_NOFOLLOW | O_CLOEXEC`, and only then checked with `fstat`/`S_ISREG`. Opening a FIFO with `O_RDONLY` blocks until a writer appears, so the `S_ISREG` rejection never runs. The sweep reads every non-symlink file regardless of extension, and the scan reads any file with an allowed name. So a FIFO in any scanned directory hangs the bump indefinitely. This is not a regression, since master's `read_bytes()` also blocked. But the new check does not reject the most plausible non-regular file type under the local-attacker model this PR hardens against.
- **Risk**: An unattended release job hangs (denial of service) instead of skipping or failing.
- **Suggested Fix**: Add `os.O_NONBLOCK` to `final_flags` when it exists. It has no effect on regular files. A FIFO read-open then returns immediately and is rejected by `S_ISREG`, and a FIFO write-open with no reader fails with ENXIO. Add an `os.mkfifo` test (skipped where unavailable) asserting that `_safe_read_bytes` raises `UnsafeRootedPath` promptly.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver disposition after this pass

This pass was stopped after round one because the confirmed findings required
source changes. It is not a clean pre-commit pass. Original findings above are
preserved; a complete new six-lens pass is required on the updated patch.

| Finding | Disposition | Change and evidence |
|---|---|---|
| 1: docs subprocess cwd | Fixed locally | Both tools use saved/restored descriptor-selected working directories. Existing website/docs symlinks are rejected. Docs-enabled root-swap tests assert the actual subprocess cwd and unchanged outside tree. |
| 2: enumeration | Fixed locally | Both discovery and sweep use `os.fwalk` relative to the retained root descriptor; manifest existence checks also open relative to it. A swap immediately after auto-detection, with different outside filenames, proves the original tree is fully updated. |
| 3: binary sweep warnings | Fixed locally | Sweep decode failures are intentionally quiet; other I/O failures remain explicit. A public CLI test includes a non-UTF-8 image. |
| 4: lost open errors | Fixed locally | Wrapped parent/final open failures retain the original OS error text; `_read` prints the exception and auto-detection describes unreadability. A regression checks missing-file cause reporting. |
| 5: FIFO blocking | Fixed locally | Final opens use `O_NONBLOCK` where available so the existing regular-file validation can reject FIFOs without hanging. A timeout-bounded public CLI test includes a FIFO. |

Validation after these changes: `python3 -m pytest -q
scripts/test_bump_version.py -k 'not TestSnapshotRegression'` passed with 232
tests and 4 deselected. Black 22.3.0 checks passed individually for both script
files; `git diff --check` passed. The unrelated snapshot failure was reproduced
on unmodified upstream. Source changes remain unpublished at this disposition.

### Effective review decision

The primary agent selected **Medium** for a bounded change in two standalone
Python scripts with moderate descriptor/error-path complexity. Repository policy
requires all six lenses sequentially on one frozen patch. Primary model:
`gpt-6.1-sol`; complementary reviewer selected from the live catalog:
`claude-opus-5.5` at `max` reasoning. This first pass completed one lens with
findings and was invalidated by the resulting fixes. The driver requires a
fresh full pass before committing; this artifact is retained as resolution
evidence, not sign-off.