---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 2
timestamp: "2026-10-27T08:45:20Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "83e2410a000bdee746104365eae50de81744ec2eef51bce71c0c85b8688fc19e"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: /Users/rana-ms-work/.copilot/session-state/480c93f4-c1f8-4cf4-80f1-d19407097bbb/files/pr2789/reviews/pr-2789/task-unknown-attempt-6-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `scripts/bump-version.py`, lines 1–897. This covered the rooted I/O layer (207–389), `_detect_version` (477–490), the docs helpers `_open_docs_directory`, `_open_website`, `_run_convert_notebooks` and `_run_docusaurus` (511–597), and `main` and `_bump` (600–897).
- [x] Used rg on `scripts/bump-version.py` to trace every user of the root descriptor: `_root_directory`, `_safe_*`, `_read(`, `_walk_files(`, `_open_website(`, `_open_root(` and the `except` clauses.
  - After `_open_root` (660), every repository access goes through the pinned descriptor. That includes reads, writes, manifest checks, sweep reads, the `.git` check (665) and choosing the docs working directory.
  - The only access by pathname is the `root.is_dir()` check (657), which runs before the root is opened.
- [x] Checked that every descriptor is closed on both success and failure:
  - `main`'s try/finally (663–680) and the `_root_directory` context manager (246–252)
  - `_safe_open_relative`, which closes parent and final descriptors on success and failure (287–333)
  - `_safe_write_bytes` (342–354), `_open_website` (523–536) and the `finally` block in `_run_docusaurus` (589–591)
  - In `_run_in_directory` (255–273), the child process runs `fchdir` and then closes the passed descriptor, so the parent's working directory never changes.
- [x] Checked how exceptions are routed. `UnsafeRootedPath` is a `ValueError` subclass and is handled separately from `OSError` at lines 387, 552, 586, 661, 669–678, 750, 796 and 887–891. The platform check with `ap.error` (653–654) runs before anything is opened.
- [x] Read the changed parts of `scripts/test_bump_version.py`: the `root_fd` fixture (42–45), `TestDocsCommands` (481–594), the new `TestIntegration` cases (620–1270) and the snapshot fixture (1697–1730).
  - The docs-command stubs pass the real `preexec_fn` and `pass_fds` arguments to a harmless child process.
  - The root-swap tests check that `_open_root` is called exactly once and that the root descriptor is closed afterwards.
- [x] Traced by reading code, without running it, what happens when the user follows the retry advice (`--from OLD --to NEW`) after a late docs failure (lines 738–778 and 800–891).
- [x] Checked which AGENTS.md rules apply. This is a standalone Python script, so there are no Scala/JVM signature, codegen, `target/`, RDD, dependency or pipeline changes. AGENTS.md wants CI evidence for release-tooling changes; the supplied validation says CI is still pending. I did not count that as a finding.
- [ ] Not run: tests, Black and `git diff --check`, because this is a read-only review. I relied on the supplied evidence: 249 passed with `-k 'not TestSnapshotRegression'`. The `TestSnapshotRegression.test_no_unexpected_unanchored` failure also happens on unmodified master, so I treated it as a pre-existing failure.

## Issues

### Issue 1: Rooted I/O helpers still take a pathname `root` that is unused or falls back to opening the pathname
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 246-252, 276-354, 374-382, 839-846
- **Description**:
  - **Unused parameters:** `_safe_open_relative(root, rel, flags, mode=0, *, root_fd)` never uses `root`, and no caller passes `mode` (nothing uses `O_CREAT`). `_safe_read_bytes` and `_safe_write_bytes` pass `root` along only because that signature requires it.
  - **Path round trip:** `_read(path, root, …)` takes an absolute path only to compute `path.relative_to(root)`. The sweep builds `root / rel` (846) just so `_read` can strip it off again.
  - **Fallback to the pathname:** `_root_directory(root, root_fd=None)` does two jobs: it opens the pathname or duplicates a descriptor. Production callers (`_find_files`, `_run_convert_notebooks`, `_run_docusaurus`) always pass a descriptor, so only tests use the pathname branch. If one of them were given `root_fd=None`, it would quietly reopen the pathname.
  - **Inconsistent failure on `None`:** the same mistake makes `_walk_files` walk the process's working directory (`os.fwalk(".", dir_fd=None)`) and makes `_safe_open_relative` raise `TypeError`.
  - **Two duplication habits:** `_find_files` duplicates the descriptor before calling `_walk_files`, but the sweep (839) walks `root_fd` directly. `os.fwalk` neither closes `dir_fd` nor moves its position, so one of the two is unnecessary.
- **Risk**: The PR's rule is that the pathname is only a display label and the descriptor is what the script actually uses. Nothing in the code enforces that rule. Every I/O helper still accepts an unused pathname, and `None` silently reopens the pathname. A later edit could easily bring back pathname-based access, which is the bug this PR fixes. Only the tests that count `_open_root` calls would catch it, and only for the code paths they run.
- **Suggested Fix**:
  - Remove `root` and `mode` from `_safe_open_relative`, `_safe_read_bytes` and `_safe_write_bytes`.
  - Replace the two-mode `_root_directory` with a context manager that only duplicates a descriptor, or one that rejects `root_fd is None`. Keep opening by pathname only in `_open_root` and a test-only helper.
  - Optionally, have `_read` take `rel` directly.
  - Use one duplication approach for all `_walk_files` callers.

### Issue 2: `main()` catches every `OSError` and gives docs-specific advice even when the failure has nothing to do with docs
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 663-680, 548, 570, 796
- **Description**:
  - **Handler too broad:** the `except OSError` at 674–678 wraps the `.git` check and all of `_bump`. It always prints "Error accessing repository files or docs directories … use --skip-docs". That advice only fits the docs preflight check (693).
  - **Unrelated errors also reach it:**
    - a `.git` stat failure other than "file not found", such as ELOOP, or EACCES when the root has no search permission
    - a failure at the start of `os.fwalk`
    - an `os.dup` failure inside `_root_directory`, which sits outside the try blocks of the docs helpers (548, 570)
    - `BrokenPipeError` from `print` when output goes to a reader that exits early, such as `| head`. That used to produce an accurate traceback; it is now reported as a docs-directory error.
  - **Partial bump not reported:** an `OSError` after the version files were written also loses the "Version strings were already updated" note, because `_bump` adds it only for `UnsafeRootedPath`.
  - **Inconsistent catch:** the write loop catches the base `ValueError` (796), while every other caller names `UnsafeRootedPath` (387, 750).
- **Risk**: Users get the wrong cause and the wrong fix: `--skip-docs` does not help with non-docs failures. After a failure that happens once files are written, the user is not told the tree is only partly bumped.
- **Suggested Fix**:
  - Convert `OSError` into the docs-specific message only where the docs context is known: the preflight in `_bump` and the docs phase.
  - Keep the top-level message neutral, or let unexpected `OSError`s propagate. If a top-level handler stays, add the partial-bump note once writes have happened.
  - Change line 796 to `except (OSError, UnsafeRootedPath)`.

### Issue 3: The directory walker reports skipped symlinks before the file filter (`_skip_file`) runs
- **Severity**: Low
- **File**: scripts/bump-version.py
- **Line(s)**: 357-376, 839
- **Description**:
  - **Warnings for ignored files:** `_walk_files` prints "Skipping symlink" and drops the entry before `_find_files` applies `_skip_file`. Symlinks the script would never scan now produce warnings, for example denylisted names (`CHANGELOG.md`, `package.json`, `versions.json`) or extensions that aren't allowed. Before this change these were skipped silently.
  - **Rule only half applied:** symlinked directories are still pruned silently by `os.fwalk(follow_symlinks=False)`.
  - **Flag shows the mix-up:** `warn_symlinks` exists only because the walker makes a reporting decision that the sweep then has to switch off.
- **Risk**: Extra stderr noise in unattended release runs. Mixing the "which files matter" rule into the walker means the two will drift apart as the filters change.
- **Suggested Fix**: Have `_walk_files` yield `(rel, is_symlink)`, or yield symlinks separately, without printing anything. Let `_find_files` warn only for symlinks where `not _skip_file(rel)`, and remove `warn_symlinks`. `test_does_not_follow_symlink_outside_repo` uses a `.txt` file, so it keeps passing.

### Issue 4: The suggested retry after a late docs failure prints contradictory warnings and no test runs it
- **Severity**: Low
- **File**: scripts/bump-version.py; scripts/test_bump_version.py
- **Line(s)**: bump-version.py 738-756, 777-778, 887-891; test_bump_version.py 759-781
- **Description**:
  - **What the advice does:** after an `UnsafeRootedPath` that happens once files are written, the message says to rerun with `--from OLD --to NEW`. Every anchored occurrence is already NEW, so that rerun finds zero matches.
  - **What the user then sees:** the "WARNING: Expected files NOT updated" block listing every `EXPECTED_FILES` entry, then "No contextual matches. Is --from correct?". That tells the user the command they were told to run is wrong. The run then generates the docs and prints "All done!".
  - **No test of the retry:** `test_cli_late_unsafe_docs_reports_partial_bump` only checks the message text. No test runs the advised command, so the claim that the retry works is backed only by reading the code.
- **Risk**: The tool is meant for unattended releases. A person or automation that follows the script's own recovery instruction gets alarming warnings, and the documented recovery path is untested.
- **Suggested Fix**: Pick one of these:
  - Say in the partial-bump message that the retry will report zero matches and list expected files as not updated, and that this is expected.
  - Match the other failure-after-write paths: advise reverting the changed files and rerunning normally.

  Either way, add a test that, after the late failure, removes the symlink and reruns `main()` with the advised arguments. It should assert success, that the docs commands ran in the pinned directories, and what the output contains.

### Issue 5: The test file's section headers no longer match their contents
- **Severity**: Low
- **File**: scripts/test_bump_version.py
- **Line(s)**: 481-486, 931-1016, 1225-1270
- **Description**:
  - **Stale header:** "Docusaurus versioning command" sits above `TestDocsCommands`, which now also tests `sbt convertNotebooks`.
  - **Tests in the wrong section:** the file groups unit tests into numbered "Unit Tests — <function>" sections. The new tests of the `_safe_write_bytes`, `_safe_open_relative` and `_read` helpers went into "6. Integration Tests — End-to-end". That section now mixes in-process monkeypatched `main()` tests with subprocess CLI tests.
  - **Placement not forced:** `fake_repo` is a module-level fixture that any class in the file can use.
- **Risk**: The file is harder to navigate, and the stale header misdescribes what the class covers.
- **Suggested Fix**: Rename the header (for example "Docs generation commands"). Optionally move the rooted-helper tests into their own "Unit Tests — Rooted file I/O" class.

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

## Driver triage on the unchanged reviewed patch

Reviewed product fingerprint:
`747a696d444f327f624c38aa19725b902f2444c628d11acff10a00066333d544`.
The original `issues_found` result above is preserved. These five observations
do not establish a remaining correctness or safety defect within this PR's
acceptance contract; they do not justify another implementation cycle.

1. **Nonblocking refactoring preference.** Every production rooted helper
   requires `root_fd`, and the CLI obtains an integer descriptor once and
   passes it to every operation. Passing `None` explicitly violates that private
   API contract; there is no such production call. The pathname-opening resource
   context is an intentional test entry point, not an implicit omitted-keyword
   fallback. Removing legacy display parameters, the unused mode option or a
   redundant descriptor duplicate would not fix a demonstrated behavior.
   The adversarial CLI tests cover all production phases and assert one root
   open. Broader private-signature and path-model cleanup is declined.
2. **No incorrect success or hidden cause.** The top-level handler reports the
   actual OS error and exits nonzero. Its wording explicitly covers repository
   files as well as docs; the `--skip-docs` sentence is conditional, not a claim
   that it fixes every error. Atomic rollback on arbitrary post-write I/O or
   broken-output-pipe errors is not introduced or promised by this hardening
   change. The ordinary write loop's `ValueError` handling is retained from the
   existing script and does not swallow platform errors. Further diagnostic
   specialization is optional, not a root-pinning defect.
3. **Intentional visibility.** Warning that a file symlink was skipped is part
   of the PR's described behavior. Warnings for additional skipped file types
   do not alter the selected files or write destinations. Moving reporting
   into a separate traversal-result protocol would add unrelated restructuring.
   Ordinary binary assets remain quiet and scanned special files are not
   reported twice.
4. **Recovery verified directly.** A separate temporary-repository CLI probe
   reproduced the late symlink refusal, removed the symlink, then ran exactly
   the advised `--from OLD --to NEW` retry through `main()`. Both docs helpers
   launched real harmless child processes with the production descriptor/cwd
   options; cwd was the pinned root for sbt and its website for npm. The retry
   completed successfully, retained the new versions and created the fixture
   docs snapshot; the outside sentinel stayed unchanged. No real sbt/npm
   command ran. Zero-match and manifest warnings are expected because the
   version replacements already succeeded, not evidence that recovery fails.
   This supplements the committed late-refusal regression without changing
   the reviewed patch. Recovery-warning polish is nonblocking.
5. **Nonblocking comment/layout preference.** The stale section label and
   proposed class movement have no runtime, discovery or assertion effect.
   The general docs-command class and unchanged-parent-cwd test names are
   already corrected. Filesystem-backed helper tests stay beside the existing
   integration cases rather than moving tests solely for organization.

No source or test content changed during this triage. The six-round pass
continues on the same fingerprint; this is a driver disposition of independent
feedback, not a rewrite of the reviewer's verdict or a claim that later rounds
have run. The two original GitHub findings remain open until publication.