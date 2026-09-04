## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: sequential
- **Model**: claude-sonnet-5
- **Artifact**: `review/pr-2628/pr-2628-attempt-1-review-5-claude-sonnet-5.md`
- **Issues Found**: 0 (2 resolved across two re-review passes)
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Ran `pytest scripts\test_bump_version.py scripts\release\test_verify_release.py scripts\release\test_release_matrix.py scripts\release\test_bump_bbcvhd.py scripts\release\test_release_workflows.py` in one combined invocation in the current worktree: **311 passed**, 0 failed — matches the Resolution Log's "complete suite passed all 311 tests" claim for Issue 2's fix.
- [x] Diffed the current worktree against `HEAD` (`git diff HEAD -- scripts/release/test_verify_release.py`) and confirmed `test_full_scope_rejects_nonzero_patch` is present and unchanged from the Resolution Log's description: it calls `verify.main([..., "--internal-patch", "1", "--scope", "full"])` and asserts return code `2` and `"use --scope internal-only"` in stderr — this is exactly Issue 2's suggested fix, not a different or weaker test.
- [x] Ran only the scope/patch regression tests in isolation (`pytest scripts\release\test_verify_release.py -k "full_scope_rejects_nonzero_patch or infers_internal_only or internal_only_scope_omits_all_oss_rows or internal_only_scope_requires_nonzero_patch" -v`): **5 passed**, confirming both invalid-combination directions (`scope=internal-only, patch=0` and `scope=full, patch!=0`) and both valid-inference directions are independently asserted.
- [x] Live-probed the exact combination the new test exercises, read-only, from `scripts/release/`: `python -c "import verify_release as verify; print(verify.main(['--version','1.1.3','--internal-patch','1','--scope','full']))"` printed `error: a nonzero Internal patch is an Internal-only hotfix; use --scope internal-only` and returned `2`, matching the test's assertions exactly.
- [x] Diffed `scripts/release/verify_release.py` against `HEAD` and confirmed Issue 1's fix (`scope = scope or ("internal-only" if internal_patch != "0" else "full")` in `run()`, and the matching inference in `main()`) is still present and unmodified since the last pass — the default-inference regression is not reintroduced.
- [x] Diffed `scripts/bump-version.py` and `scripts/test_bump_version.py` against `HEAD` and confirmed both are byte-identical to the prior pass (docusaurus dry-run reorder, `reviews` denylist entry, and their four regression tests unchanged) — no new testing-coverage gap was introduced there since the last review.
- [x] Ran `python -m black --check scripts\bump-version.py scripts\test_bump_version.py scripts\release\verify_release.py scripts\release\test_verify_release.py`: all 4 files unchanged (no formatting drift).

## Prior Findings

### Issue 1 — Low (Resolved — see Resolution Log)
- **File**: `scripts/release/test_verify_release.py` (missing test); code path in `scripts/release/verify_release.py`
- **Lines**: `verify_release.py` L491-497 (new `--scope` argument, `default="full"`) and L548 (`scope=args.scope` forwarded into `run()`)
- **Description**: Before this PR, `run()` computed `scope` internally as `"internal-only" if internal_patch != "0" else "full"`, so a nonzero `--internal-patch` on the CLI could never reach `build_plan`'s `scope == "full" and internal_patch != "0"` rejection — that branch was reachable only by calling `build_plan`/`run` directly (as `test_release_matrix.py::test_rejects_unsafe_release_scope[1-full]` already does). This PR removes that inference and instead forwards the new `--scope` flag, which **defaults to `"full"`**. That makes a previously-working invocation, `verify_release.py --internal-patch 1` with no `--scope`, now fail at the CLI (`main()` catches the `ValueError` from `build_plan` and returns 2). The mirror case is tested (`test_internal_only_scope_requires_nonzero_patch`, `--scope internal-only` with `--internal-patch 0`), and `test_main_passes_internal_only_scope_to_run` proves the flag is plumbed through, but there is no `main()`-level test for the newly-reachable "default `--scope full` + nonzero `--internal-patch`" combination, i.e. no test asserts `verify.main(["--version", "1.1.3", "--internal-patch", "1"]) == 2` with the expected `"Internal-only hotfix; use --scope internal-only"` message.
- **Risk**: Low. The failure mode is a safe, loud CLI exit (code 2, clear message) rather than a silent misreport, and the underlying validation itself is exercised elsewhere. The gap is purely in *this* diff's CLI-level regression coverage: if `main()`'s default were ever accidentally changed back to auto-inferring scope (re-introducing the old, now-removed behavior), or if the `scope=args.scope` forwarding were dropped, no test in this file would catch it.
- **Suggested fix**: Add a `main()`-level test mirroring `test_internal_only_scope_requires_nonzero_patch`, e.g. `verify.main(["--version", "1.1.3", "--internal-patch", "1"])` (no `--scope`) asserting return code `2` and `"use --scope internal-only"` in stderr, to lock in the new default-`full`-scope CLI behavior end-to-end.

### Issue 2 — Low (Resolved — see Resolution Log)
- **File**: `scripts/release/test_verify_release.py` (missing test); code path in `scripts/release/verify_release.py`
- **Lines**: `verify_release.py` L490-498 (new `--scope` argument, `choices=RELEASE_SCOPES`, `default=None`) and L538-556 (`main()`'s scope resolution and the `try/except (ValueError, RuntimeError)` around `run()`); the rejection itself is raised by `release_matrix.build_plan` where `scope == "full" and internal_patch != "0"`.
- **Description**: Issue 1's fix restored default inference, so an *omitted* `--scope` with a nonzero `--internal-patch` now succeeds. It did not add coverage for the case the new `--scope` flag itself makes newly reachable: an **explicit** `--scope full` combined with a nonzero `--internal-patch`. Before this PR, `verify_release.py` had no user-facing `--scope` flag, so a caller could never *request* `full` scope while also supplying a nonzero patch — scope was always derived from the patch. Now that `--scope` is a real CLI argument, `verify.main(["--version", "1.1.3", "--internal-patch", "1", "--scope", "full"])` is a request a caller (or an automation script) can construct, and it correctly fails (verified live: exit code `2`, stderr `"a nonzero Internal patch is an Internal-only hotfix; use --scope internal-only"`). No test in `test_verify_release.py` asserts this; the only `main()`-level scope/patch rejection test, `test_internal_only_scope_requires_nonzero_patch`, covers only the opposite combination (`scope=internal-only`, `patch=0`).
- **Risk**: Low. The underlying validation is proven correct today (live probe, plus matrix-level coverage in `test_release_matrix.py::test_rejects_unsafe_release_scope[1-full]`), so this is not a live bug — it is a one-sided regression-test gap on `verify_release.py`'s own CLI surface. If a future change altered how `main()` resolves or forwards an explicit `--scope full` (for example, overriding it back to an inferred value, or catching the `ValueError` differently for this path), nothing in this file's suite would catch the regression.
- **Suggested fix**: Add a `main()`-level test mirroring `test_internal_only_scope_requires_nonzero_patch` for the opposite combination: `verify.main(["--version", "1.1.3", "--internal-patch", "1", "--scope", "full"])` asserting return code `2` and `"use --scope internal-only"` in stderr.

## Resolution Log

### Issue 1

- **Status**: Resolved
- **What changed**: `verify_release.py` now infers `internal-only` from a
  nonzero `--internal-patch` when `--scope` is omitted, preserving the previous
  CLI and direct `run()` behavior. An explicit scope still overrides inference
  and remains subject to the matrix's scope-patch validation. Two new tests
  cover direct and CLI inference.
- **Why**: Preserving the accepted invocation is safer than locking in a new
  rejection. The explicit flag remains available for clear release evidence,
  while older commands continue to select only Internal rows.
- **How verified**: The combined suite passed all 310 tests. After formatting,
  all 31 verification tests and Black checks passed.

### Issue 2

- **Status**: Resolved
- **What changed**: `test_full_scope_rejects_nonzero_patch` now exercises the
  explicit `--scope full` and nonzero `--internal-patch` combination through
  `main()`. It asserts exit code 2 and the corrective error message.
- **Why**: Both invalid scope-patch combinations exposed by the new CLI flag
  now have direct command-level regression coverage.
- **How verified**: The focused scope tests passed, the complete suite passed
  all 311 tests, and Black passed for all four changed Python files.

## Re-review Result

**CLEAN.** Issue 2's fix is genuinely present in the current diff, not merely
claimed in the Resolution Log: `test_full_scope_rejects_nonzero_patch` in
`scripts/release/test_verify_release.py` asserts `verify.main([..., "--scope",
"full"])` with a nonzero `--internal-patch` returns `2` with the corrective
`"use --scope internal-only"` message, and it passes. Issue 1's inference fix
remains intact and unregressed. The full combined suite (311 tests) and Black
formatting on all four changed Python files are clean, and re-checking the
theme's "invalid scope and patch combinations" and "CLI scope forwarding"
checklist items now finds both directions of the scope/patch validation
covered at the `main()` level. No remaining or new test-coverage findings were
identified across either re-review pass.
