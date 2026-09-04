## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628\reviews\release-skill\task-2628-attempt-1-review-4-gpt-5.6-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] `python -m pytest -q scripts/test_bump_version.py scripts/release/test_release_matrix.py scripts/release/test_verify_release.py scripts/release/test_bump_bbcvhd.py scripts/release/test_release_workflows.py` completed with 308 tests passed.
- [x] `python -m black --check scripts/bump-version.py scripts/test_bump_version.py scripts/release/verify_release.py scripts/release/test_verify_release.py` reported all four files unchanged.
- [x] `python scripts/bump-version.py --to 1.1.4 --dry-run` exited 0 and printed both `sbt convertNotebooks` and `npm exec -- docusaurus docs:version 1.1.4`; `scripts/bump-version.py:115-131,227-239,400-427,578-582` also confirms that every `reviews` path component is excluded and that only the write path requires generated docs.
- [x] An isolated no-network `verify_release.run("1.2.3", "1", ["master"], ..., internal_upack_iteration={"master": 2}, scope="internal-only")` probe completed with seven Internal rows, zero OSS rows or OSS checker calls, and the expected `1.2.3-1-2` UPack identifier.
- [x] `scripts/release/verify_release.py:350-477,486-562` passes the explicit scope into `build_plan`, gates every OSS tag, Maven, PyPI, UPack, and wheel call on `plan.scope == "full"`, retains all Internal families, and returns exit 1 only for missing rows; the new CLI and row-filter tests cover scope forwarding and zero-patch rejection.
- [x] `scripts/release/release_matrix.py:162-310,319-392`, `az pipelines run --help`, and the live `SynapseML-OSS:/pipelines/SynapseML/SynapseML-Publish-Official.yml` definition agree on scope rules, target booleans, tag refs, independent rebuild variables, and the emitted space-separated `name=value` arguments.
- [x] `.github/workflows/release-prepare.yml:218-322`, `.github/workflows/release-tag.yml:41-185`, `.github/workflows/release-tag-spark.yml:20-197`, and `.github/workflows/release-notes.yml:42-152` support the documented merge-to-tag chain, case-insensitive Spark 4.0 opt-out boundary, derivative tag timing, branch cleanup, and public release gate. A live repository-variable lookup returned HTTP 403, matching the preflight command's fail-closed branch.
- [x] Git for Windows Bash ran `scripts/release/test_prev_tag.sh` successfully for seven historical primary-tag cases and exclusion of suffixed tags.

## Prior Findings

### Issue 1: Internal-only verification cannot select only Internal plan rows
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/SKILL.md`; `.github/skills/synapseml-release/references/preflight.md`
- **Lines**: `58-61`, `74-76`; `65-69`
- **Description**: The new instructions require an Internal-only patch to "verify only the Internal plan rows" and direct the operator to run `verify_release.py` on the in-scope rows. `verify_release.py` infers Internal-only matrix construction from a nonzero `--internal-patch`, but it still unconditionally adds the OSS GitHub tag set, every OSS Maven coordinate, PyPI, OSS UPack, and OSS pip rows. `--skip public` removes only OSS Maven and PyPI, while `--skip github` removes the OSS tags. Removing the remaining OSS UPack and pip rows requires `--skip upack`, `--skip pip`, or `--skip ado`, each of which also skips the corresponding Internal checks.
- **Risk**: The documented Internal-only procedure cannot produce the promised verification scope. A missing historical OSS feed row can keep an otherwise complete Internal hotfix at `INCOMPLETE`; trying to suppress that OSS row also suppresses the Internal artifact proof required for release completion.
- **Suggested fix**: Make `verify_release.py` scope-aware and omit OSS rows for an explicit Internal-only verification mode, then document the exact command. Alternatively, change the instructions to define OSS rows as mandatory existing-release prerequisites and remove the claim that verification covers only Internal rows.

## Resolution Log

### Issue 1

- **Status**: Resolved
- **What changed**: `verify_release.py` now accepts the same `--scope` values
  as `release_matrix.py`. With `--scope internal-only`, it builds and reports
  only Internal tags, Maven, UPack, and wheel rows. The skill, preflight
  reference, script usage, and release README now show the exact flag.
- **Why**: Internal hotfix completion no longer depends on unrelated historical
  OSS feed rows, and operators do not need broad skip flags that also hide
  Internal evidence.
- **How verified**: Three new tests cover row filtering, CLI scope forwarding,
  and rejection of a zero-patch Internal-only scope. The combined suite passed
  all 308 tests, Black passed, and a no-network CLI probe returned seven
  Internal rows and zero OSS rows.

## Re-review Result

The original finding is resolved. Explicit scope now propagates through the
CLI and matrix, Internal-only verification makes no OSS checks, and no
remaining or new detailed-correctness findings were identified.
