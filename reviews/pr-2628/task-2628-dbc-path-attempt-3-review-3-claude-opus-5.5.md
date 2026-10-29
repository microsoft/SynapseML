# PR 2628 pre-commit review: round 3, edge cases and robustness

## Review Summary

| Field | Value |
| --- | --- |
| Round and theme | 3: edge cases and robustness (error handling, boundary conditions, concurrency, failure modes) |
| Model | claude-opus-5.5 |
| Mode | Sequential direct-contract |
| HEAD | 3ce916902329c20d5c37de43d7c43d805f28e748, plus the staged archive and verifier fixes |
| Merge base | 9d51ad1acd765b3246bec15517abf6b62e1d5d70 |
| Frozen patch fingerprint | af03ceb1771bb8289ab8ecc6cffd61bc681f31c204c7bad346dbbcc935a7416e |
| Full diff SHA-256 | d10e99a786fb3baf97a1da2446afa383ea6cb9ac3b59bd119808c99b950b0e9d |
| Scope | The complete 55-path merge-base-to-index diff, plus read-only inspection of surrounding source |
| Issue count | 2 (1 medium, 1 low) |
| Verdict | ISSUES_FOUND |

The pending ZIP path correction is sound. Every member name is checked for the versioned root prefix, backslashes and `..` components before directory entries are skipped, and root or nested directory entries are accepted. Error handling in the DBC, verifier, guard and ledger paths fails closed. Both findings are in the new DBC gate. Issue 1 is where the gate runs relative to an irreversible publication. Issue 2 is a false failure on Windows in the new path tests.

Evidence limitations: this is a static review. I did not run tests, builds, the Azure pipeline, or any Databricks, Azure Storage, PyPI or GitHub calls. I did not check Databricks import and export behavior; the unit tests model it with an in-repo fake. The Windows behavior in Issue 2 is derived from CPython's `zipfile` member-name normalization and was not executed.

## Evidence Checklist

| Area | Evidence reviewed | Result |
| --- | --- | --- |
| ZIP member paths (pending fix) | `validate_archive` in `scripts/release/release_dbc.py:170-230`. The prefix, backslash and `..` checks (lines 189-193) run before the directory skip (line 194). The inventory must match exactly when notebooks are supplied. Tests cover 8 unsafe and 3 valid directory entries. | Pass on Linux; see Issue 2 for Windows |
| Empty and boundary inputs | Empty or over-10 MiB archives, more than 10,000 entries, over 100 MiB expanded, duplicate names, CRC failure, malformed `NotebookV1` objects and results, an empty notebook set, non-Python notebooks, attachments and credential-like cell text | Rejected with `ValueError` |
| Network and IO failures | The public lookup treats 404 as absent. Other HTTP, transport and truncated-read errors become sanitized errors. Redirects are refused. Databricks errors name the operation without the response body or token. Timeouts are 60, 120 and 180 seconds. | Fails closed |
| Partial upload and retries | Uploads never overwrite. Ambiguous upload errors and timeouts are reconciled by comparing anonymous downloads by bytes, plan and source digest. An existing archive is reused only for the same plan and source, after a fresh round trip. Staging directories and pipeline artifact names are per attempt. | Idempotent for the same approved source |
| Concurrency | The `/Shared` staging folder is UUID-scoped. Concurrent uploaders cannot both succeed with different bytes. The ledger uses `O_EXCL` locks and a plan claim. Tags are pushed atomically through a private ref namespace. | No race found |
| Resource cleanup | A `finally` block recursively deletes only the owned staging folder. A cleanup failure is reported without masking the primary error. | Pass |
| Verifier changes | Uses the Blob Maven endpoint. Unbound `--skip ado,internal` checks build no private profile and stay non-strict. Strict PyPI requires exactly one non-yanked wheel with the expected filename on `files.pythonhosted.org` and a reachable download. HEAD falls back to GET only on 405 or 501. | Pass |
| Evidence and release notes | Schema-4 inventories require DBC rows. Release notes recheck the live DBC hash and size against producer evidence. Legacy plan identity and scope are unchanged. | Pass |
| Pipeline ordering | Inside `Release`, the DBC build, retain and publish steps come before PyPI, ESRP and the receipt. However, `Release` depends on `Publish`, which has already run release-mode `publishBlob`. | See Issue 1 |
| ESRP staging | Version and Scala are explicit. Symlinks and paths that escape the source are rejected. The required POM, JAR and tests JAR and the POM coordinates are checked. Output goes to a fresh directory outside the Ivy cache and is renamed into place. | Pass |
| Website install links | The Spark 4.0 version is tracked separately. Notebook links point at release-tag `docs` trees for the three runtimes. | Pass |

Considered and not reported:

- `release_guard.py` handles only `ValueError` and `OSError` in `main`, so a `subprocess.TimeoutExpired` during notebook admission ends in a traceback. It still exits non-zero before any tag push.
- A top-level notebook would make `roundtrip_archive` create `<root>/.`, but the current source has no `docs/*.ipynb`, so this path cannot be reached.

## Issues

### 1. Medium: native DBC validation runs only after irreversible Blob Maven publication

**Where:**

- `pipeline.yaml:284`: release-mode `publishBlob` in `Publish`.
- `pipeline.yaml:589-590`: `Release` depends on `Publish`.
- `pipeline.yaml:646`: the first native DBC step.
- `project/build.scala:98-100` and `134-136`: release destinations must be absent and are uploaded without overwrite.
- `scripts/release/release_dbc.py:303-356`.
- `scripts/release/README.md:49-52`, `65-67` and `543-544`.

**Trigger:** any deterministic failure of `release_dbc.py build` for the approved commit. For example, `roundtrip_archive` raises when any non-blank cell's exact text changes after import and export (`release_dbc.py:322-327`), or `validate_archive` rejects the real export's entry names or inventory. Before this step, the Databricks conversion contract has only been tested against the in-repo `FakeWorkspace`. The admissibility gate at tag time reads Git but never imports or exports.

**Consequence:** By the time this step runs, `Publish` has already uploaded the release Maven coordinates to the public Blob Maven repository, with overwrite disabled. Rerunning `Release` rebuilds from the same commit and source digest, so the failure repeats. `Publish` cannot be rerun because `refuseExistingReleaseBlob` rejects the existing destination. The README forbids reusing the version for different sources and requires a new patch version after partial publication. The version must therefore be abandoned while its Maven artifacts stay public, with no receipt, verification or release notes. The README says this ordering cannot strand published packages, but that covers only PyPI, ESRP and missing blob access, not this case. `test_native_dbc_publication_is_wired_before_release_receipt` (`scripts/release/test_release_dbc_contract.py:128-160`) checks ordering only within `Release`.

**Minimal fix:** Build and round-trip the archive, and retain its pipeline artifact, before release-mode `publishBlob`. Do this either as a `Publish` step ahead of the upload or in a job that `Publish` depends on. `Publish` already runs the release guard that sets `releaseDbc`. `Release` then downloads that artifact and runs only the no-overwrite `release_dbc.py publish` before PyPI and ESRP. Extend the contract test to assert that the build precedes `publishBlob`. If the current order is intentional, document that a deterministic DBC failure after `Publish` means abandoning the version.

### 2. Low: the backslash rejection case fails on Windows

**Where:** `scripts/release/test_release_dbc.py:76`, a parameter of `test_archive_rejects_unsafe_directory_entries` (line 81), built by `archive_bytes` (lines 39-45). The check under test is `scripts/release/release_dbc.py:192`.

**Trigger:** running the README validation command, `python -m pytest scripts/release ...`, with CPython on Windows.

**Consequence:** `zipfile.ZipInfo` replaces `os.sep` with `/` in member names, both when `writestr` creates the entry and when `ZipFile` reads the central directory. On Windows the entry therefore comes back as `SynapseMLExamplesv1.2.0/nested/outside/`, a safe nested directory. `validate_archive` returns 1 instead of raising, and the case fails with "DID NOT RAISE". Linux CI and the Linux release job are unaffected, and the normalized name is not a traversal. This is still a deterministic false failure on a platform the suite otherwise supports. See the `win32` skip in `scripts/release/test_release_tag_recovery.py:21-22` and the Windows symlink-privilege handling in `scripts/release/test_esrp_staging.py:208`.

**Minimal fix:** wrap that case in `pytest.param(..., marks=pytest.mark.skipif(os.sep == "\\", reason="zipfile normalizes os.sep on Windows"))`, or assert the platform-specific outcome.

## Driver disposition, 2026-09-30

The pipeline ordering finding is confirmed. The release-mode `Publish` job
uploads Maven artifacts before the native DBC gate in its dependent `Release`
job. Moving the gate and handing off the validated archive requires a protected
`pipeline.yaml` change. Approval was requested but is not yet granted, so the
pipeline remains unchanged and this finding remains open.

The Windows test failure was reproduced: one directory case failed and twelve
passed. The test now checks the actual ZIP member-name normalization and asserts
the platform-specific result. Linux still requires rejection of the preserved
backslash, while Windows verifies the normalized, valid directory. No test is
skipped, and production archive validation is unchanged.

This test correction consumes the current frozen review pass. The pipeline
correction and a new review pass remain pending; no readiness claim is made.

Verification of the test correction: all 13 directory cases pass on Windows,
without skips. The Linux DBC, contract and public-documentation selection passes
112 tests, with the native-only checkout case deselected. Pinned Black passes.

## Authorized ordering correction, 2026-09-30

The maintainer subsequently authorized the release-only pipeline correction.
`Publish` now builds and round-trips the native archive and retains it as a
pipeline artifact before Maven publication. Either failure stops publication.
`Release` downloads the exact artifact named by the successful producer step,
validates the handoff name first, and does not regenerate archive bytes on retry.
The native steps remain excluded from snapshot CI and skipped for schema-2 plans.

Fifteen new ordering and handoff regressions failed before the pipeline change.
The corrected pipeline, workflow and DBC contract selection passes 158 tests,
including executed Bash success/failure paths and artifact-name validation.
The native Windows notebook and directory selection passes all 14 tests.
This resolves both findings locally; a fresh full-patch review and current-head
remote validation are still required before readiness.

Server-side Azure YAML preview was unavailable because the local CLI could not
authenticate. No preview result is claimed, and no build or publication was
created by those attempts. Later publication or service failures can still
leave a partial release; the recovery documentation keeps that limitation.
