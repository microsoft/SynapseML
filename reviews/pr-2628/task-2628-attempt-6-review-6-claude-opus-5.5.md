## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: sequential
- **Model**: claude-opus-5.5
- **Artifact**: `reviews/pr-2628/task-2628-attempt-6-review-6-claude-opus-5.5.md`
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Scope. I reviewed the staged DBC integration at HEAD `47e945b22402eb117583a28057738b9c45bb4432`, rebased onto master `bccec7e73d102bea655b527246e5575e81c983c9`. `git diff --cached` covers 25 files: 20 source, test and documentation files plus five review artifacts. A script comparison found the round-6 prompt's embedded diff identical to the staged diff for all 20 non-review files. The only unstaged edits are the repository-relative path fixes in the round-3 and round-4 artifacts. I did not re-raise resolved round-1, round-3 or round-4 findings.
- [x] Source read. All of `scripts/release/release_dbc.py`, lines 1-498. The staged Release job in `pipeline.yaml`, lines 589-751. `release_guard.py` lines 40-62, 83-145, 209-222 and 488-650. The staged hunks in `release_matrix.py`, `release_ops.py` and `verify_release.py`. `scripts/release/README.md` lines 1-75, 97-140, 345-372 and 494-527. `test_release_dbc.py`, `test_release_dbc_contract.py` and `test_public_release_docs.py`. The staged user-facing hunks in `README.md`, `docs/Get Started/Install SynapseML.md`, `website/src/pages/index.js` and `.github/skills/synapseml-release/SKILL.md`. The workflow callers of the notebook guard: `release-prepare.yml` lines 95-103 and 308-335, `release-tag.yml` lines 146-154, 202 and 275, and `release-tag-spark.yml` line 176.
- [x] Ordering and gating hold. The DBC build (`pipeline.yaml` 645-659), retained artifact (660-665) and publish step (666-679) all precede `sbt publishPypi` (684) and `EsrpRelease@9` (713). The receipt passes `--dbc-directory` at line 735. `test_release_dbc_contract.py::test_native_dbc_publication_is_wired_before_release_receipt` (line 128) enforces this order. The guard always emits `releaseDbc` as `true` or `false` (`release_guard.py:642`), so the step conditions never compare an unexpanded macro. `release_dbc.main` refuses plans that are not schema 4 at lines 472-473.
- [x] Documentation accuracy. `README.md`, `docs/Get Started/Install SynapseML.md` and `website/src/pages/index.js` now say that new automated releases include runtime-matched archives linked in their release notes. The release skill limits DBCs to schema-4 plans. Both statements match `notes_installation` and `verify_notebook_downloads` in `release_guard.py`. The operator README states the RBAC prerequisite, the no-overwrite upload, the pre-PyPI order and the verifier checks correctly. Issue 3 covers the one sentence operators cannot act on.
- [x] Logging and credentials. `run` (`release_dbc.py` 41-48) never prints CLI output. `main` (489-493) prints only `ValueError` text. The build and publish steps print public identity fields only: plan ID, commit, version, source digest, hash, size and notebook count. The bearer header exists only inside `Workspace.call`. Issues 1 and 2 concern diagnostic precision, not leaks.
- [x] Performance. Each target's build makes about 130 sequential Databricks calls: one staging `mkdirs`, one `mkdirs` per notebook directory, 56 JUPYTER imports, one DBC export, one DBC import, 56 JUPYTER exports and one delete. It also reads 56 notebooks with `git show`. Publish makes two anonymous downloads. Responses are capped at 20 MiB and each call has a 120-second timeout. This is small next to the Release job's Maven and ESRP work, so I raised no performance finding.
- [x] Offline reproductions. I ran these with `python -B` against the staged modules, with patched openers and fake `az`, and made no network calls. (a) Workspace failures in four operations produced only two distinct messages. (b) `URLError` and `TimeoutError` through `main build` printed only the exception class name. (c) `http.client.IncompleteRead` escaped `main` uncaught. (d) `main publish` printed the same three lines for "upload failed, no public blob" and "different public blob exists".
- [x] Tests. On Windows with Python 3.13.7, `python -B -m pytest -p no:cacheprovider -q scripts/release/test_release_dbc.py scripts/release/test_release_dbc_contract.py scripts/release/test_public_release_docs.py` gave 87 passed before this artifact existed. After it was written, the run gave 88 passed, including this artifact's public-path check. For the full suite I relied on the supplied Linux result: 945 passed, one optional SBT skip and one deselected checkout-format test.
- [x] Considered and not raised. The archive path in `release_matrix.py:868` is written separately from `verify_release.public_dbc_name` (lines 78-91). The two agree today. The Databricks client sends only the bearer token, which is least privilege, and the README makes workspace access a manual prerequisite. The in-build rerun path in `release_ops` predates this change.
- [ ] Not exercised. I did not contact Databricks, Azure Storage or hosted CI, and did not observe real Azure CLI output for a missing `dbcs` role. The native round-trip proof was supplied for the prior HEAD: 56 notebooks, 1,108 commands, a synthetic `0.0.0` archive, and no execution or publication. The rebase changed no notebooks.
- [x] Constraints kept. I ran no release operations, read no credentials, used no other reviewers, factories or review tools, made no commits and changed no source. This artifact is the only file I wrote.

## Issues

### Issue 1: Databricks failures do not name the operation, and some transport errors bypass the error contract
- **Severity**: Low
- **File**: `scripts/release/release_dbc.py`
- **Line(s)**: 252-278, 280-293, 296-316, 489-493
- **Description**: `Workspace.call` converts only `urllib.error.HTTPError`. Its message names only the endpoint and status, `Databricks {endpoint} failed: HTTP {code}` (lines 273-278). The round-trip uses `import` both for the 56 JUPYTER notebook imports and for the whole-archive DBC reimport. It uses `export` both for the DBC export and for the 56 JUPYTER exports (lines 306-316). In an offline repro, both import roles printed `Databricks import failed: HTTP 400` and both export roles printed `Databricks export failed: HTTP 404`, with no path or format. Transport failures are not converted at all. `URLError` and `TimeoutError` reach `main` as `OSError`, which prints only `error: DBC release failed (URLError)` (line 491). `http.client.IncompleteRead` is not an `OSError`, so it escaped `main` uncaught; the same applies to the `BadStatusLine` and `LineTooLong` errors that `getresponse` raises unwrapped. The step still fails closed. The traceback shows source lines, not the token, so this is inconsistency, not a leak. The same module already converts these errors for the public read (lines 69-74) and for cleanup (line 326). `test_release_dbc.py:402-417` tests only the public read. No test drives the real `Workspace.call` error path, because `FakeWorkspace` replaces it.
- **Risk**: About 130 Databricks calls share this path. When one fails, the log does not show which notebook or phase failed. An operator cannot tell a rejected notebook import from a changed DBC placement or a dropped connection without reproducing by hand. A truncated chunked response ends in a traceback rather than the sanitized exit-2 message the other failures produce.
- **Suggested Fix**: Pass a short label into `call`, such as the format, endpoint and workspace path, and raise `ValueError(f"Databricks {label} failed: HTTP {code}")`. The paths contain only public notebook names and the random staging folder, which the cleanup message already prints. Catch `(OSError, http.client.HTTPException)` in `call` and raise `ValueError(f"Databricks {label} failed: {type(error).__name__}") from None`, as `fetch_public_archive` does. Optionally, read a bounded prefix of the error body and print the Databricks `error_code` only when it matches `^[A-Z_]{1,64}$`. Never print `message`. Add a test that patches `OpenerDirector.open` for a real `Workspace` built with `Workspace.__new__`, covering an HTTP error, `URLError` and `IncompleteRead`. It should assert that the label appears and the token does not.

### Issue 2: A failed upload with no public archive is reported as a content mismatch
- **Severity**: Low
- **File**: `scripts/release/release_dbc.py`, `scripts/release/test_release_dbc.py`
- **Line(s)**: `release_dbc.py` 436-452; `test_release_dbc.py` 278-282 and 380-399
- **Description**: `az storage blob upload` can fail and the reconciliation read then find no blob. In that case `publish_archive` raises the same message it uses for a conflicting blob, "Public DBC download does not match the approved archive" (lines 443-452). `run` discards CLI output by design, and `main` prints only the outer message, so the chained upload error never appears. An offline repro through `main publish` printed identical output for both cases:

  ```text
  warning: DBC upload reported ValueError; checking public bytes before accepting publication
  error: DBC release failed (ValueError)
  Public DBC download does not match the approved archive
  ```

  The README makes the **Storage Blob Data Contributor** grant a manual prerequisite. A missing or mis-scoped grant is the most likely failure on the first schema-4 release, and it produces exactly this output. Round 3 suggested separate "no public archive" and "conflicting public archive" messages, but the fix did not carry that over; its resolution covers only the reconciliation read. The test at lines 380-399 asserts "does not match" for the absent case, which locks in the ambiguity.
- **Risk**: The message suggests a content conflict. The README says "A conflicting version is an error, not permission to overwrite it." An operator may therefore treat a fixable permission gap as a burned version or a source problem, when the fix is to grant the role and rerun the failed job. The rerun is safe because this step precedes PyPI and ESRP.
- **Suggested Fix**: Branch on `published is None`. When no blob exists, raise a message such as "DBC upload failed and no public archive exists; confirm the service connection can write to `dbcs`, then rerun the failed Release job". When bytes or metadata differ, raise "A different public DBC exists for this version; it will not be overwritten". Keep `from upload_error` on both. Optionally, print one allowlisted Azure error code from captured stderr, such as `AuthorizationPermissionMismatch`, when the token after `ErrorCode:` matches `^[A-Za-z]{1,64}$`, and print nothing else. Update `test_release_dbc.py:398` to expect the absent-archive message and keep line 281 on the conflict message. Add one README sentence that maps each message to its recovery.

### Issue 3: The README's pre-approval notebook check gives no command and no point in the flow
- **Severity**: Low
- **File**: `scripts/release/README.md`, `scripts/release/release_guard.py`
- **Line(s)**: README 66-69; `release_guard.py` 530-532 and 564-573
- **Description**: README lines 66-69 say the `full-release --repo` and `push-tags` guards check notebook admissibility. They then tell operators to "Run the same offline check on candidate source before approval." The sentence gives no command and does not say which approval it means. Plan approval in step 2 (lines 345-361) binds commits whose tags already exist, and the tag guards have already run by then. The checks that run earlier are automatic: `release-prepare.yml:101-103` runs `full-release --repo .` on `master` at dispatch. Port release PRs get no check until a tag workflow calls `push-tags` after merge (`release-tag.yml` 202 and 275, `release-tag-spark.yml` 176). The only read-only manual form is `release_guard.py full-release --version <version> --repo .` on a checkout of the candidate. It checks `HEAD` after confirming the release branches exist on `origin` (lines 564-573). The `--repo` help still says only "Also confirm every release branch exists on origin" (line 531).
- **Risk**: Operators cannot follow the instruction as written. A notebook in a port release PR that fails admissibility surfaces only after merge, when the tag job fails. Fixing it then requires another reviewed port PR.
- **Suggested Fix**: Replace the sentence with a concrete step. Before merging the release-prepare PR and each port release PR, check out its head from a clone whose `origin` is `microsoft/SynapseML` and run `python scripts/release/release_guard.py full-release --version 1.2.0 --repo .`. Note that release-prepare dispatch and the tag workflows already run the same check automatically. Change the `--repo` help to "Also confirm release branches exist on origin and check notebook admissibility at HEAD".

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Databricks failures do not identify the operation or notebook, and some transport errors end in tracebacks.
- **How verified**: Pending fix. Review evidence is an offline repro against `Workspace.call` and `main build` with a patched opener and no network.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: A missing blob and a conflicting blob produce identical diagnostics.
- **How verified**: Pending fix. Review evidence is an offline repro through `main publish` with a failing fake `az` and patched public lookups.

### Issue 3
- **Status**: Open
- **What changed**: Pending.
- **Why**: The README instruction has no command and names a point in the flow that comes after the tag guards.
- **How verified**: Pending fix. Review evidence is inspection of README lines 66-69 and 345-361, `release_guard.py` lines 530-573, and the workflow callers listed in the evidence checklist.

## Resolution verification

All three findings are resolved in the accompanying source and documentation.

1. Workspace HTTP and transport errors now identify the endpoint, format and
   JSON-escaped workspace path. Response bodies and authentication values remain
   excluded. Six real-client regression cases cover HTTP errors, URL failures
   and incomplete reads for both DBC and JUPYTER exports.
2. Publication now distinguishes an absent public archive from conflicting
   bytes. The absent-archive diagnostic directs operators to check blob write
   access and retry the failed Release job. The conflict diagnostic explicitly
   refuses overwrite. The operator guide documents both recovery paths.
3. The guide gives the exact read-only candidate check, when to run it before
   merging primary and port release PRs, and the optional-target argument.
   CLI help now describes its notebook validation as well as branch checks.

Final validation after these fixes: **954 Linux release-tooling tests passed**,
one optional live SBT test skipped, and one current-checkout notebook test
deselected only because Linux Git cannot open a Windows-created worktree.
That exact test passed separately with native Git on Windows. All 12 website
installation tests passed. Pinned Black 22.3.0 formatted the changed Python.
These diagnostic-only changes do not change native archive generation.
