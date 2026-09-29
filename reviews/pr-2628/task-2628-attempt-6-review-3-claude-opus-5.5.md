## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: sequential
- **Model**: claude-opus-5.5
- **Artifact**: `reviews/pr-2628/task-2628-attempt-6-review-3-claude-opus-5.5.md`
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Scope. I reviewed the staged DBC integration in `.worktrees\pr2628-dbc-20260928` at HEAD `ae45761b61ccffe06f4516b97a24042665212cd1`, branch `work/pr2628-dbc-20260928`, 20 staged files. I read the staged files rather than the prompt's diff snapshot, because the round-1 fixes landed after that snapshot. I did not re-raise the four resolved round-1 findings.
- [x] Source read. All of `scripts/release/release_dbc.py`, lines 1-458. Staged changes in the `pipeline.yaml` Release job, lines 589-749. `release_guard.py` lines 126-145, 488-507, 564-594 and 641. `release_matrix.py` lines 36-38 and 393. `release_ops.py` lines 809-907, 1720-1768, 1990, 2149-2239 and 2570-2580. `verify_release.py` lines 78, 90, 182-218, 355-363, 599-610 and 1097. `scripts/release/README.md` lines 28-66, 72-116 and 388-411. Both new test files, `test_release_dbc.py` and `test_release_dbc_contract.py`.
- [x] Tests. `python -m pytest -p no:cacheprovider -q scripts/release/test_release_dbc.py scripts/release/test_release_dbc_contract.py` with `PYTHONDONTWRITEBYTECODE=1` gave 33 passed. `git status` showed no new untracked files afterward. I did not re-run the full 922-test suite and relied on the supplied result for it.
- [x] Schema-2 preservation holds. The guard sets `releaseDbc` only for schema 4, and `release_dbc.py:432-433` refuses any other plan. `maven_receipt` rejects `--dbc-directory` for legacy plans at `release_guard.py:506-507`. `_validate_dbc_content` returns early for schema 2 at `release_ops.py:2150-2151`. The verifier adds a DBC row only for schema 4 at `verify_release.py:600-610`. Round 1 already confirmed identical schema-2 documents and plan IDs.
- [x] Hash and size gates hold. `staged_archive` at `release_dbc.py:348-369` binds plan, commit, version, path, SHA-256, size, notebook count and the round-trip flag. `fetch_public_archive` checks `source_commit` metadata and recomputes SHA-256 at lines 71-78. `_validate_dbc_content` requires exactly one public row and one producer artifact with equal SHA-256 and size, `release_ops.py:2149-2169`. `verify_notebook_downloads` downloads again and compares against producer evidence after `validate_evidence`, `release_guard.py:126-145` and 593-594. `validate_producer_evidence` groups actions by kind, repository and target, so `_validate_dbc_content(plan, first, ...)` gets the right target and Maven version.
- [x] Credential handling holds. The raw bytes at `release_dbc.py:243` are `f"Bearer {self.token}"`. The tool display masked the value, and the code is correct. `run` at lines 39-46 never prints CLI output, and `main` prints only `ValueError` text at lines 449-453.
- [x] Concurrency and reruns. Archive names come from each runtime's Maven version, so two runtimes never share a blob. `$(System.JobAttempt)` gives each attempt its own staging directory and artifact name. A rerun reuses a public archive only when the plan ID and source digest match, `release_dbc.py:315-320`. A conflicting archive is a hard error, and `--overwrite false` blocks replacement. `DBC_BASE` at `verify_release.py:78` is direct blob storage, not a CDN, so the post-upload read cannot hit a cached 404.
- [x] Pipeline environment. The DBC steps run after the `$CONDA/bin` PATH prepend, and `release_dbc.py` plus everything it imports uses only the standard library. Staging lives under `Build.ArtifactStagingDirectory`, and `release_dbc.py:435-436` refuses a directory inside the checkout. `.gitignore:19` ignores `*.pyc`, so `__pycache__` does not trip `validate_checkout`.
- [x] Bound plans and evidence size. `_probes` only receives plans loaded with `require_bound=True`. `test_release_ops.py:3442-3494` keeps gzip+base64 evidence under `MAX_GITHUB_EVIDENCE_CHARS` with and without Spark 4.0.
- [x] Offline reproductions ran with `python -B`, no network and no source edits. `prepare_notebooks` accepts HEAD, `origin/master`, `ms/master`, `ms/spark4.0`, `ms/spark4.1` and `upstream/spark4.1`, 56 notebooks each. 126 cells at HEAD carry an empty `attachments: {}`, which the truthiness check correctly accepts. I also reproduced the cleanup masking in Issue 4 and the exception matrix in Issue 5.
- [x] Evidence scope. The supplied native round-trip provenance names source commit `a833941704`, dated 2026-04-04. It is an ancestor of HEAD and has 57 notebooks. HEAD has 56, because 28 notebooks changed, 2 were removed and 1 was added since then. An offline profile of cell types, magics, tabs, non-ASCII text and trailing newlines found nothing at HEAD that the evidence tree lacked. I folded this gap into Issue 2 instead of raising it on its own.
- [ ] Not exercised. I did not touch the production Databricks workspace, the `mmlspark/dbcs` RBAC or anonymous-access settings, ESRP rerun behavior, or hosted CI. The instructions ruled out network and publication operations.
- [x] Constraints kept. I ran no public release operations, read no credentials, invoked no other reviewers or factories, made no commits and edited no source. This artifact is the only file I wrote.

## Issues

### Issue 1: DBC publication first uses storage write access after PyPI and Maven are already public
- **Severity**: High
- **File**: `pipeline.yaml`
- **Line(s)**: 666-681, 699-712, 713-726, 727-749. Related: `scripts/release/README.md` 38-44 and 54-59, `scripts/release/test_release_dbc_contract.py` 160, `build.sbt` 201-211.
- **Description**: The build step at `pipeline.yaml:645-659` needs only Databricks access and an anonymous GET. The login upload, which needs Storage Blob Data Contributor on `dbcs`, first runs at line 722. By then line 670 has pushed the wheel with `sbt publishPypi`, and lines 699-712 have submitted Maven through `EsrpRelease@9`. When the DBC step fails, Azure skips the receipt step at 727-744 and the `release-provenance` artifact at 745-749. Plausible causes are a forgotten or mis-scoped role grant, which README 38-43 leaves as a manual prerequisite, a transient blob error, a failed verification read, or the ambiguous upload in Issue 3. README:44 says "Missing access blocks publication." With this step order, missing access blocks only the DBC and the receipt.
- **Risk**: The wheel and Maven artifacts are public and immutable, and the build has no receipt. `_validate_manifests` refuses any build without one at `release_ops.py:1990`. `_refresh_group` marks the failed build "automatic retry is forbidden" at 2185-2191, and `_retry` refuses Maven at 2570-2580. Rerunning the whole job runs `sbt publishPypi` again. `build.sbt:201-211` calls `twine upload --non-interactive` without `--skip-existing`, so the primary target fails at PyPI before it gets back to the DBC step. Port targets skip PyPI but submit ESRP again, and I have not verified how ESRP treats that. README 54-59 covers only the reverse case, an upload that succeeds before a later step fails. Nothing documents recovery from the failure most likely on the first schema-4 release.
- **Suggested Fix**: Move "Publish and verify public release notebook archive" to directly after "Retain validated release notebook archive", before "publish python package to pypi". The current code already makes that order rerun-safe. `build_archive` reuses a public archive bound to the same plan and source at `release_dbc.py:315-320`, and `publish_archive` skips the upload and re-verifies the bytes at 374-412. There is a cost. A DBC can go public for a version whose Maven publication later fails or is rejected. It stays unadvertised until verified notes exist, and any replacement plan for that version would need the same plan ID, because `build_archive` rejects an archive bound to another plan. If the team wants the DBC published last, add a pre-PyPI step that proves login write access to `dbcs` and anonymous read, and document recovery for "Maven and PyPI published, DBC failed". Either way, extend `test_release_dbc_contract.py:160` to assert that the DBC publish or preflight step precedes the PyPI and ESRP steps, and correct README:44 and 54-59.

### Issue 2: Notebook admissibility is first checked after release tags exist
- **Severity**: Medium
- **File**: `scripts/release/release_dbc.py`
- **Line(s)**: 99-160, 313. Related: `.github/workflows/pr-validation.yml` 7 and 79, `scripts/release/release_guard.py` 564-571, `.github/workflows/release-tag.yml` 146-154 and 172-208, `.github/workflows/release-tag-spark.yml` 140-184, `scripts/release/release_matrix.py` 393.
- **Description**: `prepare_notebooks` rejects a notebook whose kernel is not Python at lines 123-124. It rejects any cell that is not code or markdown, or that has non-empty `attachments`, at 127-130. It rejects text matching the credential patterns at 132-139, including `\b(?:sk-|ghp_|github_pat_)[A-Za-z0-9_-]{30,}`. Production code calls it only from `build_archive` at line 313, inside the Azure Release job. That job runs after `release-tag.yml:172-208` pushes the master derivative tags, and after `release-tag-spark.yml:140-184` pushes port tags when a port release PR merges. `full-release` at `release_guard.py:564-571` checks only policy and branch refs. `test_release_dbc.py:229` builds a synthetic repository and never reads the real docs tree. `pr-validation.yml:7` path-ignores `docs/**`, so a notebook-only PR never reaches the release tests at line 79. Jupyter and VS Code store a pasted image as a cell attachment. A raw cell, or an OpenAI example with an `sk-` placeholder of 30 or more characters, would also merge cleanly and fail only at release time. The supplied native round-trip covered commit `a833941704`. 28 notebooks have changed since then, so nothing has round-tripped the current tree natively.
- **Risk**: One docs edit can block a schema-4 release after its tags are immutable. `release_matrix.py:393` makes every new public plan schema 4. Preparation refuses existing tag families, README 114-116, and tags never move, README:78. Nobody can fix the tagged commit, and no approved plan leaves out the DBC. The version has to be abandoned or recovered by hand.
- **Suggested Fix**: Run the offline `prepare_notebooks(repo, "HEAD")` check before any tag exists. `release_guard.py full-release --repo .` already runs before tag creation at `release-prepare.yml:101` and `release-tag.yml:151`, so it can host the check without a workflow edit. Add the same check before `push-tags` in `release-tag-spark.yml` for port commits. Add a pytest that runs it against the checkout so ordinary PR validation catches regressions. Because pr-validation ignores `docs/**`, consider a small docs-triggered job; AGENTS.md requires maintainer approval for workflow changes. A non-publishing native round-trip on the release-prepare PR would close the rest of the gap.

### Issue 3: The upload step does not check the public blob after an ambiguous failure
- **Severity**: Low
- **File**: `scripts/release/release_dbc.py`
- **Line(s)**: 39-46, 372-413, with the upload at 375-404 and verification at 405-412.
- **Description**: `publish_archive` uploads with `--overwrite false`, which sends `If-None-Match: *`. Any non-zero `az` exit, or `subprocess.TimeoutExpired` after 180 seconds, aborts the step before the verification read at line 405. Suppose Put Blob commits but the response never arrives. The storage SDK retries, gets 409 BlobAlreadyExists for our own bytes, and the CLI exits non-zero. A timeout that kills the CLI after the commit ends the same way. The step fails even though the public blob matches the approved archive exactly. `run` also reduces every failure to "az command failed", so the log cannot distinguish a 403 from a missing role, a 409 conflict, and a network error.
- **Risk**: The pipeline reports a correctly published DBC as failed, after PyPI and ESRP, and lands in the no-receipt state from Issue 1. The next attempt would succeed, because the build reuses the same-plan blob, but only through the blocked rerun path. The generic message slows diagnosis.
- **Suggested Fix**: Catch `ValueError` and `subprocess.TimeoutExpired` from the upload, then always run the verification at 405-412. Accept only identical bytes with matching `plan_id`, `source_digest`, `source_commit` and SHA-256. If no blob exists, raise "upload failed; no public archive exists". If a different blob exists, raise "conflicting public archive". Chain the original error in both cases. Optionally print one allowlisted Azure `ErrorCode` token such as `AuthorizationPermissionMismatch` or `BlobAlreadyExists`, and nothing else from CLI output. Add a fake-`az` test where the upload fails after the blob becomes visible.

### Issue 4: Cleanup in `finally` hides the round-trip error or fails a validated build
- **Severity**: Low
- **File**: `scripts/release/release_dbc.py`
- **Line(s)**: 256-261, 279-306, 449-453.
- **Description**: `Workspace.call("delete", ...)` raises `ValueError` for every HTTP error except 404, lines 256-261. URL errors and timeouts raise `OSError`. The delete runs in the `finally` block at 305-306. If the round-trip already failed, the delete error replaces the original, and `main` prints only the last exception. In an offline repro with changed notebook content and a delete returning 503, the step printed only "Databricks delete failed: HTTP 503". The real error, "DBC round-trip changed notebook content: Example.ipynb", survived only in `__context__`, which `main` never prints. If the round-trip succeeded, a delete failure throws away a fully validated archive and fails the build.
- **Risk**: An operator sees what looks like a transient cleanup error and reruns instead of investigating a content defect. A transient delete failure also turns a good build into a failed Release run, which `release_ops` will not retry automatically, so someone has to handle it by hand.
- **Suggested Fix**: Keep the primary exception. Wrap the delete in `try/except (ValueError, OSError)`. When the round-trip failed, re-raise the primary error and mention the cleanup failure in its message. When it succeeded, print a warning naming the leaked `/Shared/synapseml-dbc-release-<uuid>` path and return the validated data. If a leaked folder must stop the release, fail with one message that names both problems instead. Add tests for both cases.

### Issue 5: Malformed archives and network errors escape the DBC error contract
- **Severity**: Low
- **File**: `scripts/release/release_dbc.py`
- **Line(s)**: 56-80, 166-210. Callers: `scripts/release/verify_release.py` 355-363 and 1097, `scripts/release/release_guard.py` 641.
- **Description**: `validate_archive` converts only `BadZipFile`, `KeyError` and `TypeError` at line 206, and lines 192-204 assume every parsed object, command and result is a dict. `fetch_public_archive` handles only `HTTPError`, at 67-70. Offline repros against the staged module raised `zlib.error` for an invalid deflate stream, `RuntimeError` for an encrypted-entry flag and `NotImplementedError` for compression method 99. A top-level JSON list, string commands or list-valued `results` raised `AttributeError`. A connection failure during the public read raised `urllib.error.URLError`. `release_dbc.main` catches `ValueError`, `OSError`, `KeyError` and `TimeoutExpired`. `release_guard.main` catches `ValueError` and `OSError`. `verify_release.main` catches `ValueError` and `RuntimeError`. The verifier's other public probes convert `URLError` to `RuntimeError` at `verify_release.py:182-218`, and the DBC probe does not.
- **Risk**: Everything still fails closed. But the pipeline step, the notes guard and the verifier can die with raw tracebacks instead of the intended short error. The DBC probe also treats network errors differently from every other public probe. The inputs come from Databricks and a Microsoft-controlled blob, so this is unlikely.
- **Suggested Fix**: Check shapes explicitly. `obj` must be a dict, `obj.get("commands")` a list, each command a dict, and `results` either `None` or a dict. Extend the handler at line 206 to `zlib.error`, `EOFError` and `RuntimeError`, which also covers `NotImplementedError`. In `fetch_public_archive`, map `urllib.error.URLError`, other `OSError` and `http.client.HTTPException` to `ValueError("Public DBC lookup failed: <class name>")`. Add parametrized malformed-archive tests.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Irreversible PyPI and Maven publication runs before the only step that uses the new storage permission.
- **How verified**: Pending fix. Review evidence is the Release step order at `pipeline.yaml:645-749`, the ordering assertion at `test_release_dbc_contract.py:160`, the missing `--skip-existing` at `build.sbt:201-211`, and the retry refusals at `release_ops.py:1990`, 2185-2191 and 2570-2580.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: The only admissibility check runs after tags are immutable, and no approved plan can leave out the DBC.
- **How verified**: Pending fix. Review evidence is that `prepare_notebooks` has one production caller at `release_dbc.py:313`, `pr-validation.yml:7` ignores `docs/**`, `full-release` has no notebook check, and the supplied round-trip covered `a833941704` rather than HEAD.

### Issue 3
- **Status**: Open
- **What changed**: Pending.
- **Why**: A committed upload whose response is lost fails the step even though the public blob is correct.
- **How verified**: Pending fix. Review evidence is inspection of `release_dbc.py:39-46` and 372-413. I made no Azure calls.

### Issue 4
- **Status**: Open
- **What changed**: Pending.
- **Why**: Cleanup failures replace the round-trip error and fail builds that already validated.
- **How verified**: Pending fix. An offline repro with a fake workspace printed only "Databricks delete failed: HTTP 503" when the content also differed.

### Issue 5
- **Status**: Open
- **What changed**: Pending.
- **Why**: Malformed input and network errors bypass the `ValueError` contract and end in tracebacks.
- **How verified**: Pending fix. An offline `python -B` repro against the staged module raised `zlib.error`, `RuntimeError`, `NotImplementedError`, `AttributeError` and `URLError` as described.

## Resolution verification

All five findings above are resolved in the accompanying changes.

1. Moved DBC upload and anonymous verification before PyPI and ESRP.
   The pipeline contract test now enforces this order. The operator guide
   explains that an incomplete release can have an unadvertised archive and
   that recovery must retain the approved plan and immutable version.
2. Added offline notebook checks to `full-release --repo` and the common
   `push-tags` CLI, covering primary and port tag publication without changing
   workflows. Rejection tests prove neither path pushes tags on invalid
   notebooks. A real-checkout test covers the committed documentation tree.
   Native validation now also covers this PR's current notebook source.
3. Upload failures now trigger a public read before deciding the result.
   A warning records the sanitized failure class. Only identical bytes and
   matching plan/source metadata count as success. Tests cover successful
   publication followed by a CLI failure or timeout, and absent-blob failures.
4. Cleanup logs its unique owned path and sanitized failure class without
   replacing an active validation error. Cleanup failure after successful
   validation remains a hard error, deliberately preventing unnoticed workspace
   leaks. Tests cover both paths.
5. Explicit object, command and result checks reject malformed native JSON.
   ZIP decode failures and public network failures become bounded diagnostic
   errors. Tests cover malformed shapes, decompression/encryption/compression
   errors, and sanitized transport errors.

Verification: 139 focused archive, contract and guard tests passed.
Black 22.3.0 formatted the changes.

The real Databricks API exported, reimported and compared all **56 notebooks**
from commit `ae45761b61ccffe06f4516b97a24042665212cd1`, preserving significant
whitespace and removing saved outputs. Synthetic version `0.0.0`, 304258 bytes,
SHA-256 `83b5d57d50b19881b6ef972fcc407974ec7d9a2aa18cf0c9beae448d8d642903`.
This was non-publishing validation, not notebook execution or a production tag.
Owned temporary folders were deleted successfully.
