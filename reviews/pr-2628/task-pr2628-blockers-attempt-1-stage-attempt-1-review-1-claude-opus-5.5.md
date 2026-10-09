## Review Summary
- **Round**: 1
- **Theme**: Correctness, security vulnerabilities, logic errors, spec conformance
- **Mode**: resolved-plan (supplied review context)
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2628/task-pr2628-blockers-attempt-1-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist

**Input identity and scope.**

- [x] Manifest SHA-256 `c3e7ae984d8102b023b5cce248a5eea0a1ac7477c1a6a82855189036b10bd0ab` lists 59 source paths and no deletions, as assigned.
- [x] The embedded diff is 971,732 bytes with SHA-256 `4a549fe75b154f4226c61c38aa448f1507c999f5853c6bd40952db363b909e57`. It is byte-identical to `git diff --cached 861c3a1e14` in the worktree. HEAD is `a970c2409d1979c272fa076d2b49b6b605de9e3c`, the merge base is `861c3a1e14a9511b5604563ff1e3976cefa82e90` and the index tree is `a32337a66124ba796e00f0a43613b02ed7798d8e`.
- [x] `git diff --cached --name-only 861c3a1e14 -- . ":(exclude)reviews"` prints 59 paths. The 36 `reviews/` paths sit outside the manifest. Nothing is unstaged, so worktree reads match the reviewed index.
- [x] I read the whole assigned input, all 24,508 lines, with forced large-file reads. That covers all 59 paths, not only the pending 11-file delta.
- [x] Independence note. During this review I opened some orchestration status material outside the allowed inputs: a session checkpoint, the session task table, a local PR-loop status file, the input generator script and a listing of the input directory. I also checked whether sibling artifacts existed, and none did. I ran a no-op `git write-tree` that rewrote the existing index tree, ran a read-only `git ls-remote`, created and deleted one temporary diff file outside the repository, and fetched a public action definition. None of it is used as evidence. The findings below rest on the assigned diff, the worktree and the in-memory checks listed here.

**Requirement 1, warning-only public builds.**

- [x] `scripts/release/release_ops.py:70-86` defines `ADVISORY_TASKS` and `REQUIRED_RELEASE_TASKS`. The advisory entries are the cache tasks, the Codecov token load and the Codecov upload. Each is pinned to an official Azure task GUID and major version. The required entries are Maven preparation, ESRP publication, provenance recording and provenance upload.
- [x] `_advisory_task` at `release_ops.py:1946` matches name, task GUID and major version. It accepts the `Post-job:` prefix only for the cache task GUID.
- [x] `_checked_tasks` at `release_ops.py:1969-2069` requires the exact task key set and lower-case, nonzero, unique task GUIDs. It checks completed state, the result allowlist, a task attempt counter kept apart from the job attempt, and task windows inside the job window. Each publication task must succeed exactly once in the Release job. Unapproved names and non-advisory failures are rejected. Every non-skipped job needs a successful non-advisory task, task warnings must explain every job warning, and at least one job must carry a warning.
- [x] `_jobs` and `_checked_jobs` at `release_ops.py:2072-2157` keep Azure's raw results. `_public_jobs` at `release_ops.py:2160-2197` replaces job and task labels that are not part of the public contract.
- [x] `_time` at `release_ops.py:141` fails closed on missing, naive, non-UTC or future timestamps and accepts Azure's seven-digit fractions.
- [x] Tests: `scripts/release/test_release_warnings.py` runs the warning matrix by job, task result and job attempt. Its tamper cases fail in both `validate_producer_evidence` and `encode_evidence`, and it reconciles an old failed ledger. `test_release_ops.py` adds `test_clean_build_keeps_legacy_receipt_and_evidence_shapes` and `test_private_release_does_not_gain_partial_success_authorization`.
- [x] `scripts/release/README.md:196-220` states the same policy. The one gap is the export size limit in Issue 1.

**Requirement 2, pinned and repository-scoped App token.**

- [x] The release workflows mint the token with `actions/create-github-app-token` pinned to a full commit SHA (v3.2.0). The token is limited to the owner and repository, with `contents: read` and `pull-requests: write`. The configuration check runs before checkout and fails with a fixed message that does not echo inputs. `test_release_workflows.py` and `test_release_tag_recovery.py` cover this; the Bash parts of the second file skip on Windows.
- [x] Tag pushes use the checkout credential, so the read-only App scope does not block them.
- [x] The `actions/checkout` and `actions/setup-python` SHAs are the same pins the codeql, scorecards, dependency-review and check-dead-links workflows already use.
- [ ] Live run with the App configured. Not applicable here. The missing App variable and secret are a documented external gate and are not counted as an issue.

**Requirement 3, notebook-only PRs.**

- [x] `.github/workflows/release-notebook-validation.yml` runs on pull requests that touch `docs/**/*.ipynb`, `scripts/release/**` or the workflow file. It has `contents: read` and `persist-credentials: false`, uses no secrets, never runs sbt, and runs `pytest scripts/release/test_release_dbc.py`.
- [x] That suite contains `test_current_committed_notebooks_are_admissible`, which prepares notebooks from `HEAD`, and an autouse fixture that blocks network access. `test_notebook_only_prs_run_release_checks_without_scala_builds_or_secrets` pins the workflow shape.
- [x] Notebook import paths agree with `DatabricksUtilities.scala:625`. `test_release_dbc_contract.py` checks that the DBC build runs before the Maven upload and that the Release job reuses the validated archive.

**Requirement 4, bytes and bytearray in the primary wheel.**

- [x] `toNDArray` in `opencv/src/main/python/synapse/ml/opencv/ImageTransformer.py` now uses `np.frombuffer` for `bytes` and `bytearray`. I checked the NumPy behavior in memory. The old `np.asarray(bytes, dtype=np.uint8)` raises `ValueError`. Old and new code both return a writable view for `bytearray`, so that path did not change. Grayscale `bytes` input now returns a read-only view. That input used to raise, so this is not a regression.
- [x] The new tests are in `opencv/src/test/python/synapsemltest/opencv/test_image_conversion.py`.

**Other areas reviewed, no defect found.**

- [x] `scripts/release/verify_release.py`. I checked plan binding, schema 2 and schema 4 public evidence, and the strict PyPI wheel URL check (https, `files.pythonhosted.org`, no credentials, no query, no fragment). I also checked tag peeling, row identity and freshness in `validate_inventory`, and the size caps in `encode_evidence` and `decode_evidence`. `_get_ado_token` passes a constant argument list, so `shell=True` on Windows takes no outside input.
- [x] Ledger state, locking, retry, adoption and reconciliation in `release_ops.py`. Also `release_matrix.py`, `release_config.py`, `release_dbc.py`, `release_guard.py`, `bootstrap_release.py`, and `scripts/bump-version.py` with its recovery CLI tests.
- [x] `tools/esrp/prepare_jar.py` takes an explicit version, Scala version and module set. It rejects symlinks and paths that escape the source, checks POM coordinates and JAR files, and stages atomically outside the Ivy cache.
- [x] `pipeline.yaml` and templates. Publication waits for the mandatory and selected test jobs, tags no longer trigger the pipeline, and the plan guard runs before release side effects. `tools/ci/tests/test_pipeline_yaml.py` and `tools/ci/tests/test_e2e_impact.py` cover these.
- [x] Website and docs. `installArtifacts.js` adds a separate `spark40Version`. `index.js` links runtime-matched notebook trees instead of a fixed DBC. `installDocs.test.js` and `rSetupDocs.test.js` add version-consistency and preview guards.
- [x] Strings shown as `******` were masked by my viewing tool. Hex dumps show synthetic test literals, for example at `test_release_ops.py:3095` and `:3138`, and `release_dbc.py:265` builds its header from `self.token`. No secret is committed.
- [x] A scan of all 59 changed paths for mis-encoded text found only the two lines in Issue 2.
- [x] Examined and not reported. `.github/skills/synapseml-release/SKILL.md:74` says "Spark 4.1 PR replay is advisory". The automatic replay job is gone (`scripts/release/README.md:192`), but the branch skills still describe a manual replay compilation, so the sentence is accurate.

**Validation evidence.**

- [x] Issue 1 measurement, run in memory with `python -B` and no files written. I fed synthetic Azure timelines through the repository's own `release_ops._jobs(..., allow_warnings=True)` and `release_ops._public_jobs(..., from_timeline=True, allow_warnings=True)`. Both functions accepted them. I then applied the canonical JSON, gzip and base64 steps used by `encode_evidence`. Each timeline had unique task GUIDs, Azure-style seven-digit timestamps, one failed Codecov upload, the four Release publication tasks, and only four distinct task definitions, which favors compression. The results are in Issue 1.
- [x] Issue 2 byte check. Reversing cp1252-to-UTF-8 mis-decoding three times turns both literals into U+00B2.
- [ ] Provided facts, not re-run in this read-only stage. 1176 release and pipeline tests passed, with one optional live-SBT skip and one notebook case run separately in native Python. Pinned Black passed. The same aggregate wheel passed 16 tests on each of Spark 3.5.0 and 4.1.1 with matching JVM packages and persistence. The baseline wheel failed exactly 3 and 5 byte-conversion cases.

## Issues

### Issue 1: Warning-only (v2) public evidence cannot fit the GitHub release-notes input budget for a real release build
- **Severity**: Medium
- **File**: `scripts/release/verify_release.py`, `scripts/release/release_ops.py`, `.github/workflows/release-notes.yml`
- **Line(s)**: verify_release.py 85, 949-963, 966-990, 1157-1168; release_ops.py 50, 1969-2069, 2101-2157, 2160-2197; release-notes.yml 32-33, 83-89
- **Description**: For a warning-only build, `_jobs(..., allow_warnings=True)` exports every Task record of every job. Each record has its GUID, state, result, attempt, task GUID, task version and two timestamps. None can be dropped. `_checked_tasks` rejects any non-skipped job without successful task evidence, and the tamper tests show `encode_evidence` rejects evidence with tasks removed. `verify_release.py --github-evidence` compresses and base64-encodes the whole report. `encode_evidence` then rejects anything over `MAX_GITHUB_EVIDENCE_CHARS = 48000`, `decode_evidence` applies the same cap, and `release-notes.yml` takes the result as its `evidence_base64` dispatch input. I measured the job list alone with the repository's own code. 69 jobs with 1,380 tasks encode to 74,232 characters. 69 jobs with 1,035 tasks encode to 56,828. 69 jobs with 828 tasks encode to 46,244. The limit sits near 850 tasks, about 55 encoded characters per task. A real release build passes it. UnitTests alone has 40 matrix legs (`pipeline.yaml:962-1107`). Each leg yields about 20 timeline tasks: initialize, checkout, three cache restores and their three post-job saves, the cache check, two tool installers, setup, the test, test results, the coverage report, the Codecov token and upload, Azure coverage publication, post-job checkout and finalize. That is about 800 tasks before PythonTests (7 legs), Publish, Release and the optional test jobs. The plan, the inventory rows and a second target's producer run add more. The tests miss this. `partial_build` in `test_release_warnings.py:27` builds 3 jobs with about 8 tasks. `test_public_notes_export_fits_github_and_passes_the_real_guard` at `test_release_ops.py:3442-3494` only checks clean evidence, which has no task list.
- **Risk**: The check fails closed, so nothing wrong gets published. But the documented recovery for a published release that Azure marks `partiallySucceeded` (`scripts/release/README.md:196-220`) cannot produce release notes. `--github-evidence` exits with "compressed release evidence exceeds the GitHub input budget" for the very builds requirement 1 admits. A bigger constant will not help. GitHub caps the combined `workflow_dispatch` input payload at 65,535 characters, and `plan_json` and `approve_plan` share that budget.
- **Suggested Fix**: Export a bounded public proof and keep the full task list in the local ledger. For each job, send the ID, public name, raw result, attempt, window and task counts by result. Send full records only for advisory and publication tasks, plus a SHA-256 of the canonical full task list. Update `validate_producer_evidence` and the notes guard to check that bounded shape, and keep the full `_checked_tasks` validation on the producer side, where the complete timeline exists. Add a production-sized test, about 70 jobs with 20 tasks each plus a second target run, that asserts `encode_evidence` stays under `MAX_GITHUB_EVIDENCE_CHARS` and that the release-notes guard accepts the result.

### Issue 2: Corrupted Unicode-digit fixtures make two release_matrix negative tests pass trivially
- **Severity**: Low
- **File**: `scripts/release/test_release_matrix.py`
- **Line(s)**: 50, 351
- **Description**: Both lines hold the same nine-character mis-encoded string, UTF-8 bytes `c3 83 c6 92 c3 a2 e2 82 ac c5 a1 c3 83 e2 80 9a c3 82 c2 b2`. Reversing cp1252-to-UTF-8 mis-decoding three times gives U+00B2, superscript two. The intended non-ASCII digit case therefore never runs, neither in the `test_rejects_non_canonical_internal_patch` parameter at line 50 nor in the `spark4.0=` argument of `test_cli_rejects_invalid_iterations` at line 351. Any string with letters fails `PATCH_RE` (`release_matrix.py:32` and `:419`) and the `parse_iterations` pattern (`release_matrix.py:117`). Both tests pass no matter how the validators treat Unicode digits. The file is new in this diff.
- **Risk**: The two cases guard nothing. If a later edit weakens the ASCII-only first-digit rule, these tests still pass. The non-ASCII mojibake can also spread when the file is copied or re-saved.
- **Suggested Fix**: Use the escape `"\u00b2"` in both places so the source stays ASCII. Optionally add `"1\u0663"`, an ASCII digit followed by an Arabic-Indic digit. That case shows `parse_iterations` accepts later Unicode `\d` digits, which `int()` normalizes. Changing that pattern to `[1-9][0-9]*` would reject them.

## Resolution Log

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: The v2 public export carries one record per Azure task, and a real warning-only release build has more tasks than the 48,000-character GitHub input budget can hold.
- **How verified**: In-memory measurement with the repository's own `_jobs` and `_public_jobs` and the `encode_evidence` encoding steps. 1,380 tasks gave 74,232 characters and 1,035 tasks gave 56,828, against a 48,000 limit. Code reading of `encode_evidence`, `decode_evidence`, `release-notes.yml`, the UnitTests matrix and the warning test fixtures.

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: The fixtures do not contain the intended Unicode digit, so the negative cases pass trivially.
- **How verified**: Byte dump of lines 50 and 351, and a three-step cp1252/UTF-8 reversal that yields U+00B2. A scan of all 59 changed paths found no other mis-encoded text.

## Driver follow-up, 2026-10-07

This section preserves the original review above and records its subsequent
resolution. The reviewed corrective patch is relative to
`a970c2409d1979c272fa076d2b49b6b605de9e3c`, based on master
`861c3a1e14a9511b5604563ff1e3976cefa82e90`.

### Review decision

- Effective effort: Low, selected by the driver for the unpublished corrective
  delta on top of the retained full-PR reviews. This is not a new six-lens or
  three-model review of the entire release implementation.
- Primary model: `gpt-6-astra`.
- Reviewer: one harness-selected `code-review` capability, with retained
  same-PR context. Its exact model ID was not exposed in the result.
- Scope: all 17 unpublished implementation/test/documentation paths, then the
  narrower evidence-compaction correction. The warning state machine and
  provenance validation carry most of the branching and failure-path risk.
- User direction: complete remaining blockers, push the validated local work,
  and run the SynapseML readiness loop. No production publication, credential
  changes, approvals or merges were requested by these corrective steps.

### Issue 1 resolved

The ledger retains every validated job and task. Public warning evidence now
exports per-job outcome counts, allowlisted advisory groups and required
publication outcomes, with one `job_records_sha256` per build. That digest
links to the complete canonical ledger job records, including every task,
attempt, ID and execution window. It is an audit link, not authentication.
The producer still validates the full timeline and checks a second read for
changes before summarizing. Clean version-1 evidence keeps its original shape.

The encoded limit is 60,000 characters, with a separate 65,535-character check
covering the generated plan, approval, JSON escaping and dispatch envelope.
Both encoding and decoding enforce both limits. Boundary tests demonstrated
that the combined-budget check was absent before the correction.

The first follow-up review found that the production-size fixture reused
timestamps, overstating gzip compression. Distinct valid job/task windows
reproduced two failures out of four cases even after raising the encoded cap.
The run-level digest replaces hundreds of separate high-entropy digests rather
than raising the limit again. The fixture now uses distinct seven-digit Azure
timestamps and sequential task windows.

The reviewer exercised the actual `verified_evidence` path with in-memory
storage and external-service substitutes:

| Runtimes | Advisory pattern | Encoded characters | Full request characters |
| --- | --- | ---: | ---: |
| 2 | Sparse | 33,692 | 35,180 |
| 2 | Widespread | 35,424 | 36,912 |
| 3 | Sparse | 50,420 | 52,365 |
| 3 | Widespread | 53,028 | 54,973 |

Every case passed encode/decode. The digest equaled SHA-256 of the complete
canonical receipt jobs. Missing/malformed digests, obsolete per-job digests and
a warning-only digest attached to a clean run were rejected. The final narrow
review reported no significant issues.

Windows uses the existing decoded evidence-file guard for large inputs because
one environment variable cannot exceed 32,767 characters. Linux regressions
still use the actual encoded environment-input path used by GitHub Actions.

### Issue 2 resolved

The fixtures now use explicit `\u00b2` escapes and include Arabic-Indic digits.
Iteration parsing uses `[1-9][0-9]*`, so `1\u0663` cannot be normalized silently
to 13. Positive canonical-ASCII cases remain supported.

### Additional remote findings resolved

- The port-tag recovery message requires independent commit identification,
  detached checkout and full-release preflight. Operators create only missing
  local tags, never replace existing ones, and publish through the guarded
  atomic `push-tags` path. The message no longer recommends raw remote pushes.
- The previous-tag helper uses a non-failing `awk` filter. Empty or
  suffix-only tag lists reach their intended handling. The new regressions
  failed on the original helper.
- Both warning-only completion threads are covered by the task allowlist,
  required publishing-task checks, current-attempt windows, mixed clean/partial
  runs and old failed-ledger reconciliation without another build submission.

### Corrective validation and limits

- Final Linux command:
  `python -m pytest scripts/release tools/ci/tests/test_pipeline_yaml.py -q --tb=short -p no:cacheprovider -k 'not current_committed_notebooks_are_admissible'`
  passed 1,202 tests. One opt-in live SBT check was skipped. The committed
  notebook test was deselected only because WSL Git cannot resolve the
  Windows-created worktree pointer; it passed in the native Windows suite.
- Targeted evidence and public-release tests: 145 passed.
- Native Windows warning/notebook tests: 140 passed, including committed
  notebook admissibility and large decoded-file transport.
- Pinned Black 22.3.0: all ten changed Python files passed.
- The same candidate aggregate wheel passed 16 image, JVM transformation and
  save/load cases on Spark 3.5.0 and 16 on Spark 4.1.1 with their corresponding
  JVM packages. SHA-256:
  `064eeb6b554d9044cbc25542ff00c12e96dfe26ff773792b3bf05bb621f4081f`.
  The original wheel failed three and five cases respectively; retained
  artifact hashes and JUnit results were independently rechecked.

These checks do not establish whole-library cross-runtime compatibility, a
live GitHub App installation, production service permissions or completion of
the requested pre-merge release trial. Current-head hosted CI, required human
review, final source-bound candidate qualification and approved publication
remain separate gates.
