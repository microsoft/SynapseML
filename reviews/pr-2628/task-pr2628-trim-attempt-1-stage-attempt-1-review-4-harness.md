# Release verification simplification audit

## Review contract

- PR: microsoft/SynapseML#2628. Head: `800589ac8dfe4bd23596cb174c9ec997e574b6ae`.
- Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`. Local ancestry confirms 0 behind, 10 ahead.
- Human-selected Medium review, six disjoint assignments, harness-default models. This report is assignment 4 only, not the fleet verdict.
- Reviewer: `gpt-6-astra`. Human overrides prohibit nested reviewers, source edits, live calls, commits, pushes and tags.
- Read all nine owned files completely. All are additions against the base, totaling 6,682 lines: 2,729 production and 3,953 tests.
- High branching comes from historical versus approved plans, destination-specific receipts, retries and malformed inputs. Keep those boundaries; reduce repeated implementations.
- Audit only. No implementation is authorized or claimed. Older reports remain untouched.

## Complete-file coverage

All paths below are under `scripts\release\`; ranges refer to the reviewed head.

| File | Entire range reviewed | Conclusion |
| --- | --- | --- |
| `verify_release.py` | 1-1436 | Simplify repeated feed-row emission; fix malformed metadata and transport bounds. Preserve inventory/evidence separation. |
| `release_guard.py` | 1-783 | Reuse file hashing and remove provably redundant producer checks. Preserve approvals, integration checks and exact tag pushes. |
| `release_dbc.py` | 1-510 | Native round-trip, output stripping, source binding, immutable upload and reconciliation are substantive. No large safe production cut found. |
| `test_verify_release.py` | 1-940 | Largest safe reduction: repeated response, HEAD fallback, scope and coordinate scenarios. |
| `test_release_guard.py` | 1-936 | Small precondition-table reduction. Keep real Git merge-method coverage and independent published-byte fixtures. |
| `test_release_dbc.py` | 1-594 | Consolidate transport and publication cases, not the semantic workspace fake or notebook cases. |
| `test_release_dbc_contract.py` | 1-342 | Shorten repeated step lookup; retain ordering, retry handoff and notes CLI tests. |
| `test_release_public.py` | 1-838 | Consolidate budget, job-role and Boolean outcome tests. Most existing mutation matrices are already useful. |
| `test_artifact_content.py` | 1-303 | Retain. Independent CDN/Central bodies and changed-byte cases are not redundant with metadata-only checks. |

## Ranked reductions

Estimates are net physical lines after normal formatting, not deletion quotas. Non-overlapping owned-file opportunities total about **250-340 lines**, mostly tests. Bug fixes and new regressions may reduce that saving.

1. **Table-drive repeated verifier tests, net -150 to -190.** `test_verify_release.py:33-62,104-214,331-368,500-598,743-812`. Reuse the existing checker fake with method aliases; combine response and HEAD/GET cases, explicit/inferred scope cases, and public/private coordinate assertions. Preserve separate expected URLs, exact methods, negative responses and historical-versus-bound behavior. Evidence: these blocks repeat the same setup with different status codes, scope flags or coordinates; they do not exercise distinct implementations.
2. **One feed-family loop, net -22 to -30.** `verify_release.py:919-958` repeats the same `add` operation for OSS/Internal UPack and pip. Iterate `upack,pip`, then explicitly `oss,internal`, retaining that row order, selected-family checks, approved package names, target versions and the `internal` flag. No new helper, file or dispatch framework is needed. Evidence: only those values differ between the four branches.
3. **Reuse the existing file identity helper, net -8 to -10.** Replace `release_guard.py:469-475` with `_file_identity(path, "pypi/" + expected)` after the wheel metadata checks. Keep `440-468` intact. Remove the constructed-inventory recheck at `532` and duplicate build-ID check at `596-597`, whose authoritative checks remain in `_file_identity`, `required_public_maven_paths` and `_maven_identity`. Evidence: the Blob loop already visits exactly the required paths; wheel hashing duplicates `495-514` but lacks its stable-stat check. This does not make metadata checking and hashing atomic.
4. **Merge equivalent public-evidence test setup, net -20 to -35.** `test_release_public.py:438-470,541-587,607-653`. Parameterize the two inclusive budget boundaries, primary/secondary job outcomes and accepted/rejected Boolean representations. Retain both encode and decode checks, the secondary-skipped exception, no-extra-queue assertion and canonical public parameters. Keep privacy allowlists, old-proof rejection and complete three-target round-trip coverage elsewhere unchanged.
5. **Consolidate DBC transport/publication cases, net -25 to -35.** `test_release_dbc.py:315-328,343-366,388-418,474-518`. Share scenario setup for first upload versus existing immutable bytes, matching versus foreign plan, and sanitized public/workspace errors. Preserve format-specific error context, no-upload assertions, post-upload byte verification and failure cleanup. Do not replace `FakeWorkspace:154-202` with a success stub.
6. **Combine guard precondition cases, net -10 to -20.** `test_release_guard.py:386-424,522-537`. A small case table can retain invalid expected IDs, empty/duplicate tags, Git validation failures and changed source with one call trace. Keep the distinction between no Git calls and no staging/push calls. Leave the real merge/squash/rebase tests at `46-190` intact.
7. **Reuse local step-name lists, net -12 to -18.** `test_release_dbc_contract.py:201-219,241-250`. Use the same local `names.index(...)` approach already used at `174-200`, rather than repeated multi-line enumerations. Keep every ordering, artifact-name, condition, attempt and producer-reuse assertion.

## Cross-owner interfaces, not additional owned-file savings

- `release_ops.py:846-900` independently reconstructs the rows already describable by `verify_release.py:771-982`. The parent can replace that duplicate with the existing offline `_InventoryChecker` path, about -45 to -50 lines, after checking all repository/family/schema combinations. Do not delete the real inventory response validator.
- Coordination update: the engine reviewer proposes this same removal and reports 66 matching bound-plan cases, including public schemas 2/4 and private plans. No conflict with reduction 2: preserve `_check_plan`'s signature, row identities/order and `expected_commit` fields while shortening feed emission. Build required keys with `ops._row_key` and `.get("expected_commit")`; use `strict=True` and `_InventoryChecker`, never live `Checker` or synthetic success statuses. Count this cross-owner saving once. The 66-case result is reviewer-provided, not independently rerun here.
- `verify_release.py:1116-1130` validates inventory before `release_ops.py:3469-3473` validates it again. `decode_evidence` then precedes another full validation in `release_guard.py:700-708`. Choose one authoritative validation per boundary, but preserve comparison against the separately approved notes plan and propagate `max_age_seconds`; simply deleting the first call changes that parameter's behavior. Savings are small, not grounds for a receipt rewrite.
- There are two identical redirect-denial classes at `verify_release.py:235-237` and `release_dbc.py:32-34`. Sharing a class saves only a few lines. Do not introduce a generic HTTP client or couple DBC authentication to the orchestration module to chase that saving.

## Concrete bugs and gaps

| Priority | Exact location | Evidence and minimal repair |
| --- | --- | --- |
| P2 | `verify_release.py:591-605` | With synthetic `{"info": null}`, `Checker.public_pypi(..., True)` and the approved-plan CLI both raise uncaught `AttributeError`. The `.get` chain runs before `_pypi_release_wheel` checks shape. Validate metadata shape first and raise the existing controlled error; preserve historical metadata-only semantics. |
| P2 | `verify_release.py:427-442` | An offline response spy records `read(-1)`. The JSON path used for PyPI metadata and service inventories has no response byte limit despite bounded artifact/evidence paths. Add bounded reading with explicit oversized rejection; retain 404-only absence behavior. |
| P2 | `verify_release.py:392-424` | An offline subprocess spy confirms the ADO token request has no timeout. A stalled credential request can hang verification indefinitely. Add a finite timeout and translate timeout/failure to sanitized diagnostics rather than echoing CLI stderr. |

These are reproduced control-flow/transport findings, not claims that a digest authenticates a producer or that a public service was attacked.

## Unsafe cuts rejected

- Keep plan schemas 2 and 4 byte-for-byte identity semantics. Schema 2 remains without a DBC obligation; schema 4 requires every selected archive.
- Do not replace separate CDN and ESRP receipts with one hash, omit downloaded-byte comparison, or accept old receipts without destination proof.
- Keep exact PyPI wheel name, package/version metadata, URL restrictions, size/hash, non-yanked status and download verification.
- Do not replace the native DBC import/export and cell comparison with ZIP validity or notebook counts. Preserve markdown whitespace, removed outputs, source-commit reads and cleanup errors.
- Keep note approval, current-download rechecks, API documentation before Maven verification, approved source/runtime checks and immutable uploads.
- Keep strict incoming report allowlists, duplicate-key rejection, decoded/encoded/combined transport budgets, timeout limits and stale-evidence rejection.
- Do not trim signatures, checksums or producer inventory merely to shrink the GitHub payload. The current bounded gzip envelope is preferable to a new compact receipt schema.
- Keep evidence's honest trust boundary: hashes bind content; they do not authenticate manually supplied build facts. Structural revalidation cannot become an authentication claim.

## Evidence and minimal validation after authorization

- Baseline command below passed: **372 passed, 13 skipped** on Windows with Python 3.14.6. The skips are Linux Bash handoff cases; no live producer or native Databricks runtime was exercised.
- Parent-provided current-head baseline, not independently rerun: 1,778 Linux regression passes plus 11 native Git cases; 1,669 hosted release tests including SBT/history; Azure run `239694763` passed with 3,695 tests, 22 unchanged skips and cache-only warnings. Hosted Python 3.11 supplies branch-runtime evidence; local WSL Python 3.12 and Windows Python 3.14 do not replace it.
- Offline probes used only synthetic metadata, mocked responses and mocked subprocesses. No service calls or source-file changes were made.
- Pinned Black 22.3.0 check passed for all nine owned files through the existing WSL environment. The earlier Black 26.5.1 differences are not repository-format findings. Nothing was formatted.
- Completed baseline command below; do not rerun this broad selection during audit. After authorization, select only cases affected by actual edits and use the shared broader baseline:
  `python -m pytest -q -p no:cacheprovider scripts\release\test_verify_release.py scripts\release\test_release_guard.py scripts\release\test_release_dbc.py scripts\release\test_release_dbc_contract.py scripts\release\test_release_public.py scripts\release\test_artifact_content.py`
- Add focused cases for malformed PyPI `info`, metadata at/over the byte bound and token timeout sanitization. Preserve independent downloaded-byte fixtures, both plan identities, native-cell failures and notes approval refusal.
- If the parent consolidates cross-owner inventory/validation, select affected cases in `test_plan_evidence.py`, `test_release_ops.py` and `test_release_defaults.py`. Keep real-Git notebook/history tests on Windows because WSL Git cannot resolve this worktree metadata. Existing Linux evidence covers the Bash-only cases.

Pending parent authorization. Proposals and bugs remain unresolved; this is not merge-readiness approval.

## Authorized resolution
- Correction: Medium was the primary agent's estimate, not a human-selected tier. The later instruction authorized these owned-file edits.
- Implemented safe test consolidation, feed-row loop reuse and existing wheel/file hashing reuse across seven owned Python files: 291 insertions, 498 deletions, **net -207 lines** after adding defect regressions. `release_dbc.py` and `test_artifact_content.py` remain unchanged.
- Fixed all three reproduced defects. JSON reads allow exactly 16 MiB, matching the existing Azure service-response bound, and reject the next byte; only HTTP 404 means absence. ADO token acquisition uses the existing 60-second request timeout convention with sanitized failures. Malformed PyPI metadata now fails through the CLI's controlled error path; valid historical checks remain metadata-only.
- Preserved `_check_plan` interface, lazy selected-checker access, row order and commit fields. Left cross-boundary validation/max-age checks, approved-plan comparisons, separate destination receipts, schemas 2/4, native-cell checks and immutable gates unchanged.
- Effective runner commands below used existing WSL Python 3.12 with `TMPDIR=/dev/shm`, `PYTHONDONTWRITEBYTECODE=1`; no broad suite, native-Git history or live-service rerun occurred during implementation. No owned-file blockers, other-owner edits, commits, pushes, tags or nested agents. Parent owns broad final validation and aggregation.
- `python -m pytest -q --tb=short -p no:cacheprovider scripts/release/test_verify_release.py`: **88 passed**.
- `python -m pytest -q --tb=short -p no:cacheprovider scripts/release/test_release_guard.py -k "maven_receipt or pypi_receipt or blob_receipt or push_tags_validation"`: **30 passed, 81 deselected**.
- `python -m pytest -q --tb=short -p no:cacheprovider scripts/release/test_release_public.py scripts/release/test_release_dbc.py scripts/release/test_release_dbc_contract.py scripts/release/test_artifact_content.py -k "transport_budgets or job_outcomes_preserve or completed_boolean_representations or existing_archive_is_never or build_reuses_existing_bytes or transport_errors_name or native_dbc_publication_reuses or test_artifact_content"`: **85 passed, 131 deselected**.
- `python -m pytest -q --tb=short -p no:cacheprovider scripts/release/test_verify_release.py scripts/release/test_plan_evidence.py -k "test_run_ or internal_skip or bound_report or repository_and_family_selection"`: **7 passed, 116 deselected** on the final feed loop.
- `python -m black --check --required-version 22.3.0 scripts/release/verify_release.py scripts/release/release_guard.py scripts/release/release_dbc.py scripts/release/test_verify_release.py scripts/release/test_release_guard.py scripts/release/test_release_dbc.py scripts/release/test_release_dbc_contract.py scripts/release/test_release_public.py scripts/release/test_artifact_content.py`: **9 files unchanged**. Scoped `git diff --check` passed.
