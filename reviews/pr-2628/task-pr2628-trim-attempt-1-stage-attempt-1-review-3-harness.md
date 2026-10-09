# Release driver simplification review

PR: microsoft/SynapseML#2628. Review 3, attempt 1, stage attempt 1.
Head: `800589ac8dfe4bd23596cb174c9ec997e574b6ae`.
Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`, target `master`.
Human contract: Medium, six disjoint reviews, harness-default model. This reviewer is `gpt-6-astra`.
Parent owns aggregation and implementation authorization. No source edits, live calls, Git mutations, or nested agents.

## Coverage

Every current line and the complete base-to-head change in these four files was reviewed.
All four are additions, totaling 9,174 lines; owned source files still match the supplied head.

| File under `scripts\release` | Lines reviewed |
| --- | --- |
| `release_ops.py` | 1-3973 |
| `test_release_ops.py` | 1-4148 |
| `test_release_warnings.py` | 1-700 |
| `test_plan_evidence.py` | 1-353 |

Read worktree `AGENTS.md`, branch/release guidance, runtime pins, and adjacent verifier/configuration definitions where needed to prove reuse.
The driver already has one execution engine. Public/private differences in `_operation` and receipts are distinct transport contracts, not duplicate engines.

## Ranked reductions

Estimates are net deleted lines after replacement code, not promises based on moving code between files.

1. **Use the existing inventory description instead of re-enumerating coordinates.**
   `release_ops.py:846-900`, approximately **45-50 lines**.
   Replace `_required_rows`' parallel tag/module/family loops with rows from `verify._check_plan` using its existing `_InventoryChecker`, keyed by `_row_key` and `expected_commit`.
   An offline comparison produced identical mappings for 66 bound plans across every target, repository/family subset, and current/saved public schemas.
   Keep `_inventory`'s duplicate/missing/extra-row, status, source-commit, and completeness checks.
   Coordinate with the verifier owner through the parent. Keep independent expected-coordinate assertions; a test comparing two calls to the same descriptor would be tautological.

2. **Construct final public fixture artifacts once.**
   `test_release_ops.py:427-494`, approximately **25-40 lines**.
   `FakeRemote.succeed` builds placeholder hashes/sizes, copies them into blob artifacts, then overwrites every Maven/PyPI value with `publish_artifact` facts across four repeated public-pipeline branches.
   Use one branch, construct the destination/path lists, and populate each artifact from its independently stored synthetic bytes directly.
   Preserve collection-specific content strings, primary-wheel handling, saved-plan DBC behavior, and the separate real producer fixture at 2170-2252.
   Proof obligations are `test_fleet_public_content_fixture_hashes_are_destination_specific`, receipt-tampering tests, and the measured real-guard receipt test. Never derive downloaded bytes from a subsequently edited receipt.

3. **Reuse group selection and duplicate destination-shape validation.**
   `release_ops.py:1222-1241,1418-1431,2811-2818,2929-2938,3434-3466`, approximately **30-40 lines**.
   `_operation_group` already implements the selector repeated in state validation and adoption; adoption can pass its existing `candidate`.
   Share only the identical destination structure/name/project checks between state and exported evidence validation.
   Retain their distinct error contexts and the evidence validator's stricter production/rehearsal ID rules. Do not silently tighten historical ledger acceptance by substituting the entire evidence validator.
   Keep atomic conflicting-adoption tests, grouped retry-history tests, and production/rehearsal destination corruption cases.

4. **Share budget-fixture construction, not budget assertions.**
   `test_release_ops.py:3618-3633,3665-3671` and `test_release_warnings.py:494-508,535-542`, approximately **20-30 lines**.
   Reuse same-file helpers for the identical extra Maven artifact inventory and Windows-file versus environment evidence arguments; warnings already imports fixtures from `test_release_ops`.
   Keep both clean and warning end-to-end notes-guard tests, their separate payload limits, two/three-target cases, realistic timestamps, and widespread-warning cases.
   Do not replace the 70-job, over-1,300-task warning fixture with a tiny compressed example. That would stop measuring the required output budget.

5. **Use the existing strict JSON parser.**
   `release_ops.py:113-134`, approximately **17 lines**.
   Call `release_config.strict_json` inside `_json`'s existing labeled exception wrapper; remove `_object_pairs` and `_invalid_constant`.
   Eight offline cases matched existing success/refusal behavior for valid input, duplicates, nonfinite values, malformed JSON, invalid encoding, and wrong input types.
   The shared parser additionally converts excessive nesting into a controlled failure. Preserve the sanitized `ReleaseError` boundary rather than exposing raw input.

6. **Delete genuinely unreachable envelope compatibility, not saved-format support.**
   `release_ops.py:3485-3497,3582-3586,3629-3635`, approximately **9-12 lines**.
   Producer-envelope schema 2 cannot succeed: public plans require envelope 3; private plans reject partial success, then the final schema-2 condition rejects their clean runs.
   Accept `{1, 3}` explicitly and remove the impossible late schema-2 branch and redundant version predicate.
   Twelve offline checks across current public, saved public, private publisher, and private Maven plans confirmed the accepted envelope versions are respectively `[3], [3], [1], [1]`.
   This is not removal of saved public plan schema 2 or ledger schema 1. Their behavior must remain.
   Small separate cleanup: `lock_bytes` at 1535/1687 is never read; `_acquire`'s return at 1571 exists only for that unused assignment. Keep `owned_locks` and ownership comparisons.

## Confirmed test defect

`test_release_ops.py:109-110,210-222` routes public PyPI inventory through the `"maven"` family.
Setting `FakeRemote.missing = {("oss", "pypi")}` therefore reports PyPI `OK` and `inventory_complete=True`; reproduced offline.
This weakens the PyPI part of `test_internal_base_requires_cdn_not_new_central_publication` at 2518-2551.
Fix `public_pypi` to query `"pypi"` and include `"pypi"` in `present`'s existing Maven-family fallback so aggregate missing-Maven setup still works.
Add destination-parametrized visibility coverage for Maven Central, PyPI, and DBC. No production defect was established by this finding.

## Rejected unsafe cuts

- Keep immutable-coordinate refusal and the full suffix/signature/checksum namespace scan. Aggregate `MISSING` is not whole-namespace absence.
- Keep unknown-intent persistence, stale-adoption checks, claim initialization recovery, exact-plan approval, and stop-on-authoritative-read-failure behavior.
- Keep the second build/timeline observation during evidence export, warning job/task/build windows, receipt/download comparison, and invocation-local content observations.
- Keep ledger migration, retry history, request reconstruction, and legacy timestamp handling. No evidence establishes that their supported consumers can be dropped.
- Do not merge raw-task and compressed-summary validators merely because both explain warnings. They validate different representations and enforce the export budget.

## Verification and minimal implementation gates

Parent-supplied current-head baseline, not independently rerun here: 1,778 Linux regressions plus 11 native Git cases; 1,669 hosted release tests including SBT/history; Azure validation with 3,695 passes, 22 unchanged skips, and cache-only warnings.
Hosted tests used branch Python 3.11. Keep native Git cases on Windows; WSL cannot resolve this Windows worktree's Git metadata. Do not repeat broad validation for this audit.
Python 3.14.6, pytest 9.1.1. Socket connections were blocked in review probes.
Focused pytest selection across the three owned test files: **16 passed, 439 deselected**.
Selection: `azure_timestamps_work_with_legacy_iso_parsers or direct_azure_reads_send_the_cached_token or manifest_zip_is_read or artifact_redirect_drops_auth or evidence_cannot_approve_a_different_or_partial_release or matching_tag_family_at_wrong_commit`.
Ran with `pytest.main(["-q", "-p", "no:cacheprovider", <three owned test paths>, "-k", <selection>])`; plugin autoload and bytecode writes were disabled.
Also passed 66 coordinate-map comparisons, eight parser comparisons, twelve envelope-version checks, and reproduced the PyPI fixture defect.
The broader owned-suite invocation was interrupted without a final result; it is not counted as passing. Its temporary-Git-commit test was excluded.
After authorized edits, run `python -m pytest -q scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -k "not test_temp_git_tags_supply_the_exact_commits_used_by_cli"`.
For coordinate-description reuse, also run the explicit DBC/legacy-schema and row-coverage tests in `test_release_dbc_contract.py`, `test_release_matrix.py`, and `test_release_public.py`.
Require unchanged accepted saved plans/ledgers, zero new submissions on every refusal path, and unchanged measured destination/budget assertions before accepting the reductions.

## Authorized resolution
Implemented all six reductions in owned files, including separate PyPI visibility and Central/PyPI/DBC pending-to-complete coverage. Saved plan schema 2, ledger migration, strict exported feed IDs, realistic budgets, and all submission guards remain.
Measured code diff: `release_ops.py` -103 lines; `test_release_ops.py` +25 including new regressions; `test_release_warnings.py` -19; `test_plan_evidence.py` unchanged. Net **97 lines removed**, excluding this append.
WSL Python 3.12.3, pytest 9.1.1; TMPDIR=/dev/shm, bytecode/cache/plugin autoload disabled. The following pytest arguments ran through a socket-blocked `pytest.main`, with no live access:
`python -m pytest -p no:cacheprovider -q scripts/release/test_release_ops.py -k "destination or adoption or retry_history or legacy_state or legacy_sibling or claimed_v2 or initial_save or state_entry or approval_requires or release_json or invalid_verifier_coverage or pure_producer_evidence or fleet_r1 or fleet_r2 or fleet_r4 or public_maven or public_release_completes or public_notes_export or internal_base_requires or batched_families"`: **91 passed, 257 deselected**.
`python -m pytest -p no:cacheprovider -q scripts/release/test_release_warnings.py scripts/release/test_plan_evidence.py scripts/release/test_release_dbc_contract.py scripts/release/test_release_matrix.py scripts/release/test_release_public.py -k "production_sized or mixed_clean or verified_nonpublishing or public_content or download_fixture or public_download or new_public_plans or saved_public_plan or preview_discloses or private_package_names"`: **57 passed, 217 deselected**.
Native Windows Python 3.14.6: `python -m pytest -p no:cacheprovider -q scripts\release\test_release_ops.py -k release_json`: **7 passed, 341 deselected**.
Pinned Black 22.3.0: `python -m black --check scripts/release/release_ops.py scripts/release/test_release_ops.py scripts/release/test_release_warnings.py scripts/release/test_plan_evidence.py`: **passed, four unchanged**.
Independent original-head comparisons passed for 66 inventory mappings and six exact public fixture manifest/byte/metadata combinations. New PyPI assertions failed before the fix and pass afterward; the nesting case now uses the shared parser's portable 20,000-level regression input.
No unresolved implementation findings. No outside-ownership edits, live calls, nested agents, commits, or pushes. Parent owns final freeze and full validation.
