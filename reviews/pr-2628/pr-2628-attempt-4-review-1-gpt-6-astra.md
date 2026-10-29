# PR 2628, attempt 4, round 1

**Result: changes required.** One high-priority and two medium-priority findings.

- Theme: Broad sweep. Model: `gpt-6-astra`.
- Reviewed base HEAD: `a617374f54c7629da3216112ce8c1196bd735224`.
- Scope: current tracked dirty delta plus all six untracked implementation/test files under `scripts\release`. Reviewed public schema-2 admission, local profile configuration, bootstrap preview/dispatch/tagging, evidence export, workflow integration, tests and current operator documentation. Historical report prose was excluded; deletions were considered only as integration changes.
- Requirement: public `1.2.0` for `master`, `spark4.0` and `spark4.1` before merging the automation PR, with no private information in public inputs, evidence or packages and no invented approval.

## Findings

### R1-1. High: public evidence still transports unrestricted timeline strings

Locations: `scripts\release\verify_release.py:808-847`; `scripts\release\release_ops.py:1869-1889,2915-2921,2985`.

The new public export validator checks job field names, but delegates their values to `_jobs`, which accepts any nonempty `id` and `name`. Evidence collection copies those strings from the producer timeline. An allowlisted envelope therefore still transports arbitrary operator or private metadata through these fields.

Reproduced offline using `test_release_public.producer_report(cli)`. Independently replace the first run's first job `id` or `name` with `synthetic-nonpublic-marker`, then call `decode_evidence(encode_evidence(report))`. Both variants pass and preserve the marker. No private input was used.

Require a bounded, typed public job identity and derive or validate names against the public producer contract. Do not export unrestricted timeline labels. Add negative cases for values inside approved fields, not only additional fields.

Resolution, 2026-09-24: `release_ops._public_jobs` now admits at most 256 jobs with unique, nonzero, canonical 36-character GUID IDs. Collection retains every job and its outcome, replaces non-publication display labels with the fixed public role `Maven pipeline job`, and requires exactly one successful `Release` job as defined by `pipeline.yaml`. Public evidence admission accepts only these two role names. Failed/canceled jobs still block completion; skipped non-publication jobs remain represented, not filtered. Both compressed export and import reject mutated job values.

Exact regressions in `scripts\release\test_release_public.py`: `test_public_job_values_reject_unapproved_metadata`, `test_public_timeline_labels_are_replaced_without_dropping_jobs`, `test_public_job_contract_requires_unique_jobs_and_successful_release`, `test_public_job_export_does_not_hide_unsuccessful_required_jobs`, `test_public_secondary_job_outcomes_are_not_filtered_by_label`, and `test_public_job_coverage_is_bounded_without_truncation`.

Follow-up validation, 2026-09-24: all R1-1 regressions remain green after the subsequent owned-module cleanup. The expanded 576-test command recorded under R1-2 includes GUID bounds, value-mutation refusal, retained job coverage and required publication-job success.

### R1-2. Medium: accepted Azure boolean strings block producer evidence export

Locations: `scripts\release\release_ops.py:2739-2750,2904-2910`.

`_validate_build` deliberately accepts both boolean `true` and string `"true"` for an approved boolean parameter. `_evidence_build` retains the returned representation, but the new public comparison requires exact equality with the operation's boolean. A run accepted as complete can consequently fail evidence export without any changed approval or request.

Reproduced offline by setting the completed public producer fixture's `templateParameters["publishRelease"]` to `"true"`. Driver `status` returns exit `0` with `complete=true`; `verified_evidence` then rejects it with `Public producer evidence contains unapproved parameters`.

Normalize already-validated boolean values when constructing evidence, or compare allowed values with the existing typed equivalence while preserving exact key restrictions. Cover both service representations through status and public export.

Resolution, 2026-09-24: `_evidence_build` rechecks each approved parameter through `_parameter_equal`, then emits an actual boolean for an approved boolean parameter. Exact public key/type validation remains unchanged. Boolean `true` and service strings `"true"`, `"TRUE"` and `"TrUe"` now pass completed status, producer export, compressed round-trip and evidence validation without another queue request or a changed plan ID. Wrong values and noncanonical wire parameters still fail.

Exact regressions in `scripts\release\test_release_public.py`: `test_public_completed_boolean_representations_export_canonical_evidence`, `test_public_completed_boolean_parameter_still_rejects_wrong_values`, and `test_public_wire_booleans_remain_canonical_and_keys_remain_allowlisted`.

Validation for R1-1/R1-2: before the fixes, `python3 -m pytest scripts/release/test_release_public.py -q -k 'job_values or timeline_labels or job_contract or job_export or completed_boolean' --tb=short` reported **12 failed, 9 passed**; the same selection after the fixes reported **21 passed**. Expanded validation, `python3 -m pytest scripts/release/test_release_ops.py scripts/release/test_release_public.py scripts/release/test_plan_evidence.py scripts/release/test_release_guard.py scripts/release/test_verify_release.py -q --tb=short`, reported **464 passed**. Commands ran in WSL against the local worktree with synthetic/fake services. Black 22.3.0 and `git diff --check` passed for the three touched Python files. No external API calls, public writes, commits or pushes were performed.

Follow-up validation, 2026-09-24: `python3 -m pytest scripts/release/test_release_matrix.py scripts/release/test_release_plan.py scripts/release/test_release_ops.py scripts/release/test_verify_release.py scripts/release/test_plan_evidence.py scripts/release/test_release_guard.py scripts/release/test_release_public.py scripts/release/test_release_config.py -q --tb=short` reported **576 passed** in WSL after subsequent owned-module cleanup. This includes all R1-1/R1-2 regressions, completed-build service boolean normalization, strict public wire keys/types, and unchanged approved plan IDs. Black 22.3.0 passed for all 13 owned Python files; the owned-source whitespace check also passed. No external API calls or public writes were performed.

### R1-3. Medium: bootstrap accepts broken versioned documentation

Locations: `scripts\release\bootstrap_release.py:21-24,155-177,231`; integration: `.github\workflows\website-deploy.yml:29-61`.

`check_docs` requires only a nonempty sidebar object and a nonempty documentation tree. It does not resolve sidebar document references. Bootstrap's required check list also omits the website build, so successful Azure and Compile checks do not establish that the primary documentation can build.

Reproduced with mocked Git reads: matching version metadata, sidebar `{"docs":["missing-document"]}`, and a tree containing only `unrelated.md` pass `check_docs`. A separate mocked check response containing both required successes and a failed website `build` also passes candidate CI admission. These conditions leave the tag path unblocked despite broken documentation.

Require successful current-head website validation for the primary candidate and validate the versioned snapshot, or run equivalent complete documentation validation before permitting tag creation.

Resolution, 2026-09-24: bootstrap now queries the specific `website-deploy.yml` workflow and requires its latest exact-head canonical-source run to have completed successfully. It rejects missing, failed, pending, incomplete, fork, wrong-source and wrong-workflow responses. The existing website build validates the versioned snapshot and sidebar references; the bootstrap does not duplicate Docusaurus's parser. Primary version metadata, sidebar presence and snapshot checks remain in place.

Validation: `python3 -m pytest scripts/release/test_release_bootstrap.py scripts/release/test_release_workflows.py -q --tb=short` reported **51 passed**, including eight website-evidence refusal cases. These use synthetic workflow metadata; actual release-candidate website validation remains required before tagging.

## Evidence and limits

The baseline synthetic producer report round-tripped successfully before mutation. Reproductions used local fixtures with no external API calls or production writes. No source fixes, commits, pushes, approvals, tags or packages were created by this review.

Candidate PRs are not yet populated. This review does not establish live dispatch registration, candidate CI success, signing approval, publication, package-content confidentiality or consumer compatibility. Parent-run suites were not claimed as reviewer results. Local checks are not production proof; this artifact records round 1 only.

## Reviewer-verified closure, 2026-09-24

**Clean for this recheck: R1-1, R1-2 and R1-3 are closed.** This supersedes the original changes-required result for these findings. Original findings and implementer resolution notes remain intact. Round 2 may start.

Rechecked the current dirty tree over the same base HEAD, using `gpt-6-astra`, without expanding into an unrelated audit:

- R1-1: confirmed public job normalization before receipt storage and strict validation on evidence admission in `scripts\release\release_ops.py:1919-1956,2150-2151,2991-2994`. Unapproved labels no longer reach public evidence, job IDs are bounded and typed, and normalization retains job outcomes rather than hiding failures.
- R1-2: confirmed validation precedes boolean normalization in `scripts\release\release_ops.py:2801-2818`; public wire validation remains exact. Accepted service boolean representations now complete status and evidence export with unchanged plan identity and no additional queue request.
- R1-3: confirmed the primary candidate requires the latest matching website workflow success in `scripts\release\bootstrap_release.py:136-167`. The workflow runs the actual website tests/build at `.github\workflows\website-deploy.yml:56-61`, while snapshot metadata checks remain enforced.

Reviewer-run regression command:

```text
python -m pytest scripts\release\test_release_public.py -q --tb=short -p no:cacheprovider -k "public_job or public_timeline or public_secondary_job or public_completed_boolean or public_wire_booleans"
```

Result: **27 passed, 34 deselected**. A separate in-memory check through `check_candidate_ci` rejected all eight reported website failure cases plus an unsupported event, accepted successful PR/manual runs, and confirmed that only the primary target requests website evidence. External access and subprocess execution were blocked during that check.

No remaining defect was found in these three fixes. This closure does not claim the parent SBT/full-suite result, live candidate validation, publication or package safety. Only this artifact was edited.
