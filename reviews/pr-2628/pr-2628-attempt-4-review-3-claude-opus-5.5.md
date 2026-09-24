# PR 2628, attempt 4, round 3

**Result: changes required.** One medium and four low findings. None blocks package publication. The delta rechecks of the bounded website-lock, fork-guard and first-release docgen fixes found no defect in those fixes; R3-5 is a documentation gap they expose.

- Theme: edge cases and robustness. Model: `claude-opus-5.5`.
- Reviewed base HEAD: `a617374f54c7629da3216112ce8c1196bd735224`.
- Scope: the current tracked dirty delta plus the untracked implementation and test files under `scripts\release`. Focus areas:
  - public schema-2 allowlisted plans and evidence;
  - optional private schema-3 plans with an external profile and no defaults;
  - schema-1 plans, which are read locally only;
  - `bootstrap_release.py`: preview, dispatch, exact-source CI, website/docs checks, the atomic multi-tag push and failure recovery.

  Deleted historical review artifacts and the removed bump helper were considered only as integration changes.
- Delta recheck, requested after the first pass, covering the later bounded fixes in the same dirty tree:
  - the website docs-preview publication lock in `website\test\installDocs.test.js`;
  - the preview flag in `.github\workflows\website-deploy.yml`;
  - the explicit-false Fabric fork guard in `tools\ci\tests\test_pipeline_yaml.py`;
  - the first-release docgen fix in `scripts\bump-version.py`: finalizing the new versioned installation guide, the `--finalize-docs` recovery mode and their tests;
  - the documentation changes.
- Requirement: public `1.2.0` for `master` (Spark 3.5), `spark4.0` and `spark4.1` before the automation PR merges, with no private metadata in public inputs or evidence, and with no invented approvals, tags, packages or merges.

## Findings

### R3-1. Medium: the notes workflow rejects the bootstrap-tagged primary commit after a squash or rebase merge

Locations: `.github\workflows\release-notes.yml:70-80`; `scripts\release\README.md:84-88,134-143,252-266`; `.github\skills\synapseml-release\SKILL.md:30-39`.

Bootstrap tags `v1.2.0` at the unmerged primary candidate commit. The notes workflow refuses to run unless `refs/tags/v1.2.0` is an ancestor of `origin/master`, and tags must never be moved. GitHub squash and rebase merges create new commit IDs. Only a merge commit of the unchanged candidate head puts the tagged commit into `master`.

The local canonical `master` history has no merge commits among its last 300 first-parent commits. The repository's usual merge path therefore appears to be squash or rebase; the actual repository merge settings are unverified. The docs never say which merge method the candidate needs. They say to publish notes "after the automation merges" and add no containment check before the notes dispatch. If the tagged candidate is squash-merged, notes for `1.2.0` can never pass this workflow.

Repro, in a local synthetic repository:

1. Tag a candidate commit `v1.2.0`.
2. `git merge --squash` it onto `master` and commit.
3. The trees are identical, but `git merge-base --is-ancestor refs/tags/v1.2.0 master` (the workflow's exact check) exits 1.

Using `git merge --no-ff` instead makes the same check exit 0.

Fix:

- Before the bootstrap apply, require and document a merge method that preserves the unchanged primary candidate head's commit ID (a merge commit), and confirm that the repository settings allow it.
- Forbid amending or rebasing candidates after tagging.
- Add a `merge-base --is-ancestor` precheck before the notes dispatch, or define a reviewed containment rule for bootstrapped releases.
- Minor wording: public evidence expires after one hour (`scripts\release\verify_release.py:712,752-755,801`). "Keep verified public evidence locally" should say to regenerate the evidence just before the dispatch, as section 4 already does.

Status: a remediation is in progress and has not been reviewed. Closure is deferred to round 6.

- **Why merge commits are out.** The parent reports that the repository disables merge commits and allows squash and rebase merges; this reviewer has not verified that. A merge-commit requirement would therefore be unusable, and the parent chose a reviewed containment rule instead.
- **The rule.** `verify_primary_integration` in `scripts\release\release_guard.py` accepts either of two proofs:
  - ordinary ancestry in freshly fetched `master`;
  - canonical merged-pull-request metadata that binds the tagged head, the exact candidate branch, the canonical repositories and base `master`, plus a merge commit that is contained in `master`.
- **External facts, from GitHub's public REST documentation; no API call was made.**
  - GitHub's current REST documentation says that the associated-pull-request endpoint returns merged and open pull requests for a commit that is not in the default branch, which supports this lookup. Older third-party copies say it returns only open ones.
  - REST API version `2026-03-10` removes `merge_commit_sha`. Requests without a version header still get `2022-11-28`.
- **Round-6 closure evidence.**
  - One read-only live call against a past same-repository squash-merged pull request's head commit.
  - Refusal tests for open, fork, re-pushed, wrong-branch, wrong-base and malformed records.
  - Pinning the API version on that call, so a future change of the default version fails explicitly instead of producing a misleading "not integrated" refusal.

### R3-2. Low: a dispatch timeout gives tag-recovery guidance and echoes the command

Locations: `scripts\release\bootstrap_release.py:358-372,403-411`.

When `gh workflow run` exits nonzero, the message is "inspect workflow runs before retrying". A `subprocess.TimeoutExpired`, however, escapes to `main`, which prints `str(error)` followed by "Inspect canonical tags before retrying." A request that timed out may still have been accepted, so the operator should check workflow runs, not tags. The printed message also contains the whole command, including the plan payload and the approval argument. That data is public, but it adds about 1.6 KB of noise.

Repro, in memory with no network: stub `execute`, make `subprocess.run` raise `TimeoutExpired` for `gh workflow run`, then call `main([... "--dispatch"])`. It returns 2 with the generic tag message, gives no workflow-run guidance, and echoes the `bootstrap_plan_json=` and `approve_plan=` arguments. Retrying remains safe because of the concurrency group and the idempotent tag comparison.

Fix: catch `TimeoutExpired` in `dispatch()` and raise a fixed message such as "dispatch timed out and may have been accepted; inspect workflow runs before retrying". Add a regression test.

### R3-3. Low: deeply nested JSON inside the size limits skips the controlled refusal

Locations: the shared parser at `scripts\release\release_config.py:28-31`, and its callers at `scripts\release\bootstrap_release.py:389-411`, `scripts\release\verify_release.py:860-882`, `scripts\release\release_config.py:136-140` and `scripts\release\release_guard.py:72-83`.

For deeply nested arrays, `json.loads` raises `RecursionError`, which is not a `ValueError`, and no caller catches it. The bootstrap job pins Python 3.11. There, a 1,500-byte `BOOTSTRAP_PLAN_JSON` made of `[` characters ends in an uncaught traceback with exit code 1. It should instead exit 2 with the fixed, non-echoing message "Bootstrap refused during public plan validation". Compressed notes evidence, local profiles and the Maven plan payload behave the same way. The failure is fail-closed: no tags or builds are created.

Repro, with local Python 3.11.8:

- `bootstrap_release.main(["--plan-env"])` with that input raises an uncaught `RecursionError`.
- `decode_evidence` of gzip+base64 `b"[" * 1500` also raises `RecursionError`.

Python 3.14 behaves the same at 20,000 levels, which is still under the 60,000-byte limit.

Fix: in `strict_json`, convert `RecursionError` to `ValueError`, or limit nesting depth before parsing. Add regression tests for the bootstrap, evidence and profile paths.

### R3-4. Low: direct schema-3 inventory checks the local profile's repository, not the approved one

Locations: `scripts\release\verify_release.py:239-243,344-350,503-506,1008-1015`.

`Checker.__init__` takes `internal_repository` from the profile that is currently loaded. `_check_plan` then rebinds the feeds and `private_profile` to the plan, but not `internal_repository`. `verify_release.py --plan <schema-3> [--inventory-only]` never compares the loaded profile with the plan. With a different local profile, it checks the Internal tag rows in a repository the plan did not approve. Evidence export is not affected, because `release_ops.verified_evidence` calls `require_execution_plan`.

Repro, in memory with synthetic GUIDs and network calls stubbed out:

1. Build an Internal-only schema-3 plan under profile A.
2. Point the profile variable at profile B, which differs only in `internal_repository.id`.
3. Run `_check_plan`. Every `ado_tag` lookup uses B's repository ID.

Fix: bind `internal_repository` from `plan.private_profile`, or refuse schema-3 inventory when the loaded profile differs from the plan. Add a regression test with two profiles.

### R3-5. Low: the website lock follow-up is required and order-dependent, but the skill procedure omits it

Locations: `.github\skills\synapseml-release\SKILL.md:28-40`; `scripts\release\README.md:268-272`; `website\test\installDocs.test.js:44-56`.

After the bounded fix, the candidate website gate passes with the old lock. Master deployment then waits for a separate reviewed lock follow-up. The README describes this, but the skill's seven-step procedure ends at installation verification and never mentions it. An operator who follows the skill can therefore report the release as complete while:

- the public site still shows the previous version; and
- every master Website Deploy run fails, as designed.

The follow-up is also order-dependent. Its PR check passes only after the primary candidate's `versions.json` is on `master`. If it is opened earlier, it fails with the misleading message "update published-spark-ports.lock only after 1.1.3-spark4.0 is published".

Repro, in memory with no network. It evaluates the test file's own `validatePublicationLock` source against the real `website\versions.json` and lock:

| Scenario | Result |
| --- | --- |
| Candidate PR: `1.2.0`, old lock, preview | pass |
| Candidate merged to `master`, strict | fail, as documented |
| Lock follow-up while `master` is still `1.1.3`, preview | fail, with the message above |
| Lock follow-up after the candidate merges, preview | pass |
| Lock follow-up after the candidate merges, strict | pass |

Fix:

- Add a skill step after verification: after the primary candidate merges, land the reviewed lock follow-up, and expect master Website Deploy to fail until it does.
- State the same ordering in the README.
- Optionally, make the failure message name the lock value and the missing `versions.json` entry.
- For local checks on a candidate, document `SYNAPSEML_DOCS_PREVIEW=true npm test`. An unset variable means strict mode.

## Parent resolution evidence, pending independent closure

- R3-1: live repository settings permit squash and rebase, not merge commits.
  No repository setting was changed. `verify-primary-integration` now accepts
  either canonical master ancestry or GitHub provenance for the unchanged,
  merged canonical primary-candidate PR. Its merge result must be an ancestor
  of freshly fetched master. Origin, source tag, head, branch, base and repository
  identities are bound explicitly. The workflow repeats this read-only check
  before artifact verification; the operator guide runs it before dispatch.
  The GitHub host and REST API version are pinned. A read-only probe against
  a past squash-merged PR confirmed lookup by its original head commit.
- R3-2: dispatch timeout handling now reports that the request may have been
  accepted and directs the operator to workflow runs. It does not echo the
  command, plan or approval, and does not substitute tag-recovery guidance.
- R3-3: the shared strict JSON parser converts `RecursionError` to a controlled,
  non-echoing `ValueError`. Bootstrap environment input, compressed evidence,
  external profiles and Maven admission have direct regressions.
- R3-4: direct inventory binds the repository from the approved plan's profile.
  Synthetic A/B-profile tests cover both CLI inventory modes. Public schema-2
  and read-only schema-1 behavior remain covered.
- R3-5: the skill and operator guide require the publication-lock follow-up
  after verified publication and primary documentation integration. They explain
  the intervening failed master website check and require successful deployment
  before claiming the site updated. Evidence freshness and local preview-mode
  commands are explicit.

The related unbounded Git-operation risk is also addressed: release Git calls
now have a 180-second limit with fixed, non-echoing remote-state recovery advice.
The shallow-clone recovery requirement now includes an unshallow command in the
operator guide. Public dispatch and documented workflow commands pin the GitHub
host instead of inheriting an unrelated `GH_HOST`.

Runtime corrections went from 10 failures and one pass to 11 focused passes;
643 affected Linux tests passed. The separate dispatch-host regression went
from two failures and one pass to all 51 bootstrap tests passing. Parent notes,
workflow and public-documentation checks passed 101 tests before the final
transport additions, followed by 16 focused passes. Two Windows fixture errors
were oversized pytest IDs, fixed with short IDs while retaining full payloads.
Final frozen-tree validation and independent round-6 closure are separate gates.
No repository commits, public pushes, production tags or packages were created.

## Checked without findings

- Bootstrap refuses before any write unless all of these hold:
  - the exact primary workflow environment and SHA;
  - a clean checkout whose origin is the canonical repository;
  - unchanged candidate and canonical heads, rechecked immediately before the push;
  - the candidate contains the current canonical tip;
  - one open same-repository PR against the current base;
  - successful latest exact-head Azure and Compile checks, plus the primary website workflow;
  - runtime pins that match the real branch baselines;
  - primary documentation metadata.
- Atomic tagging:
  - Staged refs are create-only and pushed with `--atomic`.
  - It handles existing matching tags, conflicting tags and orphan peeled refs.
  - Reruns are idempotent and annotated tags are preserved.
  - A server rejection leaves no partial tag set.
  - Staging refs are removed in `finally`.
- Workflow:
  - Bootstrap runs are excluded from `release-tags`, and the mode must be an explicit boolean.
  - The concurrency group serializes runs without cancelling them.
  - Tag writes made with `GITHUB_TOKEN`, and candidate branches, trigger no prepare, spark-tag or Azure runs.
  - Bootstrap never queues packages.
- Schema 2:
  - Root and target allowlists are exact, with digest checks and full re-derivation.
  - Non-canonical numeric encodings are rejected.
  - No profile is needed.
  - The exported org, project and pipeline values are already public on `master`.
- Public evidence:
  - gzip+base64 encoding is capped below the dispatch input budget; the actual evidence size is unverified.
  - Evidence expires after one hour.
  - Job labels are fixed, and every job outcome is kept.
- Schema 1: execution is refused, and production reads use the identities sealed in the document.

## Residual, non-blocking

`release_guard._git` has no timeout, and the new bootstrap job sets no `timeout-minutes`, which matches the other workflows. A stalled `ls-remote` or `push` holds the concurrency group until the default job limit. Cancelling and rerunning is safe.

## Delta recheck: bounded integration fixes

Checked without findings:

- **Preview flag and deploy boundary.** The preview expression at `.github\workflows\website-deploy.yml:58-59` is the exact complement of the Pages upload and deploy conditions at lines 74 and 80. Every run that can upload or deploy therefore runs strict. The boundary test asserts both expressions.
  - Release preparation dispatches the website build on the release branch ref (`.github\workflows\release-prepare.yml:230`), so that build runs in preview.
  - Bootstrap accepts only same-repository `pull_request` or `workflow_dispatch` runs for the exact head (`scripts\release\bootstrap_release.py:140-167`). For a non-master candidate, both of those are preview runs.
- **Circular gate removed.** The candidate gate no longer needs a lock that may only change after publication.
- **Post-merge flow.** After a post-merge release, the red strict master website run does not block tagging. The finalize job checks the documentation files, not website CI (`.github\workflows\release-prepare.yml:288-299`).
- **Strict flag parsing.** `website\test\installDocs.test.js:18-22` accepts only unset, `true` or `false`. Any other value, such as `TRUE`, makes the file fail to load, so the check fails closed.
- **What preview still checks.** Preview keeps every coordinate, release-tag, snapshot and versioned-guide assertion. Only the lock may lag, and only to a version listed in `versions.json` with the matching port suffix. Missing keys and cross-port values fail. The production lock still names `1.1.3`.
- **Version bump and the lock.** `scripts\bump-version.py:162-170` denylists `website/test` by exact path component, so the lock and fixtures survive a version bump.
- **Snapshot finalization.** `_finalize_versioned_docs` (`scripts\bump-version.py:417-442`) runs only after `docusaurus docs:version` succeeds (`:550-558`) or from recovery mode. It:
  - removes only the new version's `## Latest master snapshot` section, up to the next level-2 heading or the end of the file, so any `###` subsections go with it;
  - refuses a missing, symlinked or hard-linked guide, more than one snapshot section, or a leftover moving badge;
  - is idempotent, and writes nothing under `--dry-run`.

  The source section (`docs\Get Started\Install SynapseML.md:33-52`) ends at `## Microsoft Fabric`, and nothing links to it in-page. The landing page links only to the `next` docs (`website\src\pages\index.js:299`), so the versioned copy gets no broken in-site anchor. Source and historical snapshots are untouched.
- **Recovery mode.** `--finalize-docs` (`scripts\bump-version.py:445-493,626-631,640-652`) never reruns the bump, `sbt` or Docusaurus. Before any write, it refuses:
  - a target that is not `X.Y.Z` or differs from the current source version, and a missing or linked version configuration;
  - a directory that is not the Git worktree root, and a shallow repository;
  - a snapshot or sidebar path already present in `HEAD` history. That check uses literal pathspecs and `--no-replace-objects`, with lazy fetch and prompts disabled;
  - any Git failure, including a missing `git`;
  - any combination with `--from`, `--skip-docs` or `--verbose`.

  The new `OSError` handlers (`:405,540`) print recovery steps instead of a traceback when `sbt` or `npm` cannot start.
- **Line endings.** The finalizer writes in text mode, unlike the byte-preserving bump writes (`:268,751`), so on Windows the rewritten guide has CRLF in the working tree. `.gitattributes` (`* text=auto eol=lf`) normalizes it on commit, so this is not a finding.
- **Fork guard.** `condition is False` accepts only a YAML boolean false. A quoted `'false'` or a missing condition still fails, and the string condition on `master` must still contain the guard.
  - Replaying the fixed test against both local port baselines' `pipeline.yaml`, which sets `condition: false`, passes.
  - The only failure in that replay is the master-only `test_fabric_e2e_keeps_key_vault_authentication_and_blocks_forks`. The ports replace it with their own `..._while_disabled` variant, and port candidates must keep that port-specific difference.

Residual, non-blocking:

- **Lock regressions surface only after merge.** PR builds now run in preview. A later master PR that moves the lock back to any listed older version passes before merge and is caught only by the strict master push run. Deployment stays blocked, so this fails closed. Limiting preview to the current or the immediately preceding listed version would restore detection before merge.
- **The "do not bump" rule is not enforced.** Nothing checks the README rule "Do not bump this publication lock during source preparation". Both modes accept a candidate lock that already names the candidate version. Optionally, the primary `check_docs` could refuse such a candidate.
- **Heading-level mismatch.** The website test accepts `##` or `###` for the moving-snapshot heading, but finalization removes only `##`. A future `###` heading makes preparation fail closed with "still uses a moving snapshot"; it would not publish a moving snapshot.
- **Recovery in a shallow clone.** `--help` states the full-history requirement, but the steps printed after a docs failure (`scripts\bump-version.py:496-512`) do not. Followed verbatim in a shallow clone, the final command refuses. This fails closed with a clear message. Adding an unshallow step to the printed guidance would avoid a dead end. Repro, read-only, in this shallow local clone with source version `1.1.3`:
  - `python scripts\bump-version.py --finalize-docs --to 1.1.3 --dry-run` exits 1 with "requires complete, non-shallow local Git history";
  - `--to 1.2.0` exits 1 with "target must equal the current source version".

Delta evidence, all local and with no network:

- `node --test` in `website`: **35 passed** each with the preview flag unset, `false` and `true`. With `TRUE`, the preview test file fails to load.
- `scripts\test_bump_version.py` and `scripts\release\test_release_workflows.py` pass on Windows with native Git on `PATH`. The 22 historical-replay cases need `git` on `PATH`.
- After the docgen fix, `python -m pytest scripts\test_bump_version.py -q -p no:cacheprovider` gave **270 passed**. The script's hash was unchanged across the run.
- `tools\ci\tests\test_pipeline_yaml.py` on Windows: the 12 `test_publication_script_respects_the_approved_artifact_family` cases fail only because this host's `bash` is the WSL launcher. That launcher does not faithfully forward the test's environment variables or shell flags. The same cases pass in the native Linux run below, and this delta does not change `pipeline.yaml`.
- In a native Linux shell, on a copy of the tracked CI inputs, `test_pipeline_yaml.py` plus `test_release_workflows.py` gave **131 passed**.
- The local candidate branches still point at their target baselines. Candidate contents, PR checks and hosted website runs are unverified.

## Evidence and limits

These tests were run by the reviewer on the local worktree, using native Git, local bare repositories and stubbed GitHub responses:

- `python -m pytest scripts\release\test_release_bootstrap.py -q -p no:cacheprovider`: **45 passed**.
- `test_release_config.py`, `test_release_public.py` and `test_public_release_docs.py`: **90 passed**.

Two test-portability notes, which are not product defects:

- With `safe.bareRepository=explicit` set, 35 bootstrap tests fail in the test setup's `git -C <bare> show-ref` helper.
- On Windows without UTF-8 mode, one test fails reading a workflow file with the locale codec.

The passing runs used a neutral Git configuration and `PYTHONUTF8=1`.

This review made no external API calls and created no pushes, tags, approvals or packages. The following remain unverified:

- actual CI, candidate PRs and website runs;
- repository merge settings and rulesets;
- live validation of dispatch inputs;
- Azure producer runs, published artifacts and evidence size.

Parent-reported suites and SBT results are not claimed here. This artifact records a single model's round 3, not multi-model coverage. No product code was changed; only this artifact was written.

## Round-6 closure (independent, current status)

The same model reviewed the frozen tree. The findings above are left as written. The details and line references are in `reviews\pr-2628\pr-2628-attempt-4-review-6-claude-opus-5.5.md`.

| Finding | Round-6 status |
| --- | --- |
| R3-1 | Closed. The notes guard accepts either `master` ancestry or a merged pull request for the canonical candidate. That pull request must have the tagged head, the exact candidate branch, the canonical repositories and base `master`, and a merge commit contained in `master`. The workflow repeats this read-only check before artifact verification, and the guide runs it before dispatch. The endpoint's semantics rest on public documentation and a probe the parent reported. |
| R3-2 | Closed. A dispatch timeout now gives a fixed, non-echoing message: "may have been accepted; inspect workflow runs". It gives no tag guidance. |
| R3-3 | Closed. `strict_json` converts `RecursionError` into a controlled refusal. The bootstrap, evidence, profile and public paths have regression tests. |
| R3-4 | Closed. Inventory binds the Internal repository from the approved plan's profile. |
| R3-5 | Closed. Skill step 8 and the guide cover the lock follow-up, its order, the expected strict failure, deployment confirmation and the preview command. |

- **Residuals.** The Git timeout and shallow-recovery residuals are closed. The other delta residuals are unchanged and non-blocking.
- **New.** R6-1 is low: after tagging, the primary candidate's head must not change before it merges. The runbook needs:
  - a merge order;
  - a rule against "Update branch", rebase or push on the tagged candidate;
  - a reconciliation-PR fallback;
  - a read-only check of the `master` merge rules before bootstrap.
- **Evidence.** 280 tests passed across the seven release test files; the R6-1 repro passed 3 cases.
- **Unverified.** Actual CI, candidate PRs, merge rules and artifacts.
