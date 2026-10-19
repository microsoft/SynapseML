---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 2
timestamp: "2026-10-19T19:52:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f73acc9e3cd077dcc9bca9aad89e5fff5bfdc004602bf8b4cfa873977fa32104"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope matches `AGENTS.md` and `pipeline.yaml`.**
  - The 17 changed paths are only under `.github/skills/` (skill docs and the helpers `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `verify_trusted_guidance.py`, `watch_azure_pipeline.py`) and `tools/ci/tests/`.
  - No product API, dependency pin, release tool, `pipeline.yaml` or workflow definition changed.
  - The existing `CIHelpers` job (`pipeline.yaml` 112–126, `python3 -m pytest tools/ci/tests/ -q`) already picks up all four test suites, so no pipeline edit is needed.
- [x] **Collector design (`collect_ci_evidence.py` ~118–160) fits the stdlib handler chain.**
  - `DeadlineReader` and `DeadlineHTTPSHandler` keep the default HTTPS context and `NoRedirect`. They add no thread, worker, option or dependency.
  - `AzureClient.get` (~214–237) rechecks the original deadline before choosing the timeout or generic error, as the docs describe.
  - The watcher reuses the collector's `collect_build` instead of copying it. Its exit codes and fixed deadline match `ci-triage.md#waiting-for-azure-pipelines`.
- [x] **`Get-PrReadiness.ps1` matches its documentation.** Its guards (~99–111, ~611–650) and output fields (653–715) agree with the `ci-evidence.md` output table (73–82) and the comment-trigger gate (94–128).
- [x] **The trusted-guidance verifier is consistent with its doc.**
  - The paths `verify_trusted_guidance.py` requires can all be satisfied, including root `CODEOWNERS`.
  - It skips untracked, ignored and symlinked files and disables fsmonitor, as `references/trusted-guidance.md` says.
  - Sibling helpers import its `trusted_executable` and `GuidanceError` rather than re-implementing them.
- [x] **Cross-document links resolve.** All of these point to existing headings: `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `ci-evidence.md#comment-trigger-gate`, `ci-evidence.md#github-snapshot`, `readiness-gates.md#review-and-validation`, `readiness-gates.md#user-value-and-scope`, `loop-control.md#evidence-invalidation`, `ci-triage.md#waiting-for-azure-pipelines`.
- [x] **No stale callers in `.github/skills/`.**
  - I searched for `RunPipeline`, `/azp run`, `completeness`, `RequiredCheck` and `scope exception`. Nothing contradicts the guarded trigger or the Azure scope exception.
  - External-contributor `SKILL.md` step 2 still says "Confirm the build". I did not flag it: the wording predates this PR, and the edited overview (39–45) and `ci-delegation.md` (25–27) apply the exception first. The worst outcome is a cautious "blocked" report.
- [x] **Test structure is sound.**
  - The four `tools/ci/tests` suites load helpers by spec, with matching `sys.path` insert/pop.
  - The streaming regression drives a real urllib opener, `HTTPConnection` and `HTTPResponse` over a socket pair.
  - No test pins the exempt-diff wording in `readiness-gates.md`: searching `tools/ci/tests` for `readiness-gates|Assess all other gates|exempt|policy evidence` found nothing. Issue 1's fix is therefore doc-only.
- [ ] **Tests and commands: not run by me.** This lens is read-only, and I did not reproduce the supplied pytest/Black results.
- [ ] **Live GitHub/Azure behavior: not verifiable here.** The Azure definition/project IDs and real REST pagination and permission responses can't be checked offline. No hosted CI result covers this pending patch.
- [ ] **Full diff display: truncated in this session.** I reviewed the current files at the named paths directly. For some unchanged doc lines, whether they are new in this PR is inferred, not confirmed from the patch.

## Issues

### Issue 1: Exempt-diff gate audit omits the evidence-validity check and never defines "complete policy evidence"
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/references/readiness-gates.md
- **Line(s)**: 125–137 (mainly 129–137)
- **Description**:
  - **Context.** On an exempt diff, the helper's built-in `microsoft.SynapseML` requirement keeps `readiness.remoteEngineeringGatesClear` and `completeness.complete` false. The bullet therefore tells the agent to "Assess all other gates from their own fields". It lists:
    - other missing/unproven checks;
    - `failedChecks` and `pendingChecks`;
    - workflow results and approvals;
    - review threads;
    - `completeness.automatedReviewCoversHead`;
    - `behindBy` and `mergeable`;
    - `readiness.unassessedGates`.
  - **The gap.** This list stands in by hand for the helper's `$remoteGatesClear` check (`Get-PrReadiness.ps1` 561–568). It leaves out that check's first condition, `$evidenceComplete` (559–560), which the helper outputs as `readiness.evidenceComplete`. That condition covers:
    - `revisionErrors`: the head, base or state changed, or the target moved, during collection;
    - `collectionErrors`: unreadable or empty REST evidence, incomplete workflow pagination, or ambiguous workflow identities;
    - `completeness.threadsWithUnreadComments`.
  - **No other listed field covers it.** `readiness.unassessedGates` (692–697) holds only a fixed list of manual gates plus unevaluated rule and protection types. Review decisions and suppressed feedback do have their own bullets (107–113); evidence validity is mentioned nowhere in this file.
  - **"Complete policy evidence" is undefined.** Lines 129 and 132–133 depend on it, but no output field defines it. The helper's `$rulesComplete` (369) is internal only. If the account lacks admin access, a failed classic-protection read shows up only as `classicProtection = null` plus a `collectionErrors` message (247–252).
  - **Inconsistent with the other manual route.** The other documented way around the helper summary, the hand-posted requeue in `ci-evidence.md` (114–118), explicitly requires `readiness.evidenceComplete=true`. `SKILL.md` step 9 (~279–286) leaves exempt-diff interpretation entirely to this bullet.
- **Risk**: An agent following the list literally can clear an exempt PR from a stale or partial snapshot:
  - `behindBy` and `mergeable` may have been computed against a target that moved during collection.
  - Truncated workflow pages can hide an `action_required` run.
  - Review threads may have unread comments.
  - After a 403, `classicProtection = null` can be read as "nothing enforced", so an enforced Azure check is misclassified as the helper's built-in requirement.

  That is exactly the unreliable evidence this PR is meant to prevent, on the exempt path the PR adds. The link at line 142 to the readiness-state table helps only partly, because the bullet presents its list as the complete substitute.
- **Suggested Fix**:
  - In the exempt bullet, require `readiness.evidenceComplete=true` before both the enforced-check classification and the field audit. That means empty `revisionErrors` and `collectionErrors`, and no `completeness.threadsWithUnreadComments`.
  - Define "complete policy evidence" as that condition, or at least as no `collectionErrors`. If it isn't met, report policy and readiness as unknown, matching the requeue route.
  - This is a one- or two-line wording change; it needs no new field, flag or mechanism.
  - Optionally, describe the audit as "every `remoteEngineeringGatesClear` input except the built-in Azure aggregate", so the list can't drift from the helper again.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver adjudication

### Issue 1: rebutted against the complete documented workflow

- **Disposition**: no required source change. Repeating the evidence-validity
  field inside the exemption bullet could improve local discoverability, but
  the claimed permission to clear stale or partial evidence does not follow
  from the full instructions.
- **Existing requirements**: `SKILL.md` step 1 requires interpreting the separate
  evidence, engineering, human and merge states through `ci-evidence.md`, not
  only a summary. The readiness guide starts by requiring current-head evidence
  for every applicable gate, and its exemption paragraph says to assess **all**
  other gates, not to replace the workflow with only its listed examples.
  Immediately afterward it links to the separate-readiness-state contract.
- **The linked contract explicitly covers the reported cases**:
  `references/ci-evidence.md` lines 9-19 reject failed revision rechecks;
  lines 45-52 say REST failures produce `collectionErrors`, not an empty policy,
  and unavailable protection reads leave readiness unknown; its output table
  defines `readiness.evidenceComplete` as readable, untruncated evidence with
  stable revisions. The exemption itself states that incomplete policy evidence
  remains unknown and that the blocked summary must never be rewritten as clear.
  Those requirements continue to apply when Azure execution is not required.
- **Verification**: the existing readiness suite covers failed revision reads,
  empty policy output, stale/moving revisions, and incomplete workflow pagination
  in `test_failed_revision_reads_during_poll_preserve_blocked_snapshot`,
  `test_empty_successful_policy_output_never_means_no_rules`,
  `test_stale_expected_revision_or_movement_invalidates_snapshot`, and
  `test_malformed_branch_or_incomplete_workflow_pagination_is_not_complete`.
  Each asserts incomplete evidence. The previously recorded passing readiness
  runs remain applicable because neither the helper nor its tests changed.
- **Decision**: do not duplicate the shared evidence-validity contract or start
  another source/review cycle for a standalone reading of one bullet that omits
  its mandatory surrounding and linked instructions. No accepted unresolved
  finding remains; the reviewer's original severity and `ISSUES_FOUND` outcome
  remain unchanged above.