---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 30
stage_attempt: 1
round: 2
timestamp: "2026-10-17T18:51:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "59ae000676059f4ad7c1b30c60426e90e8b9b4bdb1641ce1b14260c804c211b2"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-30-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the current helper sources in `.github/skills/synapseml-pr-loop/scripts/` and confirmed they match the supplied diff:
  - `collect_ci_evidence.py`: `AzureClient`, `verify_merge`, `verify_build`, `summarize_timeline`, `collect_build`, `collect_tests`, `parse_args`, `main`.
  - `verify_trusted_guidance.py`, `watch_azure_pipeline.py` and `Get-PrReadiness.ps1`.
- [x] Module layering is sound:
  - Import chain: watcher → collector → verifier. The verifier imports only the standard library.
  - The watcher fixes `--repo` to the collector's `REPOSITORY` (`watch_azure_pipeline.py` `parse_args`).
  - Documented interpreter flags fit the import design. The verifier runs with `-I -B` (`trusted-guidance.md`). The helpers that import siblings run with `-E -s -B` (`ci-evidence.md`, `ci-triage.md`).
- [x] The output table in `ci-evidence.md` (`readiness.evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval`, `merge`, `completeness.complete`) matches the object emitted by `Get-PrReadiness.ps1` (about lines 628–691).
- [x] Cross-skill links and anchors resolve:
  - `#validation-only-handoff`, `#evidence-invalidation`, `#checkpoint-before-yielding` and `#waiting-for-azure-pipelines`.
  - The external-contributor `SKILL.md` and `ci-delegation.md` link only to pr-loop references that exist.
- [x] The verifier's `REQUIRED` and `GUIDANCE_PATHS` entries exist on this branch:
  - `CODEOWNERS`, `pipeline.yaml`, and `templates/`, which `pipeline.yaml` lines 143–179 reference.
  - `.github/copilot-instructions.md` is absent, but it is only a guarded path, not a required one.
- [x] Test conventions:
  - The new tests sit under the existing `tools/ci/tests/` location, which the `pipeline.yaml` CIHelpers job already runs.
  - The readiness harness projects fake `gh --json` responses onto the requested fields (`test_get_pr_readiness.py:44`).
  - The watcher tests patch the same `collect_ci_evidence` module object that the watcher imports.
- [x] Mutation ordering in `Get-PrReadiness.ps1`:
  - Parameter validation is at 103–105, the review-request POST at 582–601, and the `-RunPipeline` guards at 608–613.
  - There is no catch before `ConvertTo-Json` (695–735).
  - Harness flag usage: `requestReview` at 272/451/467 and `runPipeline` at 562/678/708/783/955. No case sets both.
- [x] Considered and rejected as non-defects, since adding abstractions would conflict with the no-bloat scope and none of these causes a concrete failure:
  - `getattr(args, "allow_skipped_job", [])` coupling to a shared argparse namespace (the watcher only reads `attentionRequired`, `status`, `result`, `jobs` and `url`).
  - `trusted_executable` living in the verifier module.
  - The repeated `microsoft.SynapseML` literal.
  - Exit code 4 shared by two outcomes, which is documented.
- [ ] Tests were not executed. This is a read-only lens; I inspected the supplied validation evidence but did not re-run it.

## Issues

### Issue 1: `-RequestReview` posts before the `-RunPipeline` fail-closed checks, and the flag combination is not rejected
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 103–105, 582–601, 608–613, 695–735
- **Description**: The two mutating actions deliberately use different preconditions:
  - `-RequestReview` needs only an open PR and no revision errors. `test_review_request_uses_revision_gate_not_unrelated_policy_evidence` (`tools/ci/tests/test_get_pr_readiness.py:444–462`) codifies this.
  - `-RunPipeline` needs full `evidenceComplete`.

  Top-level validation rejects `-RunPipeline` with `-WaitForReview`, but not with `-RequestReview`. Inside `Get-PrSnapshot`, the review POST (594–595) runs before the `-RunPipeline` throws (608–613).

  By inspection, running `-RequestReview -RunPipeline -ExpectedHeadSha … -ExpectedBaseSha …` on an uncovered head causes a partial run if evidence is incomplete only because of a non-revision collection error. An example is the classic-protection read that needs Administration permission, as `ci-evidence.md` describes. In that case:
  1. The review request is posted.
  2. The script throws "CI trigger blocked…".
  3. `ConvertTo-Json` never runs, so the record of `automatedReviewRequested=true` is lost.
- **Risk**:
  - A side effect is applied with no evidence record of it.
  - The per-invocation dedupe (`$script:reviewRequestOutcome`) cannot stop a retry from posting another request.
  - Callers cannot tell "review requested, CI blocked" apart from "nothing happened".
  - It contradicts the script's validate-before-mutating pattern and the documented trigger-only `-RunPipeline` invocation (SKILL.md 277–285; the `ci-evidence.md` GitHub snapshot section).
- **Suggested Fix**:
  - Extend the existing top-level guard so `-RunPipeline` also refuses `-RequestReview`.
  - Update the matching "refuses `-WaitForReview`" sentences in SKILL.md and `ci-evidence.md`.
  - Add one harness case asserting the combination fails before any `requested_reviewers` call, i.e. the review-request marker is absent.
  - Alternative: evaluate the `-RunPipeline` preconditions before the review-request block.

### Issue 2: The evidence-invalidation procedure still mentions manual prompts after this PR removed manual assembly
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/loop-control.md`
- **Line(s)**: 173–175
- **Description**: This PR removed the old manual-assembly path from SKILL.md step 8. Two documents now treat `-Execute` as the only path:
  - SKILL.md line 266 says the contract "replaces manual prompt assembly with the installed `-Execute` runner".
  - `review-gauntlet.md` lines 30–42 make `-Execute` the only path; an oversized prompt stays blocked unless the user approves a complete split.

  However, the Evidence invalidation procedure still says "check that generated/manual prompts contain that exact diff". `review-gauntlet.md` links to that procedure as "the invalidation rules".
- **Risk**: Two linked policy documents disagree about whether manually assembled prompts are acceptable. An agent resuming work from the invalidation reference could treat manual assembly as still allowed, which is the provenance gap this PR intends to close.
- **Suggested Fix**: Change "generated/manual prompts" to "runner-generated prompts" and keep "or the approved complete split". No other change is needed.

### Issue 3: New watcher timeline tests add an unrequested `baseRefOid` field, so the "target movement" test cannot represent movement
- **Severity**: Low
- **File**: `tools/ci/tests/test_watch_azure_pipeline.py`
- **Line(s)**: 600, 639, 655, 676, 685–702
- **Description**:
  - `query_pr` requests only `state,headRefOid,statusCheckRollup` (`watch_azure_pipeline.py:78–93`). Timeline mode verifies the merge only from the pinned `--base-sha` and `--merge-sha`.
  - The new tests still add `baseRefOid` to the `query_pr` return value.
  - In four of those tests, `verify_merge` is patched or raises, so the field has no effect.
  - In `test_target_movement_does_not_replace_the_pinned_build`, the only "movement" is `"d" * 40` in that field. A real `gh pr view` with those `--json` fields cannot return it. The test passes the same way without the field and actually proves only that verification uses the pinned pair once.
  - The readiness harness in this same PR projects fixtures onto the requested fields specifically to avoid modeling responses `gh` cannot return.
- **Risk**: The tests overstate coverage, suggesting the watcher observes base movement. The hunks are also filler, against this PR's own no-bloat gate (SKILL.md step 4). There is no runtime defect.
- **Suggested Fix**: Remove the `baseRefOid` additions and rename the movement test to what it asserts, for example `test_merge_verification_uses_pinned_revisions_once`. Alternatively, project `query_pr` fixtures onto the requested fields, as the readiness harness does.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4363 added and 157 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 30,
  "completedPriorPasses": 29,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`7548db103b4049ecde35a8ac95d0ea914d77cdc4a135fa7b682bd8b8a3119cda`.
Round 1 was clean; round 2 ended this pass. Neither qualifies the revised patch.
The reviewed diff SHA-256 was
`41d26ee9de63c416f62ee7317f30d0f45433d590890d1d30b81dc6313463e644`;
the accompanying attempt-30 manifest records its complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
Both transports used the observed `claude-opus-5.5` model and read-only
source/policy/test tools; neither opened prior or sibling feedback.

- **Issue 1: Fixed.** Top-level validation rejects `-RunPipeline` together with
  `-RequestReview`, before either mutation. The existing unsafe-request test
  now covers an uncovered head with incomplete policy evidence and asserts
  neither side-effect marker exists. It failed before the fix because a review
  request was posted. The skill and CI reference document the restriction.
- **Issue 2: Fixed.** Evidence invalidation now requires runner-generated
  prompts, retaining the approved-complete-split exception.
- **Issue 3: Fixed.** Removed the five unrequested `baseRefOid` fixture fields
  and renamed the test to `test_merge_verification_uses_pinned_revisions_once`.
  It claims only the pinned-revision behavior it actually asserts.

Validation after these fixes:

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py -k 'unsafe_pipeline_requests or review_request or WatchAzurePipelineTests' -rs --tb=short`
  passed: 42 tests, 78 subtests; 29 deselected.
- Linux, from `tools/ci/tests` with PowerShell available:
  `python3 -B -m pytest -q test_get_pr_readiness.py test_watch_azure_pipeline.py -k 'unsafe_pipeline_requests or review_request or WatchAzurePipelineTests' -rs --tb=short`
  passed: 42 tests, 78 subtests; 29 deselected.
- `python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py`
  passed; both files unchanged.
- `git diff --check` passed.

No current-head Azure validation or final CI-qualified pass is claimed.
