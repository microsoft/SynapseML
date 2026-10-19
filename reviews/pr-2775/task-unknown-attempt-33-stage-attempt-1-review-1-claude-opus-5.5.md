---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 33
stage_attempt: 1
round: 1
timestamp: "2026-10-17T20:41:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a149f6cccf762868e41ae3001b8bf2f3f260450b969329368bec1db7861c64ef"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-33-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - parameter guards (100–105) and the per-invocation side-effect memos (126–131)
  - initial PR read and paged review queries (306–321)
  - branch, ruleset and classic-protection evidence (323–365)
  - compare, workflow and check sets (367–470)
  - revision recheck and `revisionErrors` (510–551)
  - readiness and `humanApproval` (552–572)
  - review-request and pipeline-trigger side effects (574–626)
  - per-PR loop and final JSON output (695–735)
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:
  - `command_json` resolves tools from trusted PATH entries (39–56).
  - `AzureClient` uses an endpoint allowlist, refuses redirects, keeps the token in memory and sanitizes errors (70–163).
  - Merge parents are checked in order (166–178).
  - Build checks cover definition, ref, source version and reason (181–220).
  - The timeline uses the highest attempt, and ancestor issues fail closed (223–405).
  - Test runs are paged with duplicate-ID detection and current-attempt selection; named-test scope cannot be waived by `--allow-skipped`; a deadline and `collectionStable` bound collection (466–650).
- [x] Read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - Build URLs are restricted to an allowlist (47–76).
  - Timeline mode verifies the merge once, collects the build on each poll, handles replaced builds and keeps the original deadline (96–253; timeline block 135–174).
  - Exit codes are mapped at 345–355.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - Git runs with `GIT_*` variables stripped, `--no-replace-objects` and `core.fsmonitor=false` (68–88).
  - Only regular-file tree modes are accepted, and the index must equal the tree.
  - Untracked and ignored files are rejected, and symlinked parents are rejected after strict resolution (91–150).
  - `trusted_executable` ignores relative, current-directory and descendant PATH entries (40–65).
- [x] Cross-checked the docs against the code:
  - Files: `references/ci-evidence.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `loop-control.md`, `ci-triage.md`, both `SKILL.md` files and `ci-delegation.md`.
  - Relative links and anchors resolve.
  - `review-gauntlet.md:30-77` keeps the rules removed from `SKILL.md` stage 8: frozen-patch identity, pass consumption, reviewer independence, output collisions, byte budget, blockers and the artifact-only rule.
- [x] Scope and no-bloat check:
  - The 17 paths are skill docs, the four helper scripts and their tests under `tools/ci/tests/`.
  - There are no changes to product APIs, `pipeline.yaml`, workflows, dependency pins or release tools.
  - The existing, unchanged `CIHelpers` job (`pipeline.yaml:112-126`) already runs `tools/ci/tests/`, as `ci-triage.md` says.
- [x] Checked that picking the newest run per workflow and event cannot let a newer run on the same head hide an older failure under the current `.github/workflows` triggers.
- [x] Spot-checked tests:
  - `test_get_pr_readiness.py`: the harness (20–123), its "no review-request side effect when the invocation fails" assertion (226–230), the request-guard tests (444–477) and the `-RunPipeline` guard matrix (705–741).
  - `test_collect_ci_evidence.py`: trusted-tool, bytecode and no-redirect tests (700–790).
  - `test_watch_azure_pipeline.py`: import isolation (18–34).
- [x] Cited the supplied validation without re-running it:
  - affected Windows and Linux runs: 42 passed, 78 subtests
  - earlier full readiness run: 32 passed, 89 subtests
  - collector and trusted-guidance suites: 117 passed with 1 POSIX-only skip on Windows, 118 passed on Linux
  - Black 22.3.0 and `git diff --check`

  I ran no commands in this review.
- [x] Independence: I did not open `reviews/` or session state, and searches stayed within the changed skill, script, test and config paths.

## Issues

### Issue 1: Multi-PR `-RequestReview` can post a review request, then abort and lose the record
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 100–105, 126–128, 582–600, 695–735
- **Description**:
  - The new guard (584–586) throws when a PR without a prior request attempt has revision errors or is not OPEN.
  - `-RequestReview` still accepts several PRs. Only the expected-SHA arguments and `-RunPipeline` are limited to one PR (100–105), and the memo is explicitly per PR (126–128).
  - Snapshots collect in `$results` and are printed only at line 735.
  - So `-PullRequest A,B -RequestReview` can POST `requested_reviewers` for A (594–597) and then throw on B. B blocks if it is closed or merged without review coverage of its head, or if the target branch moves between B's two branch reads (323 and 533).
  - The script then exits non-zero with no JSON, so A's snapshot is lost, including `automatedReviewRequested=true`. With `-WaitForReview`, this can follow up to `TimeoutMinutes` of polling A.
  - The error message does not name the PR that blocked.
  - This is the same failure class the PR already fixed for `-RunPipeline` combined with `-RequestReview`: a request is posted even though the invocation fails. It also breaks the harness rule at `tools/ci/tests/test_get_pr_readiness.py:226-230`.
  - No test runs `-RequestReview` with more than one PR. The only `pullRequests` fixture (line 724) fails at parameter validation.
  - `ci-evidence.md:79-83` describes single-PR behavior and does not cover this case.
- **Risk**: A review request goes out but is not reported, and the readiness evidence for earlier PRs is lost. Rerunning re-requests the review while it is still pending. The script still fails safely and never reports false readiness, so the severity is Low.
- **Suggested Fix**:
  - Simplest: reject `-RequestReview` with more than one PR at parameter validation, as the expected-SHA arguments already do, and say so in `ci-evidence.md`.
  - If multi-PR requests should stay supported: skip the request instead of throwing, record why it was blocked in that PR's snapshot (it already carries `revisionErrors` and `state`), and continue. Update `ci-evidence.md:81` to match.
  - Put `PR #$number` in any remaining error message.
  - Add a two-PR regression test. One option is a fixture where the target branch moves during the second PR's recheck. It should check for either an up-front error with no request, or a request recorded in the first emitted snapshot.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
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
  "changeSize": "17 source, test, and documentation paths; 4386 added and 162 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 33,
  "completedPriorPasses": 32,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`66a37d9ea51e6d90b8fa7d91cd931613df14f11440558d8e9620dbb142df6f9a`.
This finding ended the pass; the artifact does not qualify the revised patch.
The reviewed diff SHA-256 was
`215fd5b90c84a33a7acf89fc26847debccf47c15775f8808249981d8fef7f823`;
the accompanying attempt-33 manifest records the complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
The transport used the observed `claude-opus-5.5` model and only read-only
source, policy, and test tools, without accessing previous review feedback.

**Issue 1: Fixed.** `-RequestReview` now requires exactly one PR, checked before
any GitHub call. Read-only multi-PR snapshots remain supported. The help and CI
reference document the restriction, and a revision/state-blocked request's
error identifies its PR. The new two-PR regression failed before the fix because
a review-request side effect occurred; it now verifies the up-front refusal
with no request marker. Existing request, trigger, and polling cases still pass.

Validation:

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -k 'unsafe_pipeline_requests or review_request or failed_revision_reads_during_poll' -rs --tb=short`
  passed: 5 tests, 19 subtests; 28 deselected.
- Linux, from `tools/ci/tests` with PowerShell available:
  `python3 -B -m pytest -q test_get_pr_readiness.py -k 'unsafe_pipeline_requests or review_request or failed_revision_reads_during_poll' -rs --tb=short`
  passed: 5 tests, 19 subtests; 28 deselected.
- `python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
  passed; the file was unchanged.
- `git diff --check` passed.

No current-head Azure validation or final CI-qualified pass is claimed.
