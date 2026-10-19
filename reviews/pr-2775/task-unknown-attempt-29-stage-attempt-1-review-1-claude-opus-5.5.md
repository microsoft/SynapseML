---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 29
stage_attempt: 1
round: 1
timestamp: "2026-10-17T18:03:16Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b0fbaba7098c78ac913b8e49901f145e2c23af091530d3144cd84f7c99a31d03"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-29-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - `#Requires -Version 7.2` is at line 1. Lines 100–105 restrict expected SHAs to a single PR and refuse `-RunPipeline` when `-WaitForReview` is set or either expected SHA is missing.
  - `Get-RestEvidence` (237–271) turns non-zero exits, empty bodies and invalid JSON into `collectionErrors`, including for paged rulesets and workflow runs.
  - The revision recheck (501–537) reports read failures separately from movement.
  - Approval-rule read failures make `humanApproval=unknown` (548).
  - A missing `$compare` keeps `behind_by -eq 0` false, so the result stays blocked.
  - The review request throws only on revision errors or a PR that isn't open (571).
  - `-RunPipeline` (594–612) requires `evidenceComplete` and an OPEN PR, and posts only when the exact `microsoft.SynapseML` aggregate is absent.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` for safety:
  - `gh` and `az` are resolved through `trusted_executable` (41–42).
  - Errors are sanitized and don't echo stderr (55–57).
  - The `NoRedirect` handler (64–68) refuses redirects, so the bearer token isn't forwarded.
  - An endpoint allowlist regex is enforced (91–94).
  - Pagination over `$skip`/`$top` detects repeated IDs.
  - `verify_merge` checks ordered `[base, head]` parents.
  - `verify_build` checks the definition, repository, `refs/pull/<n>/merge`, source version and reason.
  - `validationPassed` requires a completed and succeeded build, passing job gates, passing tests and `collectionStable` (696–712).
- [x] Traced `collect_tests` against `references/ci-evidence.md`:
  - Run, build and result identities are checked against each other.
  - Test runs are matched to timeline Job attempts, and a newer-than-timeline attempt is rejected.
  - Per-job suite and test publication requirements are enforced.
  - Publication errors count only for selected runs from the current attempt.
  - `--allow-skipped` cannot exempt a test named with `--expected-test`.
- [x] Checked `summarize_timeline`:
  - Retried jobs are deduplicated by highest attempt.
  - Leaf Phase/Stage containers are included and parent cycles are rejected.
  - Ancestor issues are inherited and issue counts take the max of listed and reported counts.
  - An empty timeline is allowed only for builds that haven't completed.
  - `ci-evidence.md` deliberately makes timeline warnings a manual-disposition gate.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - `git()` (68–88) strips `GIT_*`, uses `--no-replace-objects` and `core.fsmonitor=false`, and runs no filters (`cat-file blob`).
  - `verify()` (91–152) allows only modes 100644 and 100755, requires the index to match the tree at stage 0, lists ignored files through `ls-files --others` without `--exclude-standard`, rejects symlinks in parent paths, and tolerates only CRLF differences in blobs without NUL bytes.
  - The required root `CODEOWNERS`, `pipeline.yaml` and `templates/` all exist in the checkout.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - Bytecode writing is disabled before the collector import (14–16).
  - The build-URL allowlist is enforced (47–75), and `query_pr` uses `command_json`.
  - In `monitor()`, `verify_merge` runs once (138–146) and the pinned build is collected on every poll (147).
  - Agent-loss events are deduplicated by (job, attempt).
  - The watcher waits at 600 s while Azure is incomplete, without extending the deadline.
  - A GitHub/Azure disagreement returns `attention_required`.
  - The exit-code map (352–362) matches `references/ci-triage.md` lines 38–49.
- [x] Checked the document links and CI coverage:
  - These anchors resolve: `loop-control.md#checkpoint-before-yielding` (line 7), `#state-and-cost-discipline` (68), `#evidence-invalidation`, `ci-triage.md#waiting-for-azure-pipelines`, `ci-delegation.md#evidence-required-before-triggering`, and `ci-evidence.md#github-snapshot`.
  - In `pipeline.yaml` lines 112–126, the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on the hosted image named by `UBUNTU_VERSION` (line 102). The `pwsh`-gated readiness class (`tools/ci/tests/test_get_pr_readiness.py` line 198) depends on that image.
  - The test modules disable bytecode while importing the helpers.
- [x] Checked `references/review-gauntlet.md`, `references/trusted-guidance.md` and the SKILL.md edits against each other and against `AGENTS.md`. Artifact placement, the `task-unknown` naming, the validation-only exclusions and the `-RunPipeline` description are consistent.
- [ ] Tests not executed. This review was read-only; the validation results cited are the driver's supplied evidence.
- [ ] The minimum GitHub CLI version that returns `headRepository.nameWithOwner` could not be checked in a read-only review. Issue 1 depends on that compatibility question.

## Issues

### Issue 1: Readiness recheck requires `headRepository.nameWithOwner` but no minimum GitHub CLI version is declared or checked
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`; also `.github/skills/synapseml-pr-loop/SKILL.md` and `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Line(s)**: Get-PrReadiness.ps1 292, 373–374, 501–537, 571, 594; SKILL.md 10; ci-evidence.md 9
- **Description**:
  - The new final recheck treats a missing `headRepository.nameWithOwner` as malformed evidence (line 510).
  - Line 373 takes `$headRepo` from the same subfield, and that value feeds the fork-run fallback (374) and the movement comparison (526).
  - The compatibility notes require PowerShell 7.2+ (SKILL.md line 10, ci-evidence.md line 9) but give no minimum GitHub CLI version. The only supplied verification is one current release.
  - Some `gh` releases may return `headRepository` with only `id` and `name`; the owner comes separately from `headRepositoryOwner`, which line 292 already requests. On those releases every snapshot would record "Invalid PR revision recheck evidence." and "Could not recheck PR revisions."
- **Risk**:
  - The failure is fail-closed, so it never reports false readiness. But on such clients `evidenceComplete` can never become true.
  - The existing `-RequestReview` fallback would throw for every unreviewed head (571), and `-RunPipeline` would be refused (594).
  - Fork runs with an empty `pull_requests` list would lose the unique-branch fallback (374) and add collection errors.
  - The message blames an unreadable or malformed recheck instead of the client, and gives no fix. That invites repeated polling or the temporary replacement scripts the skills forbid.
  - The readiness fixture (`tools/ci/tests/test_get_pr_readiness.py` lines 140–141) models only the `nameWithOwner` shape, so the tests can't detect this.
- **Suggested Fix**:
  - Preferred: build the head identity from `"$($x.headRepositoryOwner.login)/$($x.headRepository.name)"`, preferring `nameWithOwner` when present, and add `headRepositoryOwner` to the recheck `--json` list (line 502).
  - Alternative: keep `nameWithOwner`, but document the minimum `gh` version next to "PowerShell 7.2+". Then add an explicit collection error such as "GitHub CLI does not supply headRepository.nameWithOwner; upgrade gh" when the initial view lacks the field.
  - In either case, add a readiness fixture for the `{id, name}` plus `headRepositoryOwner.login` shape.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4315 added and 157 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 29,
  "completedPriorPasses": 28,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition after round 1

The original response and verdict above are preserved. Its SHA-256 before this
append was `8accd84f4f20353ba0ff0d835d4cb06c0cbb54ca450cb8c3c4ec6ec40426694c`.
This finding ended the pre-commit pass; rounds 2-6 did not run. The fix requires
a new full pass.

### Issue 1: Confirmed against upstream source and fixed

The published GitHub CLI source for
[`v2.70.0/api/queries_pr.go`](https://github.com/cli/cli/blob/v2.70.0/api/queries_pr.go)
includes `BaseRefOid` and `HeadRefOid`, while `PRRepository` contains only `id`
and `name`. The earlier shape is real, not merely a hypothetical missing field.
The locally exercised current CLI supplies `nameWithOwner`.

One shared resolver now prefers the supplied full name or combines the supplied
head owner login and repository name when that field is absent. The recheck
explicitly requests `headRepositoryOwner` and uses the same resolver. Missing
identity remains blocked, and an owner change remains a revision change.
No version pin, new dependency, or synthetic owner fallback was added.
The evidence reference documents the supported JSON shapes.

The PowerShell fixture now returns only requested `--json` fields, so the new
owner request is exercised rather than supplied accidentally by a permissive
fixture. The existing unique-fork test covers both identity shapes, including
workflow approval and the guarded CI trigger. A new recheck test covers an
owner change and an absent owner. Three cases failed before the fix.

### Validation

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -rs --tb=short`
  — **32 passed, 89 subtests passed**.
- Linux, from `tools/ci/tests`, with PowerShell 7.6.7 on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py -k 'unique_fork_head or legacy_head_identity' -rs --tb=short`
  — **2 passed, 30 deselected, 6 subtests passed**.
- Black 22.3.0 and whitespace checks passed. Only the readiness helper, its
  existing tests, and its evidence documentation changed.
- Reviewer transport contains only source/policy/test reads: 26 views,
  20 searches, and 2 globs. No prior/sibling feedback was read. Actual
  reviewer identity was `claude-opus-5.5`.
- Current-candidate Azure validation remains blocked on authorization.
