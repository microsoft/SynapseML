---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 31
stage_attempt: 1
round: 1
timestamp: "2026-10-17T19:24:47Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "aa0fe8f6ba6cf3452b3e527749c664aabdd0b04901c930e855a1b7eab28450ca"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-31-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` L100–105:
  - Expected SHAs are accepted only for a single PR.
  - `-RunPipeline` throws before any `gh` call when either expected SHA is missing or when it is combined with `-WaitForReview` or `-RequestReview`. So no review-request side effect can happen before the refusal.
  - This matches `tools/ci/tests/test_get_pr_readiness.py` L693 (`test_unsafe_pipeline_requests_fail_without_posting_a_comment`).
- [x] Get-PrReadiness.ps1, review request and pipeline trigger:
  - Pipeline trigger (L602–625) requires `evidenceComplete`, an OPEN PR, and an absent `microsoft.SynapseML` check (exact case). It posts `/azp run` at most once per PR.
  - `evidenceComplete` includes the expected head/base equality checks (L549–550) and the PR and target recheck after collection (L514–551).
  - Review request (L574–600) is attempted once. It throws only before that attempt, on revision errors or a non-OPEN PR.
  - Consistent with `references/ci-evidence.md` and SKILL.md step 9.
- [x] Get-PrReadiness.ps1, REST evidence and policy (L237–271, L322–369, L552–572):
  - `gh` failures, empty or `null` bodies, empty paged lines, and invalid JSON become `collectionErrors`.
  - Unknown rule types and enabled classic extras are surfaced as unevaluated.
  - `rulesComplete` is captured before the compare and workflow reads. A failed policy read gives `humanApproval=unknown`, but a compare failure does not discard a known approval.
  - `not_required` needs explicit zero-count rules with no code-owner, last-push, or reviewer requirement.
- [x] Get-PrReadiness.ps1, workflows and checks (L371–467):
  - Workflow pagination is checked against `total_count`.
  - A PR association requires both the base repository URL and the head SHA to match.
  - When GitHub omits the association, a fork run is accepted only if exactly one PR (`state=all`) uses that exact head repository and branch. Both `gh` head-identity shapes are handled.
  - Runs are scoped to this PR before newest-run selection.
  - The Azure aggregate and rule-derived required checks match exactly and case-sensitively; skipped or neutral results for them count as unproven.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` L39–163, client safety:
  - CLI and HTTP errors are sanitized, and executables are resolved through `trusted_executable`.
  - A full-match regex limits endpoints to build and test-run paths in the fixed project. Redirects are refused, and every request timeout is bounded by the deadline.
  - The token stays in memory. A grep confirmed the header interpolates `self.token`; the masked `******` in the file view is tool redaction, not the source.
  - Pagination rejects repeated or invalid IDs.
- [x] collect_ci_evidence.py L166–220, provenance:
  - The merge commit's parents must be exactly `[base, head]`.
  - The build must use definition 17563 and repository `microsoft/SynapseML`, with branch `refs/pull/<n>/merge` and `sourceVersion` equal to the merge SHA.
  - The reason must be `pullRequest` or `manual`, with a valid queue time and status.
- [x] collect_ci_evidence.py L223–446, timeline:
  - Keeps the latest attempt per (type, identifier) and rejects parent cycles.
  - Leaf Phase/Stage containers are gated, and ancestor issue counts are inherited without echoing messages.
  - Only an explicitly allowed `skipped` result clears a job gate. Completed builds require timeline jobs.
- [x] collect_ci_evidence.py L458–720, tests and result:
  - Only results from the current attempt count. An attempt newer than the timeline raises an error, and unknown job keys fail closed.
  - Every job that published an expected suite or test must publish it again in its current attempt.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - `validationPassed` requires a completed, succeeded build, passing job gates and tests, and a stable re-read. `mergeReady` is always false.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, Git checks:
  - Strips `GIT_*` variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`.
  - Requires the worktree root to equal `--checkout` and HEAD to equal the expected SHA.
  - Accepts only regular file modes, and requires every `REQUIRED` path (each confirmed present by glob).
  - The index must match the tree at stage 0.
  - Rejects untracked or ignored files (no `--exclude-standard`).
  - Rejects symlinked parents, and allows a CRLF difference only for non-binary files.
- [x] verify_trusted_guidance.py, `trusted_executable`: searches only absolute PATH entries, excluding the cwd and its descendants (also checked after symlink resolution), and expands PATHEXT on Windows.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` `monitor()` and the `main()` exit map:
  - The deadline is anchored to the kickoff time and never extended; the merge is verified once.
  - `agent_loss` is reported once per (job, attempt).
  - `azure_completed` and `attention_required` exit 4, and `success` is not a readiness claim. Matches `references/ci-triage.md` L38–49 and L76–78.
- [x] Documentation cross-check:
  - The interpreter commands that tests extract from the docs still match their regexes: `trusted-guidance.md` L18 (`-I -B`), and `ci-evidence.md` L100 and `ci-triage.md` L24 (`-E -s -B`).
  - All SKILL.md reference links resolve, and the cited anchors exist.
  - `review-gauntlet.md` keeps the pre-commit and final CI-qualified passes, frozen-patch identity, and budgets.
- [x] Tests inspected, not executed:
  - Counts: readiness 32, collector 35, trusted guidance 14, watcher 39.
  - The supplied `-k` arithmetic is consistent: 3 + 39 = 42 selected, 29 deselected.
  - The `pipeline.yaml` CIHelpers job runs `tools/ci/tests/` on hosted Ubuntu, where the `pwsh` skip should not trigger.
- [x] Scope and no-bloat: changes are confined to `.github/skills/...` and `tools/ci/tests/...`. No product API, dependency pin, release tooling, `pipeline.yaml`, or workflow changes.
- [x] Candidates considered and dismissed as non-defects:
  - Non-Azure `-RequiredCheck` entries only need to be present; the parameter docs say so and the behavior predates this PR.
  - A canceled build with no timeline returns `error` (exit 1), never success.
  - A `collectionStable` mismatch caused by list order can only fail closed.
  - A workflow-run pagination race is equivalent to a run created after the snapshot.
- [ ] Test execution: not applicable. This was a read-only review; no commands were run and no execution results are claimed.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4371 added and 158 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 31,
  "completedPriorPasses": 30,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
