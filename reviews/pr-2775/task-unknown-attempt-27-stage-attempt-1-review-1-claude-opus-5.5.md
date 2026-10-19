---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 27
stage_attempt: 1
round: 1
timestamp: "2026-10-17T17:02:57Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "236919a8d18f180a1256b84f3799b6cfc2091e8ab7266f25ed777ba8bb948816"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-27-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.** Read in full.
  - **Version and guards:** `#Requires -Version 7.2` on line 1 matches the SKILL.md compatibility line. Lines 100–105 limit expected SHAs to one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview`.
  - **REST reads (lines 237–272):** `Get-RestEvidence` records a `collectionErrors` entry when the CLI fails, the body is empty or `null`, or the JSON is invalid. Paged `tojson` output is parsed one page per line, so a `[]` rules page means no rules rather than an error.
  - **Branch rules (lines 307–357):** the live target tip is compared with the head. Every ruleset type except checks, reviews, Copilot review, deletion, and non-fast-forward is reported as unevaluated. The classic-protection property filter handles a `$null` `$classic` safely. Approval rules include classic `required_pull_request_reviews`.
  - **Workflow runs (lines 359–423):** the `total_count` pagination check, PR association by base-repo URL plus head SHA, and the fork fallback that requires a unique PR (including closed ones) all work as described. Runs are scoped to the PR before the newest run per workflow/event is selected.
  - **Checks (lines 425–454):** `microsoft.SynapseML` and rule-derived checks are matched case-sensitively, and skipped or neutral required checks count as unproven.
  - **Readiness (lines 500–548):** after the head/base/state and target-tip recheck, `evidenceComplete`, `remoteGatesClear`, and `humanApproval` are computed separately.
  - **Write actions (lines 550–600):** the review request is attempted once and needs an open PR with stable revisions. `/azp run` is posted only with complete evidence, an open PR, and no existing aggregate check.
- [x] **`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.** Read in full.
  - **Azure client and pagination (lines 38–170):** tool paths come from trusted PATH entries, stderr is never echoed, and redirects are refused. Endpoints are allowlisted, and a 404 or empty response is tolerated only for the timeline. `$skip/$top` pagination rejects invalid and duplicate IDs.
  - **Provenance (lines 173–223):** the merge parents must be exactly `[base, head]`. The build must be definition 17563, a GitHub repo, `refs/pull/<n>/merge`, the expected source version, and a `pullRequest` or `manual` reason.
  - **Timeline (lines 224–400):** the highest attempt is kept for each type/identifier. Parent cycles are rejected, and issues from ancestor records count against each descendant job.
  - **Job gates (lines 402–446):** each job must succeed or have a documented skip, with no issues and no agent loss.
  - **Tests (lines 458–640):** runs are matched to the current attempt by `phaseName.jobName`. A run with a newer attempt than the timeline is rejected. Publications are required for each job and suite, and `--allow-skipped` cannot exempt an expected test.
  - **Deadline and stability (lines 692–720):** the 600 s deadline starts before merge verification. The build is collected again after the tests are read, and that result gates `collectionStable`. `mergeReady` is always false.
- [x] **`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.** Read in full.
  - **Startup (lines 15–24):** `dont_write_bytecode` is set only under `__main__`, before the sibling imports.
  - **Build URLs (lines 47–75):** only HTTPS on the trusted host and project path is accepted, with no userinfo or fragment, and build IDs must fit in int32.
  - **Monitoring (lines 98–258):** the deadline comes from the kickoff time, and superseded or replaced runs are handled. Timeline mode verifies the merge once and reuses the same deadline. An agent-loss event is emitted once per job attempt.
  - **Exit codes (lines 352–362):** both `azure_completed` and `attention_required` exit with 4.
  - **Considered, not raised:** each poll reads GitHub before Azure. A build that finishes between the two reads can therefore end as `azure_completed` instead of `success`. `ci-triage.md` lines 46–49 document this as the "missing or lagging" outcome, with a nonzero exit and triage required. The behavior is safe and matches the docs.
- [x] **`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.**
  - **Coverage (lines 15–33):** the guidance and required paths include both skills' safety references, `CODEOWNERS`, `pipeline.yaml`, and `templates`.
  - **Tool lookup (lines 40–65):** `trusted_executable` skips relative PATH entries and the current directory and its subdirectories, checks resolved symlink targets, and tries Windows PATHEXT extensions.
  - **Git calls (lines 68–88):** `git()` strips `GIT_*` variables and runs with `--no-replace-objects`, `core.fsmonitor=false`, and a timeout.
  - **Snapshot check (`verify()`):** it checks the repo root and HEAD, accepts only regular blobs, and requires a stage-0 index that matches the tree. `ls-files --others` without exclude flags also catches ignored files. Symlinked parents are rejected, and working bytes are compared with `cat-file blob`.
  - **Docs:** this matches the `trusted-guidance.md` bootstrap of `-I -B`, followed by `-E -s -B` for the other helpers.
- [x] **Documentation cross-check.**
  - **Anchors:** these all resolve:
    - `ci-delegation.md#evidence-required-before-triggering`
    - `loop-control.md#checkpoint-before-yielding`
    - `loop-control.md#evidence-invalidation`
    - `ci-triage.md#waiting-for-azure-pipelines`
    - `ci-evidence.md#github-snapshot`
  - **Gauntlet extraction:** `review-gauntlet.md` keeps every rule removed from SKILL.md §8: final and pre-commit passes, the explicit merge-base-to-index diff, artifact placement, collision and independence rules, the six-clean frozen-patch rule, and the new-PR bootstrap handoff. Its new pre-PR blocker is consistent with the AGENTS.md session-draft rule.
  - **Skill docs:** the external-contributor `SKILL.md` and `ci-delegation.md` additions agree with the pr-loop validation-only handoff and the `loop-control.md` scenarios.
  - **Readiness claims:** the `ci-evidence.md` readiness table and its claims match the code. These include "behind" as an integration gate, the Administration read needed for classic protection, skipped/neutral checks as unproven, and warnings needing a recorded disposition.
- [x] **Tests inspected (not run).**
  - `tools/ci/tests/test_get_pr_readiness.py` mocks `gh` through a pwsh function and skips only when `pwsh` is absent (line 192).
  - `test_collect_ci_evidence.py` has 35 tests covering provenance, retries, named vs. suite scope, sanitized errors, redirect refusal, and bytecode state.
  - `test_watch_azure_pipeline.py` and `test_trusted_guidance.py` were also reviewed.
  - The CIHelpers job (`pipeline.yaml` lines 112–126, image from line 102) runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04. A grep of the new helpers and tests found no APIs or nested-quote f-strings that require Python 3.11 or 3.12.
- [x] **Scope.** All 17 changed paths are in the two skills' docs and scripts or in `tools/ci/tests`. Nothing changes `pipeline.yaml`, workflows, dependency pins, release tooling, product APIs, `AGENTS.md`, or `CONTRIBUTING.md`.
- [ ] **Not done: test execution and live GitHub/Azure queries.** This is a read-only lens, so the supplied validation results were taken as reported and not reproduced.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4268 added and 152 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 27,
  "completedPriorPasses": 26,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
