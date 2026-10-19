---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 42
stage_attempt: 1
round: 1
timestamp: "2026-10-18T04:59:14Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "98a86002dcdc91958351cb97a609921c5d3aa3cc675787d53d903e23d7bffb07"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-42-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The CI-trigger gate matches its documentation.**
  - In `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, lines 97–106 guard the arguments: `-RunPipeline` needs exactly one PR and both expected SHAs, and it rejects `-WaitForReview` and `-RequestReview`. Line 1 enforces `#Requires -Version 7.2`.
  - Lines 608–637 do the triggering. If `evidenceComplete` is false, the script throws and lists the gaps (revision errors, collection errors, truncated threads). It also requires an OPEN PR. It posts `/azp run` only when the exact-case `microsoft.SynapseML` check is missing, and at most once per invocation.
  - This matches the comment-trigger gate in `references/ci-evidence.md` (lines 93–115), including the conditions for a hand-posted requeue: a fresh snapshot, both SHAs matching, and `evidenceComplete=true`.
- [x] **Every documented comment route resolves to the canonical `#comment-trigger-gate` anchor.** These are PR-loop `SKILL.md` step 7 and step 9 (line 274), `ci-triage.md` lines 16–17, `readiness-gates.md` lines 98–99, and `ci-delegation.md` step 3.
  - The contributor `SKILL.md` (lines 41 and 66) sends CI through the PR loop's gated stages.
  - The targeted retry in `ci-triage.md` (around lines 188–215) uses the Azure Build Update API under separate authorization. It does not post a comment.
- [x] **The readiness states are kept separate.** In `Get-PrReadiness.ps1`, see lines 557–576 and the output object up to line 701.
  - `engineering` is never "ready", and `merge` never grants merge authorization.
  - `humanApproval` is `unknown` when rules can't be read. It is `not_required` only when every collected rule explicitly requires zero approvals.
  - `completeness.complete` remains an alias for `remoteGatesClear`, as the `ci-evidence.md` table and `readiness-gates.md` describe.
- [x] **Workflow runs are associated with the PR safely.**
  - `Get-VerifiedRunAssociations` (line 274) checks both the base repo URL and the head SHA.
  - The fork fallback (lines 386–420) needs exactly one PR from that fork and branch, searched across `state=all`, and it must be this PR at its current head. Anything ambiguous becomes a collection error, which blocks the trigger.
  - `tools/ci/tests/test_get_pr_readiness.py` lines 848–1050 cover these cases: a success on another PR can't hide this PR's pending approval; unverified or foreign associations; the legacy `gh` identity shape; and a shared fork branch, which blocks `-RunPipeline` with the specific diagnostic.
- [x] **The guidance verifier is isolated.** See `scripts/verify_trusted_guidance.py`, lines 40–157.
  - Git hardening: `GIT_*` variables are removed from the environment, git runs with `--no-replace-objects` and `core.fsmonitor=false`, and no content filters run.
  - Snapshot checks: HEAD, tree, index and working files must all match. The only allowed difference is CRLF line endings in text files. Untracked or ignored files, symlinks and submodules are rejected.
  - `trusted_executable` skips relative PATH entries and anything inside the current directory, including symlink targets.
  - The verifier imports only the standard library, so the `python -I -B` bootstrap in `trusted-guidance.md` works. Afterwards `-E -s -B` adds only the verified script directory to the import path.
- [x] **The collector checks provenance and fails closed.** See `scripts/collect_ci_evidence.py`.
  - The Azure client allows only listed endpoints (line 90) and refuses redirects (line 64). The `az` arguments are fixed, so no untrusted input reaches a Windows `.cmd` launcher, and stderr and response bodies are never echoed.
  - `verify_merge` requires the parent order [base, head]. `verify_build` checks definition 17563, the repository, `refs/pull/N/merge`, the source version and the build reason.
  - The timeline uses the latest attempt per job. Leaf Phase/Stage containers count as gated jobs, and issues are inherited from ancestors.
  - `collect_tests` (line 455) needs job-attempt provenance for every requested run. Results from older attempts can't satisfy a current job. A mismatch between `totalTests` and the fetched count catches truncated pages.
  - The 600-second deadline (line 690) covers both the GitHub merge check and the Azure calls. `validationPassed` (line 701) also needs `collectionStable`, and `mergeReady` is always false.
- [x] **The watcher behaves as documented.** See `scripts/watch_azure_pipeline.py`.
  - `parse_build_id` accepts only HTTPS URLs on the two trusted hosts, with the project GUID or `a365`. It rejects userinfo, fragments, other ports, and anything other than one numeric int32 `buildId`.
  - The deadline runs from kickoff and is capped at 120 minutes. The watcher reports `superseded` if the head or open state changes and `replaced` if a newer build appears.
  - In timeline mode it checks the merge once and emits `agent_loss` once per job attempt. It keeps waiting while a same-build retry is running, even when GitHub still shows the old failure. It returns `azure_completed` or `attention_required` (exit 4), never success, in those cases.
  - The exit codes (line 363) match `ci-triage.md`. Tests at `test_watch_azure_pipeline.py` lines 276–360 and 640–760 cover these paths.
- [x] **The tests are wired into CI and portable.**
  - The existing `CIHelpers` job in `pipeline.yaml` (lines 111–126) already runs `tools/ci/tests/`, so no pipeline change is needed.
  - The readiness harness (line 234) skips only when `pwsh` is missing and `TF_BUILD` is unset, as `ci-triage.md` says.
  - `test_trusted_guidance.py` (lines 51–58) sets a git identity with `-c`, so commits work on hosted agents without a global git config.
  - The changed Python files use no 3.11+ syntax or APIs, and `parse_kickoff` handles Python 3.10's fractional-second limits.
- [x] **No-bloat and scope check.**
  - The old step 8 text was moved into `references/review-gauntlet.md`, not duplicated. `SKILL.md` lines 264–267 now just link to it.
  - Grep finds no in-repo consumers of these helpers beyond the docs and tests.
  - The reviewed paths include no pipeline, workflow, dependency-pin, release-tool or product API files.
- [x] **Candidates I considered and rejected as non-defects:**
  - Duplicate triggers across separate invocations: the docs make the caller responsible.
  - `AGENTS.md` line 148 tells agents to use `/azp run` without mentioning the gate. That text predates this diff, and the skills route triggers through the gate.
  - Treating any timeline issue as blocking: this is conservative by design, with manual disposition documented.
  - A `ValueError` from malformed `git -z` output in the verifier: git doesn't produce that output.
  - Clock skew rejecting `--kickoff-at`: the error is explicit and the run can simply be retried.
- [ ] **I did not run any tests.** The plan made this a read-only review. The results quoted in the prompt come from the caller and are not my claims.
- [ ] **I did not verify live API behavior.** This includes Azure page-size caps on the test-run listing and whether `errorCount` can be null. The code fails closed in both cases. I relied on the caller's check of the format of build 241234907.
- [ ] **I did not read every collector and watcher test body line by line.** I sampled the regression and edge-case tests listed above.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4412 added and 178 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 42,
  "completedPriorPasses": 41,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
