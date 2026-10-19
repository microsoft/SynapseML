---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 46
stage_attempt: 1
round: 1
timestamp: "2026-10-18T09:50:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "093058628ebe2ef0b985e22bbd23240330dcb74b502220d225c90be06426e78e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-46-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read all four changed helpers on disk and checked them against the supplied diff:
  - `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
  - `collect_ci_evidence.py`
  - `verify_trusted_guidance.py`
  - `watch_azure_pipeline.py`
- [x] **Comment-trigger gate** (`Get-PrReadiness.ps1`, about lines 102-110 and 614-641):
  - `-RunPipeline` requires exactly one PR and both expected SHAs, each validated as a full 40-character SHA.
  - It refuses `-WaitForReview` and `-RequestReview`.
  - It throws when revision evidence, collection or thread comments are incomplete, or when the PR is not OPEN.
  - It posts only when the exact, case-sensitive `microsoft.SynapseML` aggregate is absent, and at most once per invocation.
  - The conservative complete-collection requirement is intact.
- [x] **Separate readiness states** (about lines 561-581 and 676-692):
  - `evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval` and `merge` are derived independently.
  - `$rulesComplete` is captured before the compare and workflow reads.
  - A missing compare fails closed, because `$null -eq 0` is false.
  - `merge` additionally requires approval, an OPEN non-draft PR and `CLEAN`.
  - `completeness.complete` is only an alias.
- [x] **Required checks and workflows** (about lines 361-365, 374-433 and 456-476):
  - The exact Azure aggregate is always required, compared case-sensitively.
  - Checks matched by a `-RequiredCheck` prefix become exact required checks, and skipped or neutral runs remain unproven.
  - Run associations are verified by base repository URL and head SHA.
  - The fork fallback requires exactly one same-branch PR, searched with `state=all`, whose head matches.
  - The newest run per workflow and event is chosen only after scoping runs to this PR.
  - In `Get-RestEvidence`, `--paginate --jq tojson` yields one page per line, and empty, `null` or invalid output becomes a `collectionErrors` entry.
- [x] **Collector provenance** (`collect_ci_evidence.py`, `verify_merge`, `verify_build`, `AzureClient`):
  - Merge parents must be exactly `[base, head]`.
  - The build must be definition `17563` (integer), a GitHub repository `microsoft/SynapseML`, branch `refs/pull/N/merge`, the merge SHA as `sourceVersion`, and reason `pullRequest` or `manual`.
  - Endpoints are allowlisted, and only timeline requests tolerate 404 or an empty body.
  - Redirects are refused. A grep confirmed the Authorization header uses `******
  - CLI stderr and response bodies are not echoed.
  - `$skip`/`$top` pagination rejects repeated IDs.
- [x] **Timeline and test evidence** (`summarize_timeline`, `collect_build`, `collect_tests`):
  - The highest attempt wins per (type, identifier).
  - Ancestor containers are excluded and leaf containers are gated.
  - Ancestor issues count against each job, and issue totals use the larger of the listed issues and the reported counts.
  - Test runs need a `phase.job` key that matches a timeline Job, with an attempt no newer than the timeline's.
  - Historical attempts cannot satisfy current ones, and required publications are tracked per job.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - A second build read sets `collectionStable`, and `mergeReady` is always false.
  - `pipeline.yaml` declares top-level `jobs:` at line 111 with no `stages:`, which matches the `phaseName.jobName` key format.
- [x] **Watcher** (`watch_azure_pipeline.py`, `monitor` lines 98-256, `parse_build_id` lines 47-75):
  - In timeline mode it verifies the merge once and collects the build each poll inside the kickoff deadline.
  - It prints `agent_loss` once per (job, attempt).
  - It keeps waiting while Azure is non-terminal, even if GitHub shows a stale failure.
  - It returns `azure_completed` or `attention_required` (exit 4) and never extends the deadline.
  - It enforces the URL host/project allowlist and int32 build-ID bounds.
- [x] **Trusted-guidance verifier** (`verify_trusted_guidance.py`, lines 40-157):
  - It strips `GIT_*` variables and uses `--no-replace-objects` and `core.fsmonitor=false`.
  - It compares HEAD, tree, index, untracked or ignored files, and working-file bytes (allowing only CRLF normalization), and runs no Git filters.
  - It rejects symlinks and gitlinks.
  - `trusted_executable` skips relative PATH entries and anything under the current directory, and adds PATHEXT itself for Python before 3.12.
  - The required `CODEOWNERS`, `pipeline.yaml` and `templates/` exist.
- [x] **Documented trigger routes:** I grepped `.github/skills` for `/azp run|RunPipeline`.
  - Each trigger route in the changed docs links `ci-evidence.md#comment-trigger-gate` or is the guarded helper itself: pr-loop `SKILL.md` steps 7 and 9, `ci-triage.md`, `readiness-gates.md`, and `ci-delegation.md` step 3.
  - The external `SKILL.md` routes hand off to PR-loop step 7.
  - `branch-spark4-common.md:94` only confirms that a build exists.
  - All new anchors and linked files resolve.
- [x] **Docs match code:** every flag and output field cited in `ci-evidence.md`, `ci-triage.md` and `trusted-guidance.md` exists in the helpers. The `-I` bootstrap is limited to the verifier, which has no sibling imports.
- [x] **CI and test gating:**
  - The `CIHelpers` job (`pipeline.yaml` lines 112-126) has a 15-minute timeout and runs `pytest tools/ci/tests/`.
  - The readiness suite uses `skipUnless(which("pwsh") or TF_BUILD)` (`test_get_pr_readiness.py` lines 234-236), so a missing `pwsh` fails in Azure, as `ci-triage.md` states.
- [x] **Test assertions spot-checked:**
  - The harness asserts there are no unexpected trigger or review-request side effects and checks the exact command text (`test_get_pr_readiness.py` about lines 238-288).
  - The unsafe-trigger matrix is at about lines 810-872.
  - `test_watch_azure_pipeline.py:741` covers a stale GitHub failure during a same-build retry.
  - `test_trusted_guidance.py:330` checks that the fsmonitor hook is not executed.
- [x] **Scope and no-bloat:** the 17 paths are limited to skill docs, four helpers and four test files. There are no product API, dependency pin, release tool, pipeline or workflow changes. `review-gauntlet.md` moves the old `SKILL.md` step 8 rather than duplicating it.
- [ ] **Test execution:** not performed, because this is a read-only review. Runtime results come only from the supplied validation evidence and were not reproduced.
- [ ] **Hosted-agent CIHelpers duration:** no current-candidate Azure CI exists. The supplied local full-directory run (486.94 s) fits within the 900-second job timeout, but hosted timing is unverified. This is not reported as a defect because no hosted failure has been shown.
- [ ] **`AGENTS.md:148` generic `/azp run` guidance:** this file is unchanged by the diff and must stay identical across branches, so it is outside the reviewed scope and not reported.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4760 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 46,
  "completedPriorPasses": 45,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

The original feedback above is preserved unchanged. Observed execution used
`claude-opus-5.5`; the full patch was embedded in the input and only scoped
source reads occurred. Its SHA-256 was
`b0abdf55be3bc5a738d8f2c3a85050cff640ecbc8aa0a7be313a04c70b7f0577`.
The round was clean, but fixes after round 2 invalidated this pass.
Rounds 3-6 did not run. This is not a qualifying final review or CI evidence.
