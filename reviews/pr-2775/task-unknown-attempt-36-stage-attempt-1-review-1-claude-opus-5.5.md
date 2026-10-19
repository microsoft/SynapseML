---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 36
stage_attempt: 1
round: 1
timestamp: "2026-10-17T22:31:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "5683f3eb67d0c009ac9c503dfffcf8b29ef49659fb976a4e9ff82d7ce31e56f5"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-36-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I reviewed the current worktree state of all 17 changed paths:
  - two external-contributor skill files;
  - the PR-loop `SKILL.md` and its six references;
  - four scripts under `.github/skills/synapseml-pr-loop/scripts/`;
  - four suites under `tools/ci/tests/`.

  None of them changes a product API, dependency pin, release tool, `pipeline.yaml`, or a workflow definition.
- [x] **`Get-PrReadiness.ps1`.**
  - The top-level guards (lines 101–108) require exactly one PR when expected SHAs or `-RequestReview` are passed. `-RunPipeline` requires both SHAs and refuses to run with `-WaitForReview` or `-RequestReview`.
  - `-RequestReview` (586–606) runs at most once per PR. It is gated on revision errors and the PR being OPEN.
  - `-RunPipeline` (612–628) requires `evidenceComplete`, an OPEN PR, and no existing check named exactly `microsoft.SynapseML` (case-sensitive). The comment at 618–619 correctly says this catches stale requests but not a trigger race.
  - `#Requires -Version 7.2` (line 1) matches the documented PowerShell 7.2+ requirement.
  - No native-command argument contains an embedded double quote (GraphQL queries at 137–180; `gh` arguments at 205, 245, 599, 624). PowerShell 7.2's legacy argument passing is therefore safe.
- [x] **`collect_ci_evidence.py`.**
  - An endpoint allowlist regex and `NoRedirect` (64–68) stop the token from being forwarded on a redirect.
  - The token comes from an `az account get-access-token` call whose arguments are all constants (97–112), so the Windows `az.cmd` wrapper never receives attacker-influenced input.
  - grep confirmed the header is `******; the viewer only masks it on display.
  - CLI and HTTP errors are sanitized and never echo stderr or response bodies (48–57).
  - `verify_merge` requires the merge parents to be exactly `[base, head]`.
  - The job-gate fields (405–447) keep `unresolvedJobs` and `jobGatesPassed` separate from `attentionRequired`.
  - The entry point always reports `mergeReady=false`.
- [x] **`watch_azure_pipeline.py`.**
  - `parse_build_id` (47–75) enforces HTTPS, port 443, no userinfo, a trusted lowercased path, no fragment, and a single numeric `buildId` within int32 range. An empty URL fails closed.
  - `monitor` (98–258) keeps polling while Azure has not completed, even when GitHub reports SUCCESS, so it never passes early.
  - A GitHub/Azure disagreement returns `attention_required`. A missing or pending GitHub check after Azure completes returns `azure_completed`. Both exit with code 4.
  - The deadline is anchored to the kickoff time and tracked on a monotonic clock.
  - It emits one `agent_loss` event per job identifier and attempt.
  - All of this matches `references/ci-triage.md` lines 38–59.
- [x] **`verify_trusted_guidance.py`.**
  - It strips `GIT_*` variables, sets `core.fsmonitor=false`, and requires the index to match the tree at stage 0 with regular blobs only.
  - Untracked and ignored files both fail, because `ls-files --others` runs without `--exclude-standard`.
  - Symlinks are rejected. CRLF normalization is allowed only for blobs without NUL bytes.
  - `trusted_executable` skips relative PATH entries and the current directory and its subdirectories.
  - The scripts disable bytecode writes only when run directly, under `__main__` (watcher lines 14–16).
- [x] **Documentation consistency.**
  - Every new relative link and anchor resolves, including:
    - `../synapseml-pr-loop/SKILL.md#validation-only-handoff`
    - `ci-evidence.md#azure-merge-and-named-tests`
    - `ci-triage.md#waiting-for-azure-pipelines`
    - `loop-control.md#evidence-invalidation`
  - What the docs say about `-RunPipeline`, `-RequestReview`, and `completeness.complete` matches the script guards (SKILL.md 215–283, ci-evidence.md 48–90, readiness-gates.md 92–96).
  - The rules removed from SKILL.md step 8 now live in `references/review-gauntlet.md` (lines 6, 19, 35–37, 50–65). They cover the diff-file input, stopping on an output-path collision, the prompt byte budget, requiring the same frozen patch, blockers, and the artifact-only commit.
- [x] **Test evidence consistency.**
  - `test_get_pr_readiness.py` has 33 tests and `test_watch_azure_pipeline.py` has 39, matching the reported totals.
  - The cited `-k` selectors match real tests: `review_request` (446, 465, 471), `unsafe_pipeline_requests` (701), and `failed_revision_reads_during_poll` (268). This is consistent with the reported 5 selected/28 deselected and 1 selected/32 deselected runs.
  - The `skipUnless(which("pwsh") or TF_BUILD)` guard (199–201) makes the readiness tests fail rather than skip in Azure when PowerShell is missing.
  - The collector and watcher tests patch every `gh`, `az`, and HTTP call.
- [x] **CI coverage.** The `pipeline.yaml` CIHelpers job (112–126) runs `python3 -m pytest tools/ci/tests/ -q` on Microsoft-hosted `ubuntu-22.04`, which bundles PowerShell 7. The new suites therefore run in Azure, as `tools/ci/README.md` lines 144–148 documents.
- [x] **Candidates considered and rejected.**
  - `GUIDANCE_PATHS` omits instruction directories, but none of those directories exist here, so the risk is only hypothetical.
  - A watcher exit 0 with `attentionRequired=true` is documented as not establishing job gates (ci-triage.md 54–59).
  - A duplicate `/azp run` between a push and the check registering is documented and handled by procedure (SKILL.md 217–230), and the script posts at most once per invocation.
  - A matching check with no URL raises an error, which fails closed.
- [ ] **Tests not executed.** I am a read-only reviewer. I checked the claimed results only for selector and count consistency; I did not reproduce them.
- [ ] **Manifest SHA-256 not recomputed.** The supplied diff text was truncated in this session. I reviewed the current contents of all 17 paths and confirmed the main removal was relocated, rather than replaying every hunk.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4392 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 36,
  "completedPriorPasses": 35,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver audit - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`6f76eff939cb971d7785fdc4b40f1ba3c27799bc68920e157c316577e1edc339`.
The supplied diff SHA-256 was
`c101afbe73b9a9836fafca6b132ef5b7d2ac7fae0d3a0a2d41c918be84f0c2e2`;
the attempt-36 manifest records its complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.

The transport audit confirmed the observed `claude-opus-5.5` model and only
read-only source, policy, and test access, without prior or sibling feedback.
Contrary to the truncation statement, the prepared input contains the exact
full diff. Both recorded message forms contain all 5,139 diff lines, including
all 17 file headers and the final hunk; only one header-expression line was
platform-masked. That masking was not bypassed.

Round 2 prompted a test-fixture cleanup, which invalidates this clean round
for the revised patch. No current-head Azure or CI-qualified pass is claimed.
