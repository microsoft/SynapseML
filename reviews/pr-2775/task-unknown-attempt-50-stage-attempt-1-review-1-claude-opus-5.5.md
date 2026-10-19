---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 50
stage_attempt: 1
round: 1
timestamp: "2026-10-18T15:28:01Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "123863fee4b825a997725e89eb5f2c7907de4e8757d5e12a6c18fa1672d8f359"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-50-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and no-bloat gate.** The diff touches 17 paths: the external-contributor `SKILL.md` and `references/ci-delegation.md`, the PR-loop `SKILL.md`, six PR-loop references (`ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md`, `trusted-guidance.md`), four PR-loop scripts and four `tools/ci/tests/` suites. It changes no product API, dependency pin, release tool, `pipeline.yaml` or workflow file.
  - The worktree copies of all four scripts match the supplied hunks.
  - Each file supports a stated criterion. `review-gauntlet.md` is the moved gauntlet text, and `SKILL.md` step 8 now only links to it.
  - I found no orphaned helper or duplicated contract.
- [x] **Comment-trigger gate in code.** In `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - Lines 92–111: expected SHAs are validated as 40-hex and apply to exactly one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - Lines 520–560: fresh PR, head-repository and target-branch rechecks feed `revisionErrors` and `evidenceComplete`.
  - Lines 610–649: the helper throws on incomplete evidence or a non-OPEN PR. It posts only when the exact-case `microsoft.SynapseML` aggregate is absent, and keeps the receipt separate from `pipelineRunRequested`.
  - The conservative complete-collection gate is retained, as the plan requires.
- [x] **Every documented comment route uses the gate.** The links appear at:
  - PR-loop `SKILL.md` lines 216–218 and 272–281.
  - `ci-triage.md` lines 16–17.
  - `readiness-gates.md` lines 98–99.
  - `ci-delegation.md` lines 69–71.
  - The hand-posted requeue rule in `ci-evidence.md` lines 110–114.

  The external-contributor `SKILL.md` sends its `/azp run` through the PR loop's validation-only handoff. All cross-file anchors used by the diff resolve to existing headings (`comment-trigger-gate`, `evidence-required-before-triggering`, `evidence-invalidation`, `artifact-retention`, `waiting-for-azure-pipelines`, `validation-only-handoff`, `github-snapshot`, `azure-merge-and-named-tests`, `targeted-infrastructure-retry`).
- [x] **Separate engineering and human gates** (`Get-PrReadiness.ps1` lines 559–578 and the readiness object at 683–713):
  - `remoteEngineeringGatesClear` requires, among other gates: complete evidence, head-covering automated review, no unevaluated rules or classic properties, and no unresolved or suppressed findings. Required checks must be present (case-sensitive) and successful, with no pending or approval-gated workflows, `behind_by == 0` and `MERGEABLE`.
  - `humanApproval` is `unknown` when rule reads failed. It is `not_required` only when every collected rule explicitly requires zero approvals and has no code-owner, last-push or named-reviewer requirement.
  - `merge` never authorizes a merge, and `completeness.complete` stays an alias for the remote gates only. This matches the `ci-evidence.md` table.
- [x] **Workflow scoping** (`Get-PrReadiness.ps1` lines 240–295 and 377–442):
  - Paged REST output is read as one compact JSON page per line, and empty pages or bodies become collection errors.
  - `total_count` is checked for completeness.
  - `pull_requests` associations are trusted only with a matching base repository URL and head SHA.
  - The fork fallback needs a unique same-branch PR from a `state=all` lookup; any ambiguity becomes a collection error.
  - Supersession picks the newest run per `workflow_id`/`event`, and only after PR scoping.
- [x] **Collector provenance** (`collect_ci_evidence.py`):
  - `verify_merge` (lines 203–215) requires the parents in order `[base, head]`.
  - `verify_build` (218–256) pins definition 17563, the GitHub repository, `refs/pull/<n>/merge`, the merge SHA and the queue reason.
  - `collect_tests` (487–672) binds runs to timeline Job `phaseName.jobName` keys and their attempts. That key format fits the implicit single-stage `jobs:` layout of `pipeline.yaml`.
  - It rejects attempts newer than the timeline and fails closed when a requested run has unknown provenance. On retries, each job must republish its expected suites and tests. `--allow-skipped` cannot exempt a named `--expected-test`.
  - `main` (720–749) collects the build twice to compute `collectionStable`.
- [x] **Timeline and job gates** (`summarize_timeline` lines 259–430, `collect_build` 433–475):
  - Identities and attempts are validated, and parent cycles are rejected.
  - Ancestor issues propagate to their jobs, and issue counts use the larger of the listed and reported counts.
  - Leaf Phase/Stage containers are gated but give no test-attempt evidence.
  - An empty timeline is allowed only before the build completes. Warnings, agent loss, or an unexplained skip each block `jobGatesPassed`, as documented.
- [x] **Credential handling and process safety** (`collect_ci_evidence.py` lines 39–200):
  - The endpoint allow-list regex and fixed project root hold, `NoRedirect` refuses redirects, the token stays in memory, and error messages are sanitized.
  - CLIs are resolved through `trusted_executable`. Batch launchers get a timeout and `taskkill /T /F` tree cleanup, and only constant arguments reach `az.cmd`, so batch-argument injection is not possible.
  - The `Authorization` header at line 150 appeared masked in tool output. A literal-`******` search found no match, and `Bearer` is present in both the script and `test_collect_ci_evidence.py`, so this is display redaction, not a source defect.
- [x] **Watcher timeline mode** (`watch_azure_pipeline.py`, `monitor` about lines 110–262, `parse_args` about 283–320, exit-code map about 356–366):
  - `--azure-timeline` requires `--base-sha` and `--merge-sha`, and the merge is verified once.
  - The kickoff-based deadline is never extended, and the watcher sleeps 600 seconds per poll while Azure is still running.
  - A missing, lagging or disagreeing GitHub aggregate returns `azure_completed` or `attention_required`, both exit 4.
  - With `azure=None`, behavior is unchanged from before. All of this matches `ci-triage.md` lines 45–60.
- [x] **Trusted-guidance verifier** (`verify_trusted_guidance.py`):
  - It checks HEAD, tree, index (stage 0), untracked and ignored files, and working bytes across `GUIDANCE_PATHS`. Only regular-file modes are accepted, and symlinked parents are rejected after the checkout path is resolved.
  - `GIT_*` variables are stripped and `core.fsmonitor=false` is set.
  - CLI lookup excludes relative and cwd-contained PATH entries.
  - `tools/ci/tests/test_trusted_guidance.py` covers each of these, including flag strings parsed from the documentation (`-I -B` and `-E -s -B`).
- [x] **CI coverage path.** The existing `CIHelpers` job (`pipeline.yaml` lines 112–126) already runs `python3 -m pytest tools/ci/tests/`, so all four suites run with no pipeline change. `test_get_pr_readiness.py` lines 249–251 skip only when both `pwsh` and `TF_BUILD` are absent, so a missing `pwsh` fails in Azure as `ci-triage.md` states. The helper declares `#Requires -Version 7.2`.
- [x] **Supplied validation evidence was assessed, not re-executed.** The latest Windows and Linux runs covered three suites (collector, readiness and watcher). `test_trusted_guidance.py` is covered only by the earlier full-directory baseline; the supplied record says the later edits were to readiness and collector code. I executed no tests.
- [ ] **Hosted-agent CI for this candidate:** not applicable. No current-candidate Azure run exists or was authorized. `CIHelpers` has a 15-minute job timeout, and hosted runtime for the enlarged readiness suite cannot be verified from a read-only review.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4988 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 50,
  "completedPriorPasses": 49,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Subsequent disposition

This clean round belongs to attempt 50's frozen patch. A later round identified
duplicated no-bloat guidance, which was consolidated without changing code.
That documentation change invalidates this pass; this round is retained only
as historical evidence.
