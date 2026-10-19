---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 23
stage_attempt: 1
round: 1
timestamp: "2026-10-17T14:29:39Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e7d75774d965e591bbbc1d8e750e40ee4ea415a02482361beadcc178546a1d40"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-23-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` in full. Checked these parts:
  - The guards for expected SHAs and `-RunPipeline` (99–105).
  - Paginated `--jq tojson` REST parsing in `Get-RestEvidence` (237–270).
  - `Get-VerifiedRunAssociations`, which checks the base repo URL and head SHA (272–279).
  - The fork fallback, which uses a `state=all` uniqueness check (375–397).
  - Scoping runs to this PR before choosing the newest run per `(workflow_id, event)` (398–418).
  - The revision recheck, including the expected SHAs (504–520).
  - Separate engineering, human-approval and merge states (521–541, ~632–647).
  - The once-per-invocation `/azp run`, posted only when the exact `microsoft.SynapseML` check is absent (579–597).
- [x] Read `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`. Checked these parts:
  - `AzureClient`: endpoint allowlist, refused redirects, sanitized errors, a shared deadline, and `$skip`/`$top` pagination that rejects duplicate IDs.
  - `verify_merge` and `verify_build`: ordered `[base, head]` merge parents and build provenance.
  - `summarize_timeline`:
    - Keeps the highest attempt for each `(type, identifier)` and skips ancestor containers.
    - Inherits issues along the record's lineage and counts them as `max(listed, count)`.
    - Excludes setup-type steps from test steps.
  - `collect_tests`: selects the current attempt and requires publications per job; `--allow-skipped` cannot exempt an `--expected-test`.
  - `main`: rechecks stability and requires `result == succeeded`.
  - The source builds `****** from the in-memory token. No CLI stderr or HTTP body is echoed.
- [x] Confirmed the collector's `{phaseName}.{jobName}` job key matches this pipeline's timeline identifiers. `pipeline.yaml` uses top-level `jobs:` (111), and `UnitTests` (807) has the matrix leg `image` (883–884). The `stages:` entry at line 918 is a matrix leg, not pipeline stages.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` in full. Checked:
  - The deadline is anchored to kickoff.
  - Supersession is checked before any Azure read.
  - Merge verification is cached.
  - It keeps sleeping while Azure is not completed, even if GitHub state is stale.
  - `azure_completed` and `attention_required` both exit 4.
  - `agent_loss` events are deduplicated per job attempt.
  - The trusted-URL allowlist and int32 build-ID bounds are enforced.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` in full. Checked:
  - It strips `GIT_*` variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`.
  - It requires root and HEAD to match the expected checkout and SHA.
  - The tree must contain regular files only, and the stage-0 index must equal it.
  - `ls-files --others` runs without excludes, so ignored files are caught too.
  - Paths through symlinks are rejected, and CRLF is tolerated only for non-binary files.
  - `trusted_executable` skips the current directory and its descendants.
  - Every `REQUIRED` path exists, including the root `CODEOWNERS`.
- [x] Checked the docs against the code:
  - `references/ci-evidence.md`, `references/ci-triage.md` (6–83), `references/trusted-guidance.md` and `references/readiness-gates.md`.
  - `references/review-gauntlet.md`: every obligation removed from SKILL.md §8 is kept. The only intended change is the `-Execute` runner replacing manual prompt assembly.
  - `references/loop-control.md` retention rules (7–31).
  - The external-contributor `SKILL.md` and `references/ci-delegation.md` additions.
  - All relative links and anchors resolve.
- [x] Checked CI coverage. The `CIHelpers` job in `pipeline.yaml` (112–126) runs `python3 -m pytest tools/ci/tests/ -q`. Read the fixtures and assertions in:
  - `tools/ci/tests/test_get_pr_readiness.py`, including the fake `gh` paging contract (72–77), approval and pending workflows, skipped or neutral required checks, zero-approval rules, review requests across polls, exact check matching, empty policy output, and classic-protection settings.
  - The collector, watcher and trusted-guidance test modules.
- [x] Investigated these candidates and dropped them because they fail closed or are documented:
  - Two PRs sharing one head produce an ambiguous-association error. This is correct, because `pull_request` runs test a merge ref specific to each base.
  - `failedChecks` lacks `STARTUP_FAILURE`, but `failedWorkflows` covers `startup_failure`.
  - Refusing PATH entries under the current directory is documented.
  - In timeline mode, a completed build with no timeline gives `error`, which is nonzero.
  - Pruning superseded artifacts in `loop-control.md` (15–29) requires verified history retention and falls back to keeping them in the tree.
- [ ] Tests and live GitHub/Azure queries were not run, because this reviewer is read-only. The author's reported results (29 passed/75 subtests, 72/159, and the older Linux run of 321/21/160) and the live build observations were not reproduced.

## Issues

### Issue 1: SKILL.md bans `-RunPipeline` with `-WaitForReview` only for external PRs, but the helper now refuses it for every PR
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/SKILL.md
- **Line(s)**: 283–284. Related: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` lines 103–104 (guard) and 576–578 (comment).
- **Description**: Section 9 still says "Do not combine `-RunPipeline` with the waiting loop for external PRs, where the head could change after clearance." This diff makes `Get-PrReadiness.ps1` throw for `-RunPipeline` plus `-WaitForReview` on every PR, before any query runs (103–104). `references/ci-evidence.md` (80–81) correctly says the trigger "refuses `-WaitForReview`". The "for external PRs" qualifier in the main skill implies the combination is allowed for internal PRs, which contradicts both the helper and the reference. The script comment at 576–578 is also stale: it justifies the once-per-invocation `/azp run` guard with "under -WaitForReview this runs every poll", which can no longer happen.
- **Risk**: Low. The guard fails before any side effect, so nothing unsafe is triggered. The cost is a contract mismatch in the main skill: agents following it for an internal PR waste a run and may misread the trigger's safety model.
- **Suggested Fix**: Reword SKILL.md 283–284 to say the combination is refused for every PR, for example: "The helper refuses `-RunPipeline` with `-WaitForReview` for every PR. Trigger once with the expected SHAs, then wait read-only." Optionally, reword the comment at 576–578 to describe the guard as defense in depth rather than protection against repeated polls.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Made the skill's trigger/wait prohibition apply to every
  PR, and removed the obsolete polling explanation from the helper comment.
- **Why**: The main instructions must match the unconditional runtime guard
  and the existing CI evidence reference.
- **How verified**: Compared the wording with the guard before any GitHub
  query. Only documentation and a comment changed; the existing 29-test,
  75-subtest readiness result remains applicable. No behavioral test rerun
  was needed.

The original artifact was retained before this resolution update; SHA-256:
`5e3dbbf5582c4e8cf16eeae38887999b812293de4574deb4db2121bc3de3868b`.
The original `issues_found` verdict is preserved and does not qualify the
revised patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4094 added and 146 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 23,
  "completedPriorPasses": 22,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
