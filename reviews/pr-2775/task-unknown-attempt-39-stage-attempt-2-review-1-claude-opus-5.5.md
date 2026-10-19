---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 39
stage_attempt: 2
round: 1
timestamp: "2026-10-18T02:17:51Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "beb988f204a37a4f65708b18b91b54b8e2d3e92dfacfe24b4f2988c9fb3ca3c4"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-39-stage-attempt-2-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`**: every path fails closed. I checked these sections:
  - Argument guards (~96–106): `-RunPipeline` needs exactly one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`.
  - `Get-RestEvidence` paging (~238–273): empty output, `null` pages and malformed pages become collection errors.
  - `Get-VerifiedRunAssociations` and `Get-HeadRepositoryName` (~275–294): associations require the base repository URL and the run's head SHA. Older `gh` output that lacks `nameWithOwner` falls back to `headRepositoryOwner.login` plus `headRepository.name`.
  - Branch rules and classic protection (~321–367): `rulesComplete` is computed after the classic protection read.
  - Workflow runs (~369–438): the collected count is checked against `total_count`, a fork run without an association needs a unique matching PR, and runs are scoped to this PR before picking the newest per `workflow_id`/`event`.
  - Checks (~440–472): the `microsoft.SynapseML` aggregate must match exactly and case-sensitively. Skipped or neutral required checks stay unproven.
  - Revision recheck (~518–560): an unreadable recheck is reported as unreadable, not as movement.
  - `remoteGatesClear` and `humanApproval` (~557–577).
  - Review request and `/azp run` trigger (~579–631): each is attempted once. The trigger also needs `evidenceComplete`, an OPEN PR and an absent aggregate.
  - Readiness output (~666–697): the `merge` state and the `complete` alias.
- [x] **`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`**:
  - `command_json` (37–56): looks up tools through `trusted_executable` and sanitizes CLI errors.
  - `AzureClient` (70–155): refuses redirects, allowlists endpoints, enforces the overall deadline, keeps the token in memory only, and treats a repeated ID during paging as an error.
  - `verify_merge` and `verify_build` (158–220): check the ordered merge parents, definition 17563, the repository, `refs/pull/N/merge`, the source version and the queue reason.
  - `summarize_timeline` (223–390): validates field types, keeps the highest attempt per record, detects parent cycles, gates leaf Phase/Stage records and adds ancestor issue counts to each job.
  - `collect_tests` (458–643): requires current-attempt provenance, rejects a test-run attempt newer than the timeline, and enforces the inferred job/suite and job/test publications. `--allow-skipped` cannot exempt an `--expected-test`.
  - `main` (~692–720): rechecks the build after reading tests (`collectionStable`) and always sets `mergeReady` to false. I traced every malformed nested field to `EvidenceError`, which matches the claim in `references/ci-evidence.md`.
- [x] **`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`**:
  - `parse_build_id` (47–75): allows only the trusted host, project path and int32 build IDs.
  - `monitor` (from line 95):
    - The deadline comes from the kickoff time and is never extended.
    - Merge verification runs once, and a new Azure client is created each poll.
    - Agent-loss events are deduplicated by job and attempt.
    - A stale GitHub failure during a same-build retry does not stop the watch.
    - The `azure_completed` and `attention_required` outcomes never map to success.
  - `parse_kickoff` normalizes fractional seconds for Python 3.10.
  - The exit-code map in `main` covers every outcome.
- [x] **`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`**:
  - `trusted_executable` (40–71): skips relative PATH entries and anything at or under the working directory, and handles PATHEXT on Windows.
  - `git` (~74–96): removes `GIT_*` variables and runs with `--no-replace-objects` and `core.fsmonitor=false`.
  - `verify` (~99–156) checks:
    - the worktree root and HEAD;
    - that `ls-tree` lists only regular-file modes;
    - that required files exist;
    - that the stage-0 index matches the tree;
    - that no untracked or ignored files exist (no exclude rules applied);
    - that no symlinks are traversed;
    - that file bytes match, with CRLF tolerance only for non-binary blobs.

  None of these commands run hooks, Git content filters or a pager.
- [x] **Docs match the code.** The documented commands match the `argparse` definitions:
  - collector: `references/ci-evidence.md:105`
  - watcher: `references/ci-triage.md:26,35,40`
  - verifier: `references/trusted-guidance.md:18`

  The readiness states are documented once (`references/ci-evidence.md:72–79`) and match the script output. The ruleset ignore list (`ci-evidence.md:61–62`) matches lines ~335–339 of the script. The `-RunPipeline` and `-RequestReview` rules in `SKILL.md` match the guards. Cross-document anchors and relative links resolve, and the required trusted files (`CODEOWNERS`, `pipeline.yaml`, `contributor-safety.md`) exist.
- [x] **Tests in `tools/ci/tests/`**:
  - Files and counts: `test_get_pr_readiness.py` (35), `test_collect_ci_evidence.py` (35), `test_trusted_guidance.py` (15) and `test_watch_azure_pipeline.py` (40).
  - Coverage: they cover the claimed regressions, including:
    - hidden or failed workflow approvals
    - skipped or neutral required checks
    - older `gh` identity output
    - unreadable versus moved revisions
    - fractional kickoff timestamps
    - redirect refusal
    - bytecode and fsmonitor side effects
    - a retry starting during collection
    - a skip trying to waive a named test
  - Counts match the supplied 75- and 90-test totals.
  - The PowerShell harness runs the real script against a mocked `gh`. It skips locally only when both `pwsh` and `TF_BUILD` are absent, so it does not skip in CI.
  - The existing `pipeline.yaml` CIHelpers job (~112–126) runs `python3 -m pytest tools/ci/tests/ -q`.
- [x] **Scope and no-bloat check**:
  - 17 paths: 9 docs, 4 helpers, 4 tests.
  - No product API, dependency pin, release tooling, `pipeline.yaml` or workflow changes.
  - The gauntlet text was moved into `references/review-gauntlet.md` rather than duplicated. The new no-change outcome ("validated without maintainer changes") matches the handoff wording in `SKILL.md` and the `loop-control.md` acceptance table.
- [x] **Candidate findings I rejected**:
  - **`azure_completed` while the GitHub aggregate lags.** The recheck-on-exit behavior is documented in `ci-triage.md`, it exits with 4 rather than success, and tests assert it.
  - **Azure paging stops on a page shorter than `$top=100`.** A short or repeated page fails closed through the duplicate-ID check.
  - **Ruleset `creation` rule counts as unevaluated while classic `block_creations` is ignored.** This is conservative and matches the documented ignore list. It only matters if such a ruleset targets the base branch.
  - **Suppressed review bodies on the head block the remote-clear signal.** This is conservative.
- [x] **Supplied validation (cited, not re-run).**
  - The readiness and watcher suites passed on Windows (Python 3.14.6) and Linux (Python 3.12.3): 75 tests and 165 subtests, no skips.
  - Three-suite runs:
    - Windows: 89 tests and 183 subtests passed, with one POSIX-only skip.
    - Linux: 90 tests and 183 subtests passed.
  - Isolated Python 3.10.20 `unittest`: all 90 tests passed.
  - Black 22.3.0, `git diff --check` and `git diff --cached --check` passed.
- [ ] **Tests not executed by this reviewer.** No tests, Black or PowerShell harness were run in this review (read-only tools only). Live GitHub and Azure behavior was not exercised, and there is no Azure CI run for this exact patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4371 added and 173 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 39,
  "completedPriorPasses": 38,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
