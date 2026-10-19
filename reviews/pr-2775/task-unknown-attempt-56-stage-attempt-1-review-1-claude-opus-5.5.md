---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 56
stage_attempt: 1
round: 1
timestamp: "2026-10-18T23:55:21Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6686109c9b2ef6a6482e15ea42e916ac8b8b6ac46c47ac6ed00f201679295798"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-56-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The comment-trigger gate is kept as strict as before.** In `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - Lines 103–111 require exactly one PR and both expected SHAs, and refuse `-WaitForReview` and `-RequestReview`.
  - A mismatched expected SHA is added to `revisionErrors` (lines 541–558).
  - `-RunPipeline` throws unless `evidenceComplete` is true (lines 559–560 and 613–620) and the PR is `OPEN` (line 621).
  - It posts only when `microsoft.SynapseML` is absent, using a case-sensitive `-cnotin` check (lines 626–648).
  - `pipelineRunRequested` records only the POST exit status.

  Every documented comment route links to `ci-evidence.md#comment-trigger-gate`:
  - pr-loop `SKILL.md`, steps 7 and 9;
  - `ci-triage.md`, "Waiting for Azure Pipelines";
  - `ci-delegation.md`, step 3;
  - `readiness-gates.md`.

  A search limited to `.github/skills/` found the other `/azp run` mentions in `contributor-safety.md` and `branch-spark4-common.md`. They are prohibitions or verification steps, not other ways to trigger CI.
- [x] **Review requests and readiness states are kept separate.**
  - The review request is attempted at most once per PR, and only when the PR is `OPEN` with no revision errors (about lines 584–603).
  - `remoteGatesClear` and `humanApproval` (lines 561–578) and the output object (lines 650–716, including the `readiness.merge` field) match the `ci-evidence.md` table. That includes `prBaseRefSha`, `completeness.pipelineRunComment`, and the `complete` compatibility alias.
  - `humanApproval` is `not_required` only when the rules explicitly require zero approvals and have no code-owner, last-push, or named-reviewer requirement.
- [x] **Workflow, ruleset, and branch-protection evidence fails closed.** In lines 240–425:
  - REST failures and empty responses become `collectionErrors`.
  - A workflow run's PR association is checked against the base-repository URL and head SHA.
  - The fork fallback needs a lookup that includes closed PRs (`state=all`) and finds only this PR on that branch. Ambiguous associations block.
  - Unknown rule types and enabled classic settings that the helper does not evaluate are listed in `unassessedGates`. The `restrictions` object, which has no `enabled` field, is flagged; settings with `enabled:false` are not.
  - Required-check names are compared case-sensitively.
- [x] **The collector verifies CI provenance and test results.** In `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:
  - Windows batch cleanup (lines 39–71) takes at most 5 + 5 + 5 seconds. It keeps the process ID in the error and chains the original exception.
  - Requests use a fixed endpoint pattern and refuse redirects, and the token stays in memory. The file viewer masked the header, so I used grep to confirm line 153 contains `******
  - Pagination rejects duplicate IDs.
  - The build check covers the ordered merge parents, definition `17563`, repository, `sourceBranch`, `sourceVersion`, and `reason` (lines 206–258).
  - Timeline parsing keeps the highest attempt, includes Phase/Stage records that have no child jobs, and applies issues from parent records. Malformed fields raise errors (lines 261–445).
  - Selected test runs must be from the current attempt and in the `Completed` state. Required job/suite and job/test publications are enforced, and `--allow-skipped` cannot exempt an `--expected-test`.
  - `main` enforces the 10-minute Azure deadline and re-reads the build to set `collectionStable`.
- [x] **Watcher timeline mode matches the documentation.** In `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - Build URLs must use the trusted Azure host and project, and build IDs must fit int32 (lines 47–75).
  - The merge is verified once, and each `agent_loss` event is printed once per job and attempt.
  - The watcher waits while the Azure build is unfinished. `azure_completed` and `attention_required` exit with code 4, and `replaced` and `superseded` are reported. A timeout result still includes the canonical build link (lines 98–271).
  - Kickoff timestamps are truncated, never rounded up.
  - `--base-sha` and `--merge-sha` are accepted only with `--azure-timeline`.
  - `gh` is resolved through `command_json`, which calls `trusted_executable`.
- [x] **The trusted-guidance verifier does what `trusted-guidance.md` claims.** In `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - Tool lookup (lines 40–68) uses only absolute PATH entries and skips entries at or under the current directory. It expands PATHEXT on Windows and rejects tools whose resolved target is under the current directory, while still invoking them by their launcher name.
  - Git runs with `GIT_*` variables removed, `--no-replace-objects`, and `core.fsmonitor=false` (lines 71–91).
  - `verify` (lines 94–160):
    - checks the worktree root and HEAD;
    - accepts only regular-file modes;
    - requires every file in `REQUIRED` and an index that matches the tree;
    - runs `ls-files --others` without `--exclude-standard`, so ignored files are caught;
    - rejects paths that go through symlinks;
    - tolerates CRLF line endings only for files without NUL bytes.
- [x] **All new links resolve.** I opened the target heading for every new fragment link:
  - `ci-evidence.md`: `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
  - `readiness-gates.md#user-value-and-scope`
  - `loop-control.md`: `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
  - `ci-triage.md`: `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `SKILL.md#validation-only-handoff`
  - `ci-delegation.md#evidence-required-before-triggering`

  Every documented flag matches the scripts' argument and parameter definitions.
- [x] **The pipeline works with these helpers without being edited.**
  - `pipeline.yaml` lists jobs at the top level (line 111), so timeline job identifiers take the form `<phase>.<job>`. That matches the collector's `phaseName.jobName` key and the stated `UnitTests.image` example.
  - The CIHelpers job (lines 112–126) installs only `pytest` and `pyyaml`, and the four helper test modules import only the standard library.
  - None of the 17 changed paths is a pipeline or workflow file.
- [x] **The tests check the behavior they claim to check.**
  - The readiness harness runs the real script with a fake `gh` and checks for unexpected trigger or review-request side effects in every case (`tools/ci/tests/test_get_pr_readiness.py`, lines 21–150 and 253–300).
  - When `TF_BUILD` is set, a missing `pwsh` fails the suite instead of skipping it (lines 249–251).
  - The two latest edits each assert the specific blocking error:
    - review requests on CLOSED and MERGED PRs (lines 613–637);
    - InProgress and Aborted test runs (`test_collect_ci_evidence.py`, lines 394–408).
  - The watcher tests patch the watcher module's own copies of its imports (lines 143, 668, 702 and 761).
  - The trusted-guidance tests exercise replace refs, the fsmonitor override, the documented `-I` and `-E -s -B` flags (read from the docs), ignored files, and symlinked launchers.
- [x] **Considered and not reported, because each case fails closed or is documented behavior:**
  - An invalid cached token is discarded along with its client after the error.
  - `isinstance(count, int)` also accepts `bool`.
  - A transient query error ends the watcher with a documented nonzero result.
  - A file committed with CRLF but checked out with LF would make the verifier reject the snapshot, never accept it.
  - Non-PR workflow runs on the same head SHA are included, which can only add blockers.
- [ ] **Not checked: CIHelpers runtime on hosted agents against the job's 15-minute `timeoutInMinutes`.** This needs a current-candidate Azure run. The supplied local Linux baseline for the full directory is 486.94 seconds; confirm the hosted runtime in the final CI-qualified evidence.
- [ ] **Not checked: test execution.** I had read-only tools and ran no tests. The pass and skip counts come from the supplied validation evidence.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5088 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 56,
  "completedPriorPasses": 55,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver verification and pass disposition

Rounds 1-4 returned clean. Round 5 identified one missing negative regression
for the documented skip-exemption boundary; round 6 was not started. Extending
the existing test invalidates this pass. No production behavior changed.

All five invocations observed `claude-opus-5.5`; their recorded input hashes
match the artifact headers. Every private input contains the complete
5,915-line source patch. Recorded user messages retain every source line
except two automatically masked authorization headers, including the last
hunk. Tool searches stayed within source scopes or named root files and did
not access prior/sibling feedback. Original responses were retained and
checked for publication safety.

Reviewed source manifest:
[pass 56](pr-validation-skills-attempt-56-manifest.json).
Base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`;
frozen index: `20fce201338bae32e73b00449f364605e5b3e919`.
Source diff SHA-256:
`3f6371855e1cd7c8c7f9df6e803fd1bf0569cec4cda411b69e81a67f3e8ff50c`.
Manifest SHA-256:
`a1a5edad8d830eb67b3367d3f497061ab4d6cca00a6657d0684ec57503444fe6`.

Publication, authorized current-head Azure validation and the final
CI-qualified review remain outstanding.
