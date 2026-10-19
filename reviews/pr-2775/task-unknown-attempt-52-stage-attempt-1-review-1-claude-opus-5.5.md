---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 52
stage_attempt: 1
round: 1
timestamp: "2026-10-18T18:36:08Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b998793bbf3ec2e4998d392896c0acb5f234cb133a41786d91879b4ae22cefab"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-52-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (read in full)**
  - **Windows batch timeout (~39–67):** cleanup uses `taskkill /T` and is bounded at 15 s. Errors from `command_json` (~70–91) are sanitized.
  - **Azure client (~107–171):** the client only calls a fixed Azure project and an allowlist of endpoints. It refuses redirects, keeps the token in memory, and treats an empty or 404 timeline as pending only while the build is incomplete. Line 150 displays as `******`; grep for `Bearer` and `{self.token` matches it, so the asterisks are display masking.
  - **Pagination (~173–197):** uses `$skip`/`$top` and errors on repeated IDs.
  - **Merge and build checks (~198–251):** the merge must have the parents `[base, head]` in that order. The build must match definition 17563, the GitHub repository, `refs/pull/N/merge`, the merge SHA, an allowed reason, and a known status.
  - **Timeline (~254–421):** keeps the highest attempt per `(type, identifier)` and checks for parent cycles. Phase/Stage records that have child jobs are skipped. Issue counts include each job's tasks and ancestors.
  - **Tests (~478–658):**
    - The current attempt is decided against timeline Job attempts, and a run attempt newer than the timeline is an error.
    - Each job must publish expected suites and named tests in its current attempt.
    - `--allow-skipped` cannot exempt an `--expected-test`.
    - A requested run with missing job provenance blocks the gate.
  - **`main` (~704–749):** enforces a 600-second deadline and compares `status`, `result`, and `jobs` across a second read.
- [x] **Verifier: `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` (lines 1–178)**
  - **Tool lookup (~40–69):** uses only absolute PATH entries and excludes the cwd and its descendants, including when a resolved symlink target falls inside the cwd.
  - **Git calls (~72–93):** strips `GIT_*` variables and passes `--no-replace-objects` and `core.fsmonitor=false`.
  - **`verify()` (~96–156):** checks:
    - HEAD equals the expected SHA;
    - tree entries are regular blobs;
    - the `REQUIRED` set is present;
    - the index has no conflicts and equals the tree;
    - there are no untracked or ignored files;
    - no path traverses a symlink;
    - file bytes match, with CRLF tolerated only for blobs without NUL bytes.
  - **Required root inputs:** `CODEOWNERS`, `pipeline.yaml`, and `templates/` exist at the repository root, so `REQUIRED` can be satisfied.
- [x] **Readiness: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (lines 1–759)**
  - **Version and guards:** `#Requires -Version 7.2` is present, and the parameter guards (~103–111) are in place.
  - **REST evidence (~240–275):** failures become collection errors, and paged JSON parses one page per line.
  - **Fork run association (~277–296, ~387–430):** a run is used only after its PR entry is verified; otherwise the helper requires proof that one PR owns the fork branch.
  - **Rules (~323–369):** collects effective rules and classic protection, flags rule types it cannot assess, and derives approval rules.
  - **Checks (~359, ~458–475):** the exact `microsoft.SynapseML` aggregate is matched case-sensitively, and skipped or neutral required checks count as unproven.
  - **Recheck (~519–556):** re-reads the PR and target and compares them with the expected SHAs.
  - **Separate states (~555–578, ~684–697):** evidence, remote engineering, human approval, and merge are reported separately; `merge` accounts for draft, OPEN state, and `mergeStateStatus`.
  - **Review request (~580–608):** attempted at most once per PR per invocation.
  - **`-RunPipeline` order (~610–648):** complete evidence, then an OPEN PR, then the exact aggregate absent from the rollup. A receipt-parsing failure does not turn a successful POST into a failed request.
- [x] **Watcher: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`**
  - **URL parsing (~47–76):** accepts only trusted hosts, projects, and paths, and build IDs within int32.
  - **Timeline mode (~135–175):** verifies the merge once and emits `agent_loss` once per job and attempt.
  - **Outcomes (~176–257):** it keeps waiting while Azure is incomplete. It reports `azure_completed` when the GitHub aggregate is missing or lagging, and `attention_required` when GitHub and Azure disagree; both exit with code 4.
  - **Arguments (~259–318):** the deadline is bounded by the kickoff time, and `--base-sha`/`--merge-sha` are required with `--azure-timeline` and rejected without it.
- [x] **CI job and test harness**
  - `pipeline.yaml` lines 112–126: the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, which supports the `ci-triage.md` claim that the four suites run there.
  - `pipeline.yaml` has no `stages:` block, so the collector's `phaseName.jobName` key is consistent with timeline Job identifiers.
  - `tools/ci/tests/test_get_pr_readiness.py` lines 249–251 run under `TF_BUILD` even without `pwsh`, and fail instead of skipping, as documented.
  - `tools/ci/tests/test_trusted_guidance.py` uses isolated temporary repositories.
- [x] **Links:** every intra-skill anchor in the changed docs resolves to an existing heading. Examples:
  - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot`, and `#azure-merge-and-named-tests`
  - `readiness-gates.md#user-value-and-scope`
  - `loop-control.md#artifact-retention` and `#evidence-invalidation`
  - `ci-triage.md#waiting-for-azure-pipelines`
  - `ci-delegation.md#evidence-required-before-triggering`
  - `SKILL.md#validation-only-handoff`
- [x] **Doc consistency**
  - **Comment-trigger gate:** routed consistently through PR-loop `SKILL.md` steps 7 and 9, `readiness-gates.md`, `ci-triage.md`, and `ci-delegation.md` step 3.
  - **Other gates:** the secret-bearing rule (pinned route or block) and the separate protected-workflow approval gate are preserved.
  - **Gauntlet:** `review-gauntlet.md` keeps the relocated stage-8 contract.
  - **No-bloat gate:** stated in `readiness-gates.md` and linked from SKILL.md step 4.
  - **Scope:** the diff does not change product APIs, dependency pins, release tooling, or pipeline/workflow definitions.
- [x] **Considered and rejected as non-defects**
  - A null `warningCount`/`errorCount` raises an error. This is conservative, and Azure sends integers.
  - Ancestor issues propagate to every descendant job. This is documented and does not change `jobGatesPassed`.
  - Reading classic protection requires admin access, which blocks the trigger for non-admins. This gate is intentionally retained.
  - The comment-trigger race is documented.
  - `failedChecks` omits `STARTUP_FAILURE`. This is pre-existing, `failedWorkflows` covers `startup_failure`, and an affected required check is still unproven.
  - `$skip`/`$top` are percent-encoded by `urlencode`, which servers decode normally.
  - Residual risk only: the `CIHelpers` job has a 15-minute timeout and the supplied local Linux run took about 8 minutes. Hosted-agent timing is unverified, and the pipeline is unchanged by this PR.
- [ ] Not applicable: I executed no tests or CI because this review is read-only. The supplied pytest, Black, and PowerShell results were not re-run, and no Azure build exists for this candidate.
- [x] **Independence:** I did not open `reviews/`, checkpoints, prompts, or sibling artifacts. Searches were limited to `.github/skills/` and `tools/ci/`, and root files were opened only by exact name (`pipeline.yaml`, `CODEOWNERS`).

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5016 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 52,
  "completedPriorPasses": 51,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Historical clean round, superseded by the documentation correction from round 2.
The original review is preserved above. The executed reviewer reported
`claude-opus-5.5`; its input contained the full frozen source diff, and its
tool calls did not read sibling feedback. The reviewed predecessor was
`a96e0370dab63a22a73f56c24ec7441cd36d3622`, with base
`0543bf7e5266c587207ae8c28c3b696deeed3ed1` and source diff SHA-256
`3c4121d5e82a20a6a9cdb26daed116fdd17511da78f94b0c1d4966106a4fd951`.
The matching source manifest is `pr-validation-skills-attempt-52-manifest.json`.
This round is not current-patch or Azure CI evidence.
