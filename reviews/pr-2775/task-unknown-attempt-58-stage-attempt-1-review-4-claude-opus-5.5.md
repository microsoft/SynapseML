---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 58
stage_attempt: 1
round: 4
timestamp: "2026-10-19T04:53:34Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "812d30c904334d7a664eefebb248ca7594ddac03e393136e15795bca32a3284e"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-58-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`Get-PrReadiness.ps1` lines 331–363 and 457–475 (rulesets, classic protection and required checks).**
  - Lines 331–358: the rulesets are paginated. Classic protection is fetched only when `protection.enabled -ne $false` and `protected` is true. Properties outside the allowlist count as unevaluated unless they are null, `false` or have `enabled:false`.
  - Lines 359–363: the exact required set always includes the case-sensitive `microsoft.SynapseML` aggregate. Duplicates are removed with `Sort-Object -CaseSensitive -Unique`.
  - Lines 457–475: names matched by prefix become exact names and are re-checked with `-cnotin`. Results that are SKIPPED, NEUTRAL or absent fail both the `conclusion` and the legacy `state` SUCCESS tests, so they count as unproven.
  - No case, off-by-one or set-membership errors found.
- [x] **`Get-PrReadiness.ps1` lines 371–441 (workflow runs).**
  - The paged run list is checked against `total_count` from the first page.
  - Each run's head, `workflow_id`, `id` and `event` identity is checked.
  - The fork lookup uses `state=all`. It requires exactly one PR with matching ref, head repository, base repository, PR number and head SHA.
  - Scoping happens before `Group-Object workflow_id,event`, and the newest run is chosen by numeric `[long]id`.
  - `List[string].Add` returns void, so calls inside the `Where-Object` filter do not leak values into the pipeline.
  - The approval, failed and pending sets cover `action_required`, `waiting`, `startup_failure` and `stale`.
- [x] **`Get-PrReadiness.ps1` lines 519–578 (revision recheck and gate composition).**
  - The fresh recheck compares head OID, head ref, head repository, base ref, state and the live target tip against `$baseSha`.
  - The `-ExpectedHeadSha` and `-ExpectedBaseSha` mismatches feed `revisionErrors`.
  - `evidenceComplete` and `remoteGatesClear` combine all collected sets. A null `$compare` makes `behind_by -eq 0` false, so the gate fails closed.
  - `humanApproval` accepts approval counts as either `[int]` or `[long]`. It handles the plural classic field and the singular ruleset code-owner field.
- [x] **`Get-PrReadiness.ps1` lines 580–647 (review request and `-RunPipeline`).**
  - The review request is cached per PR and refused unless the PR is OPEN with no revision errors.
  - `-RunPipeline` throws on incomplete evidence or a PR that is not open. It posts only when the exact aggregate is `-cnotin` the rollup names.
  - Failing to parse the posted comment's receipt only warns; it cannot turn a successful POST into a failure.
  - The deliberately conservative complete-collection gate was not flagged.
- [x] **`collect_ci_evidence.py` lines 107–204 (Azure client and pagination).**
  - Pagination uses `$skip=len(values)` and `$top=100`, stops on a short page and refuses repeated IDs. There is no off-by-one.
  - The endpoint allowlist and redirect refusal are in place. Only timeline requests map a 404 or empty body to `None`.
  - Line 153 displays as `f"******"`. Grep shows the file contains `Bearer` and `self.token}` and no literal asterisks. The test assertion at line 1114 of the test file behaves the same way. This is display redaction, not a defect.
- [x] **`collect_ci_evidence.py` lines 262–478 (timeline and job gates).**
  - The newest attempt is kept per `(type, identifier)`, and parent cycles are detected.
  - Ancestor issues are aggregated using `max(listed, reported count)` for each record.
  - A Phase or Stage is emitted as a leaf only when it is not an ancestor of a Job.
  - The setup-versus-test ordering compares task lists that are already sorted.
  - A job is unresolved if it is not completed, has issues, shows agent loss, or is not succeeded. The only exception is a job skipped by an allowlisted conditional.
- [x] **`collect_ci_evidence.py` lines 490–748 (test runs, arguments and final verdict).**
  - Test runs are matched to jobs through the `phaseName.jobName` key.
  - An attempt newer than the timeline is refused, and `totalTests` must equal the listed result count.
  - A requested run without matching job-attempt provenance is an error.
  - An allowed skip applies only to identities not explicitly expected, and only to Completed `NotExecuted` or `NotApplicable` results.
  - Each job's suite and test publications are checked.
  - Arguments are validated: build ID within the int32 range, lowercased 40-hex SHAs, and nonempty `Suite::Test` names.
  - `validationPassed` requires a completed, succeeded build plus job gates, passing tests and a stable collection. `mergeReady` is always false.
- [x] **`collect_ci_evidence.py` lines 38–95 (Windows batch cleanup) against `tools/ci/tests/test_collect_ci_evidence.py` (`test_failed_batch_timeout_cleanup_reports_the_process_for_inspection`).**
  - Cleanup keeps the PID in its error and chains the forced-kill or wait error as the cause.
  - The expected kill and wait calls and the `__cause__` identity match the code paths.
  - Invalid UTF-8 output and invalid JSON are reported without exposing their contents.
- [x] **`watch_azure_pipeline.py` (`monitor`, `parse_kickoff`, `parse_args`, `main`).**
  - The deadline is anchored to kickoff and polling runs every 600 seconds.
  - Agent loss is reported once per attempt.
  - A missing aggregate yields `azure_completed` only in timeline mode, and a GitHub conclusion cannot override the Azure result.
  - The exit map is 0/1/2/3/4/124/130.
  - This matches `test_legacy_expected_status_keeps_the_kickoff_deadline` (7200 s, 12 polls), `test_agent_loss_surfaces_before_job_or_build_is_terminal` (1800 s) and `test_github_conclusion_cannot_override_active_retry_or_azure_result`.
- [x] **`verify_trusted_guidance.py`.**
  - PATH lookup excludes the checkout and honors PATHEXT.
  - `GIT_*` variables are stripped, and `--no-replace-objects` and `core.fsmonitor=false` are set.
  - NUL-delimited `ls-tree` and `ls-files -s` output is parsed correctly. Symlinks, submodules and conflict stages are rejected.
  - Untracked and ignored files are both detected.
  - CRLF tolerance applies only to text blobs.
- [x] **Documentation against code.**
  - Checked: `ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `readiness-gates.md`, `loop-control.md`, `review-gauntlet.md`, both `SKILL.md` files and `ci-delegation.md`.
  - Flags, output keys, exit semantics and interpreter flags (`-I -B`, `-E -s -B`) match the code. `tools/ci/tests/test_trusted_guidance.py` extracts the interpreter flags.
  - All cross-file anchors resolve, including `#comment-trigger-gate`, `#validation-only-handoff`, `#user-value-and-scope` and `#azure-merge-and-named-tests`.
- [x] **`pipeline.yaml` (not in the diff).** The existing CIHelpers job runs `tools/ci/tests/`, so the new suites are collected without editing the pipeline.
- [x] **Leads considered and rejected.** None is a concrete defect introduced by this diff:
  - The script help (lines 15–17) and `SKILL.md` line 174 describe coverage as the "newest automated review's commit". The code checks any submitted review on the head, as the inline comment at lines 496–503 intends. This wording sits outside the changed hunks.
  - A null `errorCount` fails closed.
  - Azure capping pages below `$top` is speculative, and it would still fail closed through `totalTests` or the missing-suite errors.
  - A change in job-list order makes `collectionStable` false, which also fails closed.
- [ ] **Commands and tests not executed.** The policy for this round allows read-only view, grep and glob only. The validation results in the prompt are the author's; I checked them only against the test bodies.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5157 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 58,
  "completedPriorPasses": 57,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
