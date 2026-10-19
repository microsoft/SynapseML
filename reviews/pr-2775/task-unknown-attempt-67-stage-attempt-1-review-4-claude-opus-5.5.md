---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 67
stage_attempt: 1
round: 4
timestamp: "2026-10-19T11:51:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6d30cac9f4b00235850eee265ad1dd64633e5f443dec1921d964064ec6f4bb33"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-67-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `monitor()`):
  - The deadline comes from kickoff plus the timeout and is clamped at zero.
  - `wait_for_check_update` skips the wait when `deadline - now <= 600`, so a confirmation poll always starts strictly before the deadline.
  - A non-completed Azure status resets `waited_for_github`, but never resets the deadline.
  - `timeout_result["azure"]` is assigned only after the post-collection deadline check.
  - The replaced, missing and stale cases use `max(matching)` against `build_id` correctly.
  - The exit-code map covers every outcome.
  - `parse_kickoff` truncates fractions to six digits and never rounds them up.
- [x] **Watcher tests traced by hand against the production code** (`tools/ci/tests/test_watch_azure_pipeline.py`). For all 18 combinations in `test_timeline_completion_allows_one_check_update_before_the_deadline` (start, retry, latency), the expected poll count, collect count, sleep list and final clock match. These tests trace the same way:
  - `test_agent_loss_surfaces_before_job_or_build_is_terminal` (final clock 2400, events [1, 2])
  - `test_timeline_results_wait_for_retry_and_distinguish_agreement`
  - `test_successful_azure_collection_cannot_overrun_the_original_deadline`
  - `test_merge_verification_uses_pinned_revisions_once` (timeouts clipped to 10 seconds, deadline 7200)
- [x] **Collector** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - `command_json` catches `UnicodeError` before `ValueError` and does not swallow `EvidenceError`.
  - In `run_windows_batch`, the fallback error is rebound inside the outer `except`, so the exception chain is preserved, and cleanup is bounded at 15 seconds.
  - `AzureClient.list` advances `$skip` by `len(values)` and stops on a short page, including when the last full page has exactly 100 rows. Repeated IDs are rejected.
  - `verify_merge` checks the ordered base/head parents.
  - `summarize_timeline` keeps the latest attempt per (type, identifier), gates leaf containers, and merges issue counts with `max`.
  - In `collect_tests`, the current and known attempt sets, the required publication and test-publication sets, and the rule that an allowed skip cannot exempt an expected test are all correct.
  - `main` blocks success with its `collectionStable` check.
- [x] **Authorization header**: the viewed source shows `f"******"`. Separate greps found one match each for `{self.token}` and `Bearer`, and none for a literal `******`. This is display redaction, not a broken header.
- [x] **Verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - Field parsing for `ls-tree -rz` and `ls-files -sz` matches Git's output formats.
  - Nonexistent optional pathspecs are silently empty, not errors.
  - The `REQUIRED` root paths exist in the checkout: `CODEOWNERS`, `pipeline.yaml` and `templates/` (11 files).
  - `--others` without excludes also catches ignored files.
  - The symlink-ancestry check and CRLF tolerance are correct.
  - The documented commands (`-I -B` and `-E -s -B`), plus `sys.dont_write_bytecode` for direct runs, prevent `__pycache__` from dirtying verified paths.
- [x] **Readiness script** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, lines 1–770):
  - `Get-RestEvidence` parses each page separately with `ConvertFrom-Json`, which enumerates arrays.
  - In `$exactRequiredChecks`, `+` binds before `|`.
  - `-cnotin`/`-cin` are case-sensitive, and prefix matches are appended so they must also succeed.
  - `$unprovenRequiredChecks` requires `SUCCESS` from either a CheckRun or a StatusContext.
  - Workflows are scoped to this PR before being grouped by `workflow_id,event`.
  - In `$humanApproval`, Int64 counts and null or `""` `reviewDecision` values are handled.
  - A null `$compare` makes `behind_by -eq 0` false.
  - The `-RunPipeline` gate order is: complete evidence, then open PR, then absent exact aggregate.
  - Receipt parsing uses `catch [ArgumentException]`.
  - The `-WaitForReview` deadline loop is correct.
- [x] **Readiness test harness** (`tools/ci/tests/test_get_pr_readiness.py`): the `gh` stub enforces `--paginate --jq tojson` with one JSON page per line, immutable compare SHAs and `state=all`. It also checks trigger and review-request side effects. The `TF_BUILD` skip/fail behavior matches the claim in `ci-triage.md`.
- [x] **Cross-file assumptions**:
  - `pipeline.yaml` uses top-level `jobs:`. The `stages:` at line 918 is a matrix leg, so `phaseName.jobName` matches timeline Job identifiers.
  - The CIHelpers job runs `tools/ci/tests/` on hosted `ubuntu-22.04`, which provides `pwsh` and Python 3.10.
- [x] **Docs**: every changed relative anchor resolves:
  - `#comment-trigger-gate`, `#waiting-for-azure-pipelines`, `#azure-merge-and-named-tests`, `#validation-only-handoff`
  - `#user-value-and-scope`, `#evidence-required-before-triggering`, `#state-and-cost-discipline`, `#targeted-infrastructure-retry`
  - `#github-snapshot`, `#artifact-retention`, `#evidence-invalidation`

  The behavioral claims in `ci-triage.md` and `ci-evidence.md` match the code. These include:
  - one confirmation poll, with exit 124 retaining the last Azure evidence;
  - the ten-minute deadline and 15-second cleanup bound;
  - required skipped/neutral checks staying unproven;
  - zero-approval rules.
- [x] **Considered and rejected as non-defects**:
  - `STARTUP_FAILURE` is absent from `failedChecks`. Actions startup failures are caught by `failedWorkflows`, and third-party apps cannot set that conclusion.
  - An unevaluated ruleset `creation` rule blocks readiness. This is documented conservative behavior: it can block falsely but never passes falsely.
  - A hypothetical null `errorCount` would fail closed, and Azure DevOps omits null fields anyway.
- [ ] **No tests or commands executed**: this was a read-only review. The validation results in the prompt were not re-run.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5511 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 67,
  "completedPriorPasses": 66,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}

## Driver completion evidence

The finalized runner receipt reports exit 0 and CLEAN; runtime events confirm
`claude-opus-5.5`. Input, response and decision hashes match the receipt.
The 41 read-only tool calls stayed within named source files and source subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
privately with SHA-256 `de994c420632ba248e378a4cc8dc490f7d9f72dafe2b9ccc91b900fb6546d3ae`.
This is one lens of the six-clean pre-commit pass, not current-head CI clearance.
