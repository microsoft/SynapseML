---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 39
stage_attempt: 1
round: 2
timestamp: "2026-10-18T02:33:37Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a695c59a23b9f899197554ea65a32be2efa33eb757227989acd4574b327b0b29"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-39-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Changed helpers read in full.** I read `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 1-724), `watch_azure_pipeline.py` (1-375), `verify_trusted_guidance.py`, and `Get-PrReadiness.ps1` (1-745, read in ranges).
  - Imports run one way only: the watcher imports the collector, and the collector imports the verifier.
  - The watcher imports `REPOSITORY`, `AZURE_PROJECT_ID`, `MAX_BUILD_ID`, `AzureClient`, `collect_build` and `verify_merge` instead of redefining them.
  - No extra shared-utility module was added.
- [x] **Follows AGENTS.md conventions.**
  - The `CIHelpers` job in `pipeline.yaml` (lines 112-126) is unchanged and already runs `python3 -m pytest tools/ci/tests/ -q`.
  - Nothing under `.github/workflows/` references these helpers.
  - The Black pin `22.3.0` is in `environment.yml` (lines 57-58) and matches the supplied Black commands.
  - New Python files carry the repository copyright header.
  - New tests load scripts the same way as the existing `tools/ci/tests/` tests (`importlib.util.spec_from_file_location`).
  - `AGENTS.md` and `CONTRIBUTING.md` are untouched.
- [x] **Documentation structure holds together.**
  - Step 8 of `.github/skills/synapseml-pr-loop/SKILL.md` (lines 261-264) now points to `references/review-gauntlet.md`.
  - A grep of `.github/skills` finds no leftover `REVIEW-PROMPTS` or manual prompt-assembly guidance.
  - The "unsafe-draft procedure" sentence moved over word for word and matches `references/loop-control.md` lines 101-106.
  - Every new anchor link resolves: `ci-evidence.md#github-snapshot`, `#azure-merge-and-named-tests`, `ci-triage.md#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`, `loop-control.md#checkpoint-before-yielding`, `#evidence-invalidation`, `ci-delegation.md#evidence-required-before-triggering`, and `SKILL.md#validation-only-handoff`.
- [x] **The readiness output matches its documentation.**
  - In `Get-PrReadiness.ps1` (lines 666-695), the `readiness` object and `completeness.complete = $remoteGatesClear` match the output table in `references/ci-evidence.md` (lines 72-79), `references/readiness-gates.md` (lines 123-127) and `SKILL.md` (lines 92-93).
  - The parameter checks for `-RunPipeline`, `-RequestReview` and the expected SHAs (lines 98-106) match `ci-evidence.md` and SKILL.md step 9.
- [x] **Trusted-tool rules are applied consistently.**
  - The Python helpers locate `git`, `gh` and `az` through `trusted_executable`.
  - `references/trusted-guidance.md` documents that the PowerShell helper uses normal command lookup.
  - `tools/ci/tests/test_trusted_guidance.py` checks the documented launch flags (`-I -B` for bootstrap, `-E -s -B` for helpers) against the docs.
  - The verifier's guidance paths include `CODEOWNERS`, `pipeline.yaml` and `templates/`, which all exist. `.agents/` holds only pointer READMEs, so leaving it out is fine.
- [x] **Reviewed and deliberately not raised.**
  - `collect_build(client, args)` also receives the watcher's argparse arguments, and relies on `getattr(args, "allow_skipped_job", [])` (`collect_ci_evidence.py` line 404). This is an implicit interface, but it works today and is documented: `ci-triage.md` lines 58-59 say the watcher has no skipped-job exemptions. Asking for a new abstraction here would be speculative.
  - The shared exit code 4 for `azure_completed` and `attention_required` is documented.
- [ ] **No commands or tests were run** (this is a read-only review); I relied on the supplied validation evidence. I reviewed the test files for harness structure and the specific cases cited above, not every line.

## Issues

### Issue 1: The collector's undocumented `attentionRequired` field contradicts the watcher's `attention_required` outcome
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Line(s)**: collector 439-441; watcher 155-175, 245-250, 364-365
- **Description**: The same name means two different things in the watcher's final JSON.
  - `collect_build` emits an `attentionRequired` boolean that is true when any job has timeline issues or agent loss. No reference doc mentions this field.
  - The watcher copies that object into its final JSON as `azure`.
  - The watcher's documented outcome `attention_required` (`references/ci-triage.md` lines 48-50) means something else: GitHub's conclusion disagrees with Azure's result.

  The two values are independent, so one result can contradict itself in either direction:
  - `test_stale_github_failure_does_not_stop_an_active_same_build_retry` (`tools/ci/tests/test_watch_azure_pipeline.py` lines 718-737) expects outcome `attention_required` with `azure.attentionRequired: False`.
  - A succeeded job that logged a warning produces `attentionRequired: true` and `jobGatesPassed: false` (`tools/ci/tests/test_collect_ci_evidence.py` lines 297-327). If the GitHub check is also green, the watcher returns `outcome: success` (exit 0) next to `azure.attentionRequired: true`.

  The field adds nothing over documented outputs. Issues already appear in `unresolvedJobs`, `issueCounts` and `jobGatesPassed`, and agent loss in each job's `agentLossDetected`. Its only use is the guard at watcher line 155, which is redundant: the loop inside it already filters on `agentLossDetected`.
- **Risk**: Agents read this JSON to decide what to do next. Seeing the same term with opposite values, and finding no documentation for the field, an agent may:
  - treat a passing build that only has warnings as needing attention, or
  - look for timeline issues to explain a GitHub/Azure mismatch.

  Exit codes stay correct, so the impact is misinterpretation rather than a wrong gate. The field also goes against the PR's own no-bloat rule, since it is redundant and undocumented.
- **Suggested Fix**: The smaller fix is to remove the field:
  1. Delete `attentionRequired` from `collect_build`.
  2. Have the watcher loop over `azure["jobs"]` directly and check `agentLossDetected`.
  3. Change the two collector assertions (`test_collect_ci_evidence.py` lines 324 and 599) and the watcher test data to rely on `jobGatesPassed`, `unresolvedJobs` and `issueCounts`.

  Alternatively, rename one side (for example, make the outcome `conclusion_mismatch`) and document the field.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

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

## Driver resolution - Issue 1

**Resolved.** Removed the undocumented `azure.attentionRequired` summary.
The watcher now checks each job's `agentLossDetected` directly, retaining
deduplicated early agent-loss events. The documented `attention_required`
outcome continues to mean a GitHub/Azure conclusion mismatch. Timeline
disposition remains explicit in `jobGatesPassed`, `unresolvedJobs`, and each
job's `issueCounts`; none of those gates or assertions were weakened.

Updated five watcher fixtures to omit the deleted summary and supply the
collector's `jobs` list where previously missing. All five failed before the
watcher change with `KeyError: 'attentionRequired'`. Removed two redundant
collector assertions while keeping the assertions for unresolved jobs,
issue counts, failed job gates, and diagnostic privacy.

Validation on this follow-up:

- Windows: `python -B -m pytest -q tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 171 subtests, no skips.
- Linux, from `tools/ci/tests`:
  `python3 -B -m pytest -q test_collect_ci_evidence.py test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 171 subtests, no skips.
- Linux:
  `python3 -m black --check --required-version 22.3.0 .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py`
  passed for all four changed Python files.
- `git diff --check` passed.

This source change invalidates pass 39; it does not convert its original
verdict into a clean review. Lens 1 stage-attempt 1 failed transport, its
stage-attempt 2 retry was clean, and lens 2 found this issue. Lenses 3-6 were
not started. Runtime identity and read-only source-scoped tool use were
audited for all completed invocations; no prior/sibling feedback was read.

Original artifact SHA-256 values before driver appendices:

- Lens 1, stage-attempt 1: `25c7e13b0eff009b441a032243421ab205058c90175cd7839defc8d1af8eb524`
- Lens 1, stage-attempt 2: `5ed587383636e30eca1d19957e0006a925c07e95ecb37a1b269ef21a0c7a02e5`
- Lens 2, stage-attempt 1: `9c9180617d413aefced6088be281c80c684e503ee294b80e333062ae86ecd4ea`
