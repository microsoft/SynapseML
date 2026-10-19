---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 1
timestamp: "2026-10-17T13:25:55Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f7fb9bb9cfc6cb1ba44e79072e43afc6978087215d766dbe8556c365c0a7d532"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-21-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I checked the supplied merge-base-to-index diff against the on-disk files `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `Get-PrReadiness.ps1`, `verify_trusted_guidance.py` and `watch_azure_pipeline.py`. The on-disk content matches the diff.
- [x] `collect_ci_evidence.py`:
  - pagination and repeated-ID detection (137–162);
  - ordered merge parents (165–177);
  - build provenance (180–207);
  - newest attempt per timeline identity, ancestor and cycle handling, issue counts (210–372);
  - job gates (375–420);
  - current-attempt test selection and publication checks (432–617);
  - stability recheck, `mergeReady=false` and the 600 s deadline (665–698).
- [x] `Get-PrReadiness.ps1`:
  - `-RunPipeline` requires one PR and both expected SHAs, and rejects `-WaitForReview` (99–105);
  - pagination and empty-body handling, plus verified run associations by base URL and head SHA (237–272);
  - rulesets, classic protection, workflow scoping, single-branch fallback, and the case-sensitive aggregate and rule checks (309–455);
  - revision recheck, readiness states, and request and trigger guards (504–592);
  - output and the wait loop (594–707).
- [x] `verify_trusted_guidance.py`:
  - `GIT_*` environment variables are stripped, with `--no-replace-objects` and `core.fsmonitor=false`;
  - tree entries must be regular blobs, and the index must equal the tree at stage 0;
  - untracked and ignored files are listed because `ls-files --others` runs without `--exclude-standard`;
  - symlinks are rejected, and only CRLF differences are tolerated, in non-binary files only;
  - `trusted_executable` skips relative and cwd-descendant `PATH` entries.
  - A glob found only the root `AGENTS.md`. There is no `.github/instructions`, `.github/agents`, `.github/prompts` or `CLAUDE.md`.
- [x] `watch_azure_pipeline.py`:
  - each poll keeps the original deadline, and the merge is verified once (97–256);
  - agent-loss events are emitted once per (job, attempt);
  - a GitHub/Azure disagreement returns `attention_required`;
  - the exit-code map (346–360) matches `references/ci-triage.md` lines 34–55.
- [x] Policy docs (`references/ci-evidence.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `ci-triage.md`):
  - every SKILL.md cross-reference resolves;
  - `review-gauntlet.md` keeps the removed SKILL.md rules: pre-commit and final passes, six themes, frozen patch, collision and independence rules, budgets.
- [x] `pipeline.yaml`:
  - there are no stages, so timeline identifiers take the form `Phase.Job`, matching the collector's `job_key`;
  - CIHelpers runs `tools/ci/tests` (lines 111–126);
  - UnitTests publishes with `failTaskOnFailedTests: true` (980–984).
- [x] `tools/ci/tests/test_collect_ci_evidence.py`: the fixtures at 60–110 always set the run state to `Completed`. The scope test at 225–248 covers only a `NotExecuted` sibling result.
- [ ] Not done: I ran no tests and reproduced no live GitHub CLI or Azure evidence. This is a read-only review, so the caller's validation counts are unverified.

## Issues

### Issue 1: Finished runs that contain failures are reported as "incomplete or empty", which breaks the named-test scope
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 503–507 and 537–541. Related: `tools/ci/tests/test_collect_ci_evidence.py` 73–85 and 225–248; `.github/skills/synapseml-pr-loop/references/ci-evidence.md` 101–102, 126–130 and 135–136.
- **Description**: `collect_tests` adds "Run N is incomplete or empty" whenever `run.state != "Completed"`. That error blocks the test gate for any current run that contains a named `--expected-test`.
  - Azure DevOps `TestRunState` also has a terminal `NeedsInvestigation` value. Azure appears to use it for finished runs with unanalyzed failed results (shown as "Needs investigation" under Runs). I have not confirmed this against a live build.
  - If that holds, one unrelated failed test in the same run name makes a passing `--expected-test` return `tests.passed=false`, with a misleading completeness error.
  - That contradicts `ci-evidence.md`, which says "`--expected-test` checks only the named test" and that unrelated tests sharing a run name do not become required unless that run name is an explicit expected suite.
  - The test fixtures always use `Completed`, even for runs containing `Failed` results, so the suite cannot catch this.
- **Risk**: The collector fails closed. `validationPassed` is already false because the failing job fails. The problem is that the main triage case breaks: proving a named regression passed while an unrelated or baseline test in the same suite failed. The error points to publication or infrastructure instead of a test failure, which can lead to wrong triage or a needless rerun.
- **Suggested Fix**:
  1. Confirm the state value read-only on a completed SynapseML build that has a failed result.
  2. Treat `Completed` and `NeedsInvestigation` as finished publication states.
  3. Keep `NotStarted`, `InProgress`, `Waiting`, `Aborted`, and unspecified or missing states incomplete.
  4. Leave pass/fail to the existing per-result checks, and report the raw run state separately.
  5. Add a regression: a `NeedsInvestigation` run holding a passing named test and an unrelated `Failed` result should pass with `--expected-test` alone and fail with `--expected-suite`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected as an unconfirmed extension to legacy-state support, not a demonstrated defect in current automated CI evidence.
- **What changed**: No source or test changes. The completed-publication gate remains fail-closed.
- **Why**: Microsoft's [TestRunState enum](https://learn.microsoft.com/en-us/javascript/api/azure-devops-extension-api/testrunstate) defines `Completed` as all tests completed or skipped and explicitly marks `NeedsInvestigation` as a legacy state that should no longer be used. The [REST list example](https://learn.microsoft.com/en-us/rest/api/azure/devops/test/runs/list?view=azure-devops-rest-7.1) does show that legacy state on a 2014 manual run; it does not establish that current SynapseML automated publications use it or that accepting it would preserve this collector's job-attempt completeness contract.
- **How verified**: Read-only inspection of completed failed builds 241254457, 240986433, 240792736, 240793249, and 240792467 returned 383, 381, 363, 372, and 383 runs, respectively, all `Completed`. This sample did not contain an individual failed test result; it is not claimed as proof of failed-test state semantics. A separate read-only exercise of the existing collector with a `Completed` run containing a passing named test and a failed sibling produced `passed=true` for named scope and `passed=false` for suite scope, retaining both outcomes. Thus the supported-state named-test behavior works without a source change. Legacy-state runs remain unproven, with their raw state already reported.

The original feedback above is unchanged. Original artifact SHA-256:
`089a1a7600a8d34448bbf7d034343c5ddc951e9c9d040fe4713994cdb7e5f67f`.
Reviewed diff SHA-256:
`81f6493fcd7d9125360f56c999ce2db6708ecbf4853ba180c2211d2c626a6767`.
Target/merge base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
The source manifest is unchanged. The runner's original issues-found outcome
is retained, not relabeled as a clean six-round pass.
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
  "changeSize": "17 source, test, and documentation paths; 4022 added and 146 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 21,
  "completedPriorPasses": 20,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
