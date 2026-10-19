---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 64
stage_attempt: 1
round: 5
timestamp: "2026-10-19T08:04:29Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "81e410cc92ad4cedce2653b1d44b6dbebc00aaf4950dada0b47501ce95f6f7ff"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-64-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the timeline path of `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.
  - The deadline is computed from kickoff at lines 98–102.
  - The `verify_merge` query clips the `github_json` timeout at lines 141–143.
  - Each poll constructs `AzureClient(deadline=deadline)` at line 147.
- [x] Checked `AzureClient` in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 128–142). With `deadline=None`, `remaining_timeout()` falls back to 60 s for each token, build and timeline request.
- [x] Read `tools/ci/tests/test_watch_azure_pipeline.py` and searched it for `AzureClient`, `deadline=` and `timeout=`. None of these tests observes the deadline the watcher passes to the new timeline requests:
  - Lines 144 and 614 patch the factory with `return_value=client` and never check its arguments.
  - Line 601 builds its own `AzureClient(deadline=7200)`.
  - Lines 643–691 replace `collect_build` with `collect(*_)`, which ignores its arguments.
  - Line 821 asserts `timeout=60` only at `now=0`.
- [x] Compared sibling paths that do test this guarantee:
  - Non-timeline query clip: `test_query_timeout_is_clipped_to_remaining_budget` (line 245).
  - Collector factory: `tools/ci/tests/test_collect_ci_evidence.py:1211`, `factory.assert_called_once_with(deadline=700)`.
  - Collection stability, both true and false: lines 1220 and 1261.
- [x] Confirmed the documented contract:
  - `.github/skills/synapseml-pr-loop/references/ci-triage.md:38–39` gives a 120-minute deadline from kickoff; late starts and restarts get only the remaining time.
  - `.github/skills/synapseml-pr-loop/SKILL.md:239` says timeline mode works "without extending the original deadline".
- [x] Reviewed `run_windows_batch` (`collect_ci_evidence.py` lines 39–70) against its tests (`test_collect_ci_evidence.py` lines 904–1000).
  - The real Windows tests check the sanitized timeout message and the child-tree kill by behavior.
  - The mocked cross-platform row covers only failed cleanup.
  - Not reported: real tests exist and every regression fails closed, so mocked duplicates would be redundant.
- [x] Reviewed `verify()` in `verify_trusted_guidance.py` (lines 95–157) against `tools/ci/tests/test_trusted_guidance.py`.
  - `test_missing_safety_resource_is_not_replaced_by_workspace_copy` (lines 222–227) never creates a workspace copy.
  - Not reported: a workspace-only copy fails closed through the untracked-file guard, which line 101 tests.
- [x] Reviewed the readiness test fixtures in `tools/ci/tests/test_get_pr_readiness.py`: classic-protection metadata at lines 650–658 and `required_status_checks.contexts` at line 733.
- [x] In `pipeline.yaml`, the `CIHelpers` job (lines 112–126) runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu 22.04, so CI picks up all four suites.
- [ ] I did not run any tests (this lens is read-only); the reported results are taken as stated. I also did not verify whether the 15-minute `CIHelpers` timeout leaves enough room for the 486.94 s full-suite run reported locally on Linux.

## Issues

### Issue 1: No test checks that timeline mode passes the remaining deadline to Azure and merge-verification requests
- **Severity**: Low
- **File**: tools/ci/tests/test_watch_azure_pipeline.py (code under test: .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py)
- **Line(s)**: Watcher lines 141–147. Tests at lines 113–156 (factory patch at 144), 594–641 (601, 614), 643–691 and 799–821.
- **Description**: Timeline mode adds two new network operations. Both are meant to use the remaining kickoff-based deadline:
  - the first poll's `gh api` merge check, clipped to `min(60, max(0.001, deadline - now))`;
  - a per-poll `AzureClient(deadline=deadline)` for the token, build and timeline requests.

  No test observes either value:
  - The factory mocks never check their arguments.
  - One test builds its own client, so the watcher's argument is never used.
  - The deadline-boundary tests ignore `collect_build`'s arguments.
  - The only merge-timeout assertion runs at `now=0`, where the clipped and unclipped values are the same (60).

  Two changes would pass every test on every platform: calling `AzureClient()` without the deadline, or replacing the clip with a constant `60`. The non-timeline query path and the collector factory each have this check (line 245 and `test_collect_ci_evidence.py:1211`); the new timeline path does not.
- **Risk**: The documented promise that late starts and restarts get only the remaining time, without extending the original deadline, could break unnoticed. A poll that starts just before the deadline could run about three to four minutes past it: up to 60 s each for the token, build and timeline requests, plus up to 60 s for the first merge check. The watcher still reports `timeout`, so nothing falsely passes, but the attached background job overruns its documented window.
- **Suggested Fix**: Add assertions only; no production change is needed.
  - In the timeline test at line 113, keep the factory mock and assert `factory.call_args_list == [call(deadline=7200)] * 3`.
  - In `test_merge_verification_uses_pinned_revisions_once`, add a late-start subtest: set `self.now = 7190` and have `collect_build` return the completed result.
  - In that subtest, assert `query.assert_called_once_with("git/commits/" + "c" * 40, timeout=10)`, and that the client passed to `collect_build` has `deadline == 7200`.

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
  "changeSize": "17 source, test, and documentation paths; 5367 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 64,
  "completedPriorPasses": 63,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 is resolved in the existing
`test_merge_verification_uses_pinned_revisions_once` test. It now exercises both
the original two-poll path and a late start with ten seconds remaining. It
asserts the merge query's clipped timeout and the original deadline on every
real Azure client passed to collection. No production code, test methods, or
source files were added.

`python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed with 41 tests, 84 subtests and no skips on both Windows Python 3.14.6
(2.27 seconds after formatting) and Linux Python 3.10.20 (6.38 seconds).
Black 22.3.0 and source whitespace checks passed. In-memory guard-removal
experiments, using the test module's shared globals so its transport mocks
remain effective, produced two assertion failures when the Azure deadline was
omitted and one when the merge timeout was fixed at 60 seconds. Neither
experiment produced errors or skips, and production files were not edited.

This review used source diff SHA-256
`a4c7f00875e509e1e4581f94b23b7aa17c1186e0e1711c7b77c75f86414ac465`
and manifest SHA-256
`0e7995641ffd0831f83bda27798949c325fe41bfe10867b5789801c3977100db`.
The runner's input hash and observed reviewer identity match its receipt.
Transport inspection confirmed read-only source inspection without prior or
sibling feedback. The original response above is unchanged.

The test edit invalidates attempt 64. Only lens 5 ran; lenses 1-4 and 6 were not
run. This is not a qualifying pre-commit pass or current-head CI evidence.
