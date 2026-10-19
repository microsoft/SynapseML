---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 60
stage_attempt: 1
round: 5
timestamp: "2026-10-19T05:54:02Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "582c6f8b1484d27d367e0897b5eedb4e6a694fe943073ff675d95ce5897f258f"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-60-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (lines 98–259) and matched each timeline-mode branch to its tests in `tools/ci/tests/test_watch_azure_pipeline.py`. These tests assert the following:
  - the agreement/disagreement matrix (749–800)
  - agent-loss deduplication (622–661)
  - the absent-aggregate case (663–692)
  - an `EvidenceError` at the deadline (573–620)
  - the query-time deadline (231–243)
- [x] Read `AzureClient.remaining_timeout()` in `collect_ci_evidence.py` (lines 119–127). It checks the deadline only before each request. So the watcher's check after a successful collection (lines 152–153) is the only guard on that path, not a duplicate.
- [x] Read `Get-PrReadiness.ps1` lines 376–431. Searched `tools/ci/tests/test_get_pr_readiness.py` for `workflow_run(` overrides, `head_sha`, `workflow_id`, `sameHeadPulls` and `collectionErrors`. Also read its fixtures (187–199, 223–233) and tests (1064–1268). No fixture reaches the guards at lines 383–387 or 396–401.
- [x] Confirmed the collector's stability and validation gates are asserted (`test_collect_ci_evidence.py` 1205–1207 and 1246–1247). Its public `main()` provenance tests run the real `verify_merge` with mocked transport.
- [x] Confirmed the `pipeline.yaml` CIHelpers job (lines 111–126) runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04. The readiness tests skip only when neither `pwsh` nor `TF_BUILD` is present, so they cannot skip silently in Azure.
- [x] Checked the documented behavior these paths must meet: `synapseml-pr-loop/SKILL.md` lines 237–239 and `references/ci-triage.md` lines 45–58 and 84–88.
- [x] Independence: I did not open `reviews/`, other reviewers' artifacts or session evidence. Searches were limited to `.github/skills/` and `tools/ci/`.
- [ ] Test execution does not apply: this is a read-only review. I did not reproduce the supplied pass counts or guard-removal experiments, so they remain unverified claims.

## Issues

### Issue 1: Readiness identity guards for workflow runs and fork head lookups have no regression test
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`; `tools/ci/tests/test_get_pr_readiness.py`
- **Line(s)**:
  - Script: 383–387 and 396–401 (code that depends on them: 404–409, 411–412, 428–431)
  - Tests: 187–199, 223–233, 1111–1123, 1220–1268
- **Description**: Both new guards record a collection error, which sets `evidenceComplete` to false. No test reaches either one.
  - **Workflow-identity guard**: Every `workflow_run()` override (lines 393, 476, 1079, 1097, 1118, 1134, 1173, 1253) keeps `id`, `workflow_id`, `event` and `head_sha=HEAD`. The `[{}]` case at line 1112 malforms `pull_requests`, not the run's identity.
  - **Head-lookup guard**: Every `sameHeadPulls` row copies the complete fixture row. The `headLookupError` case fails the `gh` call before any rows are validated.
  - Deleting either guard leaves every test passing.
- **Risk**: Without these guards, inconsistent GitHub data produces `evidenceComplete: true`, the false-green result this PR is meant to prevent.
  - **Workflow-identity guard removed**:
    - Runs without `workflow_id` fall into one `Group-Object workflow_id,event` group. A newer successful run of one workflow can then hide an older failed or approval-required run of another.
    - A row with the wrong `head_sha` is dropped silently at line 412, while the pagination count still matches.
  - **Head-lookup guard removed**: An incomplete row for another PR on the same fork branch is filtered out of `$headPulls`. `uniqueForkBranch` can then become true, and runs with no PR association are credited to this PR.
- **Suggested Fix**: Add subtests that assert `evidenceComplete` is false and the specific `collectionErrors` message appears. With `-RunPipeline`, also assert that it is blocked and there is no trigger side effect. Cases:
  1. A run with `head_sha="c"*40`.
  2. Two runs without `workflow_id`: id 41 failed and id 42 succeeded.
  3. In the shared-fork test, a second `sameHeadPulls` row without `base.repo.full_name`.

### Issue 2: Deadline check after a successful Azure collection is untested
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`; `tools/ci/tests/test_watch_azure_pipeline.py`
- **Line(s)**: Watcher 147–154; tests 231–243, 573–620, 694–713
- **Description**: `AzureClient` checks the deadline only before each request. A final response that finishes at the deadline therefore returns normally, and only lines 152–153 stop a verdict after the deadline.
  - Existing tests cover the query-path equivalent (`test_query_time_counts_toward_deadline`) and the `EvidenceError` branch (the `elapsed=7200` subtest).
  - No `collect_build` mock advances the clock and then returns successfully.
  - `test_same_build_retry_keeps_original_deadline_and_successful_jobs` times out at the check at the top of the loop, after a 200-second sleep (`collect.call_count == 1`). It never reaches lines 152–153.
  - Removing those lines leaves the suite passing.
- **Risk**: A regression could let a slow final Azure read return `success`, `failed`, `attention_required` or `azure_completed` after the deadline set from kickoff. That verdict would rest on a GitHub snapshot taken before collection. This breaks the documented promise "without extending the original deadline" (SKILL.md 239; ci-triage.md 88).
- **Suggested Fix**: Add a subtest with these inputs:
  - `query_pr` returns `snapshot("COMPLETED", "SUCCESS")`.
  - The patched `collect_build` sets `self.now = 7200` and returns a completed, succeeded payload.

  Assert exit code 124 (outcome `timeout`), no sleep, and a single `query_pr` call. Also assert whether `azure` is retained. Currently the snapshot just collected is dropped, because line 154 runs after the check.

### Issue 3: The "GitHub check still pending" output of `azure_completed` is never asserted
- **Severity**: Low
- **File**: `tools/ci/tests/test_watch_azure_pipeline.py`
- **Line(s)**: Test 622–661 (assertion at 652); watcher 232–239; compare test 663–692 (assertion at 692)
- **Description**: `test_agent_loss_surfaces_before_job_or_build_is_terminal` is the only test that reaches the pending branch. In it, the GitHub check is `IN_PROGRESS` and the final Azure result is `completed`/`failed`.
  - The test asserts only `outcome == "azure_completed"`.
  - It does not check `githubCheck: "pending"` or the retained `azure.result`.
  - Only the `missing_or_stale` label is asserted (line 692).
  - ci-triage.md (lines 55–57) says `azure_completed` covers both a missing and a lagging GitHub aggregate. `githubCheck` is the only field that tells them apart.
- **Risk**: A regression that mislabels or drops `githubCheck`, or drops `azure`, in this branch would pass the suite. Triage could then confuse a lagging aggregate with a missing trigger or check registration.
- **Suggested Fix**: In that test, add `assertEqual("pending", result["githubCheck"])` and `assertEqual("failed", result["azure"]["result"])`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5208 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 60,
  "completedPriorPasses": 59,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver Dispositions

All three low-severity coverage findings are resolved. The original feedback
above is retained unchanged.

1. The readiness suite now exercises a wrong workflow head, missing workflow
   IDs that could collapse distinct runs, and an incomplete same-fork PR identity.
   Each case asserts incomplete evidence, the specific collection error, and
   refusal to trigger CI without any comment side effect.
2. The watcher suite checks a successful Azure collection just before, exactly
   at, and after the original deadline. It asserts CLI success versus timeout,
   one collection/query, no sleep, and exclusion of the late Azure snapshot.
3. The existing agent-loss test now checks the pending GitHub aggregate label
   and the retained failed Azure result.

Only two existing test files changed; there are no production or documentation
changes and no new source files.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed on Windows Python 3.14.6 / PowerShell 7 (83 tests, 215 subtests, no skips,
154.64 s) and Linux Python 3.10.20 / PowerShell 7.6.7 (83 tests, 215 subtests,
no skips, 215.92 s).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py`
passed.

Isolated guard-removal checks produced two workflow-identity failures, one
fork-identity failure, two post-collection deadline failures, one pending-label
failure, and one missing-Azure assertion failure. There were no test errors or
skips, and no production files were edited.

This pass stopped after lens 5; the remaining five lenses did not run. The
test changes invalidate attempt 60 as a qualifying pass. Its original source
diff was `066854c1e077fa829dc49defdee49784bc6ddca8a5c9e1e425c594694a5394ad`
against base `d34f762a785ee8a7c7c624806f8c112256168833` and local predecessor
`38685fad3d2e3ec93e6757adf36aa270cc3a24d1`.
Transport audit confirmed the requested model, exact embedded diff, independent
read-only source access, and publication-safe output.
