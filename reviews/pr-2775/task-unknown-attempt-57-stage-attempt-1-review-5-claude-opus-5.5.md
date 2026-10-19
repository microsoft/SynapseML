---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 57
stage_attempt: 1
round: 5
timestamp: "2026-10-19T03:06:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bd39063252bf0c04784cc499cbd4ad142d3fed9b57d1d49de412fc5e06b1b9eb"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-57-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **CI wiring.** `pipeline.yaml:112-126` shows the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on `$(UBUNTU_VERSION)` after installing only pytest and pyyaml, so all four new suites run in CI. When `TF_BUILD` is set, `test_get_pr_readiness.py` fails instead of skipping if `pwsh` is missing (`skipUnless(shutil.which("pwsh") or TF_BUILD)`).
- [x] **Readiness gates.** I traced these regions of `Get-PrReadiness.ps1` against every `PrReadinessTests` case:
  - classic read and allowlist: 346-355;
  - failed, pending, check-name, missing and unproven checks: 442-474;
  - remote clear: 559-568;
  - guarded `-RunPipeline`, including `$azureCheck -cnotin $checkNames` (~626);
  - wait loop: 726-752.

  The harness (`test_get_pr_readiness.py:21-150`) projects only the requested `--json` fields, records every POST in side-effect files, asserts the exact POST bodies, and throws on any unexpected `gh` call.
- [x] **Collector.** I traced `verify_merge`/`verify_build` (`collect_ci_evidence.py:206-259`), `summarize_timeline`/`collect_build` (262-478), `collect_tests` (490-675) and `main` (723-756).
  - The latest skip-waiver subcases (`test_collect_ci_evidence.py:589-620`) assert both failure and the exact `Required result is not passed: ...` error.
  - The first assertion in that test is not vacuous: without `identity not in expected_tests`, no other error would remain.
  - The entry-point test uses the real `AzureClient.list` pagination with a patched `get`.
- [x] **Watcher.** I traced timeline-mode `monitor` in `watch_azure_pipeline.py` through these tests in `test_watch_azure_pipeline.py`:
  - empty queue to completion (113-151): sleeps are `[600, 600]`;
  - agent-loss deduplication (617-656): events for attempts `[1, 2]`, three 600-second sleeps;
  - absent aggregate: outcome `missing_or_stale`;
  - `attention_required` subcases;
  - truncated response, yielding an error or the original timeout.

  The legacy StatusContext aggregate is covered by three watcher tests.
- [x] **Verifier.** `verify_trusted_guidance.py` `git()` pins `--no-replace-objects` and `core.fsmonitor=false`, and strips `GIT_*`. Tests cover replace refs, untracked and ignored files, symlinks, CRLF, the fsmonitor hook, and PATH/cwd lookup.
- [x] **Documented contracts.** I read `ci-evidence.md:41-68` and `### Comment-trigger gate`. The comment routes in `synapseml-pr-loop/SKILL.md` and `ci-delegation.md` link to `ci-evidence.md#comment-trigger-gate`.
- [ ] **Test execution not performed.** This is a read-only review. The supplied Windows/Linux pass counts and guard-removal results are accepted as unverified evidence.
- [ ] **Hosted CIHelpers runtime not checkable statically.** The job has a 15-minute `timeoutInMinutes`. The only full-directory timing is a local Linux baseline of 486.94 seconds, taken before the latest edits.

## Issues

### Issue 1: Readiness tests never exercise StatusContext (`context`/`state`) rollup entries
- **Severity**: Medium
- **File**: `tools/ci/tests/test_get_pr_readiness.py` (code under test: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`)
- **Line(s)**: Tests: 153-176 (`fixture()` rollup), 542-565, 828-834. Code: 442-445, 455-457, 468-474, and the trigger condition at ~626.
- **Description**: Every readiness rollup fixture has the CheckRun shape (`name`, `status`, `conclusion`). The only `"context"` key in the suite is a ruleset parameter at line 242. The gates still branch on the StatusContext shape:
  - `failedChecks` matches `state -in ERROR, FAILURE`;
  - `$checkNames` falls back to `context`, which feeds both `missingRequiredChecks` and the `-RunPipeline` absent-aggregate test;
  - `unprovenRequiredChecks` checks `state -ne SUCCESS`.

  This shape occurs in practice. `license/cla` is a commit status. This PR's watcher also supports and tests a legacy-status `microsoft.SynapseML` aggregate (`test_legacy_status_context_is_supported`, `test_legacy_expected_status_*`).
- **Risk**: Regressions would pass every test:
  - **Fail-open.** Dropping the `state` clause from `failedChecks` lets a failed, non-required status context leave `remoteEngineeringGatesClear=true`. Pending, missing and unproven checks would not catch it.
  - **Duplicate trigger.** Dropping the `context` fallback from `$checkNames` makes `-RunPipeline` post a duplicate `/azp run` while a legacy-status aggregate is already pending. The comment-trigger gate exists to prevent exactly this: it "only requests an absent Azure aggregate".
  - **Permanently blocked.** Dropping `state -ne SUCCESS` marks a successful, rule-required `license/cla` as unproven forever.
- **Suggested Fix**: Extend existing tests instead of adding a suite.
  - In `test_skipped_or_neutral_required_checks_do_not_clear_remote_gates`, or one small parametrized case, add a rule-required `{"context": "license/cla", "state": S}`:
    - `SUCCESS` clears;
    - `PENDING` appears in `pendingChecks` and `unprovenRequiredChecks`;
    - `FAILURE` and `ERROR` appear in `failedChecks` and `unprovenRequiredChecks`.
  - Add a non-required `{"context": "other", "state": "FAILURE"}` and assert that `failedChecks == ["other"]` and the remote gates do not clear.
  - In `test_unrelated_required_check_does_not_trigger_azure_again`, add a subcase with the aggregate as `{"context": "microsoft.SynapseML", "state": "PENDING"}`, `runPipeline=True` and no `allowTrigger`. Assert `pipelineRunRequested` is false; the harness already fails on any POST.

### Issue 2: Classic-protection fixtures omit the fields the allowlist exists for, so the documented clearing contract is untested
- **Severity**: Low
- **File**: `tools/ci/tests/test_get_pr_readiness.py` (code under test: `Get-PrReadiness.ps1`; contract: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`)
- **Line(s)**: Tests: 599, 672-675, 713. Code: 349-355. Docs: `ci-evidence.md:64-68`.
- **Description**: GitHub's `branches/{branch}/protection` response always includes `url`. It normally also includes `enforce_admins` (`{url, enabled}`), `required_conversation_resolution`, `allow_force_pushes`, `allow_deletions`, `block_creations` and `allow_fork_syncing`. The three classic fixtures contain only `required_pull_request_reviews`, `required_status_checks`, or the single setting under test. Only the blocking direction is tested (`test_unassessed_classic_protection_blocks_remote_clear`). No test pins the documented rule that "Classic metadata, admin enforcement of existing rules, force-push/deletion permissions, branch creation, and fork-sync permissions add no separate merge gate."
- **Risk**: Suppose `url`, `enforce_admins` or `required_conversation_resolution` is removed from the allowlist, or the property filter is changed. Every classic-protected target would then report `remoteEngineeringGatesClear=false`, with spurious gates such as `Classic protection requires assessment: url`, and every test would still pass. This fails closed, but it breaks the helper's main signal for those branches.
- **Suggested Fix**: Use a realistic payload in the classic branch of `test_zero_approval_rules_without_other_review_requirements_need_no_approval`, which already asserts the clear `requires_engineering_evidence` outcome. Add `url`, `enforce_admins: {"url": ..., "enabled": True}` and `required_conversation_resolution: {"enabled": True}`. Give `allow_force_pushes` `{"enabled": True}` so the allowlist is exercised, not just the `enabled -ne $false` clause, and give `allow_deletions`, `block_creations` and `allow_fork_syncing` `{"enabled": False}`. This takes a few fixture lines and no new test.

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

## Driver resolution

Both findings are resolved in the pending readiness tests:

- Issue 1: one focused test covers successful, pending, failed and errored
  required legacy statuses, plus a failed non-required status. It asserts
  the exact failed, pending, missing and unproven sets and remote readiness.
  The existing no-duplicate-trigger test now also covers a pending legacy
  Azure aggregate, with its unexpected-POST assertion unchanged.
- Issue 2: the existing zero-approval test now uses classic-protection
  metadata, admin enforcement, conversation resolution, force-push/deletion,
  branch-creation and fork-sync fields. Its existing successful merge-state
  assertion proves these fields do not introduce spurious gates.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py -q --tb=short -rs`
passed on Windows (40 tests, 129 subtests, no skips, 160.19 seconds) and Linux
(40 tests, 129 subtests, no skips, 219.93 seconds).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
passed.

Four isolated guard-removal checks produced three assertion failures for
failed legacy statuses, one for duplicate triggering, one for a successful
required legacy status, and one for classic metadata. None produced test
errors or skips, and no production file was modified.
This is one test-file change, with no new helper, documentation or source
file. A new complete review pass is required.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5104 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 57,
  "completedPriorPasses": 56,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
