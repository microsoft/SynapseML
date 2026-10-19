---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 67
stage_attempt: 1
round: 5
timestamp: "2026-10-19T10:56:26Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a7f3c12d50c6e321b4c8ecd1f7131603045924258aa7c0dd941c88348229e086"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-67-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Watcher confirmation poll** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`)
  - `wait_for_check_update` (lines 114–120) sleeps once for 600 seconds, and only when `deadline - now > 600`. The allowance resets when Azure is not completed (lines 164–166).
  - I traced all 18 subtests of `test_timeline_completion_allows_one_check_update_before_the_deadline` in `tools/ci/tests/test_watch_azure_pipeline.py` (lines 773–868):
    - Start 6599 exits 0 and start 6600 exits 4, so the test catches a change from `<=` to `<`.
    - The retry row needs the allowance reset to pass: 4 polls, 3 sleeps, clock at 1800.
    - The query-latency and collection-latency rows end exactly at 7200 and return 124. They keep the `azure` evidence and assert exact query and collection counts.
  - The `main()` exit mapping starts at line 340.
- [x] **Other watcher timeline tests** (`tools/ci/tests/test_watch_azure_pipeline.py`)
  - Agent loss (lines 693–736): one event each for attempts `[1, 2]`, four sleeps, and outcome `azure_completed`/`pending`.
  - Absent aggregate (lines 738–771): exit 4, `missing_or_stale`, 2 queries, 1 sleep.
  - Merge verification (lines 901–935): exactly one `git/commits/<merge>` query, with timeouts clipped to 60 and 10. Every real `AzureClient` gets deadline 7200.
  - Agreement matrix (lines 937–993): 7 conclusion/result pairs, checking exit codes, wait counts and the retained Azure result.
  - Truncated-response and overrun tests (lines 594–691): an error before the deadline and a timeout at the deadline.
- [x] **Collector** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`)
  - The `command_json` operation labels (lines 74–110) are checked by `test_cli_errors_are_not_success` (watcher test, lines 493–538). It asserts the exact messages for timeout, launch, UTF-8, exit-code, JSON and non-object failures, and that nothing marked `private` leaks.
  - The collector suite covers:
    - `run_windows_batch` cleanup chaining, which keeps the PID in the error.
    - The `AzureClient` endpoint allowlist, token check, deadline clipping and redirect refusal.
    - `$skip`/`$top` pagination that rejects duplicate IDs.
    - `verify_merge` parent order and `verify_build` provenance.
    - `collect_tests` handling of attempts, publications and allowed skips.
    - Public `main()` and `collectionStable` invalidation.
- [x] **Readiness** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`)
  - The humanApproval logic (lines 569–578) and merge state (lines 688–691) are checked by `test_zero_approval_rules_without_other_review_requirements_need_no_approval` in `tools/ci/tests/test_get_pr_readiness.py` (lines 626–668).
  - That test runs 7 policies against both classic protection and rulesets, for 14 subtests. A count of 1 and a null count both give `unknown`/`blocked`, so removing the `-ne 0` guard or the type guard fails it.
  - Other tests in the suite cover:
    - Rerun scoping by workflow and event.
    - Head-repository identity, fork fallback and legacy shapes.
    - The receipt variants.
    - 15 unsafe `-RunPipeline` refusals with no trigger side effects.
    - Unevaluated rules and a case-sensitive required check.
- [x] **Trusted guidance**: `tools/ci/tests/test_trusted_guidance.py` has 17 tests for `verify_trusted_guidance.py`.
  - They cover replace refs, untracked and ignored files, symlinks, inherited `GIT_*` variables and PATH lookup.
  - The fsmonitor test does catch the bug, because `ls-files` refreshes fsmonitor.
  - The `-I -B` and `-E -s -B` flags are taken from the docs, not hard-coded.
- [x] **Docs match behavior**
  - `references/ci-triage.md` lines 55–65 describe the single confirmation poll and the 124 timeout with retained evidence, matching the code.
  - Every link anchor resolves: `SKILL.md:95`, `ci-evidence.md:7/94/126`, `readiness-gates.md:20` and `loop-control.md:53/71/136`.
- [x] **CI wiring**
  - The CIHelpers job in `pipeline.yaml` (lines 111–126) runs `python3 -m pytest tools/ci/tests/ -q`. There is no conflicting pytest config.
  - In Azure (`TF_BUILD`), the readiness suite errors rather than skips if pwsh is missing, which matches the docs.
  - Test counts (42/42/42/17) and the platform-only skips match the supplied evidence.
- [x] **Authorization header**: the tool display shows the ****** as `******`. A count-mode grep confirmed the source has `****** and the test asserts `****** so this is display redaction, not a defect.
- [x] **Considered but not reported** (not concrete, or blanket coverage the plan excludes):
  - A query error during the confirmation poll, before the deadline, returns `error` without Azure evidence. `ci-triage.md` lines 70–71 already document query errors as nonzero results.
  - The `KeyboardInterrupt` cleanup path is untested.
  - Classic `checks` follows the same path as the tested `contexts`.
  - A `null` warning count and stage-prefixed job IDs are speculative.
- [ ] **Tests not executed**: this was a read-only review. It relied on reading the source and the supplied validation evidence, and makes no claim that tests were run.

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
The 68 read-only tool calls stayed within named source files and source subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
privately with SHA-256 `2cc74d846c663a28c370c713dd5e79abe7252cc5f084e87508c3460cd1107544`.
This is one lens of the six-clean pre-commit pass, not current-head CI clearance.
