---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 66
stage_attempt: 1
round: 3
timestamp: "2026-10-19T10:09:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "621da4c129c616f440c91906e58d838375f2953f988c77c2514fd662314516d8"
---

I found one low-severity issue: the watcher's new confirmation poll doesn't reserve time for its own queries, so near the deadline it can report `timeout` instead of the documented outcome. Everything else in the 17 paths held up under edge-case review.

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-66-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Watcher deadline and confirmation logic.** Traced `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (98–275): the confirmation gate (114–120), the reset when an Azure retry is observed (164–166), the deadline exits (122–133, 157–163) and the unresolved outcomes (~202–268).
  - Compared against `test_timeline_completion_allows_one_check_update_before_the_deadline` (`tools/ci/tests/test_watch_azure_pipeline.py` 773–849). Starts at 0, 6599 and 6600, plus the retry case, give the asserted 2/2/1/4 polls for pending, missing and stale-failure checks.
  - The mocked `query_pr` and `collect_build` do not advance the clock (basis of Issue 1).
- [x] **Watcher input validation.** URL trust, the int32 build-ID range, the `--repo` restriction, kickoff parsing (future, naive and overflow cases) and the exit mapping in `main()` (0/1/2/3/4/124/130). These match the outcome descriptions in `references/ci-triage.md` lines 34–107.
- [x] **Collector fail-closed paths** in `collect_ci_evidence.py`:
  - `AzureClient` (125–212): endpoint allowlist, redirect refusal, deadline clipping, and timeline 404 or empty accepted only before a build completes.
  - `$skip`/`$top` pagination detects repeated or omitted IDs.
  - `verify_build` and `summarize_timeline`: type checks, parent-cycle detection, highest attempt per (type, identifier).
  - `collect_tests` (505–690): `totalTests` truncation check, rejection of attempts newer than the timeline, `collectionStable` re-read in `main` (738–770).
  - Every malformed nested field I traced ends in a sanitized `EvidenceError`/`MonitorError`, matching the claim in `references/ci-evidence.md`.
- [x] **Windows batch cleanup.** `run_windows_batch`/`command_json` (39–111): `taskkill /T /F`, then a fallback kill/wait, chained causes, the PID in the cleanup diagnostic and no echoed arguments or stderr. Tests: `tools/ci/tests/test_collect_ci_evidence.py` 904–1001.
- [x] **Bytecode writes.** Both helpers disable them before sibling imports (collector 18–21, watcher 14–17), so normal helper runs leave no `__pycache__`. The verifier rejects even ignored files, so this matters on resume.
- [x] **Readiness `-RunPipeline` gate** in `Get-PrReadiness.ps1`:
  - Preconditions (103–111): one PR, both expected SHAs, and no `-WaitForReview` or `-RequestReview`, so repeated polls cannot re-post `/azp run`.
  - Trigger block (610–655): complete evidence, an open PR, the exact aggregate absent, and receipt parsing that cannot turn a successful POST into a failure.
- [x] **Readiness data collection** in `Get-PrReadiness.ps1`:
  - Paged REST uses `--paginate --jq tojson` (240–274), so each page arrives as one compact JSON line.
  - Workflow scoping (375–432): identity checks, fork association with closed PRs, and grouping by workflow and event.
  - Approval-count guard (569–580) and revision recheck (523–567).
- [x] **Trusted-guidance verifier** (`verify_trusted_guidance.py`): `GIT_*` variables are stripped, `core.fsmonitor=false` is set and `--no-replace-objects` is used. No hooks or content filters run for `rev-parse`, `ls-tree`, `ls-files` or `cat-file`. It rejects untracked/ignored files, symlinks, submodules and conflicts, and tolerates only CRLF differences in text files.
  - All `REQUIRED` paths exist in this checkout, including root `CODEOWNERS` and `references/contributor-safety.md`.
- [x] **Doc anchors.** All 20 `*.md#anchor` links in the two skills resolve to existing headings, including `ci-evidence.md#comment-trigger-gate`, `loop-control.md#evidence-invalidation` and `ci-delegation.md#evidence-required-before-triggering`.
- [x] **CI coverage.** `PrReadinessTests` runs under `TF_BUILD` even without `pwsh` (`tools/ci/tests/test_get_pr_readiness.py` 249–251), so Azure cannot skip it silently. The `CIHelpers` job (`pipeline.yaml` 112–126) has a 15-minute timeout. The supplied full-directory Linux baseline was 486.94 s, before the latest readiness and collector edits; hosted-agent timing is unverified. Not a finding.
- [ ] **Test execution not performed.** This was a read-only review. The supplied pytest, Black and PowerShell results are cited as given, not re-run.

## Issues

### Issue 1: The confirmation poll reserves no query time, so near the deadline it reports `timeout` instead of the documented `azure_completed`/`attention_required`
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`. Related: `tools/ci/tests/test_watch_azure_pipeline.py`, `.github/skills/synapseml-pr-loop/references/ci-triage.md`
- **Line(s)**:
  - Watcher: 114–120 (gate), 122–133 and 157–163 (deadline exits), ~202–268 (unresolved outcomes).
  - Test: 773–849, the `(6599, False, 0, 2)` row.
  - ci-triage.md: 55–62.
- **Description**:
  - `wait_for_check_update()` allows the extra poll whenever `deadline - now > POLL_SECONDS`. It reserves the 600-second sleep but no time for the poll's own work: `gh pr view`, `az account get-access-token`, and the build and timeline requests.
  - Those calls are clipped to whatever remains: `query_pr(timeout=min(60, remaining))` and `AzureClient(deadline=deadline)`.
  - If the remaining budget at the decision falls between 600 s and 600 s plus the poll's latency, the confirmation poll hits the deadline mid-query. The watcher then returns the bare `timeout_result` (exit 124).
  - It had already established `azure_completed` (pending or missing check) or `attention_required` (disagreement), both exit 4 with a classification message. That result is discarded.
  - `ci-triage.md` promises that insufficient time for the poll returns those outcomes.
  - The new 6599 row leaves 1 s for the poll. It passes only because the mocked query and collection take no clock time. A real `az` launcher takes seconds, so in practice that row's poll would expire.
- **Risk**:
  - In a narrow but reachable window, a completed Azure build with a lagging or disagreeing GitHub aggregate is reported as a generic timeout. The triage doc treats that as "leave CI unresolved".
  - The distinction between a missing/pending aggregate and a stale FAILURE disagreement is lost.
  - There is no false green: the exit stays nonzero and the `azure` evidence remains attached. The cost is mis-triage.
- **Suggested Fix**:
  - Keep the design of one extra poll strictly before the deadline, but make the confirmation best-effort:
    - Store the unresolved result computed before the sleep.
    - If the confirmation poll reaches the deadline, return that stored result instead of `timeout_result`.
    - Clear the stored result when an Azure retry is observed.
  - Simpler alternative: also reserve the normal 60-second query allowance in the gate (`<= POLL_SECONDS + 60`) and shift the boundary rows accordingly.
  - Either way, add a latency-aware row: advance the mocked clock inside the `query_pr`/`collect_build` side effects during the confirmation poll, then assert exit 4 and the original outcome.

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
  "changeSize": "17 source, test, and documentation paths; 5489 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 66,
  "completedPriorPasses": 65,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 is resolved as a documentation ambiguity plus a latency-coverage gap.
The deadline remains authoritative: eligibility to **start** a confirmation
poll is not a guarantee that all its requests can finish. If those requests
exhaust the remaining budget, `timeout` (124) is the accurate result, with the
last collected Azure evidence still attached. Returning the previous
GitHub classification would present an unconfirmed observation as the outcome
of that expired confirmation. Reserving an arbitrary 60 seconds would not
guarantee completion of multiple network calls either.

`references/ci-triage.md` now explicitly distinguishes insufficient time to
start another poll (the existing unresolved outcome) from an in-flight
confirmation exhausting the deadline (`timeout` with Azure evidence).
The existing public-CLI matrix adds six latency-aware subtests: for pending,
missing and stale-failure checks, advance the clock within either the GitHub
query or Azure collection during confirmation. They assert exit 124, retained
Azure evidence, exact call counts and no deadline extension. Production code
is unchanged; no stale-result fallback, timing constant, test method or source
file was added.

`python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed with 42 tests, 102 subtests and no skips on Windows Python 3.14.6
(4.76 seconds) and Linux Python 3.10.20 (6.37 seconds).
Black 22.3.0 and source whitespace checks passed.

The receipt's input hash and observed reviewer identity were verified.
Transport inspection confirmed read-only source inspection with no prior or
sibling feedback. Root-relative globs selected only `CODEOWNERS` and
`.github/*.md`; they did not enumerate review artifacts. The original response
above is unchanged. Its source diff SHA-256 was
`3fdbcbc805985a56ef869b71eb8532535cb316bc52a7dd56c930c7dbd8ffab4c`,
with manifest SHA-256
`99869b8a1e3f9d2f60767a2a95afaabae9f2d78f4523050b43aabfa11f5b88a8`.

The documentation and test edits invalidate attempt 66. Only lens 3 ran.
This is not a qualifying pre-commit pass or current-head CI evidence.
