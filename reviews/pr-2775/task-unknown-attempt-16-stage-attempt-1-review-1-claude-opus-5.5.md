---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 1
timestamp: "2026-10-17T10:52:54Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5752c5a377844e46c9f57c72f10038a1aad01b2f5a917ae3144354aeb6ce310f"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-16-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the full current source of all four changed helpers and confirmed it matches the supplied diff:
  - `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (647 lines)
  - `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
  - `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`
  - `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (666 lines)
- [x] The collector's test checks fail closed (`collect_ci_evidence.py`):
  - A test-run attempt newer than the timeline raises (451–454).
  - A job reference that is unknown or not an int never counts as current (444–456), and a requested run without provenance is an error (489).
  - `--allow-skipped` cannot exempt a named `--expected-test` (511).
  - Required publications are tracked per job, so another job's success cannot hide a missing retry publication (519–537).
- [x] The collector's build and job checks fail closed (`collect_ci_evidence.py`):
  - The timeline is read again after the tests and compared, so movement blocks `validationPassed` (623–633).
  - `mergeReady` is always false (634).
  - Leaf Phase/Stage records, any issue count, and agent loss all block the job gate (351–370).
- [x] Azure client and tool-lookup hardening:
  - Endpoint paths are allow-listed with a regex (`collect_ci_evidence.py` 88–92).
  - Redirects are refused, with no follow-up request (62–66).
  - CLI and HTTP errors are sanitized and stderr is not echoed (37–55, 111–127).
  - The token is held in memory only.
  - `trusted_executable` skips relative PATH entries and anything under the current directory, and expands PATHEXT on Windows (`verify_trusted_guidance.py` 40–64).
- [x] Trusted-guidance verifier (`verify_trusted_guidance.py` 67–150):
  - It strips `GIT_*` variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`, using only plumbing reads.
  - It rejects symlinks and submodules, an index that differs from the tree, untracked or ignored files (126), and content drift, tolerating only CRLF differences (140).
  - The required paths (`CODEOWNERS`, `pipeline.yaml`, `templates/`, skill and safety references) exist in the repository. `.gitattributes` sets `* text=auto eol=lf`.
- [x] Readiness gate separation (`Get-PrReadiness.ps1`):
  - `-RunPipeline` requires both expected SHAs and refuses `-WaitForReview` (101). It also requires complete evidence and an open PR, and posts only when the exact aggregate is absent (540–560).
  - The PR and target are read again, and movement or an expected-SHA mismatch is recorded (483–495).
  - Human approval becomes `unknown` when rules cannot be read (504–508), and merge is never automated (598–603).
  - Fork workflow runs count only when this PR is the unique one on that head (349–392).
- [x] `pipeline.yaml` uses top-level jobs (no stages) and publishes test results per job. This matches the collector's `phaseName.jobName` job key.
- [x] Docs cross-check:
  - New anchors resolve to real headings in `ci-triage.md`, `loop-control.md`, `ci-evidence.md` and `ci-delegation.md`.
  - `references/review-gauntlet.md` keeps the step-8 policy removed from `SKILL.md` (six themes, frozen patch, budgets, publication) and adds a "stop when the runner rejects the destination" rule.
  - The validation-only and no-maintainer-change semantics agree across both `SKILL.md` files and `ci-delegation.md`.
- [x] Test coverage:
  - `tools/ci/tests/test_collect_ci_evidence.py` 297–339 covers malformed timeline fields and run-reference fields, but not unhashable `id`, `name` or `outcome` values.
  - The watcher tests mock `collect_build` with jobs already present.
  - `test_get_pr_readiness.py` has no case for a rule type the helper does not evaluate.
- [ ] Not done: I ran no tests, because this is a read-only review. I did not independently reproduce the supplied pytest, WSL and Black results or the live GitHub CLI and Azure pagination claims. The inspected code paths fail closed if those claims are wrong.

## Issues

### Issue 1: Unhashable Azure field values cause an uncaught `TypeError`, breaking the documented sanitized-error contract
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (contract: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`)
- **Line(s)**: `collect_ci_evidence.py` 142, 422, 501, 565, 637; `ci-evidence.md` 121–122
- **Description**: `ci-evidence.md` says malformed nested Azure fields "produce sanitized error evidence rather than an uncaught traceback." Several hashing operations run before any type check:
  - `identity in seen` (142) fails when a run or result `id` is a list or dict.
  - `summary.get("name") not in run_names` (422) fails when a run `name` is a list or dict.
  - `Counter(row["outcome"] ...)` (501, 565) fails when a result `outcome` is a list or dict.

  Each raises `TypeError`, and `main` catches only `EvidenceError` (637).
- **Risk**: The process ends with a traceback and no final JSON object, so a consumer expecting `{"outcome": "error"}` gets output it cannot parse. The exit code is still nonzero and no secrets are printed, so the gate itself stays closed. The harm is a broken documented contract and less useful diagnostics.
- **Suggested Fix**:
  - Validate scalars before hashing. In `AzureClient.list`, require `id` to be an int or a non-empty str. Require the run `name` to be a str, and the result `outcome`/`state` to be a str or `None`. Raise `EvidenceError` otherwise.
  - Alternatively, add a final sanitized catch for `TypeError`, `KeyError` and `AttributeError` in `main`.
  - Add subtests with list or dict values for run `id`, run `name`, result `id` and result `outcome`.

### Issue 2: In timeline mode, the watcher ends with `error` instead of waiting when a queued build has no timeline records yet
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: `collect_ci_evidence.py` 111–127, 182–190, 193–202, 354; `watch_azure_pipeline.py` 138–150; `references/ci-triage.md` 17–21, 38–47
- **Description**: `verify_build` explicitly accepts the `notStarted` and `postponed` statuses, but `collect_build` then requires a non-empty timeline straight away:
  - An empty or `null` timeline body raises `EvidenceError` from the JSON decode or non-object check.
  - A `records: []` body raises "Azure timeline has no job records."
  - Before the deadline, the watcher turns any `EvidenceError` into `MonitorError`, which ends with `outcome: "error"` and exit 1.

  `ci-triage.md` tells agents to start the watcher right after the build queues. It also says timeline mode "can monitor an explicit build even without the GitHub aggregate" and "continues waiting at the normal cadence until Azure completes." The code has no non-error state for "queued, no job records yet," and no test covers it. I could not confirm from a read-only review how Azure responds for every queued build; the gap is that the code cannot represent this state at all.
- **Risk**: In the exact window the docs describe (build just queued, GitHub aggregate not yet registered), the single attached watcher can end immediately. The agent then has to relaunch it or poll, which goes against the one-watcher, no-short-poll contract. It fails closed with no false success, but it weakens the reliability this PR claims.
- **Suggested Fix**: When the build is not `completed` and the timeline is absent, empty, or has no Job/Phase/Stage records, return a pending summary (`jobs: []`, `jobGatesPassed: false`) so the watcher sleeps for `POLL_SECONDS`. Keep this case fatal for `completed` builds. Add a watcher regression test that goes from `notStarted` (no records) to `inProgress` to `completed`.

### Issue 3: `remoteEngineeringGatesClear` silently ignores ruleset rule types it does not evaluate
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (contract: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`)
- **Line(s)**: `Get-PrReadiness.ps1` 312–333, 497–502, 604; `ci-evidence.md` 30–31, 44
- **Description**: The script reads every effective branch rule but evaluates only `required_status_checks` and `pull_request`. Other merge-blocking rule types (`workflows`, `code_scanning`, `required_deployments`, `merge_queue`) do not affect `$remoteGatesClear` and are not listed in `readiness.unassessedGates`. `ci-evidence.md` defines `remoteEngineeringGatesClear` as "Observable integration, reviews, checks, and workflow gates are clear," yet these rules are visible in the same `effectiveRules` response and are dropped silently.
- **Risk**: `remoteEngineeringGatesClear` and its alias `completeness.complete` can be true while GitHub still blocks on an unmet required-workflow, code-scanning or deployment rule. A required workflow that never triggered leaves no run or check for the helper to catch. `readiness.merge` stays `blocked` because it checks `mergeStateStatus`, so there is no automated-merge false positive. The engineering signal is overstated, though, and the reason for the block goes unexplained.
- **Suggested Fix**: Collect the rule types that are neither evaluated nor known to be irrelevant to merging. For merge-blocking types, either keep `remoteEngineeringGatesClear` false or report them, for example `unevaluatedRules` plus a `readiness.unassessedGates` entry per type. Add a pwsh test with a `workflows` or `code_scanning` rule.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed locally; publication pending
- **What changed**: Added explicit scalar checks before hashing test IDs, run names, and outcomes. Applied the same contract to consumed run, phase, job-attempt, build-result, and queue-time fields without a broad exception catch.
- **Why**: Malformed evidence must produce sanitized error JSON rather than a traceback or an echoed invalid object.
- **How verified**: List/dict field regressions failed before the guards and now produce error JSON without the diagnostic payload. The stricter parser also accepted real historical Azure evidence.

### Issue 2
- **Status**: Fixed locally; publication pending
- **What changed**: Timeline-only HTTP 404, empty bodies, and null responses represent absence. Nonterminal builds expose `timelinePending` and nonpassing empty job gates; completed builds still reject missing jobs. Authentication and malformed-response errors remain errors.
- **Why**: A watcher started immediately after queueing must wait for job creation rather than end prematurely.
- **How verified**: A watcher regression uses the real shared collector across a queue with no timeline or GitHub check, running jobs, and successful completion. It sleeps exactly twice for 600 seconds. Separate cases reject absent timelines on completed builds and reject non-timeline or authorization failures.

### Issue 3
- **Status**: Fixed locally; publication pending
- **What changed**: Unevaluated merge rules are exposed and block automatic engineering clearance. Copilot automation uses the existing current-head review gate; only deletion and non-fast-forward restrictions are inapplicable to ordinary merges.
- **Why**: Visible workflow, scanning, or future merge rules must not silently disappear from readiness. The repository's actual Copilot auto-review rule must also not become a false blocker when current-head coverage is proven.
- **How verified**: Offline PowerShell cases cover required workflows, code scanning, an unknown future rule, Copilot with and without coverage, and inapplicable rules. Live collection recognized the actual `copilot_code_review` rule, reported complete evidence and no unevaluated types, and still blocked engineering on the outstanding review/CI gates.

## Driver validation and identity

- Reviewed predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
- Target and merge base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed pending diff SHA-256: `a11e1f0ed8f9667b799a175b7d29f999d4d01bede15b301ad482252b240287b1`.
- Original artifact SHA-256: `cdaabd3d55579936ff17c3d5b8ade5cc448bb5a2d0672c9c13f722f336d382f8`.
- Windows final affected modules: `python -B -m pytest -q tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py --tb=short` passed: 70 tests and 148 subtests.
- Windows final readiness suite: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py --tb=short` passed: 21 tests and 40 subtests. The unchanged guidance suite passed in the preceding four-module run.
- Linux: `python3 -B -m pytest -q tools/ci/tests -rs --tb=short` passed: 321 tests and 160 subtests; 21 PowerShell tests skipped and covered on Windows.
- Black 22.3.0 and whitespace checks passed.
- Read-only historical build 241234907 collection: named test passed, selected count 1, stable timeline, no test errors. Overall exit 1 and `validationPassed=false` correctly preserve 27 unresolved job gates; this is not current-PR CI.
- This failed pass is superseded by the fixes. It is not a clean or CI-qualified review.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; approximately 3500 added lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 16,
  "completedPriorPasses": 15,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
