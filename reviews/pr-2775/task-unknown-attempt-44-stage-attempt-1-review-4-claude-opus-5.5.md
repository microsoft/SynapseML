---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 44
stage_attempt: 1
round: 4
timestamp: "2026-10-18T07:47:14Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6059af66d3e8e81b08808570319fe23ee2acdd2bfa48c5bae1d342dc03e77d20"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-44-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Azure client and entry point** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`). Checked `AzureClient.get` and `AzureClient.list`:
  - Only allow-listed endpoints are requested, and redirects are refused.
  - A 404, empty or `null` timeline counts as pending only when the build is not yet completed.
  - Pagination uses `$skip=len(values)` with `$top=100`, stops on a page shorter than 100, and fails on a repeated ID.
  - `verify_merge` requires the parents in the exact order `[base, head]`.
  - `verify_build` checks pipeline definition 17563, the GitHub repository, `refs/pull/N/merge`, the merge SHA, the queue reason, `queueTime` and status.
  - `main` sets a 600-second deadline, calls `collect_build` a second time to set `collectionStable`, always sets `mergeReady=False`, and exits nonzero unless the build `succeeded`, job gates pass, tests pass and the collection is stable.
  - No off-by-one or type-coercion defects.
- [x] **Test selection** (`collect_tests`).
  - The `phaseName.jobName` key is matched only against Job-type timeline attempts. A test run newer than the timeline raises an error.
  - `current_results` includes only current-attempt rows from requested runs. Publication errors block only requested runs that are current.
  - `required_publications` and `required_test_publications` require each job to republish after a retry.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - All of this matches the "Azure merge and named tests" section of `references/ci-evidence.md`.
- [x] **Timeline summary and job gates** (`summarize_timeline`, `collect_build`).
  - Only the latest attempt per `(type, identifier)` is kept, and parent cycles are detected.
  - Leaf Phase/Stage records are kept, and ancestor issues apply to each descendant job.
  - Each record's issue count is `max(listed issues, reported count)`.
  - The unresolved-job rule, allowed skips and `timelinePending` behave as documented.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`). Worked the deadline arithmetic by hand against the tests:
  - 12 queries and 12 sleeps up to 7200 seconds; a late start gets 3 queries; a restart sleeps 200 seconds once.
  - The replacement window ends at 13800 seconds, and query timeouts are clipped to the remaining budget (30 seconds in the test).

  Also checked:
  - **URL trust:** lowercased path set, port 443 or none, no userinfo or fragment, exactly one ASCII-digit `buildId`, and a length check before `int()`.
  - **Outcomes:** the ordering of `superseded`, `replaced`, `azure_completed` and `attention_required`.
  - **Agent loss:** at most one event per (identifier, attempt).
  - **Exit codes:** the exit-code map.
  - **Kickoff time:** fractional seconds are truncated to 6 digits for Python 3.10.
- [x] **Readiness helper** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`).
  - **Inputs:** parameter guards, and `Get-RestEvidence` parses one compact JSON page per line and rejects empty or `null` output.
  - **Workflow runs:** total-count check, verified PR associations, the unique fork-branch fallback, and newest run per workflow/event.
  - **Checks:** exact case-sensitive required checks versus the prefix-based `-RequiredCheck`, plus `unprovenRequiredChecks`.
  - **Readiness states:** revision recheck, and separate `evidenceComplete`, `remoteEngineeringGatesClear`, `humanApproval` and `merge` states.
  - **`-RunPipeline`:** throws with the full gap list before posting anything, requires an OPEN PR, and posts at most once per invocation, only when the exact `microsoft.SynapseML` check is absent. The conservative complete-collection gate is unchanged.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`).
  - **Tool lookup:** `trusted_executable` uses only absolute PATH entries and excludes the current directory and its descendants, including resolved symlink targets. It expands PATHEXT on Windows.
  - **Git calls:** strip `GIT_*` variables, pass `--no-replace-objects` and turn fsmonitor off.
  - **Snapshot checks:** the index must equal the pinned tree at stage 0, and `ls-files --others` runs without exclude rules, so ignored files are caught. Paths may not traverse symlinks. Text files may differ only by CRLF line endings.
  - **Imports:** standard library only, so the `-I` bootstrap works.
- [x] **Docs against code.** Checked `references/ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `review-gauntlet.md`, `loop-control.md`, `readiness-gates.md`, both `SKILL.md` files and `ci-delegation.md`.
  - Every new anchor resolves: `#comment-trigger-gate`, `#validation-only-handoff`, `#artifact-retention`, `#state-and-cost-discipline`, `#evidence-invalidation`, `#targeted-infrastructure-retry` and `#waiting-for-azure-pipelines`.
  - Every documented comment route, including a hand-posted requeue, goes through the comment-trigger gate.
- [x] **Test suites** (`tools/ci/tests/test_collect_ci_evidence.py`, `test_watch_azure_pipeline.py`, `test_get_pr_readiness.py`, `test_trusted_guidance.py`).
  - The assertions match the behaviour I traced.
  - Module loading keeps patches from leaking between suites.
  - The readiness harness mocks `gh` offline, and fails rather than skips when `pwsh` is missing under `TF_BUILD`.
  - There are no Python 3.11+-only APIs, so the unchanged `CIHelpers` job in `pipeline.yaml` (ubuntu-22.04, system Python 3.10) can run them.
- [ ] **No tests or commands were run.** This was a read-only review; pass results come from the validation evidence you supplied.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4423 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 44,
  "completedPriorPasses": 43,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
