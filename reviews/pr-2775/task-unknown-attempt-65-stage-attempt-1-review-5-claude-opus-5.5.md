---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 65
stage_attempt: 1
round: 5
timestamp: "2026-10-19T08:29:18Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "571e5d29127e687bedc8ec507f771ba31ce1f8b6f8745915fde9ab1eb3ebcf34"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-65-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Hosted CI collects all four suites.** The `CIHelpers` job in `pipeline.yaml` (lines 112–126, ubuntu-22.04, 15-minute timeout) runs `python3 -m pytest tools/ci/tests/ -q`. `PrReadinessTests` skips only when neither `pwsh` nor `TF_BUILD` is present (`tools/ci/tests/test_get_pr_readiness.py`, lines 249–251), so a missing `pwsh` fails in Azure rather than skipping. This matches `.github/skills/synapseml-pr-loop/references/ci-triage.md` lines 98–106. `Get-PrReadiness.ps1` line 1 declares `#Requires -Version 7.2`.
- [x] **The readiness mock is faithful and catches side effects.**
  - The harness (`test_get_pr_readiness.py` lines 21–143) rejects `--slurp`, requires `--paginate --jq tojson`, and emits one compact JSON page per line.
  - It projects `gh pr view --json` fields and produces a real native `$LASTEXITCODE` from a child process for the protection-permission case.
  - `snapshot()` (lines 253–305) asserts there is no CI-trigger or review-request side effect on every path. That includes all 15 refusals in `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (lines 902–972) and the receipt variants (lines 988–1016).
- [x] **Readiness gates are each exercised by a test.** I traced `Get-PrReadiness.ps1` lines 330–455 (rules, classic protection, workflow pagination, scoping, grouping by workflow and event) and lines 540–647 (revision recheck, the separate evidence, engineering and human-approval states, guarded `-RunPipeline`). Each maps to a test: paged and malformed rules (869–878), unevaluated rules (822–852), unassessed classic protection (756–785), the zero-approval matrix (626–671), and rerun scoping by event and workflow (1078–1105).
- [x] **Collector provenance is tested through public entry points.** I cross-checked `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` against `tools/ci/tests/test_collect_ci_evidence.py`.
  - The real `main()` asserts the exact `gh api …/git/commits/<merge>` query and `deadline=700` (tests 1166–1222).
  - Reversed parents are rejected before any `AzureClient` is created (tests 1224–1239).
  - A retry that starts during collection clears `collectionStable` (tests 1241–1262).
  - The real `AzureClient.get` (source 125–218) is checked for the endpoint allowlist, `Bearer` header, bounded token timeout, and refusal to fetch credentials once the deadline has expired (tests 1077–1139).
- [x] **Windows batch cleanup is covered on Linux CI too.** The real-batch tests (905–965) are Windows-only. The cross-platform mocked test (967–1001) covers `run_windows_batch` (source 39–71) and runs in CI. It checks normal fallback, denied kill and timed-out wait, PID retention, sanitized messages and exception chaining. In timeline mode, `monitor()` in `watch_azure_pipeline.py` (lines 136–151) turns any cleanup `EvidenceError` into a terminal error; it does not keep polling.
- [x] **Watcher fixtures match the real collector.**
  - The hand-built Azure dictionaries use the keys real `collect_build` emits: `identifier`, `attempt`, `state`, `agentLossDetected`, `status`, `result` and `url` (collector lines 426–493).
  - One test runs the real collector from an empty queue to completed jobs (113–156).
  - Other tests cover the 7199/7200/7201 deadline boundary (643–691), agent-loss deduplication per attempt (693–735), and `missing_or_stale` with exit 4 (737–766).
  - Merge verification runs once with clipped 60- and 10-second timeouts, and every real client keeps deadline 7200 (799–833). The GitHub/Azure agreement matrix is covered (835–886).
- [x] **The CI interpreter (Python 3.10) is handled.** `parse_kickoff` (`watch_azure_pipeline.py` lines 259–274) normalizes fractional seconds and a `Z` suffix before calling `fromisoformat`. Tests assert 7-digit truncation, 2-digit padding and offset normalization (569–581). The fake clock patches the shared `time` module, and `subprocess` keeps its own `monotonic` reference, so subprocess timeouts are unaffected.
- [x] **Trusted-guidance tests run the documented commands.**
  - `tools/ci/tests/test_trusted_guidance.py` reads the flags from `references/trusted-guidance.md` line 18 (`-I -B`), `references/ci-evidence.md` line 140 and `references/ci-triage.md` line 31 (`-E -s -B`). It runs them with a hostile `PYTHONPATH` or a shadowing `json.py` (127–216).
  - Negative controls cover fsmonitor, replace refs, ignored bytecode, CWD decoys and symlinked tool targets.
  - No `__pycache__` exists under `.github/skills/`.
- [x] **New cross-document anchors resolve.**
  - `synapseml-pr-loop/SKILL.md#validation-only-handoff` (line 95)
  - `ci-evidence.md#comment-trigger-gate` (line 94)
  - `ci-evidence.md#azure-merge-and-named-tests` (line 126)
  - `ci-triage.md#waiting-for-azure-pipelines` (line 6)
  - `readiness-gates.md#user-value-and-scope` (line 20)
- [x] **Considered and excluded as blanket coverage of defensive guards, not concrete defects:**
  - No fixture for the malformed-commit or SHA-mismatch branch of `verify_merge` (lines 223–233). GitHub returns the commit that was requested.
  - No fixture for the untyped-rule guard (`Get-PrReadiness.ps1` lines 333–335). `type` is the API's discriminator field, so GitHub always sends it.
  - No test for the `KeyboardInterrupt` arm of `run_windows_batch`. It uses the same cleanup path as the tested timeout.
  - In `test_same_build_retry_keeps_original_deadline_and_successful_jobs` (768–787), the job-attempt assertion only checks a mocked value passed through. Collector retry tests cover the real behavior, and the test's deadline assertion is real.
- [ ] **Tests were not executed.** This was a read-only review with view, grep and glob only. The supplied Windows and Linux results were not reproduced. The whole-directory run is a prior baseline, not a run of the current candidate.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5379 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 65,
  "completedPriorPasses": 64,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver disposition

This clean lens is invalidated by the subsequent watcher completion fix.
See the [lens 3 disposition](task-unknown-attempt-65-stage-attempt-1-review-3-claude-opus-5.5.md#driver-disposition)
for the resolution, validation and independence audit. The original response
above is unchanged; this lens is not a qualifying final-patch signoff.
