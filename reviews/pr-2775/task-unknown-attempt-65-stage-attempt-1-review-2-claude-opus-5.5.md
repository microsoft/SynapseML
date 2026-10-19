---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 65
stage_attempt: 1
round: 2
timestamp: "2026-10-19T09:26:06Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b60eb5c68021fc32df23b41597e858c9ecfed8b4e4671529fa74179012118bea"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-65-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The three helper scripts form a one-way import chain, and each shared value is defined once.**
  - `watch_azure_pipeline.py:17-27` imports from `collect_ci_evidence.py`: the project, build-ID and repository constants, `AzureClient`, `EvidenceError`, `command_json`, `collect_build`, `github_json` and `verify_merge`.
  - The collector imports only `GuidanceError` and `trusted_executable` from `verify_trusted_guidance.py` (`collect_ci_evidence.py:21`). The chain is verifier → collector → watcher, with no cycle.
  - Pipeline and repository provenance checks live in one place: `verify_build` at `collect_ci_evidence.py` ~236-277.
  - This matches the "reuse this watcher" and "shared evidence collectors" guidance in `.github/skills/synapseml-pr-loop/SKILL.md:237-239, 250-252` and in `ci-delegation.md` step 5.
- [x] **Errors are converted only at layer boundaries, and messages never echo command details.**
  - `command_json` (`collect_ci_evidence.py` ~74-112) turns trusted-tool lookup failures, timeouts, launch failures, invalid UTF-8 and invalid JSON into `EvidenceError`.
  - Messages use fixed labels: `gh pr view`, `gh api` and `az account get-access-token`. They never include arguments or error output.
  - The watcher converts `EvidenceError` to `MonitorError` only at its boundaries (`watch_azure_pipeline.py:91-92, 148-151`). Its exit-code map matches `references/ci-triage.md`.
  - In `tools/ci/tests/test_collect_ci_evidence.py`, `test_cli_failure_does_not_echo_secret_bearing_stderr` asserts these exact labels. `test_missing_trusted_tool_reports_lookup_failure_without_running_it` asserts that the tool is never launched.
- [x] **The watcher's deadline governs every network call.**
  - `AzureClient.remaining_timeout` (`collect_ci_evidence.py` ~134-171) limits both the access-token call and each HTTPS request to the caller's deadline.
  - The watcher creates `AzureClient(deadline=deadline)` on each poll and limits the one-time `verify_merge` query the same way (`watch_azure_pipeline.py:137-147`).
  - This matches SKILL.md step 7 ("without extending the original deadline") and the window measured from kickoff time in ci-triage.
- [x] **The comment-trigger gate is defined in one place and enforced in code.**
  - `references/ci-evidence.md:94-124` defines the gate.
  - SKILL.md step 7 (lines 213-215 and 227-228) and `ci-delegation.md` step 3 link to it. Step 9 (lines 269-277) describes the helper invocation that enforces it.
  - `Get-PrReadiness.ps1:103-111` requires exactly one PR and both expected SHAs, and rejects `-WaitForReview` and `-RequestReview`.
  - `Get-PrReadiness.ps1:613-648` checks, in order:
    - it refuses incomplete evidence and lists the gaps;
    - it requires an open PR;
    - it posts only when the exact, case-sensitive `microsoft.SynapseML` aggregate is absent;
    - a failure to parse the receipt cannot turn a successful POST into a failed request.
  - The conservative complete-collection requirement is retained.
- [x] **Evidence, engineering, human-approval and merge states stay separate.**
  - `Get-PrReadiness.ps1:559-579` computes `evidenceComplete`, `remoteGatesClear` and `humanApproval` independently.
  - `completeness.complete` remains a documented backward-compatible alias (`.DESCRIPTION` lines 40-45, output object ~650-716), so the existing output shape is preserved.
  - `ci-evidence.md:73-81` and SKILL.md step 1 match the code.
- [x] **A review is requested at most once per PR per invocation.**
  - `$script:reviewRequestOutcome` (`Get-PrReadiness.ps1:581-608`) blocks the request on bad revision evidence only before the first attempt. Later `-WaitForReview` polls (`:718-755`) reuse the recorded outcome.
  - This matches `ci-evidence.md:83-92`.
- [x] **Trusted-guidance safeguards are applied consistently.**
  - Both scripts that import a sibling set `sys.dont_write_bytecode` under `__main__` before that import (`collect_ci_evidence.py:18-21`, `watch_azure_pipeline.py:14-27`). Running them therefore cannot leave bytecode files in the verified guidance paths.
  - The documented Python flags differ for a reason. The collector and watcher use `-E -s -B`; the standalone verifier uses `-I -B`, because `-I` would remove the script directory that sibling imports need.
  - `tools/ci/tests/test_trusted_guidance.py` parses these commands from `trusted-guidance.md`, `ci-evidence.md` and `ci-triage.md`, so the docs and the code cannot drift apart unnoticed.
- [x] **The test suites follow existing conventions and run in CI without pipeline changes.**
  - All four suites carry the Microsoft copyright header and use `unittest.TestCase`.
  - The Python-helper suites load scripts with `importlib.util.spec_from_file_location`, like the existing `tools/ci/tests/test_website_doctest.py`.
  - The readiness suite runs only where PowerShell 7 is available or under Azure Pipelines (`skipUnless(shutil.which("pwsh") or TF_BUILD)`).
  - The unchanged CIHelpers job (`pipeline.yaml:112-126`, `python3 -m pytest tools/ci/tests/ -q`, 15-minute limit) picks them up by directory discovery. No pipeline or workflow edit was needed, consistent with the AGENTS.md ask-before-changing rule.
- [x] **Documentation structure and links are intact.**
  - SKILL.md step 8 (lines 259-262) delegates to `references/review-gauntlet.md`. Its `reviews/pr-<n>/` artifact placement matches AGENTS.md.
  - The step 4 no-bloat gate resolves to `readiness-gates.md:20-28`, whose artifact-pruning clause links to `loop-control.md#artifact-retention`.
  - These anchors all exist: `#comment-trigger-gate`, `#validation-only-handoff`, `#waiting-for-azure-pipelines`, `#evidence-invalidation` and `#user-value-and-scope`.
  - Searches limited to `.github/skills/` found no stale manual-prompt references, no unguarded `-RunPipeline` route, and no use of `completeness.complete` as a readiness signal.
  - The external skill's compatibility line now lists the PowerShell, Python and Azure CLI requirements of the helpers it delegates to.
- [ ] **Tests were not run.** This is a read-only review. The Windows/Linux pytest, mutation and Black results supplied with the request were checked against the source but not reproduced.
- [ ] **Three observations are not reported as issues; no change is requested.** The supplied diff text is cut off after the early SKILL.md hunks, so the first two could not be attributed to this PR. None of the three is a behavioural inconsistency.
  - The `.DESCRIPTION` in `Get-PrReadiness.ps1` (lines 13-18) still says coverage compares the "newest" automated review. The code (`:496-516`) correctly matches reviews by commit and explains why in a comment.
  - SKILL.md step 9 (lines 279-284) states what `-WaitForReview` waits for in two adjacent paragraphs.
  - `trusted_executable` lives in the verifier and the collector reuses it. Moving it to its own module would add a file, which the no-bloat gate argues against.

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
