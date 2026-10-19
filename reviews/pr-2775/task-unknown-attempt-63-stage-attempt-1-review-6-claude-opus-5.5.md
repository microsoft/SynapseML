---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 63
stage_attempt: 1
round: 6
timestamp: "2026-10-19T07:42:08Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4439a036b6109cb65019b9fd2cf20ea8442bf7a5e0be1b1574daaae59e125d25"
---

I found one issue: a Low-severity observability regression in the shared CLI wrapper. The documentation, anchors and gates I checked match the code.

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-63-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope read.** I read the whole merge-base-to-index diff and the changed sources:
  - Scripts: `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py`.
  - Pr-loop docs: `SKILL.md`, `ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md`.
  - External-contributor docs: `SKILL.md`, `ci-delegation.md`.
  - The four helper test suites under `tools/ci/tests/`.
- [x] **Readiness contract.** The `ci-evidence.md` output table and comment-trigger gate (L94-122) match `Get-PrReadiness.ps1`:
  - Revision recheck at L520-557.
  - `evidenceComplete`, `remoteGatesClear` and `humanApproval` at ~L558-578.
  - The `-RunPipeline` incomplete-evidence throw lists the gaps, requires an open PR, matches `microsoft.SynapseML` case-sensitively, and parses the receipt separately from the POST exit status (~L612-648).
  - `engineering`, `merge` and `unassessedGates` at ~L683-697.
- [x] **CI-trigger gate retained, not relaxed.** Every comment route points to `ci-evidence.md#comment-trigger-gate`: pr-loop `SKILL.md` L213-214, `ci-delegation.md` L69-70, `ci-triage.md` L16-17 and `readiness-gates.md` L101. Hand-posted requeues still need a fresh snapshot with `evidenceComplete=true`.
- [x] **Watcher contract.** `ci-triage.md` L28-96 matches `watch_azure_pipeline.py`:
  - `query_pr` at L78-95.
  - `monitor`: one deduplicated `agent_loss` event per job attempt; `azure_completed` for a missing, stale or pending GitHub check; `attention_required` on disagreement; `replaced`; and the deadline boundary at L118-121.
  - The exit-code map, and timeline flags pairing with `--base-sha`/`--merge-sha`.
- [x] **Collector contract.** `ci-evidence.md` claims match `collect_ci_evidence.py`:
  - `run_windows_batch` (L39-71): 5+5+5 s cleanup bound and PID diagnostic.
  - `AzureClient` (L110-202): endpoint allowlist, `NoRedirect`, `$skip`/`$top` pagination with repeated-ID detection.
  - `verify_build` and `summarize_timeline` (L262-430).
  - `collect_tests` (L490-675): `availableRunNames`; named tests cannot be skip-exempted; current-attempt gating.
  - `main`: 600 s deadline, `collectionStable` recheck, `mergeReady=false`.
- [x] **Trusted bootstrap.** `verify_trusted_guidance.py` covers HEAD, index, untracked/ignored files, symlinks and content checks (`trusted-guidance.md` L15-29).
  - Both entry points disable bytecode writes before sibling imports (watcher L14-16; collector L18-19), so helper runs leave the verified snapshot clean.
  - The required root files exist (`AGENTS.md`, `CODEOWNERS`, `pipeline.yaml`; named-file check only).
- [x] **Anchors.** All new cross-file anchors resolve:
  - `readiness-gates.md#user-value-and-scope` (L20)
  - `SKILL.md#validation-only-handoff` (L95)
  - `ci-evidence.md#comment-trigger-gate` (L94)
  - `ci-triage.md#waiting-for-azure-pipelines` (L6)
  - `ci-delegation.md#evidence-required-before-triggering` (L25)
  - `loop-control.md` anchors (L53, L71, L136)
  - `ci-triage.md#targeted-infrastructure-retry` (L193)
- [x] **Performance.** No defect found:
  - The fork-fallback PR lookup runs once per snapshot (`Get-PrReadiness.ps1` L389-410).
  - `-WaitForReview` is bounded: poll 5-600 s (default 60), timeout 1-240 min (L69-73).
  - Timeline lineage uses dict lookups.
  - The watcher refreshes its Azure token once per 600 s poll.
- [x] **Independent verification of supplied claims.**
  - "Four suites run in CIHelpers": `pipeline.yaml` L111-125 runs whole-directory pytest.
  - "Missing pwsh fails under TF_BUILD": `test_get_pr_readiness.py` L249-251, `skipUnless`.
  - Each readiness snapshot is one `pwsh` launch with an in-process `gh` fake and a 30 s cap (L21-60, L253-272).
- [ ] **Hosted CIHelpers runtime not evidenced** (not counted as an issue). The job has `timeoutInMinutes: 15`, and the supplied 486.94 s local whole-directory baseline predates the latest edits. Pipeline edits are out of scope, and the final CI-qualified run must confirm the job fits.
- [ ] **No commands or tests run by this reviewer** (read-only review). I checked the supplied validation results for consistency but did not reproduce them.

## Issues

### Issue 1: Shared CLI wrapper reports timeouts and launch failures as invalid evidence
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**:
  - `collect_ci_evidence.py` L74-96 (`command_json`), L68 (re-raise after Windows cleanup) and L746-751 (collector `main` output).
  - `watch_azure_pipeline.py` L89-92 and L352-353.
- **Description**: `command_json` now handles every `gh` and `az` call in both the collector and the watcher: `query_pr`, `verify_merge`, and Azure token acquisition.
  - It folds four different failures into one message, "`<tool>` did not return valid evidence.": `subprocess.TimeoutExpired`, `OSError` (the process could not start), `UnicodeError` and `ValueError` (invalid JSON).
  - On Windows, `run_windows_batch` re-raises the original `TimeoutExpired` after a successful process-tree cleanup (L68), so batch-launcher timeouts land in the same message.
  - Both entry points print only `str(error)`, so the chained cause never reaches the user.
  - The removed watcher `query_pr` had a distinct timeout message ("GitHub status query timed out.") and separate launch, UTF-8 and JSON errors.
  - The message also does not say which query failed. The watcher issues both `gh pr view` and `gh api .../git/commits/<sha>`.
  - `test_cli_errors_are_not_success` (`tools/ci/tests/test_watch_azure_pipeline.py` L493-525) checks only the exit code and exception type, so tests did not catch this regression.
- **Risk**:
  - The attached watcher can run for up to 120 minutes and reports its result in one final JSON line.
  - A transient 60 s `gh` timeout before the deadline, or a slow `az account get-access-token`, ends the run with `{"outcome":"error","message":"gh did not return valid evidence."}`. That reads like malformed output or an installation problem.
  - Operators may then debug the gh/az install or the JSON shape instead of relaunching the watcher for the remaining window or checking connectivity.
  - Gates remain fail-closed with a nonzero exit, so this is a diagnosability regression only.
- **Suggested Fix**: Split the broad `except` into cause-specific messages that still exclude stderr and response bodies:
  - `TimeoutExpired` → "`gh pr view` timed out after 60s."
  - `OSError` → "Could not run `gh`."
  - `UnicodeError`/`ValueError` → "`gh pr view` returned invalid UTF-8/JSON output."
  - Name the subcommand with the leading argv tokens, which contain no secrets at any current call site.
  - Then assert the expected message for each failure type in the existing `test_cli_errors_are_not_success` subtests, without adding new test methods.

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
  "changeSize": "17 source, test, and documentation paths; 5306 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 63,
  "completedPriorPasses": 62,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver Disposition

The low-severity diagnostic regression is fixed in `command_json`. Timeouts,
launch failures, invalid UTF-8 and invalid JSON now produce distinct messages.
Known operations are identified with fixed labels for `gh pr view`, `gh api`
and Azure token acquisition; arguments, stderr, response bodies and exception
details are never copied into these messages. Existing cleanup-failure PID
diagnostics and chained causes remain intact.

The existing watcher CLI-error test asserts the final JSON error messages and
redaction. Existing collector cases verify operation labels without leaking
arguments, and the Windows batch-timeout test asserts the distinct timeout.
The strengthened watcher/collector tests produced five assertion failures before
the production fix:
`python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py -k 'cli_errors_are_not_success or cli_failure_does_not_echo' -q --tb=short -rs`.

After the fix,
`python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed on Windows Python 3.14.6 (83 tests, 253 subtests, no skips, 14.28 s) and
Linux Python 3.10.20 (81 tests, 249 subtests, two Windows-only skips, 14.86 s).
`python3 -m black --check --required-version 22.3.0 .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py`
passed. No new source files or test methods were added.

Attempt 63 is invalidated by the code/test fix. Lens 5 was clean; lens 6 found
this issue; lenses 1-4 did not run. The reviewed source diff was
`c4bbbd060727764133572821af3491651a885314311cfa0a82901e05c5134061` against
base `d34f762a785ee8a7c7c624806f8c112256168833` and local predecessor
`38685fad3d2e3ec93e6757adf36aa270cc3a24d1`.
Transport audit confirmed the requested model, exact embedded diff,
independent read-only source access, and publication-safe output.
