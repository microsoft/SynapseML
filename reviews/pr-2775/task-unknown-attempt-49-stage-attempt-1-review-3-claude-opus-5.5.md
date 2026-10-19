---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 49
stage_attempt: 1
round: 3
timestamp: "2026-10-18T14:54:59Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "956163ac881da777e6e06ffa954826977007df6a215bcadf2594c2a430a6b716"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-49-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - mode guards (90–110)
  - scoped StrictMode hardening (189–195, 298–302)
  - `Get-RestEvidence` (239–273)
  - branch, rules and classic-protection reads (322–347)
  - fresh revision recheck (519–539)
  - review-request POST (600–606)
  - comment-trigger gate, `/azp run` POST and receipt parsing (609–650)
  - wait loop and output (717–753). There is no top-level `try` or `trap`.
- [x] Inspected `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:
  - `command_json` (39–57)
  - `AzureClient` deadline, token and redirect handling (71–162)
  - timeline attempt summarization, `collect_build`, and test-run pagination and provenance checks
  - the 600-second deadline in `main` (684–686)
- [x] Inspected `watch_azure_pipeline.py`: the monitor loop (95–262), Azure collection on each poll (147), and the exit map (355–366).
- [x] Inspected `verify_trusted_guidance.py`. On Windows, `trusted_executable` (40–66) tries only PATHEXT-suffixed names (44–50), so `az` resolves to a batch launcher. It also strips `GIT_*`, disables fsmonitor, and checks the index, worktree, untracked files and symlinks.
- [x] Cross-checked the code against these documented contracts:
  - `references/ci-evidence.md` (9, 44–56, 84–112, 176–184)
  - `references/ci-triage.md` (34–58)
  - `trusted-guidance.md`, `review-gauntlet.md`, `loop-control.md` (1–140) and `ci-delegation.md`
  - both SKILL.md hunks. The documented invocations (SKILL.md 271 and 274; ci-evidence.md 9) run the script in the caller's session without `-NoProfile`.
- [x] Checked `pipeline.yaml`. The root-level `jobs:` (111) produces `Phase.job` timeline identifiers that match the collector's job keys. The CIHelpers job (112–126) runs `tools/ci/tests/`. The UnitTests step retry and per-attempt PublishTestResults (~964–979) are consistent with the per-attempt result logic.
- [x] Inspected the tests:
  - The `tools/ci/tests/test_get_pr_readiness.py` harness (1–300) stubs `gh` as `function global:gh` under `pwsh -NoProfile`, so native-command semantics never apply.
  - `test_collect_ci_evidence.py` (837–855) and `test_watch_azure_pipeline.py` (488–500) mock `subprocess.run` or inject `TimeoutExpired` directly.
  - `test_trusted_guidance.py` was reviewed in full.
- [x] Considered and dropped these candidates because they are documented or fail closed:
  - `azure_completed` when GitHub lags (ci-triage.md 53–56)
  - the complete-collection trigger gate, which is intentionally retained
  - timeline warnings and ancestor issues blocking `jobGatesPassed`
  - a completed build with an empty timeline producing an error
  - the `-RunPipeline` race with an in-flight build (Get-PrReadiness.ps1 623–624)
  - receipt parse failures (handled at 633–648)
  - the kickoff-in-future check versus clock skew
- [ ] Not done: no commands or tests were run, because this is a read-only review. The supplied pytest, Black and PowerShell results were treated as caller-provided evidence and were not reproduced.

## Issues

### Issue 1: An inherited `$PSNativeCommandUseErrorActionPreference` turns documented nonfatal `gh` failures into script termination
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 99; 244–245 (callers 325, 330, 346, 373, 375, 394, 539); 519–524; 600–605; 628–632
- **Description**:
  - Line 99 sets `$ErrorActionPreference = "Stop"`. Each nonfatal `gh` failure is handled by checking `$LASTEXITCODE` after the call (245, 523, 602, 630).
  - The script never pins `$PSNativeCommandUseErrorActionPreference`. That setting is mainstream in PowerShell 7.4+, which is within the supported 7.2+ range. The documented invocations run in the caller's session, so a profile or session value of `$true` is inherited.
  - With that value inherited, a nonzero `gh` exit throws `NativeCommandExitException` before the `$LASTEXITCODE` branch runs. These calls sit outside the `try` blocks (which start at 254 and 526), and nothing catches the exception at the top level. The run therefore ends with no JSON.
  - The script already guards against an inherited StrictMode for this same "no snapshot at all" reason (189–195, 298–302). This preference is the unguarded sibling case.
- **Risk**: The documented contracts in ci-evidence.md break:
  - REST failures no longer become `collectionErrors` (48–51).
  - A caller without Administration read gets a 403 or 404 from `/protection` (346). That should leave approval unknown (52–55); instead every snapshot for such a caller terminates.
  - Later `-WaitForReview` polls abort instead of reporting revision errors (89–90).
  - A failed review request terminates the run instead of only warning (604–605).
  - A failed `/azp run` POST surfaces a generic exception instead of the "inspect existing comments and builds before retrying" warning (631–632; doc 106–108). The server may already have accepted the comment, so a blind retry can post a duplicate trigger.
  - The outcome still fails closed. The tests can't catch this, because the function stub bypasses native-command semantics.
- **Suggested Fix**: Add `$PSNativeCommandUseErrorActionPreference = $false` beside line 99. It is an inert ordinary variable on 7.2 and 7.3. Optionally, add one regression that sets the preference to `$true` and uses a native `gh` stub that fails `/protection`. The test should expect a snapshot whose `collectionErrors` cite the permission.

### Issue 2: On Windows, the `az` timeout cannot enforce the collector and watcher deadlines
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 39–57, 96–111, 686; watch_azure_pipeline.py 147; verify_trusted_guidance.py 44–50
- **Description**:
  - `AzureClient.get` bounds the token fetch with `self.remaining_timeout()` (109). That bound is meant to enforce the collector's 600-second deadline (686) and the watcher's deadline (147).
  - On Windows, `trusted_executable("az")` resolves `az.cmd` or `az.bat`, which runs through cmd.exe. The real az python.exe is a grandchild that inherits the captured pipes.
  - When `subprocess.run(..., capture_output=True, timeout=...)` (41–47) times out, CPython on Windows kills only cmd.exe. It then calls `communicate()` with no timeout, and that call blocks until pipe EOF, which happens only when the orphaned az process exits.
  - POSIX and the native `gh.exe` are not affected.
- **Risk**:
  - A stalled `az account get-access-token` (for example, a network or proxy stall during token refresh) overruns the documented ten-minute deadline (ci-evidence.md 181) and the 120-minute watcher deadline (ci-triage.md 38). No timeout or error JSON is produced.
  - Agents are told to wait for the completion notification without polling (ci-triage.md 42–44), so the loop can stall indefinitely, and the az process leaks.
  - The outcome fails closed, and the mocked tests never exercise this path.
- **Suggested Fix**:
  - Smallest fix: in `command_json`, send stdout and stderr to `tempfile.TemporaryFile()` objects instead of pipes. The post-kill wait then returns once cmd.exe is dead; read and decode the files afterwards.
  - For full cleanup, use `Popen` and, on timeout, kill the whole process tree before a bounded `communicate()`. On Windows, use System32 `taskkill.exe /T /F` or a Job Object; on POSIX, use `start_new_session=True` with `os.killpg`.
  - Add a Windows-only regression: a `.cmd` launcher that starts a long-sleeping grandchild, asserting that `command_json(..., timeout=1)` raises `EvidenceError` promptly.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
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
  "changeSize": "17 source, test, and documentation paths; 4861 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 49,
  "completedPriorPasses": 48,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Maintainer disposition

The original feedback above is preserved. Both findings were reproduced and
fixed in the pending follow-up; they invalidate this pass's earlier clean rounds.

- Issue 1: the existing protection-permission test now inherits
  `$PSNativeCommandUseErrorActionPreference = $true` and invokes a genuinely
  failing native command. Before the fix it exited without JSON. The readiness
  script now sets that preference to false in its own scope, preserving explicit
  exit-code handling and the incomplete-evidence snapshot.
- Issue 2: a real Windows batch launcher with a sleeping child took 6.21 seconds
  to report a two-second timeout before the fix. Windows batch launchers now use
  bounded communication and exact-PID process-tree termination. Native executables
  and POSIX invocation retain their existing path. Tests cover valid JSON, failed
  exits, invalid output, prompt timeout, child termination, and an explicit
  cleanup-failure diagnostic. Output stays in memory; temporary output files
  were rejected because the command can return an access token.
- Windows validation:
  `python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
  passed: 120 tests and 338 subtests, no skips, 138.77 seconds.
- Linux validation was still running when this disposition was recorded.
  This is not current-head Azure evidence or a clean review of the changed patch.

Linux validation subsequently passed with the same three-suite command:
118 tests and 334 subtests passed in 493.72 seconds. The two Windows-only batch
launcher tests were intentionally skipped on Linux and passed on Windows.
PowerShell parsing/help and pinned Black 22.3.0 checks also passed.
