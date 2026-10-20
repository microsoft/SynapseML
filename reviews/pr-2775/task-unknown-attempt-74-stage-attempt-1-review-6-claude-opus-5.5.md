---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 74
stage_attempt: 1
round: 6
timestamp: "2026-10-20T01:29:32Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "86ee34b7ea9b969a460300ffda7465954abc1803af8521e7d10c8ff97bef5a77"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\singhrana\Documents\SynapseML\.worktrees\pr-validation-skills-20261016\reviews\pr-2775\task-unknown-attempt-74-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **All 17 manifest paths checked.** The diff display was truncated, so I opened each named path directly. Every change is under `.github/skills/` or `tools/ci/tests/`. There are no changes to product APIs, dependencies, releases, `pipeline.yaml` or workflow definitions.
- [x] **Nonpublishing-job guidance matches the real CI job.** In `pipeline.yaml` (lines 112-126), CIHelpers runs `python3 -m pytest tools/ci/tests/ -q` and has no test-results publisher. Four docs follow this consistently:
  - `ci-evidence.md` "Jobs without published test results" (lines 243-271)
  - `ci-triage.md` (lines 105-117)
  - pr-loop `SKILL.md` step 7 (lines 263-268)
  - `loop-control.md` (line 255)

  They separate named local proof (`-v -rs`, with the revision, command, platform and baseline recorded) from aggregate CI corroboration. Jobs that publish results must still provide their expected named results. The docs forbid invented or unrelated selectors and relabeling a failed collection. They do not let local runs replace required CI-only hardware evidence.
- [x] **The local command is consistent across docs.** `ci-triage.md` line 111 runs all four helper suites with `python -B ... -v -rs`, matching the generic `pytest <test-node-ids> -v -rs` form in `ci-evidence.md` line 251.
- [x] **PowerShell requirement and gating are documented accurately.** `Get-PrReadiness.ps1` line 1 has `#Requires -Version 7.2`, matching the "PowerShell 7.2+" lines in both skill descriptions. In `test_get_pr_readiness.py` (lines 249-250), the suite skips only when `pwsh` is missing and `TF_BUILD` is not set. In Azure the suite runs and fails without `pwsh`, which matches what `ci-triage.md` says (lines 115-117).
- [x] **The streaming deadline works as documented.** In `collect_ci_evidence.py` (lines 126-145), `DeadlineReader.readinto` sets the remaining timeout before every raw receive and closes the original stream when the deadline passes. The test (lines 1087-1180) uses a real `HTTPConnection` over `socket.socketpair()` and covers three cases: complete response, expiry between reads, and a stalled body. In the stall case, 0.2 s remains against a 2 s socket timeout and the query must stop within 1.5 s. Checking `fileno() == -1` is meaningful because urllib's `do_open` has already closed `h.sock`.
- [x] **Error messages are sanitized as documented.** The `AzureClient.get` error path rechecks the original deadline before choosing a message. A `TimeoutError` or `URLError(TimeoutError)` with time remaining is reported as a query timeout. Other errors stay generic, and `HTTPError` reports only the status code. The `NoRedirect` handler and default verified HTTPS context remain. No runtime thread, timer or dependency was added. This matches `ci-evidence.md` (lines 194-199).
- [x] **Readiness policy fields behave as documented.** The script outputs `ruleRequiredChecks` and `classicProtection`. Classic `contexts` and `checks[].context` both feed the list of exact required checks. Skipped or neutral required checks go to `unprovenRequiredChecks`. Tests 513-567 cover these cases:
  - ruleset, classic and no policy
  - absent, SUCCESS, SKIPPED and NEUTRAL checks
  - side-effect assertions

  The `microsoft.SynapseML` aggregate is always required, so exempt diffs stay blocked in the summary, as `readiness-gates.md` says.
- [x] **The pipeline ID is consistent.** `AZURE_PIPELINE_ID = 17563` (`collect_ci_evidence.py` lines 26 and 294) matches `ci-evidence.md` lines 135 and 162.
- [x] **Performance stays bounded.**
  - **Collector:** 10-minute total deadline with a final recheck. Pagination uses `$top=100` with a duplicate-ID guard and a `totalTests` cross-check. Windows cleanup can take up to about 15 s, matching the doc. The extra `settimeout` call on each receive costs little compared with network I/O.
  - **Watcher:** keeps the 600 s cadence, does one merge verification and at most one confirmation poll.
  - **Tests:** waits are bounded. The 6 s sleeps appear only in the Windows-only tests at lines 906 and 934.
- [x] **Diagnostic output is adequate.**
  - **Watcher:** emits started and finished JSON, `deadlineAt`, and `agent_loss`, deduplicated per job and attempt.
  - **Collector:** reports `availableRunNames`, `issueCounts` (counts without messages), `skippedJobs`, `unresolvedJobs`, `collectionStable`, `revisionScope`, sanitized errors, and `mergeReady=false`.
  - **Readiness:** reports `collectionErrors`, `revisionErrors` and `unassessedGates`.
- [x] **The token is not exposed.** Line 214 builds `****** and the test at line 1226 checks for the fixture header. The masked header I saw on screen comes from the review tool's display; a literal-pattern search found the real text. The token stays in memory and is never printed.
- [x] **The trusted-guidance verifier and its tests match `trusted-guidance.md`.** Required paths exist: root `CODEOWNERS`, `pipeline.yaml`, `AGENTS.md`, and the skill and safety references. Symlink and regular-file checks run before any read. Ignored and untracked files are rejected. The tests cover edits to CODEOWNERS, `pipeline.yaml` and templates, shadow imports, the fsmonitor hook, and replace refs. The two POSIX-only skips are at lines 108 and 349.
- [x] **Links and anchors resolve.** Every anchor used in the changed docs exists, including:
  - `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`
  - `#jobs-without-published-test-results`, `#user-value-and-scope`
  - `#waiting-for-azure-pipelines`, `#who-counts-as-external`

  The sibling skill and reference files also exist.
- [x] **No stray bytecode.** The tests import helpers under `sys.dont_write_bytecode`, and `.gitignore` covers `*.pyc` and `.pytest_cache/`.
- [x] **Rejected as not concrete defects** (cosmetic or hypothetical):
  - STARTUP_FAILURE is missing from rollup `failedChecks`, but workflow runs catch it and required checks stay unproven.
  - Verifier mismatch messages don't name the offending path.
  - An empty REST body is reported as "Invalid JSON".
  - The watcher fetches a new token on each 10-minute poll.
  - The connect, TLS and send steps use the timeout calculated when the request opens, but they are still reported as timeouts.
- [ ] **Tests not executed.** As a read-only reviewer I couldn't run them. The pass, skip and mutation results come from the supplied evidence.
- [ ] **Hosted Azure results are for an earlier head.** Build 241706733 ran on head 0487209b…, not this pending patch. CIHelpers reports only aggregate counts, so the names of the tests skipped in CI can't be checked.
- [ ] **Two test files only partly read.** `test_watch_azure_pipeline.py` and `test_trusted_guidance.py` were reviewed through targeted searches and their test-name lists, not line by line.
- [ ] **Frozen identity not recomputed.** I couldn't recompute the index tree 87aae225… or the diff SHA-256 a2014dcc…, because running commands is not allowed.