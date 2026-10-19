---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 69
stage_attempt: 1
round: 6
timestamp: "2026-10-19T18:07:52Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4b11e7eba8b9339b7ecfb10358d988631b57d0a90abd0d6ead0756eaf363e7ee"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Display limitation handled.** The supplied diff display was truncated partway through `.github/skills/synapseml-pr-loop/SKILL.md` step 8. I reviewed the exact working-tree files of all 17 manifest paths instead: 9 skill documents, 4 skill scripts and 4 `tools/ci/tests` suites. Every visible hunk matched the working tree.
- [x] **Deadline reader traced end to end.** I followed `collect_ci_evidence.py:126–171` through `http.client` `begin()`, `read()` and `_safe_read`:
  - Every raw receive re-arms `settimeout(min(60, deadline − now))`.
  - `EvidenceError` passes through the `except` at line 229 without being caught.
  - Closing the response closes BufferedReader, then `DeadlineReader`, then `SocketIO`, then the real socket once `io_refs` reaches 0.
  - `do_open(..., context=self._context)` keeps the standard HTTPS context, and `NoRedirect` replaces the default redirect handler.
- [x] **New streaming regression checked.** `test_streaming_response_stops_at_the_original_deadline` (`tools/ci/tests/test_collect_ci_evidence.py:1087–1170`):
  - It uses a real opener, `HTTPConnection` and `HTTPResponse` over `socket.socketpair()`.
  - It patches only the connection factory, `getproxies` and the clock.
  - Every wait is bounded: 2-second socket timeouts, `wait(2)` and `join(timeout=3)`.
  - It asserts `fileno() == -1`, an unfinished body and a stopped sender, so a regression fails instead of hanging the `CIHelpers` job.
- [x] **Readiness helper's Azure requirement checked.** `Get-PrReadiness.ps1:359–363, 458–469, 561–568, 686–691`, compared against `test_get_pr_readiness.py:487–510`, uses a fixture with no rules (152–222). This is the basis for Issue 1.
- [x] **Watcher deadline handling checked** (`watch_azure_pipeline.py:98–170`):
  - An `EvidenceError` at or after the deadline returns `timeout`.
  - At most one confirmation poll runs, and only if it can start strictly before the deadline.
  - `agent_loss` is reported once per job and attempt.
  - Exit codes match the "Waiting for Azure Pipelines" section of `ci-triage.md`.
- [x] **Performance bounds checked:**
  - `collect_tests` fetches results only for requested run names (line 572) and compares `totalTests` with the fetched count (line 623), so a truncated page is detected.
  - Windows batch cleanup is capped at 5 + 5 + 5 seconds, matching the "fifteen additional seconds" claim in `ci-evidence.md:197–198`.
  - The watcher reads one build and one timeline per 600-second poll.
- [x] **Docs checked against configuration and code:**
  - `ci-triage.md:105–113` matches `pipeline.yaml:112–126`: the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu.
  - The readiness suite does not skip under `TF_BUILD` (`test_get_pr_readiness.py:249–250`).
  - `verify_trusted_guidance.py` matches `trusted-guidance.md`: `ls-files --others` covers ignored files, non-regular files are rejected before reading, and no hooks or content filters run.
- [x] **All new cross-document anchors resolve** to existing headings: `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`, `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`, `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`, `#user-value-and-scope` and `#evidence-required-before-triggering`.
- [x] **Intentional gate not flagged.** The retained complete-collection `-RunPipeline` gate (`Get-PrReadiness.ps1:612–620`) was treated as intentional, as the plan instructs.
- [ ] **Not executed:** I ran no tests, linters, Black or TLS probe (read-only contract). The supplied Windows and Linux pytest, Black and real-TLS evidence was not reproduced.
- [ ] **Not verified:** whether the reviewed working tree is byte-identical to frozen index tree `8068a88c…` or matches the stated source-diff SHA-256.
- [ ] **Not inspected:** hosted CI and Azure results. No CI was requested, per the plan, so this is not a CI-qualified pass.
- [ ] **Not opened (independence and search-scope rules):** `reviews/`, prior or sibling artifacts, session evidence and `.github/workflows/`.

## Issues

### Issue 1: Exempt-diff readiness guidance relies on helper states that cannot clear without an Azure run
- **Severity**: Medium
- **File**: `.github/skills/synapseml-pr-loop/SKILL.md`; `.github/skills/synapseml-pr-loop/references/readiness-gates.md`. The behavior comes from `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.
- **Line(s)**: SKILL.md 279–283; readiness-gates.md 125–126 and 131–136; Get-PrReadiness.ps1 359–363, 468–469, 561–568 and 686–691
- **Description**:
  - The helper always puts `microsoft.SynapseML` in `$exactRequiredChecks`. When that check is absent, it is listed in `missingRequiredChecks`, which forces `remoteEngineeringGatesClear=false`, `engineering=blocked`, `merge=blocked` and `completeness.complete=false`.
  - This happens even when no ruleset or classic branch protection requires the check. `test_azure_leg_does_not_replace_missing_aggregate` shows it with `rules: [[]]` and classic protection disabled.
  - An exempt diff (only `.github/skills/` and `reviews/`) correctly has no Azure run. For that case:
    - SKILL.md 279–283 says the read-only snapshot keeps "the helper's report of actual GitHub gates".
    - readiness-gates.md 131 says to use the helper's "separate readiness states".
  - Neither file says these states are always blocked for an exempt diff without an Azure run. Neither explains how to tell the helper's built-in entry apart from a check GitHub actually enforces (`ruleRequiredChecks[].context`, `classicProtection.required_status_checks`). Readers need that distinction before applying the merge-policy mismatch rule at readiness-gates.md 126.
- **Risk**:
  - Every exempt PR that follows the documented path ends with a readiness signal that stays blocked.
  - An agent may report a false blocker.
  - An agent may label the helper's own requirement as an enforced GitHub mismatch when GitHub enforces nothing.
  - An agent may override the summary states by hand, which could also hide a check GitHub really does enforce.
  - This undermines reliable, separate readiness evidence for exactly the PRs the new policy covers, and pushes agents toward the `/azp run` that the policy forbids.
- **Suggested Fix**: Clarify the docs only, in SKILL.md step 9 or at readiness-gates.md 125. No helper change or new mechanism is needed. State that:
  - On an exempt diff with no Azure run, the helper lists `microsoft.SynapseML` as missing, so the summary states stay blocked.
  - That entry is a merge-policy mismatch only if it appears in `ruleRequiredChecks` or `classicProtection.required_status_checks`. Otherwise, record Azure as not required.
  - All other gates are judged from their own fields: other missing or unproven checks, `failedChecks`, `pendingChecks`, workflows, review threads, `automatedReviewCoversHead`, `behindBy` and `mergeable`.
  - The summary states must never be cited as clear for such a PR.

### Issue 2: Acceptance-scenario tables still treat any absent Azure build as a missing gate
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/loop-control.md`; `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md`
- **Line(s)**: loop-control.md 215 and 232; ci-delegation.md 92 (compare lines 25–27)
- **Description**:
  - loop-control.md 232 says, without conditions: "No failures but no Azure build → Missing gate, not green; trigger only after authorization and safety checks."
  - For exempt diffs, SKILL.md step 7 and readiness-gates.md 125 now say the opposite: Azure is not required, should be recorded as not required rather than passed, and should not be triggered just to satisfy the workflow.
  - ci-delegation.md 92 ("CI missing → Request CI once without another confirmation") also has no exemption, even though lines 25–27 of the same file exempt skill and review-only diffs.
  - loop-control.md 215 requires exercising these tabletop cases whenever the workflow changes, yet no row covers the new exemption. This contradicts the claim that the CI-scope policy is documented consistently.
- **Risk**: These tables serve as quick decision references and as the checklist for workflow edits. On an exempt PR, an agent using them may report a missing Azure gate or post `/azp run` under delegation, both of which the new policy rejects. A future editor checking against the table could also remove the exemption by mistake.
- **Suggested Fix**:
  - Qualify row 232 as "Non-exempt diff: no failures but no Azure build".
  - Add a row: "Diff limited to `.github/skills/` and `reviews/`, no Azure build → Azure not required (not passed); run local tests and applicable Actions; report an enforced Azure check as a merge-policy mismatch; do not trigger."
  - Change ci-delegation.md 92 to "applicable CI missing".

### Issue 3: Collector reports deadline and timeout expiry as "query failed or returned invalid JSON"
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: 229–232, together with 180–188 and 216–218 (compare 104–106)
- **Description**:
  - When less than 60 seconds of budget remain, `DeadlineReader` sets the socket timeout to the remaining budget.
  - If Azure stalls rather than trickling, the receive raises `TimeoutError` exactly at the original deadline. A connection stall surfaces as `URLError(TimeoutError)`.
  - Both are caught at line 229 and reported as "Azure evidence query failed or returned invalid JSON."
  - The more specific "Azure evidence deadline expired." appears only when expiry is noticed:
    - between receives (the trickle case the new test covers),
    - before a request starts, or
    - at the final recheck in `main`.
  - `main` prints only `str(error)`, so the timeout cause is lost.
  - Other paths handle this better: the watcher rechecks the deadline and reports `timeout` (`watch_azure_pipeline.py:158–161`), and the CLI path reports "timed out after N seconds" (lines 104–106). The HTTP path does neither.
- **Risk**:
  - The same cause, an expired budget, produces two different messages depending on whether bytes were still arriving.
  - When a stall causes the expiry, it looks like an API or JSON defect. That contradicts the deadline behavior documented in `ci-evidence.md:192–194` and can send CI triage toward malformed responses or authentication instead of a slow service.
  - The exit status is not affected.
- **Suggested Fix**:
  - In the handler at line 229, call `self.remaining_timeout()` before raising the generic error, so an expired budget reports "Azure evidence deadline expired." with the socket error as its cause.
  - Report `TimeoutError`, and `URLError` whose `reason` is a `TimeoutError`, as "Azure evidence query timed out."
  - Optionally, add one stalled-sender row to the existing streaming test to lock this in.

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

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver resolution

The original feedback above is retained unchanged. Attempt 69 ended when these
fixes changed the reviewed source; its partial pass does not qualify that source.

1. **Fixed: exempt-diff readiness interpretation.** The readiness guide now
   explains the helper's unconditional Azure requirement and blocked summaries.
   With complete policy evidence, only a matching ruleset/classic requirement
   makes an absent aggregate an enforced merge-policy mismatch. Incomplete
   policy remains unknown. Other check, workflow, review and integration fields
   must be assessed independently; the summary must not be rewritten as clear.
   The main skill links directly to this interpretation.
2. **Fixed: acceptance tables.** The no-Azure missing-gate row now explicitly
   applies to non-exempt diffs, a separate row covers the exemption, and delegated
   triggering requires missing applicable CI.
3. **Fixed: timeout diagnostics.** The HTTP exception handler rechecks the
   original deadline before classifying errors. Expiry reports the existing
   deadline error; a direct or URL-wrapped socket timeout before expiry reports
   a sanitized query timeout. Other failures keep their existing sanitized
   classification. One four-row regression covers open/read timeouts before
   and at the deadline, message sanitization, and response cleanup.

Before the production diagnostic fix, all four new subcases failed their
expected-message assertion; no fixture or import error caused those failures.

```text
python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs
```

Passed on Windows Python 3.14.6: 87 tests, 282 subtests, no skips, 16.76 seconds.
Passed on Linux Python 3.10.20: 85 tests, 278 subtests, two Windows-only
batch-launcher skips, 16.47 seconds. Black 22.3.0 and staged whitespace passed.

The complete corrected 17-path patch is frozen for attempt 70 with source-diff
SHA-256 `423e6347af8a31453a06041f5d750ca13cbed82b3c855bb5707d78fb03b39f76`.
No hosted CI result is claimed here.