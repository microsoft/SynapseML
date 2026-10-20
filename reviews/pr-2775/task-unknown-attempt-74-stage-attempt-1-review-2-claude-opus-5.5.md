---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 74
stage_attempt: 1
round: 2
timestamp: "2026-10-20T02:03:06Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "46d6493a549b444cc13d094c4becc49eafd896197265551a684737e443c471ca"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\singhrana\Documents\SynapseML\.worktrees\pr-validation-skills-20261016\reviews\pr-2775\task-unknown-attempt-74-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope follows `AGENTS.md`.** All 17 manifest paths are under `.github/skills/` or `tools/ci/tests/`. The diff does not touch `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `AGENTS.md` or `CONTRIBUTING.md`. Because the diff changes `tools/` paths, this PR is correctly non-exempt under the new scope exception (`.github/skills/synapseml-pr-loop/SKILL.md` lines 207-217).
- [x] **Each rule is defined once and linked elsewhere.**
  - The Azure scope exception lives only in `SKILL.md` §7. These files link to it: `SKILL.md:282`, `references/ci-evidence.md:96`, `references/readiness-gates.md:100`, `synapseml-external-contributor-review/SKILL.md:42` and `references/ci-delegation.md:25`.
  - The comment-trigger gate lives only in `ci-evidence.md:94`. These files reference it: `SKILL.md:227`, `ci-triage.md:17`, `ci-delegation.md:74` and `readiness-gates.md:103`.
- [x] **Every anchor in the changed docs points to an existing heading.** Checked anchors:
  - `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`
  - `#jobs-without-published-test-results`, `#azure-merge-and-named-tests`, `#github-snapshot`
  - `#user-value-and-scope`, `#review-and-validation`
  - `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
  - `#evidence-required-before-triggering`

  `SKILL.md` ends at line 336, under the 500-line limit.
- [x] **`scripts/Get-PrReadiness.ps1` matches its documentation.**
  - `#Requires -Version 7.2` (line 1) matches both skills' compatibility fields.
  - `microsoft.SynapseML` is always added to the exact required checks (line 359).
  - The `evidenceComplete`/`remoteGatesClear` logic (lines 541-568) and the output object (lines 650-712) match the `ci-evidence.md` outputs table. The output includes the `readiness.*` fields, the `completeness.complete` alias, the raw `classicProtection` object and `ruleRequiredChecks`.
  - Since `classicProtection` is the raw protection object, the `readiness-gates.md:131` reference to `classicProtection.required_status_checks` is valid.
- [x] **The Python helpers are layered correctly.**
  - `verify_trusted_guidance.py` imports no sibling modules and runs under `python -I -B` (`trusted-guidance.md:18`).
  - The collector imports only `GuidanceError` and `trusted_executable` from it (`collect_ci_evidence.py:22`) and runs under `-E -s -B` (`ci-evidence.md:144`). The watcher uses the same flags (`ci-triage.md:31`) and reuses the collector's Azure client and merge verification instead of duplicating them.
  - `DeadlineReader` resets the socket timeout before each read (line 137), and `NoRedirect` is kept (line 119).
  - The collector rejects runs with no suite or test selector (lines 779-780), which matches the documented rule that a real published selector is required.
- [x] **The no-published-results contract matches `pipeline.yaml`.**
  - The `CIHelpers` job (lines 112-125) runs `python3 -m pytest tools/ci/tests/ -q` and has no `PublishTestResults` task; the first one is at line 401, in another job.
  - This matches the `ci-evidence.md` section "Jobs without published test results" and the local `-v -rs` command in `ci-triage.md` (lines 105-116).
  - The readiness suite is skipped only when neither `pwsh` nor `TF_BUILD` is present (`test_get_pr_readiness.py:249-250`). That matches the documented behavior: missing PowerShell fails in CI and skips locally.
- [x] **The new tests follow the existing `tools/ci/tests` pattern (unittest plus spec-loading) and exercise public entry points.**
  - `test_get_pr_readiness.py` lines 513-567 run the real PowerShell script. They assert where the policy came from and the missing, skipped and neutral check outcomes.
  - The real-socket streaming test in `test_collect_ci_evidence.py` (about lines 1087-1180) covers a complete response, expiry between reads and a stalled body.
- [x] The stated Black 22.3.0 version matches the pin in `environment.yml:57-58`.
- [ ] I ran no commands or tests; I had read-only access. All pass, skip and mutation results in the prompt were supplied by the caller and not verified here.
- [ ] I did not search `.github/workflows/` or other root paths, per the scope restrictions. Whether any GitHub Action also runs `tools/ci/tests/` is unverified.
- [ ] Hosted build 241706733 ran on the earlier head `0487209b…`, not on the pending index tree, so I did not count it as evidence for this patch.
- [ ] I read the larger test files (`test_collect_ci_evidence.py`, `test_watch_azure_pipeline.py`, `test_trusted_guidance.py`) in targeted ranges, not end to end.

## Issues

### Issue 1: The manual readiness check for exempt PRs leaves out the helper's evidence-completeness gate
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/readiness-gates.md` (related: `.github/skills/synapseml-pr-loop/SKILL.md`)
- **Line(s)**: `readiness-gates.md` 125-137 (the field list is lines 134-137); `SKILL.md` 282-290
- **Description**:
  - `Get-PrReadiness.ps1` always requires `microsoft.SynapseML` (line 359). On an exempt PR with no Azure run, `remoteEngineeringGatesClear` and `completeness.complete` are therefore always false.
  - The new exempt-PR procedure replaces the helper's combined check (lines 559-568) with a hand-written list of fields to assess: other missing or unproven checks, `failedChecks`, `pendingChecks`, workflow results and approvals, review threads, `completeness.automatedReviewCoversHead`, `behindBy`, `mergeable` and `readiness.unassessedGates`.
  - That list omits the first condition of the combined check, `readiness.evidenceComplete`. This field requires three things to be empty:
    - `revisionErrors`: the head, base or state changed, or the target moved, during collection; or an expected SHA did not match.
    - `collectionErrors`.
    - `completeness.threadsWithUnreadComments`.
  - The list's "with complete policy evidence" condition covers only branch-policy collection, not workflow collection. For example, a workflow run with no clear PR association is recorded in `collectionErrors` and dropped from `workflows` (lines 415-421). The approval, failed and pending workflow lists can then be empty, so "workflow results and approvals" looks clear.
  - The exempt snapshot in `SKILL.md` §9 also omits `-ExpectedHeadSha` and `-ExpectedBaseSha`.
  - This is inconsistent with the PR's other manual route around the helper. A hand-posted requeue requires "both expected SHAs matching the cleared head/base pair and `readiness.evidenceComplete=true`" (`ci-evidence.md:114-118`).
- **Risk**: An agent following the exempt checklist could report every non-Azure gate as clear from a snapshot that:
  - was taken while the PR or target branch moved;
  - is not tied to the reviewed head and base; or
  - has truncated thread comments or dropped workflow runs.

  That is a weaker standard than the helper applies to non-exempt PRs. I rated this Low because other gates partly cover it: the head-SHA match, the workflow-run inspection, and the thread and suppressed-feedback checks.
- **Suggested Fix**: Documentation only; no runtime change is needed.
  - In the exempt bullet, require `readiness.evidenceComplete=true`, meaning empty `revisionErrors`, `collectionErrors` and `completeness.threadsWithUnreadComments`.
  - Alternatively, define the manual check as "every condition of `readiness.remoteEngineeringGatesClear` except the built-in `microsoft.SynapseML` requirement", so the list cannot drift from the script.
  - Optionally, also name `reviewDecision` and `suppressedReviewBodiesForHead` in that list.
  - In `SKILL.md` §9, pass `-ExpectedHeadSha <sha> -ExpectedBaseSha <sha>` for the exempt snapshot, as `ci-evidence.md:16-17` already directs for a cleared pair.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver adjudication

### Issue 1: duplicate, rebutted against the complete workflow

No source change is required. This repeats the
[attempt 70 architecture finding and its retained adjudication](https://github.com/microsoft/SynapseML/blob/0487209b131885d5b06b7036300acdc4ce37b09d/reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-2-claude-opus-5.5.md#driver-adjudication).
The driver rechecked the current instructions rather than relying only on the
prior disposition:

- `SKILL.md` step 1, lines 88-93, requires interpreting the separate evidence,
  engineering, human and merge states through `ci-evidence.md`, not the combined
  summary alone. This applies before either exempt or non-exempt final checks.
- `readiness-gates.md` begins by requiring current-head evidence for **every**
  applicable gate and requires matching local, remote and GitHub head SHAs.
  Its exempt paragraph says to assess **all other gates** and never rewrite the
  blocked summary as clear. It is not an instruction to replace the surrounding
  workflow with a new exhaustive field list.
- The mandatory linked snapshot contract rejects failed/malformed revision
  rechecks (`ci-evidence.md` lines 9-19), ambiguous workflow association
  (lines 21-35), and REST/comparison/recheck failures (lines 45-52). Its output
  table defines `readiness.evidenceComplete` as readable, untruncated evidence
  with stable revisions. The readiness guide links that same contract immediately
  after the exempt interpretation. No Azure exemption waives it.
- That contract already says to pass both expected SHAs **for a cleared pair**.
  An initial read-only discovery snapshot is distinct from reuse of an already
  cleared pair; omitting flags from the abbreviated discovery instruction is not
  permission to validate a different pair.
- Existing tests cover failed revision reads, moving or mismatched revisions,
  empty policy responses and incomplete workflow pagination. The helper is
  unchanged by this follow-up, and the current policy-source regressions also
  passed without changing the evidence-completeness logic.

Repeating the completeness field in the exempt bullet could improve local
discoverability, but no route in the complete workflow authorizes treating
stale, truncated or unassociated evidence as clear. This is a nonblocking
wording suggestion, not a newly demonstrated bypass. The original Low severity
and `ISSUES_FOUND` verdict remain intact; no counter reset or source change was
made, and the remaining lenses continue on the same frozen patch.

## Subsequent human-directed review change

The user then requested: "run light reviews and get this ready".
The in-progress round 3 was canceled and its partial transport retained locally;
it produced no completed review artifact. Round 4 did not run. Attempt 74 is
therefore incomplete, not a six-round signoff. Its four completed original
reviews and dispositions are preserved.

Further review uses the installed Low-tier harness under that explicit human
override. This changes review effort, not the frozen source, CI requirements,
trust/permission gates, or the obligation to address concrete findings.