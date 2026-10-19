---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 6
timestamp: "2026-10-19T18:31:19Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "40ab5dfa8f091b30ae842af524ec6523f240e2a66bbd50b1d631d1084e495d7d"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Deadline handling and its docs agree.**
  - In `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `DeadlineReader`/`DeadlineHTTPSHandler` (~127-166) apply the remaining budget before every raw receive. They keep the default verified HTTPS context and `NoRedirect`, and they close the original stream on cleanup.
  - Each receive costs only one clock read and one `settimeout`, with no timer thread or worker.
  - When an HTTP failure occurs, `AzureClient.get` (~180-245) rechecks the original deadline. A direct `TimeoutError` or a `URLError` wrapping one, with budget remaining, becomes a sanitized query-timeout message.
  - `main` (~788-823) rechecks `remaining_timeout()` after the final collection.
  - This matches `references/ci-evidence.md` lines 192-203. Per-read enforcement is scoped to "response read"; connect and TLS overruns still end as unresolved evidence through the final recheck.
- [x] **Regression coverage in `tools/ci/tests/test_collect_ci_evidence.py`:**
  - The streaming test (~1087-1168) uses a real urllib opener, `HTTPConnection` and `HTTPResponse` over a socket pair. Only the connection factory, proxy lookup and clock are mocked. It asserts normal completion, expiry before the body completes, `reader.fileno() == -1`, and a bounded sender join.
  - The four-row diagnostic test (~1251-1287) covers both failure shapes before and at expiry, plus `response.close` on read failures.
  - The test-method counts (45 collector + 42 watcher = 87, with exactly two `skipUnless(win32)` at lines 906 and 934) are consistent with the supplied evidence: 87 tests on Windows and 85 plus 2 skips on Linux.
- [x] **Watcher matches `references/ci-triage.md` and pr-loop `SKILL.md`:**
  - In `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`: `POLL_SECONDS = 600` and `MAX_TIMEOUT_MINUTES = 120` (lines 29-30), with the deadline counted from kickoff.
  - `--timeout-minutes` can only shorten that limit, `--repo` is restricted to `microsoft/SynapseML`, and `--base-sha`/`--merge-sha` require `--azure-timeline` (295-337).
  - Observability: startup and final JSON events, one `agent_loss` record per (identifier, attempt), and the documented exit map 0/1/2/3/4/124/130.
  - The 6599 vs. 6600 confirmation-poll boundary is tested (`test_watch_azure_pipeline.py` ~773).
- [x] **Readiness field names cited by the docs exist with the documented shapes.**
  - In `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`: `ruleRequiredChecks` holds objects with `.context` (356-358, 668), and `classicProtection` holds the raw protection object with `required_status_checks` (361-362, 667).
  - Also present: `readiness.unassessedGates` (692), `completeness.automatedReviewCoversHead` (708), and the separate `engineering`, `humanApproval` and `merge` states.
  - These match `references/readiness-gates.md` 125-141 and `references/ci-evidence.md`. The suite has 42 readiness test methods (308-1315), matching the supplied evidence.
- [x] **Verifier matches `references/trusted-guidance.md`.**
  - In `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, `GUIDANCE_PATHS` (15-23) includes `CODEOWNERS`, `pipeline.yaml` and `templates`.
  - It strips `GIT_*`, uses `--no-replace-objects` and `core.fsmonitor=false`, and sets a 30s timeout on each git call.
  - It rejects symlinks and non-regular files (including FIFOs) before reading, and tolerates CRLF only for non-binary blobs.
  - Spawning one `git cat-file` per file across the roughly 50-file guidance set is an acceptable cost for a bootstrap step.
- [x] **CLI and link accuracy.**
  - Every documented flag exists in argparse: collector `parse_args` (744-785) and watcher (295-337). That covers `--expected-suite`, `--expected-test`, `--allow-skipped`, `--allow-skipped-job`, `--azure-timeline`, `--timeout-minutes`, `--kickoff-at`, `--checkout` and `--expected-sha`.
  - All cited anchors resolve to existing headings, including `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`, `#github-snapshot`, `#user-value-and-scope` and `#waiting-for-azure-pipelines`.
- [x] **The Azure scope exception is worded consistently** across pr-loop `SKILL.md` §7/§9, `references/readiness-gates.md`, `references/loop-control.md`, external-contributor `SKILL.md`, and `references/ci-delegation.md` (25-27, 66-101). Under that wording this PR's `tools/ci/tests` changes correctly keep it non-exempt.
- [x] **Credentials stay out of output.** The Authorization header is built in memory once (`****** and diagnostics exclude response and error text.
- [x] **The helper suites already run in CI.** `pipeline.yaml` job `CIHelpers` (112-126) runs `python3 -m pytest tools/ci/tests/ -q`. Readiness tests skip only when `pwsh` is absent and `TF_BUILD` is unset (`test_get_pr_readiness.py` 249-251).
- [ ] **Nothing was executed.** I did not reproduce the tests, Black, whitespace checks, manifest or patch roundtrip, or hashes. The supplied pass counts were cross-checked only against static test-method and skip counts.
- [ ] **Frozen tree not confirmed.** I could not verify that the checked-out files I read equal frozen index tree `5950b3b821bc3ab7b6ba16189543965e5a56f5cd`, because git commands were not available to me.
- [ ] **Truncated diff.** The supplied diff was cut off at pr-loop `SKILL.md` §8, so the later changes were reviewed as current file contents, not as hunks with their removed lines.
- [ ] **Some test bodies only listed by name.** These are `test_get_pr_readiness.py` after line 330, most of `test_trusted_guidance.py`, and parts of `test_watch_azure_pipeline.py`.
- [ ] **Excluded by the independence rules.** I did not read `reviews/`, prior or sibling artifacts, checkpoints, or private probe logs.
- [ ] **No CI result for this head.** No hosted CI result qualifies this pending head. Current-head Azure validation is a separate gate outside this read-only review.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and current task catalog",
    "stageBehavior": "pinned target PR-loop sequential six-lens policy",
    "bounds": "explicit user direction to make decisions and continue after the reported blocker; one concrete remedial cycle, no counter reset"
  },
  "changeSize": "17 source, test, and documentation paths; 5853 added and 209 removed lines",
  "cyclomaticComplexity": "Moderate coupled provenance, pagination, retry, readiness, deadline and filesystem interactions",
  "rationale": "Review the full validation-tooling patch, including the bounded transport fix and CI-applicability guidance; no product or CI configuration changes",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833; user's explicit 2026-10-19 autonomous continuation",
  "reviewAttempt": 70,
  "completedPriorPrecommitPasses": 69,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [6, 3, 5, 1, 2, 4],
  "verifiedTaskId": null
}
