---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 3
timestamp: "2026-10-19T18:56:11Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "97cff0ba5036fb8dca26b396642ab5c920acc5faf11d84777e0f0f29d8748c1e"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Deadline on every receive.** I traced this through `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 126-166.
  - Before every raw receive, `DeadlineReader.readinto` sets the socket timeout to the remaining budget. That covers the status line, headers, chunk framing and body reads that `HTTPResponse` makes through `BufferedReader`.
  - When the deadline expires, `EvidenceError` is raised. It subclasses plain `Exception` (line 36), so the transport `except` at line 229 does not catch it.
  - It therefore reaches the standard library's `getresponse`/`do_open` cleanup, or the `with` block's response close.
  - `DeadlineReader.close` closes the detached `SocketIO`, which releases the socket after urllib marks it closed.
  - The proxy-tunnel response and the default zero debug level both pass only keyword arguments to the factory, so `response(sock, **options)` stays valid.
- [x] **Error classification in `AzureClient`** (lines 180-242):
  - A budget of zero or less raises before the token fetch, before the request opens, and before each receive.
  - `HTTPError` is caught before its `URLError` superclass. A timeline 404 or empty body returns `None`.
  - Transport, protocol and JSON failures recheck the deadline first. They then report either "timed out" (`TimeoutError` or `URLError(TimeoutError)`) or the generic message.
  - No message includes response, CLI or exception text.
  - `command_json` (lines 75-112) catches `UnicodeError` before `ValueError` and never echoes stdout or stderr.
  - `run_windows_batch` (lines 40-72) kills the process tree, falls back to a forced kill, and fails closed, reporting only the PID.
- [x] **Regression tests** in `tools/ci/tests/test_collect_ci_evidence.py`:
  - The streaming test (about lines 1087-1168) uses a real opener, `HTTPConnection` and `HTTPResponse` over a socket pair. It has a normal row and a row where the deadline expires before the body completes. It checks that the reader's file descriptor is closed and that the sender thread's join is time-limited.
  - The diagnostic test (about lines 1251-1287) has direct `TimeoutError` and `URLError(TimeoutError)` rows, each before and at expiry. It checks the exact sanitized messages and cleanup.
  - `references/ci-evidence.md` lines 192-199 claims enforcement only on response reads, which matches the code.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, lines 98-391):
  - It polls every 600 seconds with no busy loop.
  - The deadline is anchored to the run's kickoff and cannot be extended by restarting the watcher.
  - It allows at most one extra GitHub wait, and only when more than 600 seconds remain.
  - Replaced, superseded, timeout and error outcomes have separate exit codes.
  - Each poll creates a new `AzureClient` with its own deadline budget.
- [x] **Readiness helper** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - Rule and classic-protection collection (lines 325-369) records malformed or incomplete data in `collectionErrors`.
  - `humanApproval` is `unknown` when rule collection is incomplete (line 569).
  - It outputs `ruleRequiredChecks` and `classicProtection` (lines 666-668). These match the exempt-diff guidance in `references/readiness-gates.md` lines 125-137, which also treats incomplete policy evidence as unknown.
  - I also checked the `-RunPipeline` exclusivity and SHA guards, the read-only `-WaitForReview` loop, and that `ConvertTo-Json -Depth 12` is deep enough for the output.
- [x] **Scope-exception consistency:**
  - These documents agree that any `tools/` change disqualifies the exception:
    - `.github/skills/synapseml-pr-loop/SKILL.md` §7 (lines 205-218) and §9 (lines 277-287)
    - the `references/loop-control.md` acceptance rows
    - `.github/skills/synapseml-external-contributor-review/SKILL.md`
    - `references/ci-delegation.md`
  - Every changed cross-file anchor resolves.
  - The CIHelpers job in `pipeline.yaml` (lines 112-126) runs `tools/ci/tests`. This PR changes four files there, so it is correctly not exempt.
- [x] **Readiness test harness and executable lookup:**
  - `tools/ci/tests/test_get_pr_readiness.py` (lines 249-272) skips only when `pwsh` is missing outside Azure; `TF_BUILD` forces it to run in CI.
  - It limits each harness run to 30 seconds and asserts that no CI trigger or review request happens unless the test expects one.
  - In `verify_trusted_guidance.py`, `trusted_executable` excludes the working directory and everything under it.
- [x] **Concerns considered and not raised:**
  - Connecting and the TLS handshake use the budget left when the request opens. Any overrun is bounded by the 60-second per-request cap, a later expiry is still reported, and the docs only claim per-read enforcement.
  - The streaming test has a timing window of about 200 ms. It would flake only after a long scheduler stall.
  - `HTTPError` response bodies are freed by Python's reference counting rather than an explicit close. The CLI is short-lived and each poll uses a new client.
  - A transient Azure or GitHub error ends the watcher with `error`. The docs say to resume with the original kickoff time.
- [ ] **No tests or commands run:** this is a read-only review. The Windows/Linux pytest and Black results in the request are the caller's evidence and were not reproduced.
- [ ] **Frozen patch not verified:** the diff display was cut off inside `.github/skills/synapseml-pr-loop/SKILL.md`. I reviewed the rest from the current source files by exact path, but could not check them against the stated index-tree and manifest hashes.
- [ ] **Authorization header not verified:** its value at `collect_ci_evidence.py` line 214 is masked in the review display. Redirects are refused, so credentials are not forwarded.
- [ ] **Real TLS not covered by committed tests:** they cover per-read deadlines only over a plain socket pair. The real-TLS probe described in the request is the caller's evidence and cannot be checked independently.