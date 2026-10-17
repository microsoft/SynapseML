## Review Summary
- Round: 1
- Theme: Broad sweep
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 (max, complementary; primary gpt-6-astra)
- Issues Found: 1
- Verdict: ISSUES_FOUND

## Evidence Checklist
- [x] Read the whole frozen staged patch (17 files, +2097/−109). The worktree's `git diff --staged` matches the frozen diff byte for byte (same SHA-256), and there are no unstaged changes.
- [x] Traced the `--azure-timeline` branches of `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (lines 133–277) against the watcher contract in `.github/skills/synapseml-pr-loop/references/ci-triage.md` (lines 38–49) and its new "Targeted infrastructure retry" section (lines 172–194).
- [x] Checked that the `status`, `result`, `attentionRequired` and `agentLossDetected` fields the watcher reads match what `collect_build` and `summarize_timeline` produce in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 182–316).
- [x] Read the timeline tests in `tools/ci/tests/test_watch_azure_pipeline.py` (lines 488–591). The same-build retry test (line 542) only uses a GitHub `IN_PROGRESS` snapshot (line 544).
- [x] Confirmed the `pipeline.yaml` CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so CI runs the new tests.
- [x] Confirmed the new cross-reference anchors resolve in ci-delegation, loop-control, ci-triage and ci-evidence. The gauntlet text moved from `SKILL.md` to `references/review-gauntlet.md` with no content lost; only two lines were intentionally edited.

I did not run any repository code or tests in this review.

## Issues

### Issue 1: Watcher reports `failed` for a same-build retry that Azure is still running when the GitHub check is out of date
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Lines:** 231–277 (key lines: 235–238 and 261–277)
- **Description:** In `--azure-timeline` mode, the code handles only one GitHub/Azure disagreement: GitHub reports `SUCCESS` while Azure is unfinished or did not succeed (lines 261–271). Two other disagreements fall through to line 273 and return `outcome: failed` (exit 1) on the first poll:
  - The `microsoft.SynapseML` check for the pinned build is `COMPLETED`/`FAILURE`, while `collect_build` reports `status: "inProgress"` with no agent loss.
  - GitHub shows `FAILURE`, but Azure shows `completed`/`succeeded`.

  The new ci-triage "Targeted infrastructure retry" flow leads directly to these states. It says to wait for the build to fail, retry failed jobs on the same build ID with `retry=true`, then "resume the existing watcher with the **original kickoff**" (ci-triage.md:192). The watcher queries GitHub as soon as it starts (line 133). At that point the GitHub check still shows the first attempt's `FAILURE` until Azure sends GitHub an update. That update is asynchronous at best, and some external reports say the check only changes when the retried run finishes.

  This contradicts the documented contract. ci-triage.md:45 says "a GitHub/Azure disagreement returns `attention_required`", and ci-triage.md:49 says the watcher keeps observing "that build or its retry". The only retry test covers a GitHub `IN_PROGRESS` snapshot, so this path is untested.
- **Risk:** The agent has already used its single authorized retry when the watcher returns `failed` for a build that is still running, or that passed on retry. The agent may then report the retry as failed or CI as blocked, or record a false failure in the checkpoint. Relaunching the watcher hits the same immediate exit, so it can't wait out the retry at the documented 600-second cadence. That pushes agents toward the ad hoc fast polling the guidance forbids.
- **SuggestedFix:** In timeline mode:
  - If the GitHub check for `args.build_id` is finished but `azure["status"] != "completed"`, keep sleeping `POLL_SECONDS` instead of returning, because Azure's state is the reliable one while a same-build retry runs.
  - If GitHub shows anything other than success but Azure is `completed`/`succeeded`, return `attention_required`, or `azure_completed` with a stale-check marker, instead of `failed`.
  - Add tests for GitHub `COMPLETED/FAILURE` combined with Azure `inProgress`, and combined with Azure `completed/succeeded`.

## Driver provenance and resolution

- Direct read-only task transport, requested `claude-opus-5.5` at maximum
  reasoning; no claim of an installed Execute receipt or independently observed
  CLI model identity. The same Medium/sequential pre-PR fallback decision
  recorded with attempt 2 applies.
- Source base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed diff SHA-256: `59082d54b7791ef2373480a873086ce5de6684e29e60b5c5e95d5f14cba9989e`.
- Pass stopped after round 1; no six-round sign-off is claimed.
- Fix: a provenance-verified active Azure build is observed at the normal cadence
  regardless of a stale terminal GitHub check. After Azure completes, disagreement
  in either direction is reported as `attention_required`, never a false retry
  failure or false success. Added a focused retry-transition regression.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `b6359aac92b8a71745a2758f0cd6870fb19feb1c7c03a4a3cc4e373ee5303d14`
- Reviewed diff SHA-256: `59082d54b7791ef2373480a873086ce5de6684e29e60b5c5e95d5f14cba9989e`
- Original artifact SHA-256: `2a84956e9c92e8f2ef2eade57f06aee9ec0f54ff00266613423a11dd72e54796`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
