## Review Summary
- **Round:** 1
- **Theme:** Broad sweep
- **Mode:** sequential independent direct-task fallback
- **Requested Model:** claude-opus-5.5
- **Issues Found:** 2
- **Verdict:** ISSUES_FOUND

## Evidence Checklist
- [x] **Diff integrity:** The supplied `review.diff` has the expected SHA256 (`4768fd6e…df86d`). It has 2467 lines across 17 files and matches `git diff --staged --stat` in the review worktree (base `0543bf7e`). I read the whole diff.
- [x] **Gauntlet move and links:** I compared the moved gauntlet text with its original location. Only the relative link paths changed. The new relative link targets and anchors exist.
- [x] **Code trace:** I followed the revision and evidence gates in `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (L264–L282, L420–L440, L481–L482, L511). I also traced how the merge and agent-loss checks flow through `scripts/watch_azure_pipeline.py` (L142–L177) and `scripts/collect_ci_evidence.py` (L141–L152, L212–L213, L294).
- [x] **Live GitHub data (read-only API, 2026-10-16):**
  - The `master` tip is `0543bf7e`. All four open PRs (#2753, #2698, #2628, #2626) have a `baseRefOid` that differs from that tip.
  - For #2753, `baseRefOid` is `812cfea1`. GitHub's test merge `c9905530` has parents `[0543bf7e, dd66cfcf]`.
  - Comparing `812cfea1...head` gives `ahead` with `behind_by` 0. Comparing `master...head` (what the code did before this patch) gives `diverged` with `behind_by` 14.
- [x] **Live `master` rules:** Required checks are `license/cla`, `WIP` and `microsoft.SynapseML`. `strict_required_status_checks_policy` is false, and the `pull_request` rule needs 1 approval. Classic `protection.enabled` is false. I checked the script's rule parsing against this shape and found no defect there.

## Issues

### Issue 1: `baseRefOid` is used as the live target identity, which blocks or misreports almost every open PR
- **Severity:** High
- **File:** `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`; also `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Lines:**
  - `Get-PrReadiness.ps1`: L282, L429, L431, L440, L481–L482, L511
  - `watch_azure_pipeline.py`: L142–L150 and L154–L162
  - `collect_ci_evidence.py`: L149–L152
  - `ci-evidence.md`: L62–L64
- **Description:**
  - GitHub's `baseRefOid` (REST `base.sha`) records the target SHA from the last head push. It does not follow later pushes to the target branch.
  - GitHub's test merge, which is what Azure PR CI builds, is based on the live tip. For #2753 the merge's first parent is `0543bf7e`, but `baseRefOid` stays `812cfea1`.
  - The patch treats `baseRefOid` as the current target in several places:
    - **L282:** Comparing against `baseRefOid` instead of the target branch makes #2753 report `ahead`/`behind_by 0` when it is actually `diverged`/14 behind. The `behind_by` gate at L440 and the `behindBy`/`targetStatus` outputs are therefore wrong.
    - **L429:** `$branch.commit.sha -ne $view.baseRefOid` raises "Target branch SHA differs from the PR snapshot." This fires for every PR whose target moved since its last push, which is the normal state, not a race during collection. It sets `evidenceComplete` to false for a misleading "stale evidence" reason. All four open PRs currently hit this, not just historical ones.
    - **L431 and L481–L482:** For a PR that is behind, no `-ExpectedBaseSha` value can pass both L429 and L431. As a result, `-RunPipeline` always throws "CI trigger blocked by incomplete or stale evidence." Validation-only external PRs (requirement 5, no maintainer edits) can't be triggered unless the contributor rebases, even though Azure would test against the live tip anyway.
    - **Watcher, with `--azure-timeline`:** `--base-sha` must equal the snapshot `baseRefOid` (L145), or the watcher returns `superseded` immediately. It must also equal the merge commit's first parent (`verify_merge`, `collect_ci_evidence.py` L150), or the watcher fails with a `MonitorError`. For a PR that is behind, these two values differ, so no input can monitor the build in attempt-aware mode.
    - **Collector docs:** `ci-evidence.md` says to "match that pair to the live readiness snapshot". The readiness `baseSha` (L511) is `baseRefOid`, which never matches a correctly verified merge parent for such PRs.
- **Risk:**
  - The main CI trigger path and attempt-aware monitoring can't be used for most open PRs.
  - Operators are pushed toward rebases or workarounds that aren't needed.
  - `behindBy` understates how far a PR is behind.
- **Suggested fix:**
  - Use the live target tip (re-read at the end of collection) or the test merge's first parent as the target identity.
  - Compare `<tip>...head` so `behind_by` is accurate.
  - Report `baseRefOid` only for information, and treat "behind" as an integration gate rather than an evidence error.
  - Validate `ExpectedBaseSha` against the tip or merge parent, and have readiness output report that SHA.
  - In the watcher, drop the `baseRefOid` equality check and rely on `verify_merge` parents (or the live tip) for target identity.

### Issue 2: The agent-loss diagnostic always ends the watcher, so the documented "keep waiting" step can't be followed
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`; related: `scripts/collect_ci_evidence.py`, `references/ci-triage.md`, `references/loop-control.md`
- **Lines:**
  - `watch_azure_pipeline.py`: L171–L177
  - `collect_ci_evidence.py`: L212–L213, L294
  - `ci-triage.md`: L172–L174
  - `loop-control.md`: L232
- **Description:**
  - `attentionRequired` is `any(job["agentLossDetected"] ...)` over the latest-attempt timeline, whether or not the build has finished.
  - `monitor()` returns `attention_required` as soon as that flag is set. It does this before handling the check state and before any sleep.
  - The agent-loss issue stays in the latest attempt until a retry, so every poll exits this way. That includes polls after the build completes, which means `azure_completed`, `failed` and `success` can never be reached.
  - The docs require the opposite. `ci-triage.md` says: "Wait for the build to become terminal before retrying. Keep the same attached watcher and 600-second cadence while waiting." The `loop-control.md` row says: "wait at the normal cadence, no premature retry."
  - Relaunching the same command exits immediately with code 4, so waiting turns into rapid relaunches (the "faster polling" the docs forbid).
  - Dropping `--azure-timeline` loses Azure-side completion detection. When the GitHub check lags (a documented agent-loss symptom), the agent can burn through the original 120-minute deadline that the same-build retry has to stay within.
- **Risk:**
  - Agents can't follow the documented agent-loss procedure with the watcher as written.
  - The likely outcomes are rapid relaunch loops, retries before the build is terminal, or using up the fixed kickoff deadline.
- **Suggested fix:**
  - Make the diagnostic non-terminal. Emit it once, keep polling at `POLL_SECONDS` until the build or check finishes, and return the terminal outcome with `attentionRequired: true`.
  - Alternatively, add an acknowledgement option keyed to the attempt, so a relaunch with the same kickoff deadline resumes waiting instead of exiting.
  - Document the relaunch behavior in `ci-triage.md`.

## Driver provenance and resolutions

- Transport: independent read-only `code-review` task, using the repository's
  pre-PR session-draft fallback. The installed `review-code -Execute` path was
  not used because it rejects session-only output directories.
- Requested reviewer: `claude-opus-5.5`, maximum reasoning. This records the
  explicit harness request, not a separately observed CLI runtime identity.
- Source base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed diff SHA-256: `4768fd6e79e5cc1776c4a5caa95d7b3225ff8be90cd85cd116c0edd0239df86d`.
- This pass was stopped after round 1; it is not a completed six-round sign-off.
- Issue 1 fixed: readiness pins and rechecks the live target tip, reports the PR
  metadata SHA separately, and allows evidence collection/authorized merge CI
  when integration is behind. Watcher provenance uses the queued merge parents.
- Issue 2 fixed: hosted-agent loss emits one diagnostic per affected job attempt
  and continues waiting at the original cadence and deadline.
- Verification: focused helper tests cover stale PR base metadata, actual target
  movement during collection, behind-target validation-only triggering, and a
  persistent agent-loss issue through terminal Azure state. Read-only live
  evidence for microsoft/SynapseML#2753 correctly reports 14 commits behind with
  complete evidence rather than a false stale-snapshot error.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `952302faadae81bef4443a560a8957d06aad85f0e910af364302f5cbde247d97`
- Reviewed diff SHA-256: `4768fd6e79e5cc1776c4a5caa95d7b3225ff8be90cd85cd116c0edd0239df86d`
- Original artifact SHA-256: `7ba26289aa88ed757442b9056bdb42fbf66fdd5682d1042f9d23bcbd1b6a9557`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
