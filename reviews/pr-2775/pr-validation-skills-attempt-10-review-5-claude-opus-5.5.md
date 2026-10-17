# Review Round 5 — Testing & Coverage

## Summary
- Round: 5
- Theme: Testing & Coverage
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 3
- Verdict: CHANGES REQUIRED

## Evidence Checklist
- [x] `tools/ci/tests/test_get_pr_readiness.py:195`: an Azure leg name can't stand in for the missing `microsoft.SynapseML` aggregate check.
- [x] `tools/ci/tests/test_get_pr_readiness.py:272`: a stale expected head/base, or the head/base moving during collection, invalidates the snapshot.
- [x] `tools/ci/tests/test_get_pr_readiness.py:333`: the unsafe `-RunPipeline` test matrix asserts that no pipeline trigger side effect happens.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:375-383`: legacy `$RequiredCheck` names use prefix matching; ruleset and classic contexts use exact matching (`-cnotin`).
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:34-35,67,84,87-93`: `GIT_*` variables are stripped; the file-mode check, `--others` (without `--exclude-standard`) and symlink guards are present.

## Issues

### Issue 1: Rejection of ignored files and symlinks is untested
- **Severity:** Medium
- **File/lines:** `tools/ci/tests/test_trusted_guidance.py:63-67`, which covers `verify_trusted_guidance.py:67,84-85,87-93`
- **Risk:** The only negative test plants an untracked file that isn't ignored. Every test still passes if someone adds the common `--exclude-standard` flag or removes the file-mode or `is_symlink` guards. Ignored `__pycache__/*.pyc` files or a committed symlink inside the guidance paths would then pass verification as trusted.
- **Minimal fix:**
  - Add `__pycache__/` to `.git/info/exclude`, plant a `.pyc` under `.github/skills`, and assert that verification rejects it.
  - Commit a `120000` (symlink) entry with `update-index --cacheinfo`, which works on every platform, and assert the "regular files" error.

### Issue 2: The test's Git fixture inherits the caller's `GIT_*` environment
- **Severity:** Medium
- **File/lines:** `tools/ci/tests/test_trusted_guidance.py:44-47` (used at :24, :25, :30, :42, :71, :83)
- **Risk:** In linked worktrees, which this workflow uses (`.worktrees/*`), Git passes absolute `GIT_DIR`/`GIT_INDEX_FILE` paths to hooks, and `-C` overrides neither. If the suite runs from a pre-commit or pre-push hook, the fixture:
  - writes `core.autocrlf` into the developer's real Git config,
  - stages the fixture `AGENTS.md` and skill files into the real index,
  - runs `update-ref HEAD`, which moves the developer's branch to a fixture commit with no parent.

  The production helper already strips these variables (`verify_trusted_guidance.py:34-35`).
- **Minimal fix:** Pass the same `GIT_*`-stripped `env` in the test helper.

### Issue 3: The "not prefix matches" test can't detect prefix matching
- **Severity:** Medium
- **File/lines:** `tools/ci/tests/test_get_pr_readiness.py:251-270`
- **Risk:** The status-check rollup contains only `microsoft.SynapseML`. If ruleset and classic contexts were routed through the `StartsWith` path (`Get-PrReadiness.ps1:375-381`), both would still be reported missing, so the test would pass. In that case a check named `Required check (leg)` would satisfy `Required check`. `unprovenRequiredChecks` (:384-388) only matches exact names, so the remote gates would report clear while GitHub still blocks the merge.
- **Minimal fix:** Add rollup entries `Required check (leg)` and `Classic check-extra` with a SUCCESS conclusion, and keep both `assertIn` assertions.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `dd627e6928a8fca3712f03b636772fd1a86133df66dad20fa84456f479878d81`
- Reviewed diff SHA-256: `e1ccbd8f666192d9a59f46862c4cf55988be271ccf0a9a3b894c64091916a980`
- Original artifact SHA-256: `1bb59df1358cf3fe793960769f3c8e5e05ce4078b06848f7403927a8f8b9243e`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
