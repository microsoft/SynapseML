# Review Round 2 — Architecture & Patterns

## Summary
- Round: 2
- Theme: Architecture & Patterns
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

No significant issues found in the reviewed changes.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102`: The parameter guards hold. `-RunPipeline` requires both expected SHAs, rejects `-WaitForReview`, and accepts only one PR. At `:496-513`, it throws on a stale or closed PR and triggers a run only when the aggregate check is missing. At `:471`, `-RequestReview` only runs once `evidenceComplete` is true.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:293-305, 375-388, 434-461, 540-572`: The compare step uses the live branch-tip SHA. The aggregate check and the required-rule checks match names exactly, and a skipped or neutral result that can't be proven is not treated as passing. Revisions are re-checked before readiness is built, and `complete` is set to `remoteGatesClear`.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:15-23`: The watcher imports the shared helpers from `collect_ci_evidence` and doesn't copy the API layer. The state transitions at `:206-217`, `:231-233`, `:249-256` and `:261-267` match the documented states. Arguments are validated at `:316-325`, and exit code 4 is at `:374-375`.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:20, 52, 111, 183, 516-527`: These lines cover the fixed project `API_ROOT`, the `NoRedirect` handler and sanitized errors (including `HTTPException`). Also checked: timeline leaf gating, the `collectionStable` re-check, and `mergeReady`, which is always `False`.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:35, 69, 84, 89-93, 96-98`: The script removes `GIT_*` environment variables and rejects symlinks and submodules. It also rejects untracked or ignored files (`ls-files --others` with no exclude) and paths that pass through a symlink. Line endings are normalized from CRLF before comparison.
- [x] The doc anchors resolve:
  - `references/loop-control.md:53` and `:120`
  - `references/ci-triage.md:6` and `:172`
  - `references/ci-evidence.md:7`
  - `references/ci-evidence.md:40-42`, `references/readiness-gates.md:115-122` and `SKILL.md:268` match the script's `-RunPipeline` contract (both expected SHAs required).
- [x] `pipeline.yaml:112-126`: The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04, where pwsh and git are available. The new `tools/ci/tests` will run in CI without changing the pipeline.
- [x] All 17 manifest blobs match the worktree files (checked with `git hash-object` using filters). The diff's SHA256 matches the manifest.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `dd627e6928a8fca3712f03b636772fd1a86133df66dad20fa84456f479878d81`
- Reviewed diff SHA-256: `e1ccbd8f666192d9a59f46862c4cf55988be271ccf0a9a3b894c64091916a980`
- Original artifact SHA-256: `30bcbad26ebdb6bc483f820a1e46c8b352bb233b7cfddaa4c5a4c86d8cc31d4c`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
