## Review Summary
- Round: 2
- Theme: Architecture & Patterns
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 0
- Verdict: CLEAN
## Evidence Checklist
- [x] Patch integrity: `review.diff` (135,710 bytes, 2,882 lines) matched its recorded SHA-256 and was read in full. Worktree HEAD is base `0543bf7e`, and all 17 staged blobs match `manifest.json`.
- [x] Gauntlet extraction: the 67 lines removed from `synapseml-pr-loop/SKILL.md` are identical to the 67 lines in `references/review-gauntlet.md`. The only difference is the link `references/loop-control.md` → `loop-control.md`, which is the correct path now that both files are in `references/`. Nothing was changed or lost in the move.
- [x] Cross-references: every new anchor resolves:
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
  - `loop-control.md#state-and-cost-discipline` and `#evidence-invalidation`
  - `ci-evidence.md#github-snapshot`
  - `ci-delegation.md#evidence-required-before-triggering`
- [x] Readiness layering: `Get-PrReadiness.ps1` L485-503 and the readiness object at L537-548 keep evidence, engineering, human, and merge readiness separate, matching the `ci-evidence.md` table. `completeness.complete` stays as an alias of `remoteGatesClear`, so existing consumers keep working. `-RunPipeline` requires exactly one PR and both SHAs, and rejects `-WaitForReview`. It only triggers when the exact aggregate check is absent; otherwise it fails closed with a documented error.
- [x] Branch-protection source: a live read-only `gh api` check shows `master` and `spark4.0` are protected through rulesets, with classic `protection.enabled=false`. That means the script never needs the admin-only `/branches/{b}/protection` endpoint. The `master` rules map onto the script's gates: a pull request with 1 approval, required checks `license/cla`, `WIP`, and `microsoft.SynapseML`, and `copilot_code_review`. The concern about needing admin permission is rejected.
- [x] Watcher/collector boundary: timeline mode in `watch_azure_pipeline.py` (L151-191, L206-217) reuses the existing poll interval and deadline and adds no retries. The watcher imports `collect_ci_evidence` at the top with `sys.dont_write_bytecode=True`, and a test asserts that no `__pycache__` is created. `collect_build` and `collect_tests` in `collect_ci_evidence.py`:
  - only read data
  - sanitize their error output
  - match Job attempts
  - never let skipped tests count in place of expected tests
- [x] CI wiring: the CIHelpers job in `pipeline.yaml` (ubuntu-22.04, pwsh available) runs `python3 -m pytest tools/ci/tests/ -q`. The new tests, including `test_watch_azure_pipeline.py`, therefore run in CI without changing the CI definition.
- [x] Scope and verifier: the verifier checks HEAD, the index, untracked and ignored files, symlinks, and CRLF line endings. Both skills document the "no maintainer change" outcome. No product code or CI definitions are modified.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `349ad3ee8cf47b535b84bb506de98e758bac3bdcc344d787fbbee91d8cb2eb4e`
- Reviewed diff SHA-256: `dc44095292caf8ac982bfcb6651df03af93c998f90367cfedbafe87330ff2866`
- Original artifact SHA-256: `d95d28a6a90b4f745b01209f1a1145c70c92207b9399fad8fdbf07ee52a49fbc`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
