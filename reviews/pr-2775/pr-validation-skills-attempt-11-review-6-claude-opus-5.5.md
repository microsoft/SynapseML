# Review Round 6 — Polish & Hardening

## Summary
- Round: 6
- Theme: Polish & Hardening
- Mode: manual prompt fallback (not Execute/observed CLI receipt)
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/references/ci-evidence.md:30-43` matches `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102,448-449,496-514`. The readiness fields line up with the documented table. `-RunPipeline` behaves as documented:
  - It needs exactly one PR and both SHAs.
  - It refuses `-WaitForReview`.
  - It stops when evidence is incomplete or stale.
  - It only comments `/azp run` when the `microsoft.SynapseML` aggregate is missing.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:151-273` matches `.github/skills/synapseml-pr-loop/references/ci-triage.md:38-47`:
  - An Azure build that's still running takes priority over a stale GitHub result.
  - `agent_loss` is printed once per (job, attempt).
  - `azure_completed` is returned when the GitHub aggregate is missing or pending.
  - `attention_required` is returned when GitHub and Azure disagree.
  - Both outcomes exit nonzero, as the doc says.
- [x] `pipeline.yaml:112-126`: the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, so CI picks up all four new or changed `tools/ci/tests/test_*.py` files without a pipeline change.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:101` sends a real bearer `Authorization` header. The `******` in `review.diff` is only display masking. Every new link anchor resolves:
  - `ci-evidence.md#github-snapshot`
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
  - `loop-control.md#evidence-invalidation` and `#state-and-cost-discipline`
  - `ci-delegation.md#evidence-required-before-triggering`
- [x] `.github/skills/synapseml-pr-loop/references/review-gauntlet.md:1-69` is the text removed from `SKILL.md`, with only the links rewritten to be relative.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `4af6a3a53f6b1b6ba7a52f18e640eb6ef8f98717bee36ac8af18d70fea1852b5`
- Reviewed diff SHA-256: `bffec5c72c2886366b0342d7b080da336e45895af8e222e4bc90b7ddf0d3aeb6`
- Original artifact SHA-256: `ef1345f166acc45f2c81e2da40d9b11789b09bf218de249cf2d5240a8cfe57ce`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
