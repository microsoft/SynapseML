# Review Round 6 — Polish & Hardening

## Summary
- **Round:** 6
- **Theme:** Polish & Hardening (performance/N+1, logging and observability, doc accuracy and naming, unused code, compatibility)
- **Mode:** manual prompt fallback. This was not an Execute run, and I did not observe a CLI receipt.
- **Requested model:** claude-opus-5.5 (max)
- **Issues:** 0
- **Verdict:** CLEAN

No significant issues found in the reviewed changes. I read all 17 files in `review.diff` (3065 lines, SHA256 `E1CCBD8F…A980`).

These looked possible but don't need changes:
- **Admin-only classic `/protection`:** it fails closed, and a test covers it.
- **One `git cat-file` per file in the verifier:** about 35 files, so the cost is negligible.
- **GitHub API calls on each `-WaitForReview` poll:** fine at the 60s default.
- **Out-of-date "by prefix" comment:** comment only.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102`: Expected SHAs only apply to a single PR. `-RunPipeline` needs both head and base SHAs and can't be combined with `-WaitForReview`, which matches SKILL.md.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:496-513`: `/azp run` is blocked when evidence is incomplete or the PR isn't OPEN. It is posted at most once per invocation (`pipelineRunOutcome`), and the script warns if the comment fails.
- [x] `.github/skills/synapseml-pr-loop/references/ci-evidence.md:32-36`: The readiness table matches the object written at `Get-PrReadiness.ps1:548-559` (`blocked`, `requires_manual_evidence`, `requires_engineering_evidence`).
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:206-266`: If Azure has completed but the GitHub check is missing, stale, or pending, the watcher returns `azure_completed`. If GitHub and Azure disagree, it returns `attention_required`. Both exit with code 4 (`:374-375`), as the docs say.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:171-191`: The watcher reports each `agent_loss` once per (job, attempt), so repeated polls don't flood the output.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:59-139`:
  - The `NoRedirect` opener (`:65`) stops the bearer token from being sent on to a redirect target.
  - HTTP errors are reduced to the status code (`:107-114`).
  - `$skip/$top` paging rejects repeated or missing IDs (`:119-139`).
  - The `Authorization` value at `:101` shows as redacted in the tool view only; grep confirms it is present in the file.
- [x] `.github/skills/synapseml-external-contributor-review/SKILL.md:89-94`: The "validated without maintainer changes" outcome is consistent with validation-only mode skipping the commit and gauntlet stages.
- [x] `pipeline.yaml:112-126`: The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu, where pwsh is installed, so CI runs the new tests.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `dd627e6928a8fca3712f03b636772fd1a86133df66dad20fa84456f479878d81`
- Reviewed diff SHA-256: `e1ccbd8f666192d9a59f46862c4cf55988be271ccf0a9a3b894c64091916a980`
- Original artifact SHA-256: `627eabc6b207aafe32e2c21637688dd3a86f00dcbf2d7b95255dcdc6b78b4870`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
