# Review Round 2 — Architecture & Patterns

## Summary
- Round: 2
- Theme: Architecture & Patterns
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

I read all 17 files in `review.diff`, and its size and SHA256 match the manifest. This was a static review: I ran no tests or scripts and only computed a read-only hash.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:15-23`: the watcher reuses the collector's `AzureClient`, `collect_build`, `verify_merge` and `github_json` rather than copying them. It also sets `sys.dont_write_bytecode` before that import. At `:371-372`, both `attention_required` and `azure_completed` exit with code 4, which matches the "both exit nonzero" rule in `references/ci-triage.md`.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - **Guards (`:96-102`):** expected SHAs require a single PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview`. This matches `.github/skills/synapseml-pr-loop/SKILL.md:268-278` and `references/ci-evidence.md:40-42`.
  - **Trigger (`:496-505`):** it posts only when evidence is complete, the PR is OPEN, and the exact aggregate check is missing.
  - **`complete` field (`:564-571`):** it is an alias for `remoteGatesClear`, which matches `references/readiness-gates.md:115-119`.
- [x] The new section links point to headings that exist:
  - `.github/skills/synapseml-pr-loop/references/loop-control.md:53,120`
  - `references/ci-triage.md:6,172`
  - `references/ci-evidence.md:7`
  - `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md:25`
- [x] `.github/skills/synapseml-pr-loop/references/review-gauntlet.md` is the old SKILL step 8 moved word for word; only its relative link changed. `.github/skills/synapseml-pr-loop/SKILL.md:256` links to it.
- [x] `pipeline.yaml:113-125`: the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so it picks up the four new tests with no pipeline change. The readiness test is skipped when `pwsh` isn't installed.
- [x] The documented CLI flags match the scripts' argparse definitions. `python -B` is used every time, at `references/ci-triage.md:24`, `references/ci-evidence.md:52` and `references/trusted-guidance.md:18`.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `1d7987ebe5de682ada6cfd4c0f66db85d028ebd8e1418f424f0f9b69d54d6636`
- Reviewed diff SHA-256: `df7ac7c309f23bf644e55765296cb4a92594b70392e178c7040cf8616bf65733`
- Original artifact SHA-256: `1b1c3eaafd62993f62432e15b3cb5082dea95e7bbb54596e8895343846db15bd`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
