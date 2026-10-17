# Review Round 4 — Detailed Correctness

## Summary
- Round: 4
- Theme: Detailed Correctness
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:293-306`: The script reads the PR head and the current base-branch tip, then compares them by SHA instead of trusting a PR base OID that may be out of date. At `:434-446`, it checks the revisions again after collecting evidence. At `:448-461`, evidence, remote gates and human approval stay separate. At `:569-570`, `complete` is only an alias and does not count as approval.
- [x] `Get-PrReadiness.ps1:234-262,329-357`: REST calls use `gh --paginate` and record collection errors. Workflow runs are checked against `total_count`, and only the newest run per workflow and event is kept. At `:378-388`, the exact Azure aggregate check must match, and any required check it can't prove leaves the evidence incomplete.
- [x] `Get-PrReadiness.ps1:496-514`: `-RunPipeline` throws unless the evidence is complete and the PR is OPEN. It triggers a run only when the pinned `microsoft.SynapseML` aggregate check is missing, and it does not wait for the run.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:151-191`: Merge parents are verified once, and the build is collected again on every poll. Agent-loss events are deduplicated by `(identifier, attempt)`.
- [x] `watch_azure_pipeline.py:206-267`: A missing or older aggregate check becomes `azure_completed` only after Azure finishes; until then the watcher sleeps 600 s. A running Azure build takes precedence over an outdated GitHub failure, and a mismatch after completion becomes `attention_required`.
- [x] `watch_azure_pipeline.py:374-375,316-325`: These outcomes exit nonzero, and `--azure-timeline` requires both base and merge.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:142-157`: The merge commit's parents must be exactly `[base, head]`, and build provenance is checked strictly.
- [x] `collect_ci_evidence.py:380`: The job key `Phase.Job` matches the timeline identifiers produced by the top-level `jobs:` block in `pipeline.yaml:111`. Attempt numbers come only from Job records.
- [x] `collect_ci_evidence.py:520-529`: `validationPassed` requires a completed and succeeded build, passing job gates and tests, and a stable recheck. `mergeReady` is always `False`.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - `:35-38` removes `GIT_*` environment variables and runs git with `--no-replace-objects`.
  - `:55-60` requires HEAD to equal the expected SHA and the tree entries to be regular files.
  - `:75` requires the stage-0 index entries to equal the tree.
  - `:84` runs `ls-files --others` without `--exclude-standard`, so ignored files are caught too.
- [x] `.github/skills/synapseml-pr-loop/references/review-gauntlet.md`: The text is identical to what was removed from `SKILL.md`, except relative links changed from `references/loop-control.md#…` to `loop-control.md#…`. The link targets exist at `references/loop-control.md:53,120` and `references/ci-triage.md:6,172`.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `dd627e6928a8fca3712f03b636772fd1a86133df66dad20fa84456f479878d81`
- Reviewed diff SHA-256: `e1ccbd8f666192d9a59f46862c4cf55988be271ccf0a9a3b894c64091916a980`
- Original artifact SHA-256: `81066f65fca10c0f08143064439a01b354c65371a2ca509a862fb9b869ad0021`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
