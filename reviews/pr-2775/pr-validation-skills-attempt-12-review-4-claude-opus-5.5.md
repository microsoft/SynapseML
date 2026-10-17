# Review Round 4 — Detailed Correctness

## Summary
- Round: 4
- Theme: Detailed Correctness
- Mode: manual prompt fallback (I didn't run the CLI, and I saw no CLI receipt)
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN. I found no significant issues in the reviewed changes.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:119-141`: `list()` pages through results using `$skip=len(values)` and `$top=100`. A duplicate or missing `id` raises an error, and paging stops when a page has fewer than 100 items.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:52,78`: with `NoRedirect`, any redirect becomes an `EvidenceError`. `get()` catches HTTP, URL, OS, decode and `HTTPException` failures and turns them into sanitized errors.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:142`: `verify_merge` checks that the merge commit's parents are exactly `[base, head]`, in that order.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:516-529`: `validationPassed` is true only when the build is completed and succeeded, the job gates and tests pass, and `collectionStable` holds. The exit code is 0 only when it is true.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:126-182`: the merge is checked once (`merge_verified`). Agent-loss events are reported once per `(identifier, attempt)` through `reported_agent_losses`.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:210,251,263,374-375`: the `azure_completed` and `attention_required` outcomes both exit with code 4, so neither is reported as success.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:100-101,496-504`: `-RunPipeline` needs both expected SHAs and can't be combined with `-WaitForReview`. It runs only when `evidenceComplete` is true, the PR is `OPEN`, and the `microsoft.SynapseML` check is absent.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:383,438-448`: required-check names are matched case-sensitively with `-cnotin`, and any `revisionErrors` make `evidenceComplete` false.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:35-38,60-67,84`: the script removes `GIT_*` environment variables and runs git with `--no-replace-objects`. It accepts only regular-file tree entries (modes `100644`/`100755`) and rejects untracked guidance files, including ignored ones.
- [x] `pipeline.yaml:111`: the pipeline has a top-level `jobs:` and no stages, so the `{phaseName}.{jobName}` test-run keys match the timeline Job identifiers.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `1d7987ebe5de682ada6cfd4c0f66db85d028ebd8e1418f424f0f9b69d54d6636`
- Reviewed diff SHA-256: `df7ac7c309f23bf644e55765296cb4a92594b70392e178c7040cf8616bf65733`
- Original artifact SHA-256: `6626a1ea76d8412e0a826b0557a79258e9e644ef3d5297c3e16ff8e36b844cd4`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
