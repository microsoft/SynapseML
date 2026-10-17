# Review Round 3 — Edge Cases & Robustness

## Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

No significant issues found in the reviewed changes.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:98-101` — The script rejects an `az` token that is empty or not a string before using it. The tool output shows the header value on line 101 as `******`, but a grep for `self\.token|token\}` matches that line. So the masking comes from the tool's redaction, and the bearer token is sent.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:103-114` — `HTTPError`, `URLError`, `OSError`, `ValueError` and `HTTPException` are all turned into sanitized `EvidenceError`s. The `with` block closes the response.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:117-136` — The paged `list()` fails when IDs are duplicated or missing across `$skip`/`$top` pages.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:150-170` — An `EvidenceError` becomes a timeout or `MonitorError`, and each Azure client call stays within the overall deadline.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:204-216` — If the aggregate check is missing, the watcher reports `azure_completed` or waits 600 s and checks again. It never treats a missing check as a pass.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:231-233,261-267` — A running Azure build overrides an outdated GitHub status, and `attention_required` exits with code 4.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102` — `RunPipeline` has argument checks before it does anything.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:234-263` — REST failures are collected in `collectionErrors` rather than being silently dropped.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:434-447` — The head and base SHAs are fetched again before the verdict, so a PR that moves during the check makes the evidence invalid.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:496-511` — The pipeline is triggered only when the evidence is complete, the PR is OPEN and the aggregate check is absent.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `1d7987ebe5de682ada6cfd4c0f66db85d028ebd8e1418f424f0f9b69d54d6636`
- Reviewed diff SHA-256: `df7ac7c309f23bf644e55765296cb4a92594b70392e178c7040cf8616bf65733`
- Original artifact SHA-256: `1d2423ef38dae89cb07aefd1715da6f2c9b4329a045a0ace13e48e932a5eb353`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
