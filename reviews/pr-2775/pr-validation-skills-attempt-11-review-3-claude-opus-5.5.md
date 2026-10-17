# Review Round 3 — Edge Cases & Robustness

## Summary
- Round: 3
- Theme: Edge Cases & Robustness (null, empty and boundary inputs; races; partial network or IO failures; timeouts, retries and resource cleanup; safe error messages)
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

No significant issues found in the reviewed changes. I read the full 17-file patch and didn't execute any code. Every failure path I traced ends in an error, an `attention_required` result or a timeout. None of them reports a pass by mistake.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:52-65`: `NoRedirect` refuses Azure redirects, so the auth header is never sent to the redirect target. The opener is built with this handler at line 65, and error messages don't leak secrets.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:183-214`: `summarize_timeline` raises `EvidenceError` when the timeline has a parent cycle (214). In `collect_tests` (340+), missing or empty test runs and totals can't produce `validationPassed`.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:114-170`: one monotonic deadline, set from the original kickoff time, governs the whole wait (116-118). Each subprocess call is limited to `min(60, remaining)` seconds (133, 159), and the Azure client gets the same deadline (163). Every sleep stops at the remaining time (216, 232, 257).
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:263, 326-327, 374-376`: a mismatch returns `attention_required` (exit code 4). `--timeout-minutes` must be between 1 and 120, and a timeout exits with code 124.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:100-101, 448, 471, 496-504`: `-RunPipeline` requires the expected head and base SHAs. Both `-RequestReview` and `-RunPipeline` only act when `evidenceComplete` is true, so a missing permission counts as incomplete rather than clear. Before the `/azp run` comment is posted, the script also checks that the PR is open and that the `microsoft.SynapseML` check isn't already present.
- [x] `pipeline.yaml:1025`: the InternalCompat job sets `continueOnError: true`, so a `succeededWithIssues` result deliberately doesn't count as a passed gate. The collector's `{phaseName}.{jobName}` keys match the job names in the timeline, because the pipeline has no explicit stages.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `4af6a3a53f6b1b6ba7a52f18e640eb6ef8f98717bee36ac8af18d70fea1852b5`
- Reviewed diff SHA-256: `bffec5c72c2886366b0342d7b080da336e45895af8e222e4bc90b7ddf0d3aeb6`
- Original artifact SHA-256: `cd8b12213a0bdbfe3dda44d1f8dfe6be442b045f1ab2c9e30dbc11601132b45e`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
