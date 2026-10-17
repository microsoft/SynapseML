# Review Round 4 — Detailed Correctness

## Summary
- **Round:** 4
- **Theme:** Detailed Correctness
- **Mode:** manual prompt fallback (not Execute, not an observed CLI receipt)
- **Requested model:** claude-opus-5.5 (max)
- **Issues:** 1
- **Verdict:** CHANGES REQUIRED

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:101` — The `Authorization` header really is built from `Bearer` and `{self.token}`; the `******` shown is only display redaction. Redirect, HTTP and `IncompleteRead` errors are sanitized.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:183,340` — `summarize_timeline` and `collect_tests` gate on leaf Phase/Stage records, raise an error on cycles, and use the real Job attempt. Exemptions cannot waive an expected test.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:210,251,263,374-375` — An active Azure run takes precedence over a stale GitHub check. A completed mismatch returns `attention_required`, and a missing or pending gate returns `azure_completed`. Both exit with code 4.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-101,445-446,455,496-504` — The expected-SHA and `-RunPipeline` guards hold, and `behind_by` gates only remote readiness.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:69-98` — Rejects symlinks, submodules, untracked files and symlinked parent directories. Files are compared to the committed blobs after CRLF normalization.

## Issues
### Issue 1: `-f` binds tighter than `+`, so the timeout warnings print raw placeholders
- **Severity:** Medium. It affects diagnostics only; the JSON `missingRequiredChecks` field is still correct.
- **File:** `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Lines:** 602-606 (modified) and 608-610 (new). The same existing pattern appears at 597-599.
- **Risk:** PowerShell parses `"A{0}" + "B" -f $x` as `"A{0}" + ("B" -f $x)`. Only the last string segment gets formatted, and it has no placeholders. The warnings therefore print literally:
  - `PR #{0}: required check(s) '{1}' never appeared on {2} within {3} minute(s)`
  - `PR #{0}: the Azure aggregate is absent…`

  In a multi-PR `-WaitForReview` run, the operator cannot tell which PR, check or head timed out. That defeats the warning's purpose of directing the operator to inspect the right checks or run `-RunPipeline` on the right PR. The assertions at `tools/ci/tests/test_get_pr_readiness.py:407-409` check only fixed substrings, so the tests still pass.
- **Minimal fix:** Wrap the concatenation in parentheses before formatting, for example `Write-Warning (("PR #{0}: … " + "…") -f $number, …)`, or use a single string. Extend the test to assert that the warning contains the PR number and the missing check name.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `4af6a3a53f6b1b6ba7a52f18e640eb6ef8f98717bee36ac8af18d70fea1852b5`
- Reviewed diff SHA-256: `bffec5c72c2886366b0342d7b080da336e45895af8e222e4bc90b7ddf0d3aeb6`
- Original artifact SHA-256: `8919a8cd7bee27796ba252c85f03a8756dcdf88c3e59fb4684a3c5d86493d112`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
