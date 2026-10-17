## Review Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] **Patch integrity:** `review.diff` (2,893 lines, 17 files) has the SHA-256 recorded in `manifest.json`. Worktree HEAD is base `0543bf7e52`. All 17 staged index blobs and file modes match the manifest, and there are no unstaged changes. I read the whole diff and the full text of each staged script.
- [x] **`watch_azure_pipeline.py` lines 151–273:** I checked the deadline/timeout, agent-loss and `azure_completed` exit paths. `EvidenceError` is converted to a sanitized `MonitorError`.
  - Each poll reads GitHub status before Azure status (line 133 vs. lines 163 and 249–256). If the GitHub gate lags, a healthy build can still exit with `azure_completed`.
  - `ci-triage.md` lines 44–47 document that this outcome covers a "missing or lagging" GitHub gate. It is intended behaviour, not a defect.
- [x] **`collect_ci_evidence.py`:** `collect_tests` only counts tests from the current attempt and handles skipped and expected tests correctly. `AzureClient` has deadline, redirect and pagination guards. Every failure path fails closed, so the worst case is a missed pass, never a false pass.
- [x] **`Get-PrReadiness.ps1` lines 282–445:** `RunPipeline` checks all of the following and fails closed through `collectionErrors`:
  - exactly one PR
  - both SHAs bound
  - no `WaitForReview`
  - `evidenceComplete`
  - an exact aggregate

  A non-admin `/protection` 404 marks evidence incomplete and blocks `RunPipeline`, which is the documented fail-closed behaviour. `RequestReview` now waits on `evidenceComplete`, and the reason shows up in `automatedReviewRequested` and `collectionErrors`, so this is a design choice, not a hidden regression.
- [x] **`verify_trusted_guidance.py`:** It checks HEAD, tree, index and untracked files; `ls-files --others` also covers ignored files. It resolves symlinks and parent directories and normalizes CRLF. Every mismatch fails closed.
- [x] **`references/review-gauntlet.md`:** The text moved out of `SKILL.md` is unchanged. The only edit is the link `references/loop-control.md` → `loop-control.md`, which is correct from inside `references/`.
- [x] **`pipeline.yaml`:** It has jobs but no stages, so the `Phase.Job` keys match the timeline identifiers. `PublishTestResults@2` uses `failTaskOnFailedTests: true`. Any misclassification there would only affect diagnostic hints, not pass/fail.
- [x] **Candidates I rejected as speculative, pre-existing or intended:**
  - PowerShell 5.1 `ConvertFrom-Json` array quirks (the tests require pwsh 7)
  - an empty timeline for a build that was only just queued
  - a transient Azure error ending the watcher, which matches the existing fail-fast design
  - timeline records with `issues: null`
  - results cut off by the `$top` limit
  - how workflow runs are grouped by `workflow_id`/event
  - case sensitivity in required-check names

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `f8da412a509c20a072221c3624759e6e4ce933838a5033c87df8d52324e6770a`
- Reviewed diff SHA-256: `a12c517d8d7db7aba8aaf65ba29a57220351aa98f4460eea036b95ab72d47fbf`
- Original artifact SHA-256: `25b12138b6bada581e49acd1e57efc67d49fa13876cb87ad5e5105f4621adcb6`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
