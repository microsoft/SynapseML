## Review Summary
- **Round:** 1
- **Theme:** Broad Sweep
- **Mode:** sequential direct-task fallback
- **Requested Model:** claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- **Issues Found:** 0
- **Verdict:** CLEAN

## Evidence Checklist
- [x] **Patch integrity:** `review.diff` is 135,710 bytes and its SHA256 matches (DC4409…2866). It has 2,882 lines and 17 `diff --git` headers, which match the manifest. The base is `0543bf7e`. I read every line of the diff.
- [x] **`references/review-gauntlet.md` (review.diff lines 454–528):** The new file matches the section removed from `synapseml-pr-loop/SKILL.md`. Only the title and the relative links changed (`references/loop-control.md` → `loop-control.md`). Every target anchor exists in the reference files: `#github-snapshot`, `#targeted-infrastructure-retry`, `#state-and-cost-discipline`, `#evidence-invalidation`, `#evidence-required-before-triggering` and `#waiting-for-azure-pipelines`.
- [x] **`scripts/collect_ci_evidence.py:338–451`:**
  - Each test run must come from the latest timeline job attempt (lines 379–388). A newer attempt raises an error, and a run without provenance is recorded as an error.
  - Expected suites and tests must be published in the current attempt (lines 427 and 438–443).
  - A skip allowance can't hide an expected test: `allowed = identity in allowed_skips and identity not in expected_tests` (line 430). Even when a skip is allowed, only `NotExecuted` or `NotApplicable` outcomes pass (line 433).
- [x] **`scripts/watch_azure_pipeline.py:118–257`:** One monotonic deadline is set at line 118 and passed into `AzureClient(deadline=deadline)` (line 163). Request timeouts are capped to the time left (line 159). Every sleep is `min(POLL_SECONDS, remaining)` (lines 216, 232, 257), so polling again never resets the time budget.
- [x] **`scripts/Get-PrReadiness.ps1` — `-RunPipeline` guards:**
  - Lines 97–101: expected SHAs need exactly one PR. `-RunPipeline` needs both expected SHAs and can't be combined with `-WaitForReview`.
  - Lines 434–437: a SHA mismatch blocks `evidenceComplete`.
  - Lines 485–493: the script refuses to post `/azp run` if evidence is incomplete, the PR isn't `OPEN`, or the `microsoft.SynapseML` check already exists. This prevents duplicate pipeline runs.
- [x] **`scripts/Get-PrReadiness.ps1:295–304`:** Rulesets are always read. The classic `/protection` endpoint is read only when the branch is protected and protection isn't explicitly disabled. If that read fails, for example because the user lacks permission, the failure goes to `collectionErrors`. The result is unknown rather than a silent pass, as the comment and readiness-gates doc describe.
- [x] **`scripts/verify_trusted_guidance.py:35–98`:**
  - It removes `GIT_*` environment variables (line 35).
  - It checks committed blobs with `ls-tree -rz <sha>` and rejects symlinks and submodules (lines 60–69).
  - It checks the index with `ls-files -sz` (line 75) and rejects untracked files (line 84) and symlinked parent folders (lines 89–93).
  - It compares bytes exactly, only converting CRLF to LF, and rejects NUL bytes (line 98).
- [x] **`pipeline.yaml` alignment:** The pipeline uses jobs only, with no stages. The "Unit Test" and "Publish Test Results" task names match what the watcher and collector expect.
- [x] **Considered and rejected:**
  - A user without admin rights gets incomplete evidence, which blocks `-RunPipeline` and skips `-RequestReview`. This is the documented fail-closed design.
  - `Sort-Object -Unique` ignores case. This has minimal impact.
  - The "superseded" message says "target". This is wording only.
  - A newly queued build might return an empty or 204 timeline. This is speculative.
  - A duplicate check per build on retry already existed before this change.

  None of these is a defect worth reporting.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `349ad3ee8cf47b535b84bb506de98e758bac3bdcc344d787fbbee91d8cb2eb4e`
- Reviewed diff SHA-256: `dc44095292caf8ac982bfcb6651df03af93c998f90367cfedbafe87330ff2866`
- Original artifact SHA-256: `e32835f682a3c669652434acc7f35155edbf2650f66d350e65345b979059caa0`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
- Publication-only whitespace cleanup: removed two spaces from one blank line.
