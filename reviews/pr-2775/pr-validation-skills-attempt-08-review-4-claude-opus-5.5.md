## Review Summary
- Round: 4
- Theme: Detailed Correctness
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] **Patch integrity:** `review.diff` matches the SHA256 in the manifest. I read all 2893 lines across the 17 changed files. The worktree HEAD is the base commit `0543bf7e52`, and each of the 17 file versions listed in `manifest.json` matches the git index and the worktree. There are no unstaged differences.
- [x] **`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:**
  - `verify_merge` (line 142) requires the merge parents in the order [base, head].
  - `verify_build` (line 157) checks the build's source.
  - `summarize_timeline` (line 183) takes the latest attempt for each job, phase and stage. It handles skipped parent records and leaf containers, and it detects cycles.
  - `collect_build` (line 301) handles the allowed-skipped job and reports unresolved jobs.
  - `collect_tests` (line 340):
    - matches test runs to jobs using `phase.jobName`;
    - separates the current attempt from earlier attempts;
    - checks `totalTests`;
    - doesn't let the allowed-skipped setting excuse missing expected tests.
  - `main` (line 508) re-checks that the collection is stable. Paging with `$skip`/`$top` detects repeated pages.
  - The `Authorization` header at line 101 is well formed; only the tool output hides its value.
- [x] **`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:**
  - `monitor` (line 114) checks the merge commit once. On each poll it collects the build with a client limited by the deadline.
  - It reports a lost agent only once for each (job, attempt) pair.
  - Each status case is handled correctly:
    - a missing summary check leads to a sleep or `azure_completed`;
    - an Azure build still in progress leads to a sleep;
    - a pending GitHub check with a finished Azure build gives `azure_completed`;
    - a disagreement between the two gives `attention_required` and exit code 4.
  - `main` (line 333) turns off bytecode writing before its imports.
- [x] **`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:**
  - Line 100 rejects `-RunPipeline` unless both expected SHAs are given and `-WaitForReview` isn't set.
  - `Get-RestEvidence` (line 234) parses both paged and slurped results correctly.
  - Lines 485–493 only allow a pipeline run when evidence is complete, the PR is OPEN, and the `microsoft.SynapseML` check is missing.
  - These are also correct:
    - exact checks for both rulesets and classic branch protection;
    - using the newest workflow run for each `workflow_id` and event;
    - handling of `action_required` and waiting approvals;
    - the `behind_by` and `mergeable` merge gates.
  - The tests in `tools/ci/tests/test_get_pr_readiness.py` cover these paths.
- [x] **`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:** `main` (line 111) checks:
  - the git top-level folder is the checkout;
  - HEAD is the expected commit;
  - `ls-tree` file modes contain no symlinks or submodules;
  - all required files are present;
  - the index matches the tree with no unresolved merge entries;
  - there are no untracked files, ignored ones included;
  - no parent folder is a symlink.

  CRLF line endings are tolerated. No gaps found.
- [x] **`.github/skills/synapseml-pr-loop/references/review-gauntlet.md`:** The extracted text is identical to what was removed from `SKILL.md`, apart from the header and the rewritten relative links. All new anchors resolve:
  - `#evidence-required-before-triggering` in `synapseml-external-contributor-review/references/ci-delegation.md`
  - `#state-and-cost-discipline` and `#evidence-invalidation` in `references/loop-control.md`
  - `#targeted-infrastructure-retry` in `references/ci-triage.md`
  - `#github-snapshot` in `references/ci-evidence.md`
- [x] **Concerns considered and not raised:** I looked at each of these and none is a high-confidence defect introduced by this patch:
  - A non-admin user gets a 404 on the branch protection API, so evidence is reported as incomplete. The docs say permission failures should count as unknown.
  - The timeline may be empty for a build that hasn't started. Azure's actual behavior here is unclear.
  - `issues` or `message` fields could be null. This is speculative.
  - A same-build retry after the original deadline can't be watched. This was an explicit requirement.
  - Redirecting with `2>$null` could fail under Windows PowerShell 5.1 with errors set to stop. The repo already uses this pattern, and the tests require PowerShell 7.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `f8da412a509c20a072221c3624759e6e4ce933838a5033c87df8d52324e6770a`
- Reviewed diff SHA-256: `a12c517d8d7db7aba8aaf65ba29a57220351aa98f4460eea036b95ab72d47fbf`
- Original artifact SHA-256: `dcf3b141450226f516f3510595a39d25797a5b82c85498b88bf8dd652fbd9855`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
