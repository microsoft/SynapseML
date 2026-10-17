## Review Summary
- Round: 6
- Theme: Polish & Hardening
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 2
- Verdict: ISSUES_FOUND

## Evidence Checklist
- [x] Patch integrity: the diff is 136,110 bytes and its SHA-256 (`A12C517D…72D47FBF`) matches. All 17 manifest blobs match the index and the worktree. Base is `0543bf7e52`.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:234-252`: paged calls add `--paginate --slurp` (line 238). Line 239 throws stderr away with `2>$null`, and line 241 records any nonzero exit as "check GitHub permissions". The callers at line 295 (rulesets) and lines 320-321 (workflow runs) feed `$rulesComplete` and the workflow pagination check.
- [x] `gh api --slurp` first shipped in gh 2.48.0, but the Ubuntu 24.04 LTS apt package is gh 2.45.0. The skill's compatibility note says "GitHub CLI" with no minimum version, and the tests mock `gh`, so CI can't catch a version gap.
- [x] Compared `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:146-150` (superseded branch) with the `query_pr` fields at line 86 and with `references/ci-triage.md:49`.
- [x] Every new relative link and anchor in the changed Markdown points to an existing heading.
- [x] The `pipeline.yaml` CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so the new tests run in CI.
- [x] Checked and dismissed:
  - The `collect_ci_evidence.py` Authorization header is a real f-string; it only looks redacted in the tool display.
  - The per-file `git cat-file` in trusted-guidance verification covers about 29 files, so its cost is negligible.

## Issues
### Issue 1: `--slurp` blocks readiness on gh older than 2.48 and blames permissions
- Severity: Medium
- File: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- Lines: 234-252 (callers at 295 and 320-321)
- Description: `Get-RestEvidence -Paged` now runs `gh api --paginate --slurp`. On gh older than 2.48.0, gh exits 1 with `unknown flag: --slurp`; Ubuntu 24.04's packaged 2.45.0 is one example, and that is a normal WSL/Linux setup. Because stderr is thrown away and every failure is labeled a permissions problem, the script records two errors:
  - "Could not read repos/.../rules/...; check GitHub permissions."
  - "Workflow run pagination is incomplete."

  As a result, `evidenceComplete` and `complete` stay false on every run, and `-RunPipeline` throws "incomplete or stale evidence. Recheck the trusted safety gate."
- Risk: The script fails safe, but it permanently blocks a supported platform. The error message sends maintainers to check token scopes and branch protection instead of the gh version. The problem doesn't show up on a newer gh (2.102.0 here) or in the mocked tests.
- Fix:
  - Check `gh --version` up front, fail with a clear error below 2.48.0, and state that minimum in the skill's compatibility note. Alternatively, avoid `--slurp` by paging manually with `page=` or streaming with `--jq`.
  - Capture stderr and report a sanitized cause (unknown flag, 403, 404) instead of always saying "permissions."

### Issue 2: The watcher's superseded message mentions a target change it never checks
- Severity: Low
- File: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- Lines: 146-150 (fields at line 86)
- Description: The message now says "PR head, target, or open state changed…", but the condition still compares only `headRefOid` and `state`. `query_pr` requests only `state,headRefOid,statusCheckRollup`. `references/ci-triage.md:49` says explicitly that target-tip movement does not cancel observation. The test fixtures add `baseRefName` and `baseRefOid`, but the watcher never reads them.
- Risk: On a `superseded` result, an operator may look for target movement and start an unneeded rebase and revalidation. The real cause is a new push to the head or a closed/merged PR. The message also contradicts the documented behavior.
- Fix: Change the message back to "PR head or open state changed". Better still, report which condition fired: the observed head SHA against the expected one, and the state.

## Driver resolution

- Source base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`; reviewed diff SHA-256
  `a12c517d8d7db7aba8aaf65ba29a57220351aa98f4460eea036b95ab72d47fbf`.
- Both findings accepted and fixed after the entire six-theme pass finished.
  This was not a clean pass; a complete new pass remains required.
- Avoided the new CLI-version dependency rather than adding a minimum version:
  paginated REST calls now use the established `--paginate --jq tojson` interface,
  parsing one compact JSON page per line. Query errors report the exit code and
  mention authentication, permissions, and network access without echoing raw
  stderr or claiming every failure is a permission problem.
- The offline CLI fixture rejects `--slurp` and emits actual JSON-lines pages.
  A regression covers an empty first rule page, a later required check, and
  malformed paginated JSON. Existing multi-page workflow assertions still pass.
- Restored the watcher message to its actual head/open-state condition.
- Validation: 71 tests and 91 subtests passed across the four helper suites;
  all seven Python files passed Black 22.3.0 `--check`; `git diff --check` passed.
- Live read-only readiness smoke for microsoft/SynapseML#2753 returned five
  rules, six workflows, current base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`,
  complete evidence, and no collection errors using the compatible paging path.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `f8da412a509c20a072221c3624759e6e4ce933838a5033c87df8d52324e6770a`
- Reviewed diff SHA-256: `a12c517d8d7db7aba8aaf65ba29a57220351aa98f4460eea036b95ab72d47fbf`
- Original artifact SHA-256: `5978b14fe1ff173837c5deacb3a25dd1f594699d4b4c60ef03bb0875caa1cbff`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
