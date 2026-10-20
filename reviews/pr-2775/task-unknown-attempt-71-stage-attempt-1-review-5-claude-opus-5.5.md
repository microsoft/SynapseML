---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 71
stage_attempt: 1
round: 5
timestamp: "2026-10-19T23:46:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d3532c59705525775ad69614368f17038eeea89cf0656b4510a6cb39891c355a"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-71-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Collector gates and tests.** I read the rest of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` on disk (lines 290–823): `verify_build`, `summarize_timeline`, `collect_build` (502), `collect_tests` (556), `parse_args` (744) and `main` (789). `tools/ci/tests/test_collect_ci_evidence.py` asserts these behaviors, which match `references/ci-evidence.md:204-216`:
  - jobs with timeline issues block the job gate (391-393, 729-731);
  - the skipped-job allowance (654-658);
  - collection stability and `validationPassed` (1336-1344, 1426-1442);
  - the final deadline recheck (1364).
- [x] **Deadline transport** (`collect_ci_evidence.py:126-166`, `190-242`). `DeadlineReader` sets the socket timeout to the remaining budget before each raw `readinto` and closes the original stream. Only the URLError/OSError/ValueError/HTTPException branch (229) rechecks the deadline, which matches the contract. Covered by:
  - `test_streaming_response_stops_at_the_original_deadline` (1087);
  - `test_http_timeout_errors_report_deadline_or_query_timeout` (1251);
  - `test_expired_deadline_does_not_fetch_credentials` (1079);
  - `test_client_opener_refuses_redirects_without_followup_request` (1057).

  The HTTPError branch (223) does not recheck. That can only change the diagnostic text and still fails closed, so I am not reporting it.
- [x] **Watcher outcomes** (`scripts/watch_azure_pipeline.py:98-275` plus the exit map in `main`). Every outcome is asserted in `tools/ci/tests/test_watch_azure_pipeline.py`:
  - `replaced` (228), `superseded` (337), `error` (419, 445, 516);
  - `azure_completed` (724, 768, 781-782), `attention_required` (786, 945-948);
  - `timeout` (174-268, 597, 652-653, 885);
  - exit codes 0, 4 and 124 (856).

  The documented behavior in `references/ci-triage.md:45-64` matches the code.
- [x] **Readiness harness** (`tools/ci/tests/test_get_pr_readiness.py:20-130`, `253-305`).
  - The mock `gh` logs any comment or reviewer POST before throwing. `snapshot` then requires the side-effect files to match `allow* and expected_error is None` (277, 282), so an unguarded `/azp run` or review request fails the test.
  - The suite has 42 tests, matching the supplied count.
  - `skipUnless(pwsh or TF_BUILD)` (249-251) together with `#Requires -Version 7.2` (`Get-PrReadiness.ps1:1`) makes CI fail rather than skip when `pwsh` is missing.
  - `ruleRequiredChecks` and `classicProtection` (`Get-PrReadiness.ps1:667-668`) come from the same inputs the tests exercise at 721-740, 742-754 and 1315-1325. These output shapes match the exempt-diff guidance in `references/readiness-gates.md:125-137`.
- [x] **Verifier** (`scripts/verify_trusted_guidance.py` `verify`, 95-158). Each guard has a discriminating test in `tools/ci/tests/test_trusted_guidance.py`:
  - HEAD mismatch, working and staged drift (83-91);
  - replace ref (93), untracked files (102), ignored bytecode (118);
  - symlinked directory (109, POSIX only);
  - committed 120000 mode (174). The working bytes equal the blob, so this test fails if the mode check is removed.
  - FIFO or directory, run as a subprocess with a timeout (181);
  - missing required file (263), CODEOWNERS/pipeline/templates drift (270);
  - fsmonitor (380) and `GIT_*` isolation (67);
  - the documented `-I -B` and `-E -s -B` flags, parsed by regex from `trusted-guidance.md:18`, `ci-evidence.md:144` and `ci-triage.md:31` (128, 221).
- [x] **CI scope.** `pipeline.yaml:112-126` (unchanged) runs `python3 -m pytest tools/ci/tests/ -q` with no `--junitxml` or `PublishTestResults@2`. `.github/workflows/pr-validation.yml` runs no pytest. The only Linux skips are the two `skipUnless(win32)` batch tests (`test_collect_ci_evidence.py:906, 934`), consistent with the supplied 2 hosted skips.
- [ ] **No tests executed.** I ran no tests or commands. All counts (Windows 87/282, Linux 85+2 skips, hosted 383/2/429, Black) are supplied evidence.
- [ ] **Hosted CI log is aggregate only.** The run used `-q` without `-rs`, so the log names neither tests nor skip reasons. Identifying the two CI skips as the Windows batch cases is an inference from the platform and the local `-rs` run (see Issue 1).
- [ ] **No real Azure payload available.** The mapping from `phaseName.jobName` to the timeline `identifier`, and integer `warningCount`/`errorCount` values, were checked against test fixtures only.
- [ ] **TLS verification checked by reading only.** `DeadlineHTTPSHandler` uses the default HTTPS context and never installs an unverified one, but no test pins this. Per the review policy, a correct implementation without an extra assertion is not a defect.
- [ ] **Untested defensive branches.** A few fail-closed branches have no direct test: malformed parents in `verify_merge`, the verifier's non-root and index-conflict checks, and a readiness rule without `type`. Other checks back them up, so they are not reported.
- [ ] **Truncated diff.** The prompt's diff was cut off, so I reviewed the remaining content from on-disk files at exact paths. I could not recompute the supplied tree or diff hashes (`6b699ea…`, `423e634…`). Identity with the frozen index relies on the supplied clean-checkout statement.

## Issues

### Issue 1: Helper-suite regressions can never meet the documented named-CI-evidence gate, and no fallback is documented
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/loop-control.md`; `.github/skills/synapseml-pr-loop/SKILL.md`; `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Line(s)**: loop-control.md:254; SKILL.md:263-265; ci-evidence.md:154-155. Context: ci-triage.md:105-113; pipeline.yaml:112-126 (unchanged)
- **Description**: Three new rules require named, published regression results:
  - SKILL.md step 7 says to use the collector to verify "named regression results, including skips".
  - ci-evidence.md says an increased count alone cannot prove a regression was discovered.
  - loop-control.md:254 says to block the evidence gate when an expected regression is absent from published results, "regardless of aggregate counts or green jobs".

  However, ci-triage.md:105 says all four helper suites run in `CIHelpers`. That job publishes no Azure test runs and logs with `-q`, which prints neither test names nor skip reasons, and no GitHub workflow runs these suites. For any helper regression, the collector can only report "Expected suite did not run in a current attempt". So for every non-exempt helper PR, including this one, the documented gate can never be cleared, and the docs give no alternative. This PR's own final evidence already relies on an approach the skill does not document: the aggregate `CIHelpers` count (383 passed, 2 skipped), local named red/green runs, and inferred skip identities.
- **Risk**: The tooling fails closed, so there is no false green. An agent following the skill on a helper PR, though, will do one of three things:
  - stop blocked;
  - misread the missing publication as a CI failure and requeue, wasting CI;
  - treat aggregate counts as clearing the gate, which is exactly what the rule forbids, weakening that rule for product PRs.

  This PR's readiness claim for its own regressions also lacks a documented basis.
- **Suggested Fix**: A documentation-only change; no pipeline change, which would need separate approval.
  - Near ci-evidence.md:154 and in the loop-control row, add a rule for jobs that publish no Azure test runs (currently `CIHelpers` for `tools/ci/tests`). Named regression evidence for them comes from a local run of the exact tests on the reviewed patch, recording the command, platform, counts, `-rs` skip reasons and red/green proof.
  - Cite the job's log summary on the validated merge only as aggregate corroboration, never as named CI evidence or skip identity.
  - Limit loop-control.md:254 to jobs that publish results.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver resolution

Issue 1 is accepted. The follow-up changes four existing guidance files:
`SKILL.md`, `references/ci-evidence.md`, `references/ci-triage.md` and
`references/loop-control.md`. Jobs without a publisher by design now use named
local regression proof plus independently verified current-merge aggregate CI
corroboration. This is explicitly not named published CI evidence or hosted
skip identity. Failed publication, missing expected results from publishing
jobs, unexplained skips and required CI-only validation remain blockers.
The collector still requires real published selectors; the guidance forbids
invented selectors, unrelated suites and relabeling a failed collection.

Verification: documentation links/new anchors and `git diff --check` passed.
The four helper suites ran with `-v -rs --tb=short` on Windows Python 3.14.6:
145 tests and 432 subtests passed, with two explicit POSIX-only skips.
Both omitted POSIX cases and both named transport regressions then passed on
Linux Python 3.10.20: four tests and six subtests, zero skips. The prior
pre-fix failures remain the red/green baseline; no executable changed here.

The fix is captured in source manifest
`dea367dfa6f6ed64a257cf920f0a3a203a997856f8e572fd8314b9549bef3306`.
This changes reviewed documentation, so attempt 71 remains an incomplete,
superseded final pass: rounds 1-4 were clean, round 5 found this issue, and
round 6 did not run. Its original verdict is preserved. Publication and
fresh final-head review/CI are still required.