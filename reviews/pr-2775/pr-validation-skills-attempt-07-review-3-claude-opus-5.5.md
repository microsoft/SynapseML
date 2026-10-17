## Review Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 1
- Verdict: ISSUES_FOUND

## Evidence Checklist
- [x] `F\review.diff` (135710 bytes, SHA256 `dc440952…ff2866`) matches the manifest. W HEAD is `0543bf7e`. Staged blobs match the manifest and there are no unstaged changes. I read the full diff.
- [x] I compared `references/review-gauntlet.md` with the text removed from `SKILL.md`. It is the same word for word except for the heading and one link, which changed from `references/loop-control.md` to `loop-control.md`.
- [x] Every path that `verify_trusted_guidance.py` marks REQUIRED exists in the index. `pipeline.yaml` declares no explicit stages, so the job-id/`__default` identifier mapping is correct.
- [x] I traced `collect_ci_evidence.py:235-272` against the real step names at `pipeline.yaml:658/687/693` and `965/981/987`. A standalone `re` check shows that `Generate Codecov report` does not match line 256. `Publish Test Results`, `Upload Coverage Report To Codecov.io` and `Publish Code Coverage to Azure DevOps` all match.
- [x] Two behaviours are documented and not defects:
  - When GitHub lags behind Azure, the script exits with code 4 so the case goes to triage.
  - A 404 from classic branch protection is treated as `unknown`, which blocks rather than passes.

## Issues
### Issue 1: The post-test publication hint misses the coverage step that fails jobs in this pipeline
- **Severity:** Medium. It affects diagnostic accuracy only; no gate is bypassed.
- **File:** `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; test in `tools/ci/tests/test_collect_ci_evidence.py`
- **Lines:** 249-272 (regex at 256); test at 262-287
- **Description:** The script sets `publication_after_test_step` only when every failed task name matches `publish|upload|coverage`.
  - In this repo, the coverage task that can fail a job after the tests pass is `Generate Codecov report`, at `pipeline.yaml:693, 760, 796, 987`.
  - That step runs `sbt coverageReport` with `retryCountOnTaskFailure: 1` and `condition: and(succeededOrFailed(), eq(variables.runCoverage, true))`. It has no `continueOnError`, so a failure fails the job.
  - The name "Codecov" does not contain "coverage", so the regex misses it.
  - Trace: `Unit Test` succeeds, `Publish Test Results` succeeds, `Generate Codecov report` fails. Line 256 then evaluates False. The branch at 261-270 is False because the test step succeeded. Line 271 is also False. The job ends as `unclassified`.
  - The other post-test steps do match the regex. The unit test passes only because it uses an invented step name, `Publish coverage`, instead of a real one.
- **Risk:**
  - Separating setup failures from publication failures is a stated requirement. `ci-evidence.md:94-95` and `ci-triage.md:115-120,178` say to record that tests passed before coverage publication failed.
  - On coverage-enabled PR builds, all four jobs that run `sbt coverageReport` will report this failure as `unclassified`.
  - The summary lists only `testSteps` (286-293), not the name of the failed task. So the evidence shows a passing test step on a failed job with nothing pointing to the coverage step.
  - Agents must then dig through logs by hand, or may wrongly treat a post-test infrastructure failure as an unexplained test-job failure.
  - Readiness still blocks, so this does not let a bad PR through.
- **Fix:**
  - Classify by order: if every failed task is a non-test task that runs after the last successful test task, return `publication_after_test_step`. At minimum, add `codecov` to the regex at line 256.
  - Include failed task names in the job summary.
  - Change the test to use the real step names `Generate Codecov report` and `Publish Test Results`.

## Driver resolution

- Source base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`; reviewed diff SHA-256
  `dc44095292caf8ac982bfcb6651df03af93c998f90367cfedbafe87330ff2866`.
- Accepted the narrow coverage-stage recognition gap: added `codecov` to the
  publication/coverage hint and replaced the synthetic test task with real
  pipeline task names. The regression covers report generation, test-result
  publication, Codecov upload, and Azure coverage publication.
- Did not classify every arbitrary non-test failure as publication: a later
  unrelated step still needs triage. Hints are not infrastructure proof and
  never waive the failed gate. Detailed failed-task inspection remains the
  documented timeline/log workflow rather than a new output surface.
- Validation passed: 70 tests and 91 subtests across the four helper suites,
  Black 22.3.0 formatting, and `git diff --check`.
- This pass stops after round 3. The next patch requires all six clean rounds.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `349ad3ee8cf47b535b84bb506de98e758bac3bdcc344d787fbbee91d8cb2eb4e`
- Reviewed diff SHA-256: `dc44095292caf8ac982bfcb6651df03af93c998f90367cfedbafe87330ff2866`
- Original artifact SHA-256: `780b1db77d8e2b4c08c5a8ef330cd4cf714abd15c41d519e2e9f3892134fc382`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
