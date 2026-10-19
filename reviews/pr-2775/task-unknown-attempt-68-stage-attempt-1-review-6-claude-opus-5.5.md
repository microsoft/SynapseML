---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 68
stage_attempt: 1
round: 6
timestamp: "2026-10-19T13:24:05Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ddb1670ea1e95875beab5e80af8ab83b4b18a0ac95b16ff1c52261f75a345d04"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-68-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I reviewed the supplied merge-base-to-index diff for all 17 changed paths. I also read the current source of:
  - `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `verify_trusted_guidance.py`, `watch_azure_pipeline.py`, and `Get-PrReadiness.ps1`;
  - the pr-loop and external-contributor `SKILL.md` files and their references;
  - the four suites under `tools/ci/tests/`.

  This review was read-only. I ran no commands, tests or CI. The pytest, Black and Azure results in the request are the caller's evidence and were not reproduced.
- [x] **Latest verifier fix (FIFO).** In `verify_trusted_guidance.py`, the symlink walk (L135-140) runs before `Path.is_file()` (L141-142). Both run before `git cat-file` (L143) and `read_bytes()` (L144). `is_file()` only stats the path and does not open it, so a FIFO or a directory is rejected without blocking.
  - The sentence in `trusted-guidance.md` ("rejects … non-regular working files (including named pipes) before reading their contents") is accurate.
  - The regression in `tools/ci/tests/test_trusted_guidance.py` (L181-219) calls the public CLI with `-I -B` under a 30 s `subprocess.run` limit, which kills the child on timeout. The directory case runs on every platform, and the FIFO case runs only where `os.mkfifo` exists. The `finally` block restores the file.
- [x] **Latest collector fix (deadline).** In `collect_ci_evidence.py`, `main` (L738-768) computes the deadline once (`monotonic()+600`) and passes it to `AzureClient`. It calls `client.remaining_timeout()` at L759, after `mergeReady` is set and before `print`. That method raises when `remaining <= 0` (L140-141), which matches "at or after" in `ci-evidence.md`.
  - `test_collect_ci_evidence.py` (L1241-1295) runs the real `main`, `collect_build` and `collect_tests`. Only `get`, `command_json` and `monotonic` are patched.
  - Rows 699, 700 and 701 assert either exit 0 with `validationPassed`, or the exact error JSON. The test also checks `deadline=700` on the client and exactly two timeline reads.
  - No new option, worker or error type was introduced.
- [x] **Performance.**
  - The watcher sleeps for `min(600, remaining)` between polls. It verifies the merge once (L148-156) and creates one `AzureClient` per poll (L157). One token fetch and two Azure GETs every 10 minutes is negligible, and a fresh client per poll also tolerates token expiry.
  - `Get-PrReadiness -WaitForReview` is bounded by `TimeoutMinutes` (at most 240) and `PollSeconds` (at least 5, default 60).
  - The verifier starts one `git cat-file` per guidance blob. That is about 45 files under `.github/skills` and `templates/` today, so it costs seconds at most.
  - `summarize_timeline` is O(jobs × records), which is trivial at this pipeline's size.
- [x] **Collection-cost consideration (not a defect).** None of the seven `PublishTestResults@2` tasks in `pipeline.yaml` (L401, 686, 752, 789, 980, 1546, 1555) sets `testRunTitle` or `mergeTestResults`, so the same run name may repeat across many runs.
  - `collect_tests` fetches every run with a matching name and all of its result pages (`$top` 100), one request at a time, within the fixed 600 s budget.
  - The cost grows linearly with the number of matching runs. On expiry the collector fails closed with `"Azure evidence deadline expired."` and never reports a false success.
  - `ci-evidence.md` already warns against assuming a unique run title.
  - I have no measured run count or latency showing the budget is exceeded, so I am not reporting this as an issue.
- [x] **Windows cleanup timing.** In `run_windows_batch` (L39-71), the worst case is the 5 s taskkill timeout (L54), plus the 5 s `communicate` (L57), plus the 5 s fallback `wait` (L61). That matches the documented "up to fifteen additional seconds". On failure it reports only the PID; stderr is discarded (L44).
- [x] **Bytecode writes and import safety.**
  - Both helpers set `sys.dont_write_bytecode` when run directly (`collect_ci_evidence.py` L18-19, `watch_azure_pipeline.py` L14-16).
  - The docs prescribe `-I -B` for the bootstrap and `-E -s -B` for the helpers (`trusted-guidance.md` L18, `ci-evidence.md` L140, `ci-triage.md` L31).
  - Together these keep `__pycache__` out of the guidance tree, so rerunning the verifier on resume does not trip its untracked-file check (`ls-files --others`, L131-132).
  - `test_documented_credential_helpers_ignore_untrusted_pythonpath` parses those exact flags from the docs.
- [x] **Watcher output matches `ci-triage.md#waiting-for-azure-pipelines`.** It prints start and finish JSON. It emits `agent_loss` once per (identifier, attempt) (L167-173) and keeps the last Azure evidence collected before the deadline in the timeout result (L164). It allows one confirmation poll only when more than 600 s remain (L114-120). Exit codes are 4 for `attention_required` and `azure_completed`, and 124 for `timeout`.
- [x] **Readiness helper matches `ci-evidence.md#comment-trigger-gate` and its output table.**
  - Guards (L103-111): `-RunPipeline` needs exactly one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`.
  - The complete-collection gate throws with the evidence gaps (L612-620).
  - It posts only when the exact, case-sensitive aggregate is absent (`-cnotin $checkNames`, L626). `microsoft.SynapseML` is always required (L359), and `-RequiredCheck` values are case-insensitive prefixes (L459-466).
  - `pipelineRunRequested` comes from the POST exit status (L631), and the receipt records `id`, `html_url` and `created_at` (L643).
  - Stage 9 of `SKILL.md` describes these guards correctly.
- [x] **Naming and output fields.** These new fields match their documented meanings: `evidenceComplete`, `remoteEngineeringGatesClear`, `humanApproval`, `merge`, `jobGatesPassed`, `validationPassed`, `collectionStable`, `timelinePending`, the `failureStageHint` values, `availableRunNames` and `expectedPublications`. `completeness.complete` is documented and commented as a compatibility alias.
  - `--expected-test` matches `automatedTestName`, falling back to `testCaseTitle`. A wrong name fails closed, and `runs[].results[].name` shows the exact names for the selected run.
- [x] **Links and CI claims.**
  - Every new relative link and anchor resolves: `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`, `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`, `#state-and-cost-discipline`, and ci-delegation's `#evidence-required-before-triggering`.
  - The statement that the helper suites run in `CIHelpers` is backed by the unchanged `pipeline.yaml` (L112-127, `python3 -m pytest tools/ci/tests/`).
  - The documented `TF_BUILD` behavior (strict in Azure, skipped locally) matches `test_get_pr_readiness.py` L249-250.
- [x] **Scope and no-bloat.** The diff contains no changes to product APIs, dependency pins, release tooling, `pipeline.yaml` or workflows. The latest follow-up adds three production lines plus one public-CLI regression per helper. The conservative complete-collection trigger gate is kept. The stage-8 text moves from `SKILL.md` into `review-gauntlet.md` rather than being duplicated.
- [ ] **Not verified: hosted CI and test runs.** I did not reproduce the supplied 101 + 42 test runs, the Black 22.3.0 check, or the Azure build 241650336 baseline on the pre-fix head. Per the resolved plan, they do not qualify as a final CI pass.

## Driver completion evidence

The finalized receipt, retained file hashes and observed runtime model match.
All 53 tool calls were read-only and limited to named source paths/subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
with SHA-256 `f45890d7f0079aa82f6a28bb72bfe73bc9affaa6f2304eb933cd2bf5f63b85f2`.
This clean lens supplies pre-commit evidence, not current-head CI clearance.