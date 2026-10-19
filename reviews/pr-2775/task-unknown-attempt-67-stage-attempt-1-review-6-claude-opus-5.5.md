---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 67
stage_attempt: 1
round: 6
timestamp: "2026-10-19T11:05:36Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "732500d95b5fd6cb9414370839e5132ef73e71448ecf5c9e54647fd9ce7846c3"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-67-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Watcher confirmation poll.** In `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, the `wait_for_check_update()` helper inside `monitor()` waits only when `deadline - time.monotonic() > POLL_SECONDS`. That means the extra poll starts strictly before the original deadline, and it can only happen once. An Azure status other than `completed` resets the allowance but not the deadline. `timeout_result["azure"]` is updated only after a collection finishes before the deadline. The exit codes in `main()` are: success 0; failed and error 1; superseded 2; replaced 3; attention_required and azure_completed 4; timeout 124; interrupted 130. This matches `references/ci-triage.md` "Waiting for Azure Pipelines": one extra poll, a 124 timeout carrying the last Azure evidence, no deadline extension, and nonzero triage outcomes.
- [x] **Watcher test timing.** `test_timeline_completion_allows_one_check_update_before_the_deadline` in `tools/ci/tests/test_watch_azure_pipeline.py` patches `monotonic`, `time` and `sleep`, so it never really sleeps. I traced the arithmetic by hand:
  - Deadline is 7200 seconds.
  - Start at 6599: 601 seconds remain, so it sleeps until 7199. A one-second query or collection delay then reaches the deadline, giving 124.
  - Start at 6600: no extra poll, giving exit 4.
  - Retry case: four polls and three sleeps.
  - The call-count and clock assertions agree with the code.
- [x] **Collector performance and diagnostics** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - `collect_tests` fetches run details and results only for expected run names, so unrelated runs are never fetched.
  - `AzureClient.list` pages 100 rows at a time and rejects duplicate IDs.
  - A mismatch with `totalTests` fails closed.
  - `main()` gives the whole collection one 600-second deadline.
  - `command_json` labels operations without echoing arguments, stderr or tokens.
  - Windows batch cleanup is capped at 5+5+5 seconds and keeps the PID in its error.
  - The collector imports `trusted_executable` from the verifier rather than duplicating it.
  - All of this matches `references/ci-evidence.md` "Azure merge and named tests".
- [x] **Readiness helper vs. docs** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, from snapshot assembly to the end):
  - `-RunPipeline` throws on incomplete evidence or a non-open PR.
  - It posts only when `microsoft.SynapseML` is absent from the rollup names (case-sensitive `-cnotin`).
  - `pipelineRunRequested` reflects the POST exit status regardless of whether the receipt parses; `pipelineRunComment` keeps `id`, `html_url` and `created_at`.
  - The `readiness` fields (`evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval`, `merge`, `unassessedGates`) and the `completeness.complete` alias match the tables in `references/ci-evidence.md` ("GitHub snapshot" and "Comment-trigger gate").
  - `-RequestReview` is gated only on revision errors and an open PR, as documented.
  - `Get-VerifiedRunAssociations` does no network I/O, so calling it per run adds no API cost.
  - The conservative complete-collection trigger gate is retained.
- [x] **Trusted-guidance verifier vs. docs.** `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` checks:
  - HEAD, `ls-tree`, the stage-0 index, and untracked or ignored files via `ls-files --others`;
  - working-tree bytes, with CRLF normalization for text only;
  - symlinked parents.

  It also runs Git with `--no-replace-objects` and `core.fsmonitor=false` and strips `GIT_*` environment variables. Its output fields (`verified`, `source`, `commit`, `fileCount`, `verifiedPaths`) and the cwd-excluding `trusted_executable` that keeps the launcher path match `references/trusted-guidance.md`.
- [x] **Cross-links.** All 20 fragment links across both changed skills resolve to existing headings. Examples:
  - `ci-evidence.md#comment-trigger-gate`
  - `SKILL.md#validation-only-handoff`
  - `readiness-gates.md#user-value-and-scope`
  - `loop-control.md#artifact-retention`, `#evidence-invalidation` and `#state-and-cost-discipline`
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
  - `ci-delegation.md#evidence-required-before-triggering`
  - `contributor-safety.md#who-counts-as-external`
- [x] **Comment-trigger routing.** The PR-loop `SKILL.md` step 7, `readiness-gates.md`, `ci-triage.md` and `ci-delegation.md` step 3 all send comment triggers, including requeues, to the single gate in `ci-evidence.md`. That gate's requeue rule (a fresh snapshot, both expected SHAs, `evidenceComplete=true`) matches the helper's absent-aggregate-only behavior.
- [x] **CI wiring and prerequisites.**
  - The `pipeline.yaml` `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, which collects all four helper suites as `ci-triage.md` says.
  - In `test_get_pr_readiness.py`, `skipUnless(shutil.which("pwsh") or os.environ.get("TF_BUILD"))` matches the documented behavior: a missing `pwsh` fails under Azure but is skipped locally.
  - `#Requires -Version 7.2` matches the PowerShell 7.2+ compatibility fields in both `SKILL.md` frontmatters.
- [ ] **Hosted `CIHelpers` runtime against its 15-minute `timeoutInMinutes` was not observed.** No Azure run exists yet for this pre-commit candidate, and local timings are not hosted CI. This is a limitation of this pass, not a finding.
- [ ] **No tests or commands were executed.** This was a static, read-only inspection. The supplied validation results are the caller's evidence and were not reproduced here.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5511 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 67,
  "completedPriorPasses": 66,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}

## Driver completion evidence

The finalized runner receipt reports exit 0 and CLEAN; runtime events confirm
`claude-opus-5.5`. Input, response and decision hashes match the receipt.
The 44 read-only tool calls stayed within named source files and source subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
privately with SHA-256 `f31ee07736074821540d303c87441911a9fc0303275bfac03e2e3859518de45c`.
This is one lens of the six-clean pre-commit pass, not current-head CI clearance.
