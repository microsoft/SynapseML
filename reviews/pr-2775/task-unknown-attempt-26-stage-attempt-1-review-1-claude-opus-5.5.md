---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 26
stage_attempt: 1
round: 1
timestamp: "2026-10-17T16:06:25Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "85e8c8d0ee0195056ac49f48d54f8f76ec9efb83c95cf30a2c027b0699cb0caa"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-26-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector provenance and transport** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 38–219):
  - `command_json` resolves the tool through `trusted_executable` and hides CLI stderr.
  - `NoRedirect` refuses redirects, so credentials are not sent to another host.
  - `AzureClient.get` accepts only a fixed set of endpoints, applies the deadline, and returns `None` only for an empty or 404 timeline. Its error messages contain the HTTP code only. A grep confirmed line 113 builds the bearer header from the in-memory token.
  - `verify_merge` requires the merge parents in the exact order `[base, head]`.
  - `verify_build` requires definition `17563`, the GitHub repository `microsoft/SynapseML`, `refs/pull/<n>/merge`, a `sourceVersion` equal to the merge SHA, and reason `pullRequest` or `manual` (manual is disclosed in the output).
- [x] **Timeline and test gates** (`collect_ci_evidence.py`, lines 222–646 and 690–719):
  - The timeline keeps the highest attempt per (type, identifier), detects parent cycles, and gates leaf Phase/Stage records. Issues on a Stage or Phase count against every job under it, and invalid counts raise errors.
  - A run with no `pipelineReference`, or a job key that is not a timeline Job, cannot supply current results. If such a run was requested, an error is recorded.
  - A run attempt newer than the timeline raises an error.
  - Required publications are tracked per (job, suite) and per (job, test). `--allow-skipped` cannot exempt an `--expected-test`.
  - `collectionStable` re-reads the build after the tests are collected, and `mergeReady` is always false.
  - I found no path that passes an expected test from the wrong build, job, attempt or run.
- [x] **Job identity against the real pipeline** (`pipeline.yaml`, lines 111–1003): the pipeline is a single implicit stage with top-level `jobs:`, so the collector's `phase.job` keys fit Job identifiers. UnitTests publishes `**/test-reports/TEST-*.xml` through `PublishTestResults@2` (line 980), which is consistent with the run-name semantics in the evidence doc.
- [x] **Watcher state machine** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, lines 46–360):
  - The build-URL allow-list checks host, project and path (case-normalized), rejects credentials in the URL, and bounds build IDs to int32.
  - Only a head change or a non-open PR returns `superseded`; target movement does not cancel monitoring.
  - The merge is verified once. Agent-loss events are reported once per (identifier, attempt).
  - The watcher keeps waiting while Azure is not completed. `azure_completed` and `attention_required` both exit 4.
  - The deadline comes from the kickoff time and is never extended. This matches `references/ci-triage.md`.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - `GIT_*` environment variables are stripped, `core.fsmonitor=false` is forced, and `--no-replace-objects` is used.
  - It checks HEAD, the tree, the index (rejecting conflicted stages), and `ls-files --others` without exclude options, so ignored files are caught too.
  - It rejects symlinks, submodules, symlinked parent directories, and any content difference other than CRLF line endings.
  - `trusted_executable` uses only absolute PATH entries outside the current directory, expands PATHEXT on Windows, and checks symlink targets.
  - All tool arguments are constants or validated hex/integers, so launching a `.cmd` wrapper has no injection risk.
- [x] **Readiness snapshot** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - `-RunPipeline` requires both expected SHAs and refuses `-WaitForReview`. The expected SHAs require exactly one PR (lines 99–105).
  - Paged REST output uses `--jq tojson`, and empty or `null` output is recorded as a collection error.
  - Rulesets and classic protection that are not evaluated become blocking `unassessedGates`.
  - The Azure aggregate check name must match `microsoft.SynapseML` exactly, case-sensitively.
  - Workflow runs are associated through a base-repo URL plus head-SHA check, with a fallback for a unique fork branch (closed PRs included). Ambiguous runs fail closed.
  - Head, base and target-tip SHAs are re-read after collection. `humanApproval` is reported separately from engineering readiness.
  - `/azp run` is posted only when evidence is complete, the PR is open, and the exact aggregate check is absent.
- [x] **Reference docs** (`references/ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `review-gauntlet.md`, `loop-control.md`, `readiness-gates.md`, both `SKILL.md` files, and `synapseml-external-contributor-review/references/ci-delegation.md`):
  - Every new cross-link anchor resolves to an existing heading.
  - Documented behavior matches the code: the 600 s cadence, the 120-minute deadline, the exit codes, the `-I -B` bootstrap and `-E -s -B` helper flags, and the readiness state values.
  - The validation-only and no-bloat wording is consistent across the skill files.
- [x] **Sampled tests** (`tools/ci/tests/test_get_pr_readiness.py`, `test_collect_ci_evidence.py`, `test_watch_azure_pipeline.py`):
  - The PowerShell harness asserts that the trigger and review-request side effects happen exactly when allowed.
  - The collector tests cover redirect refusal, error sanitization, a retry starting during collection, and checking that a `--help` run writes no bytecode cache.
  - In timeline mode, the watcher test goes through the shared `collect_build` with a mocked client and asserts two 600 s sleeps.
  - I read these tests; I did not run them.
- [x] **Considered and dismissed (non-blocking):**
  - If `-RequestReview` and `-RunPipeline` are combined, the review request is sent before the pipeline refusal throws. This follows `ci-evidence.md`, and the documented flow uses a separate `-RunPipeline` invocation.
  - Offset pagination detects repeated IDs but not pure omissions. Result counts are cross-checked against `totalTests`.
  - The order-sensitive `collectionStable` comparison and the propagation of Stage/Phase issues to every job both fail closed and are documented.
  - The `__pycache__/*.pyc` files under the scripts directory are gitignored by `*.pyc` and are not part of the diff.
  - The `CIHelpers` job runs `pytest tools/ci/tests/` but publishes no JUnit results. Final-pass evidence for this PR's own helper tests will therefore have to come from job logs, not the collector. Pipeline changes are outside this PR's scope.
- [ ] I did not run tests, Azure or GitHub API calls, or PowerShell. The review mandate was read-only, so the author's validation results are reported as supplied, not reproduced.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4182 added and 151 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 26,
  "completedPriorPasses": 25,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
