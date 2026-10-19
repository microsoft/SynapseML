---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 57
stage_attempt: 1
round: 4
timestamp: "2026-10-19T02:51:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "0a48b153859c00f569d4cab35a7012afd1bdaaee7b0418f77e3f2145500af76a"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-57-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector client and provenance checks** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, about lines 1–259), read line by line:
  - **Windows batch cleanup:** a timed-out batch is stopped with `taskkill /T /F`, falling back to `kill`/`wait`. If cleanup fails, the error keeps the PID and chains the cause.
  - **Error hygiene:** `command_json` sanitizes failures and does not echo stderr.
  - **Endpoint allowlist:** `AzureClient.get` accepts only the allowlisted endpoints.
  - **Timeline special cases:** only the timeline endpoint accepts a 404, an empty body or a `null` response.
  - **Pagination:** `$skip=len(values)` with `$top=100` stops only on a short page, and a repeated ID raises an error.
  - **`verify_merge`:** the parents must be exactly `[base, head]`.
  - **`verify_build`:** checks the definition `17563` (int), GitHub repository, `refs/pull/N/merge`, `sourceVersion`, the `pullRequest`/`manual` reason, `queueTime` and the status enum.
- [x] **Timeline summarizer and job gates** (same file, about lines 262–478):
  - **Attempt selection:** keeps the highest attempt per (type, identifier).
  - **Parent cycles:** a cycle in the parent chain raises an error.
  - **Leaf containers:** a Phase or Stage with no Job under it is kept in the job gates.
  - **Issue inheritance:** issues on parent Phase/Stage records count against each job below them.
  - **Issue counts:** each record uses the larger of its listed issues and `errorCount`/`warningCount`.
  - **Failure-stage hints:** the order comparison is strict.
  - **`unresolvedJobs`:** blocks a job that is not completed, has issues, lost its agent, or did not succeed without an allowed skip.
  - **`jobGatesPassed`:** requires a non-empty job list.
- [x] **Test gate** (same file, about lines 490–655):
  - **`job_key` format:** `f"{phaseName}.{jobName}"` matches real timeline identifiers. `pipeline.yaml` lines 111–1001 have no `stages:` key, and a matrix leg such as `image` gives `UnitTests.image`.
  - **Attempt validation:** bool and non-positive attempts are rejected, and an attempt newer than the timeline raises an error.
  - **Publication errors:** they are added only for requested runs from the current attempt.
  - **Per-job requirements:** required suite and test publications are inferred from every attempt with known provenance.
  - **Skip exemptions:** the only exemptions are `Completed` results with `NotExecuted`/`NotApplicable`, and never for an `--expected-test`.
  - **Final checks:** the four final loops (expected suite, per-result outcome, per-job suite, per-job test) cannot be satisfied by another job's results.
  - **Run-name split:** `parse_args` and the run-name derivation both split on the first `::`, so they agree.
  - **`main`:** the 600-second deadline covers `verify_merge` and every Azure call, and `collectionStable` compares status, result and jobs.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`):
  - **Deadline:** it comes from the kickoff time, and sleeps are capped at the remaining time, so there is no off-by-one at expiry.
  - **Merge check:** `verify_merge` runs only once.
  - **Agent loss:** events are deduplicated by (identifier, attempt).
  - **Branch order:** the GitHub check missing or older than the build, a replacement build, Azure not yet completed, and a pending check with Azure completed are handled in the documented order.
  - **Disagreement:** `attention_required` is decided by comparing `SUCCESS` with `succeeded`.
  - **Exit codes:** both diagnostic outcomes exit 4, and a timeout exits 124.
  - **Timestamps:** `parse_kickoff` truncates or pads fractions to six digits.
  - **Flag coupling:** `--azure-timeline` requires both SHAs and is the only mode that accepts them.
- [x] **Readiness script** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - **Parameter guards** (about lines 99–111): expected SHAs and `-RequestReview` allow one PR only, and `-RunPipeline` refuses to run without both SHAs or with `-WaitForReview`/`-RequestReview`.
  - **`Get-RestEvidence`:** with `--paginate --jq tojson`, gh prints one compact page per line. Empty or `null` output becomes a collection error.
  - **Rules and protection:** ruleset and classic-protection exclusions match `ci-evidence.md`.
  - **Workflow runs:** pagination is checked against `total_count`, the fork-branch uniqueness lookup is checked, and runs are scoped to this PR before keeping the newest per (`workflow_id`, `event`).
  - **Required checks:** the Azure aggregate and rule checks match exactly and case-sensitively; `-RequiredCheck` matches by case-insensitive prefix.
  - **Readiness states:** revision rechecks, `evidenceComplete`, `remoteGatesClear` and the multi-line `humanApproval` if/elseif are correct.
  - **Trigger guards:** the review request refuses CLOSED/MERGED PRs and revision errors. `-RunPipeline` requires complete evidence and an open PR, and fires only when the exact aggregate is absent. A receipt-parse failure does not undo a successful POST.
  - **Null handling:** `$null` member access and comparisons fail closed.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, all lines):
  - **Required files exist:** root `CODEOWNERS`, `pipeline.yaml`, and the external-contributor `contributor-safety.md` and `ci-delegation.md` are all present in this checkout.
  - **Index comparison:** `ls-tree` output is compared with `ls-files -s`, and any stage other than 0 is rejected.
  - **Untracked files:** `--others` without exclude rules also catches ignored bytecode.
  - **Line endings:** the CRLF allowance applies only to blobs without NUL bytes.
  - **Isolation:** `GIT_*` variables are stripped, and `--no-replace-objects` and `core.fsmonitor=false` are set.
  - **Tool lookup:** PATH lookup skips the current directory and its descendants, including symlink targets.
- [x] **Documentation checked against code:**
  - **Files:** `ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `readiness-gates.md`, `review-gauntlet.md`, `loop-control.md`, both `SKILL.md` files and `ci-delegation.md`.
  - **Claims that match the code:** the 600-second deadline, up to 15 seconds of cleanup, microsecond truncation, the 4/124 exit codes, `-RequiredCheck` semantics, receipt fields, `-RunPipeline` refusals, classic-protection exclusions, ignored rule types, and the documented `-E -s -B` and `-I -B` invocations.
  - **Anchors:** every new or changed cross-reference anchor resolves to an existing heading, including `#comment-trigger-gate`, `#azure-merge-and-named-tests`, `#user-value-and-scope`, `#validation-only-handoff`, `#evidence-required-before-triggering`, `#evidence-invalidation`, `#artifact-retention`, `#state-and-cost-discipline`, `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry` and `#github-snapshot`.
- [x] **Tests inspected** (not executed):
  - **Collector:** `tools/ci/tests/test_collect_ci_evidence.py` fixtures (lines 1–115) and the skip-semantics test (lines 589–620) assert the exact `Required result is not passed` message for Failed, Aborted, Timeout and InProgress results.
  - **Readiness:** the `pwsh`/`TF_BUILD` skip condition in `test_get_pr_readiness.py` (lines 249–251) matches `ci-triage.md`.
  - **Verifier:** `test_trusted_guidance.py` exercises every verifier rejection path.
  - **CI wiring:** the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, which collects all four helper suites.
- [x] **Candidates checked and rejected as non-defects:**
  - **Interrupted batch cleanup:** a `KeyboardInterrupt` whose cleanup then fails is reported as `error`, not `interrupted`. It is still nonzero and keeps the PID.
  - **Pagination wording:** the "repeated or omitted" message is only cosmetic.
  - **Job identifier:** it is not type-qualified, but the Phase and Job identifiers in this pipeline cannot collide.
  - **Duplicate check runs:** a rollup listing the same check twice fails closed.
- [ ] **Live GitHub/Azure behavior was not exercised.** This was a read-only review: no commands, tests or API calls were run. Two facts could not be confirmed from the repository: whether Azure ever returns a present-but-null `errorCount`, and the Runs-List page cap. Neither is reported as a defect. Prior review artifacts, `reviews/` and session evidence were not opened.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5104 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 57,
  "completedPriorPasses": 56,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
