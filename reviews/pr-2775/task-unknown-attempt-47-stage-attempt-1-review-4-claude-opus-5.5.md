---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 47
stage_attempt: 1
round: 4
timestamp: "2026-10-18T11:52:00Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3649db50b874107df1cc4a29719af6af15f40639be7888d289e93b03d3d3f358"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-47-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`Get-PrReadiness.ps1` guards and trigger order.**
  - The guards at ~L98–109 work as documented. Expected SHAs need exactly one PR. `-RunPipeline` needs both expected SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - The trigger checks run in a safe order:
    1. Incomplete evidence throws and lists the revision errors, collection errors and truncation (L611–618).
    2. A PR that isn't open throws (L619–621).
    3. Only an absent aggregate check, tested case-sensitively with `$azureCheck -cnotin $checkNames` (L624), leads on.
    4. Exactly one POST is sent (L627–628).
  - This matches `references/ci-evidence.md#comment-trigger-gate` (L94), pr-loop `SKILL.md` steps 7 and 9, and step 3 of `ci-delegation.md`.
- [x] **Receipt data flow (L629–644).**
  - `pipelineRunRequested` comes only from `$LASTEXITCODE`.
  - The `$commentText -join "`n"` parse sits inside `catch [System.ArgumentException]`. PowerShell matches the ArgumentException inside the CmdletInvocationException that ConvertFrom-Json raises.
  - Empty output, invalid JSON or a partial receipt leads to a warning with no retry. A complete receipt is projected to `id`/`html_url`/`created_at`.
  - I traced all five cases of `test_pipeline_trigger_preserves_receipt_without_retrying` (`tools/ci/tests/test_get_pr_readiness.py` L901–928) against the fake `/issues/1/comments` route (L86–99). Every case produces the asserted `pipelineRunRequested`, receipt and warning.
- [x] **Per-snapshot state.**
  - `$script:collectionErrors` is reset at L304.
  - `$rulesComplete` is captured at L368, after the branch, rules and classic-protection reads. So `humanApproval = "unknown"` reflects only gaps in rule evidence.
  - The compare call runs only for a 40-hex base (L370–374). A `$null` `behind_by` is not equal to 0, so the gate blocks.
  - The recheck (L519–557) separately reports a CLI failure, invalid evidence, a moved head/base/state, and a moved target.
  - `evidenceComplete`, `remoteGatesClear` and `humanApproval` (L558–577) match `ci-evidence.md`.
- [x] **Workflow scoping (L375–443).**
  - First-page `total_count` is compared with the collected count, and identities are checked.
  - A verified association needs the base repo URL and the head SHA.
  - The fork fallback needs a unique `state=all` PR on the same repo/branch with matching number and head SHA.
  - Supersession (`Group-Object workflow_id,event`, then `[long]id` descending) runs after PR scoping.
  - `List[string].Add` returns nothing, so it cannot leak output into the `Where-Object` filters.
- [x] **Check gates (L358–362, ~L444–479).**
  - The aggregate, rule and classic contexts are deduplicated case-sensitively, and `+` binds before `|`.
  - Missing and unproven checks use `-cnotin`/`-cin`.
  - `-RequiredCheck` prefix matches are added as exact names.
  - A check is unproven unless its conclusion or state is SUCCESS. NEUTRAL and SKIPPED are conservatively unproven, so this can only fail closed.
- [x] **`collect_ci_evidence.py` `AzureClient` (`get` L90–137, `list` L139–163).**
  - The endpoint allowlist rejects absolute and traversal paths.
  - A 404, empty or `null` body is accepted only for the timeline.
  - `NoRedirect` raises `EvidenceError` outside the caught exception tuple. HTTPException subclasses are sanitized.
  - Pagination uses `$skip=len(values)` and `$top=100` and stops on a short page, with no off-by-one. The 101-row test shows a second call with `$skip=100`. Repeated IDs raise an error.
  - Grep confirms that `Bearer` and `{self.token}` are in L114. The `******` in the viewer is display masking, not a defect.
- [x] **`summarize_timeline`.**
  - Deduplication by (type, identifier) keeps the highest attempt.
  - Ancestor containers are skipped, but leaf Phase/Stage records are still gated.
  - The lineage walk detects cycles and passes ancestor issues only along the current job's parent chain. An old attempt's phase warnings are correctly excluded, as in the final block of `test_ancestor_issues_block_descendant_job_gates`.
  - Each record counts max(listed, reported), so issues are never undercounted.
  - `AGENT_LOSS` matches all four test messages. Diagnostic text never reaches the output.
- [x] **`collect_tests` (L451–618) with `pipeline.yaml` L111.**
  - `pipeline.yaml` has top-level `jobs:` and no stages, so `f"{phaseName}.{jobName}"` equals the timeline Job identifier.
  - Attempts come only from records with `timelineType == "Job"`. An attempt newer than the timeline raises.
  - Publication errors from older attempts don't block a current retry.
  - `--allow-skipped` cannot exempt a `--expected-test`.
  - Required publications are checked per job, so another shard cannot mask a missing retry.
  - I traced every `collect_tests`/`collect_build`/`main` test in `test_collect_ci_evidence.py`: retry, sharding, named test, partial historical publication, leaf containers, malformed scalars, provenance, and missing or unknown references.
- [x] **`main` (~L662–702).**
  - The monotonic deadline is +600 s; the test asserts `AzureClient(deadline=700)`.
  - A second `collect_build` drives `collectionStable`, and `validationPassed` requires all conditions together.
  - `mergeReady` is always false. An `EvidenceError` produces sanitized JSON with exit code 1.
- [x] **`watch_azure_pipeline.py`.**
  - The deadline is anchored to kickoff, and sleeps are clipped to it.
  - Timeline-mode outcomes and exit codes match `ci-triage.md`.
  - Agent loss is deduplicated by (identifier, attempt).
  - `parse_kickoff` (L259–274) converts `Z` to `+00:00` and truncates or pads fractions to 6 digits with `[:6].ljust(6, "0")`, which Python 3.10 requires. The readiness receipt's `created_at` therefore works as `--kickoff-at`.
  - I traced `test_watch_azure_pipeline.py` L113–151, L526–555 and L617–778.
- [x] **`verify_trusted_guidance.py`.**
  - Filtering on ls-tree mode and kind rejects symlinks and submodules.
  - It checks the index stage and that the index equals the tree.
  - `ls-files --others` runs without `--exclude-standard`, so ignored files are caught.
  - The symlink-ancestry check uses the resolved path. CRLF tolerance applies only to NUL-free blobs, and `GIT_*` variables are stripped.
  - All REQUIRED paths exist. I traced `test_trusted_guidance.py`.
- [x] **Documented commands match the test extraction regexes.**
  - `trusted-guidance.md` L18 uses `-I -B`.
  - `ci-evidence.md` L133 and `ci-triage.md` L31 use `-E -s -B`. This keeps the script directory on the path, which the watcher needs to import `collect_ci_evidence`.
  - All three lines exist with the exact `<trusted-script>`, `<collector>` and `<watcher-script>` placeholders.
- [x] **Docs.**
  - Every cited output field exists in the code.
  - Cross-document anchors resolve, including `#comment-trigger-gate`, `#validation-only-handoff`, `#artifact-retention` (`readiness-gates.md` L24) and `#evidence-invalidation`.
  - The "validated without maintainer changes" outcome agrees between the two skills.
  - The CIHelpers job (`pipeline.yaml` L112–125) runs `tools/ci/tests/`. The readiness tests are gated by `skipUnless(shutil.which("pwsh") or TF_BUILD)`.
- [x] **Considered and rejected as hypothetical or fail-closed.** Each item below either fails closed or is documented as intended:
  - a null `warningCount`/`errorCount` (raises a sanitized `EvidenceError`);
  - a workflow run inserted during pagination;
  - case-variant duplicate JSON keys;
  - a `startup_failure` conclusion (covered by `failedWorkflows` and the unproven gate);
  - a caller-set `$PSNativeCommandUseErrorActionPreference`;
  - `--kickoff-at` clock skew (gives an explicit parser error);
  - a deleted fork's null `head.repo`;
  - recheck wording that says "changed" rather than "missing";
  - the watcher returning `azure_completed` while GitHub lags (documented);
  - `collectionStable` being sensitive to job order;
  - `--allow-skipped-job` matching identifiers regardless of record type;
  - "Spark 4.1" in `ci-triage.md` (the AGENTS.md version rule covers only AGENTS.md and CONTRIBUTING.md).
- [ ] **Tests not executed.** This was a read-only review. I relied on the caller-supplied results:
  - 39 tests and 117 subtests passed on Windows Python 3.14.6 and on Linux Python 3.10.20, both with PowerShell 7.
  - An earlier full-directory run passed 370 tests and 338 subtests.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4827 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 47,
  "completedPriorPasses": 46,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`2f9fc76614f340012f3a015c599c78993cfaaa007d231033d94b76481faa08af`.
This round was clean. A test change after round 5 invalidated the pass;
round 6 did not run. This is not qualifying final review or CI evidence.
