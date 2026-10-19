---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 46
stage_attempt: 1
round: 2
timestamp: "2026-10-18T10:09:16Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9aad853f14b2de9c245300c93c2e128c00006ef2ba6c24128873dac81e6fac64"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-46-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed all 17 changed paths under `.github/skills/` and `tools/ci/tests/` in the working tree. The supplied diff text was cut off after the `SKILL.md` §9 hunk, so the remaining hunks were read from the worktree. The visible hunks match the worktree.
- [x] Traced the `-RunPipeline` control flow in `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - parameter guards: lines 102–110
  - memo state: lines 135–136, 612–615 and 630–637
  - complete-collection and open-PR gates: lines 616–627
  - main loop: lines 710–748. The only repeated `Get-PrSnapshot` call is inside `if ($WaitForReview)` (lines 718–745).
- [x] Cross-checked `tools/ci/tests/test_get_pr_readiness.py`:
  - fake `gh pr comment`: lines 86–89
  - one-line trigger assertion: lines 276–280
  - unsafe-request matrix with the `wait`, `request_review` and `multiple_prs` cases: lines 810–872
- [x] Compared the documented trigger provenance with what the helper outputs:
  - `references/ci-triage.md` lines 19–22 and 35
  - `references/ci-evidence.md` lines 93–119 and the snapshot table near line 80
  - `synapseml-external-contributor-review/references/ci-delegation.md` lines 68–74 and 80–82
  - A grep of the PS1 for `issues/|created_at|createdAt|--json` matched only the `gh pr view --json` calls at lines 311 and 523.
- [x] Traced how the watcher reuses collector code:
  - `scripts/watch_azure_pipeline.py`: imports at lines 17–27, calls at 139–147, parser at 277–318
  - `scripts/collect_ci_evidence.py`: `collect_build` at lines 397–440 with the `getattr` at line 404; parser and `main` at lines 660–713
  - the documented watcher policy at `ci-triage.md` lines 60–61
- [x] Verified these cross-skill anchors exist:
  - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot` and `#azure-merge-and-named-tests` (line 121)
  - `SKILL.md#validation-only-handoff`
  - `loop-control.md#evidence-invalidation`
  - `ci-triage.md#waiting-for-azure-pipelines`
- [x] Checked that `references/review-gauntlet.md` lines 7–72 keep the obligations removed from SKILL.md §8:
  - diff file
  - noncolliding artifact names
  - prompt byte budget
  - reviewer isolation
  - the same frozen patch for all six rounds
  - unavailable reviewers as blockers
  - the artifact-only commit
  - the bootstrap handoff

  A grep found no stale `-RunPipeline` guidance.
- [x] Checked the AGENTS.md scope rules. There are no pipeline, workflow, dependency-pin or release-tool changes, and the existing CIHelpers job (`pipeline.yaml` lines 112–126) already runs `tools/ci/tests/`. The new tests follow the precedent of `test_watch_azure_pipeline.py`.
- [ ] Not executed: I ran no commands, tests, Black or PowerShell parsing, because this review is read-only. I did not reproduce the caller's results (370 Linux tests, 130 Windows tests, Black 22.3.0, `Get-Help`).

## Issues

### Issue 1: The `-RunPipeline` idempotence memo can no longer run
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 135–136, 613–615, 630–632, 637 (the guards are at 102–110; the main loop is at 710–748)
- **Description**: Line 108 now rejects `-RunPipeline` combined with `-WaitForReview` or `-RequestReview`, and requires both expected SHAs. Lines 102–103 then limit those SHAs to exactly one PR. So every `-RunPipeline` invocation calls `Get-PrSnapshot` exactly once.
  - `$script:pipelineRunOutcome` is always empty when it is read (lines 613–615 and 632).
  - The value written at line 637 is never read.
  - The comments at line 135 ("Likewise for `/azp run` comments") and line 630 ("Keep the trigger idempotent within this invocation") describe a repeated-poll path that the new guard removed.

  `ci-evidence.md` lines 95–97 already document the one-PR, no-wait contract. The companion `$script:reviewRequestOutcome` is still needed, because `-RequestReview` can still be combined with `-WaitForReview`.
- **Risk**: This is an unreachable duplicate guard, which conflicts with the PR's own no-bloat gate (SKILL.md §4). Its comments suggest the script itself prevents repeat triggers. The real protection is the line 108 guard plus `ci-delegation.md` line 68 ("Record trigger requests so resuming cannot post twice"). That can mislead maintainers.
- **Suggested Fix**: Delete:
  - lines 135–136
  - the read-back at 613–615
  - the `ContainsKey` clause and its "idempotent" comment at 630–632
  - the write at 637

  Assign `$pipelineRunRequested` directly from the single comment attempt. Leave the complete-collection and open-PR gates (lines 616–627) unchanged. The existing `pipelineRunRequested` assertions and the one-line trigger assertion (test lines 276–280) still cover the behavior.

### Issue 2: The required trigger route loses the comment ID and kickoff time
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, plus `references/ci-triage.md`, `references/ci-evidence.md` and `synapseml-external-contributor-review/references/ci-delegation.md`
- **Line(s)**: PS1 lines 635–636 and 703; ci-triage.md lines 19–21; ci-evidence.md lines 115–116; ci-delegation.md line 74
- **Description**: This diff makes the helper the only comment route when the Azure aggregate is absent:
  - SKILL.md §7: "use the guarded helper invocation"
  - ci-delegation.md step 3
  - ci-evidence.md lines 115–116: "do not bypass the helper with an unguarded trigger comment"

  The same documents also require the comment's identity and time:
  - ci-triage.md lines 19–20: record "the trigger comment's `created_at` as its kickoff time". This value is the watcher's required `--kickoff-at`. `queueTime` is allowed only for runs queued "without that comment" (line 21).
  - ci-delegation.md line 74: "preserve the comment/build IDs and kickoff time"

  The helper discards the `gh pr comment` output with `*> $null` (line 635) and emits only the boolean `completeness.pipelineRunRequested` (line 703). `Get-PrSnapshot` reads no issue comments. The old step 3 had agents post `/azp run` themselves, so they saw the created comment; the required route now hides it.
- **Risk**: Agents must find the kickoff with an ad hoc comment search, which ci-delegation.md lines 80–82 discourage. Choosing an older `/azp run` comment cuts the 120-minute watcher window short. Using `queueTime` or the time the build was noticed extends the window past the documented kickoff rule (ci-triage.md lines 21–22 and 35). The checkpoint also loses the comment ID that step 3 asks agents to preserve.
- **Suggested Fix**: Post with `gh api "repos/$owner/$name/issues/$number/comments" -f "body=/azp run"`, which returns `id`, `html_url` and `created_at`. Emit these as one nullable field, for example `completeness.pipelineRunComment`.
  - Keep `pipelineRunRequested` tied to the exit code, so a failure to parse the response cannot lead to a second post.
  - Update the fake `gh` branch and the trigger assertion (test lines 86–89 and 276–280).
  - Add the field to the ci-evidence.md snapshot table near line 80, and name it in ci-triage.md line 20.

### Issue 3: `collect_build` takes its skip policy from another CLI's argument namespace
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` and `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Line(s)**: collector lines 397–404, 660–665, 690 and 692; watcher lines 139–147 and 277–318; ci-triage.md lines 60–61
- **Description**: The watcher passes its own argparse namespace (the parsed command-line arguments) to the collector's `verify_merge` and `collect_build`. `collect_build` reads the skip policy with `getattr(args, "allow_skipped_job", [])` (line 404).
  - Only the collector's parser defines `--allow-skipped-job` (lines 660–665).
  - The watcher's parser never defines it.

  So the documented rule "no skipped-job exemptions in the watcher" (ci-triage.md lines 60–61) depends on an attribute the watcher never defines, not on an explicit argument. The policy is hidden behind a silent default in an untyped argument object.
- **Risk**: The gate depends on hidden coupling between two modules. If the collector's flag is renamed or the attribute misspelled, the `getattr` default silently drops every allowed skip. Adding a watcher option with the same name would change the watcher's gate with no visible change at the call site. Current behavior is correct; the design is fragile.
- **Suggested Fix**: Change the signature to `def collect_build(client, args, allowed_skipped_jobs=()):` and remove the `getattr`. Pass `args.allow_skipped_job` from the collector's `main` at lines 690 and 692. Leave the watcher's call unchanged, so "no exemptions" becomes the visible default. No new abstraction is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4760 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 46,
  "completedPriorPasses": 45,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

The original feedback above is preserved unchanged. Observed execution used
`claude-opus-5.5`; the full patch was embedded in the input and the reviewer
used only scoped source reads, including the source beyond the displayed diff
excerpt. Patch SHA-256:
`b0abdf55be3bc5a738d8f2c3a85050cff640ecbc8aa0a7be313a04c70b7f0577`.

- Issue 1: fixed. Removed the pipeline memo, read-back, and duplicate guard.
  The existing single-PR/no-wait argument contract permits only one trigger
  attempt; review-request polling retains its necessary memo.
- Issue 2: fixed. The guarded trigger now posts through `gh api` and returns
  `completeness.pipelineRunComment` with the comment ID, URL, and kickoff.
  POST outcome is independent of parsing. Failed requests and unusable receipts
  emit an explicit warning to inspect comments/builds before retrying; no retry
  is automatic. The evidence and watcher docs now describe this receipt.
- Issue 3: not a current defect. Both callers intentionally share the collector.
  Missing `allow_skipped_job` means no exemptions, as documented for the watcher.
  `test_only_documented_conditional_job_skips_clear_job_gates` exercises both
  the missing attribute (blocked) and explicit exemption (allowed), including
  failure outcomes that remain blocked. The finding itself confirms current
  correctness and depends on a hypothetical future option/rename. No additional
  signature or state was introduced for that speculation.

The new receipt regression test failed on the old trigger in all five cases.
`python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py --tb=short -rs`
then passed on Windows: 39 tests and 117 subtests. Successful, failed,
invalid-JSON, empty, and incomplete receipts all assert exactly one POST.
PowerShell parsing, `Get-Help -Full`, and pinned Black 22.3.0 also passed.
The source fixes invalidate this pass; rounds 3-6 did not run.
