---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 1
round: 4
timestamp: "2026-10-18T17:59:38Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "7cb2ed7fd13a93236ac7e2757caf1b45c68318b231fb94c0ba7bceaf5e251575"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Merge and build provenance** in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.
  - `verify_merge` (L203-215) requires the exact merge SHA with ordered parents `[base, head]`.
  - `verify_build` (L218-256) checks:
    - definition ID `17563`, as a true int;
    - GitHub repository identity;
    - `refs/pull/<n>/merge` and the source version;
    - reason is `pullRequest` or `manual`;
    - a nonempty `queueTime` and a valid status enum.
- [x] **Timeline logic**: `summarize_timeline` (L259-430) and `collect_build` (L433-475).
  - It keeps the latest attempt per (type, identifier) and detects parent cycles.
  - Each job inherits issues from its Stage and Phase ancestors.
  - Per-record issue counts take the larger of the listed issues and the reported count.
  - Leaf Phase and Stage records stay in the gate, while ancestor containers are dropped.
  - In the setup-before-tests hint, `failed[0]` and `test_tasks[0]` are both order-sorted, so the comparison is correct.
  - `allow_empty` applies only to builds that have not completed, and `jobGatesPassed` requires at least one job.
- [x] **Test attribution**: `collect_tests` (L487-672) and `main` (L720-756).
  - The `phaseName.jobName` key matches timeline Job identifiers because `pipeline.yaml` is stageless (top-level `jobs:`).
  - Booleans are rejected as attempt numbers.
  - A run attempt newer than the timeline raises an error.
  - Historical suite and named-test publications create current-attempt obligations.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - Publication errors gate only current, requested runs.
  - `validationPassed` requires all five conditions, including `collectionStable`; `mergeReady` is always false.
- [x] **`AzureClient` and the CLI wrappers** (L39-200).
  - Endpoints are allowlisted, and the token's timeout is bounded by the deadline.
  - A missing or empty timeline is treated as absent only for the timeline endpoint.
  - Protocol and truncated-body errors are sanitized.
  - Pagination uses `$skip=len(values)`, stops on a short page, and detects duplicate IDs.
  - The Authorization header shows as `******` only because the viewer masks it. A targeted grep confirms the source builds a ****** from the cached token.
  - Batch-launcher cleanup has a worst case of 5 s (`taskkill`) + 5 s (`communicate`) + 5 s (`wait`). That matches the documented "up to fifteen additional seconds".
  - The bare `raise` re-raises the original timeout or interrupt.
- [x] **Watcher** in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.
  - `monitor` (L98-256) maps the kickoff wall-clock deadline onto the monotonic clock and caps each query timeout at `min(60, remaining)`.
  - It re-checks the deadline after every query and verifies the merge only once.
  - Agent-loss events are deduplicated by (identifier, attempt).
  - The missing/older-build check runs before the replacement check.
  - A disagreement test `(conclusion == "SUCCESS") != (result == "succeeded")` returns `attention_required`.
  - `parse_build_id` (L47-75) pre-checks length before `int()` and enforces the int32 bound.
  - `parse_kickoff` (L259-274) truncates to microseconds and requires a time zone.
  - The exit-code map in `main` matches `ci-triage.md`.
  - I traced the 600/600-second, clipped-200-second, and stale-failure timelines in `tools/ci/tests/test_watch_azure_pipeline.py`.
- [x] **Guidance verifier**: `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.
  - `verify` (L95-156) parses fields in the correct order for both commands:
    - `ls-tree -rz`: `mode type blob<TAB>path`;
    - `ls-files -sz`: `mode blob stage<TAB>path`.
  - It rejects:
    - nonzero index stages and modes other than regular files;
    - untracked or ignored files (no `--exclude-standard`);
    - symlinked parents (the checkout path is resolved first).
  - It accepts only CRLF-normalized differences in text blobs.
  - `trusted_executable` (L40-69) skips relative PATH entries and entries inside the current directory, and expands PATHEXT on Windows.
  - Root `CODEOWNERS`, `pipeline.yaml`, and `templates/` exist, so `REQUIRED` can be satisfied.
- [x] **Readiness helper**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.
  - The guards (L103-111) and the native-error preference (L100) are in place.
  - Paged parsing reads one page per line via `--jq tojson` (L240-274).
  - Workflow association checks the base URL and head SHA; the fork fallback uses a unique branch lookup that includes closed PRs.
  - Required checks need an exact, case-sensitive aggregate; extra checks match by prefix; any non-SUCCESS required check is unproven.
  - The expected base is compared with the live branch tip (L541-557), not the lagging `baseRefOid`.
  - `$evidenceComplete` gates the trigger (L613-620).
  - The trigger posts only when the aggregate is absent (L626).
  - A receipt-parse failure does not flip `pipelineRunRequested`.
  - The harness in `tools/ci/tests/test_get_pr_readiness.py` asserts trigger and review-request side effects for every case.
- [x] **Docs and anchors.**
  - Every link target resolves:
    - `readiness-gates.md#user-value-and-scope`
    - `loop-control.md#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
    - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
    - `ci-triage.md#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
    - `SKILL.md#validation-only-handoff`
    - `ci-delegation.md#evidence-required-before-triggering`
  - Interpreter flags are consistent:
    - the verifier uses `-I -B` because it has no sibling imports;
    - the collector and watcher use `-E -s -B` because they need the script directory on the path.
  - `tools/ci/tests/test_trusted_guidance.py` reads these flags from the docs.
  - Every comment route points to the comment-trigger gate:
    - `SKILL.md` L213-214;
    - `ci-triage.md` L16-17;
    - `readiness-gates.md`;
    - `ci-delegation.md` step 3;
    - the requeue route in `ci-evidence.md`.
- [x] **Considered and rejected as non-defects:**
  - `collectionStable` is sensitive to list order. This can only fail closed, and a completed timeline does not change.
  - A null `errorCount` would raise an error. This also fails closed.
  - In the non-batch path, the post-kill `communicate()` has no timeout. This only matters for `.exe` children holding pipes.
  - Pagination ends on a short page, which is correct for the requested `$top=100`.
  - The CIHelpers 15-minute timeout may have limited headroom. Hosted timing is unverified, and pipeline edits are out of scope.
- [ ] **Not executed:** I ran no tests (read-only lens) and relied on the supplied validation evidence. I could not view merge-base file content directly; I reviewed the current files against the supplied diff hunks.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4986 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 51,
  "completedPriorPasses": 50,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Subsequent disposition

Round 5 identified two public-path coverage gaps. Extending the existing tests
invalidated this pass. This original clean round is retained as historical
evidence, not counted toward the changed patch's review.
