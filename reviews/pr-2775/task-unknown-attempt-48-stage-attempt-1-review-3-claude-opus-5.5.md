---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 48
stage_attempt: 1
round: 3
timestamp: "2026-10-18T12:57:47Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3e4299bfe08582e472d73c636f6153fc4794ca712fc2f63d9d80645e53b2a848"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-48-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector failure modes** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - I traced every Azure field access in `AzureClient.get` (90-137), `list` (139-163), `summarize_timeline` (222-394), `collect_build` (397-439), `collect_tests` (451-~640) and `main` (~683-713).
  - Malformed, null or missing values become a sanitized `EvidenceError`. I found no path that leads to an uncaught `KeyError` or `TypeError`.
  - Redirects are refused. The 600-second deadline bounds token and HTTP calls.
  - Pagination stops on a short page and rejects repeated IDs. If results are truncated, the `totalTests`/expected-suite checks fail closed.
  - An absent or empty timeline is tolerated only while the build is not completed (`allow_empty`, 400-403).
- [x] **Job-attempt boundary:**
  - `pipeline.yaml` uses top-level `jobs:` (line 111) with no `stages:`. Timeline identifiers are therefore `Phase.Job`, which matches `job_key = f"{phase}.{job_name}"` (~506).
  - A missing or unknown `pipelineReference` gives "lacks matching job-attempt provenance". A test-run attempt newer than the timeline raises an error (~515-518).
  - `collectionStable` re-collects after reading tests, so a retry that starts mid-collection cannot pass.
  - I traced all six subcases of `test_other_job_cannot_mask_missing_retry_publication_or_named_test` (`tools/ci/tests/test_collect_ci_evidence.py` 249-290) against this logic. The suite and test scopes produce exactly the asserted pass/fail results, error text and publication lists.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `monitor` 98-256):
  - The deadline is rechecked after each external call. Errors near the deadline become `timeout`; earlier ones become `error`.
  - While Azure is still running, the watcher sleeps instead of trusting a stale GitHub conclusion (214-216).
  - Completed Azure with a missing or pending GitHub check returns nonzero `azure_completed` (189-203, 232-239). `references/ci-triage.md` 53-56 documents this as the "missing or lagging" outcome.
  - A GitHub/Azure disagreement returns `attention_required` (244-250).
  - `success` requires both the exact build's GitHub SUCCESS and Azure `succeeded`.
  - Agent-loss events are de-duplicated per job and attempt.
- [x] **Readiness trigger gate** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - `#Requires -Version 7.2` (line 1) guarantees the PS7 `ConvertFrom-Json` behavior that the receipt `catch [System.ArgumentException]` depends on.
  - The guards (102-110) refuse `-RunPipeline` without both expected SHAs, or combined with `-WaitForReview`/`-RequestReview`.
  - A trigger requires all of the following (~609-627):
    - `evidenceComplete`, including the fresh head/base/target recheck (519-559);
    - an OPEN PR;
    - an aggregate that is absent by exact, case-sensitive name.
  - A failed POST or an unusable receipt produces a warning, with no retry (~628-647).
  - These cases all fail closed:
    - a null `$compare`, since `$null -eq 0` is false;
    - a workflow `total_count` shortfall or invalid page JSON;
    - ambiguous fork associations (372-445).
  - Mock and receipt subcases inspected: `tools/ci/tests/test_get_pr_readiness.py` 86-98 and 901-929.
- [x] **Verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - `GIT_*` variables are stripped, fsmonitor is disabled, and Git calls have a 30-second timeout. `TimeoutExpired`, `OSError` and `UnicodeError` all map to `verified:false`.
  - It rejects index conflicts, untracked or ignored files, symlinked parents, and non-regular modes.
  - CRLF is tolerated only for non-binary blobs.
  - The `-I` bootstrap in `references/trusted-guidance.md` works because the verifier imports only the standard library. The collector and watcher use the documented `-E -s -B`, which keeps the sibling imports they need.
- [x] **Documentation matches the scripts:**
  - Invocations in `SKILL.md` (271-281), `references/ci-evidence.md` (comment-trigger gate and the collector CLI), `references/ci-triage.md` and `references/trusted-guidance.md` match the actual parameters and refusals.
  - No document instructs a combination the helper now rejects.
- [x] **Considered and dismissed as not concrete defects:**
  - An explicit `null` `warningCount`/`errorCount` would raise an error. This is hypothetical: the collector already parsed real timelines from build 241234907, which included conditionally skipped jobs.
  - `azure_completed` is returned immediately during a GitHub propagation lag. This is documented, tested, fails closed and needs only triage.
  - A single transient Azure error ends the watch. This matches the documented "query errors are nonzero results".
  - The conservative complete-collection trigger gate is intentionally retained, and this review does not ask to relax it.
- [ ] **Test execution:** I ran nothing; this was a read-only review. Runtime pass/fail claims rely on the caller's validation evidence, and behavior was checked only by reading the source.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4853 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 48,
  "completedPriorPasses": 47,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`0c88845cb68201d5bfd121dfaafcee5192e83a9071eb69aedf817a1f25fed50c`.
This round was clean. A test change after round 5 invalidated the pass;
round 6 did not run. This is not qualifying final review or CI evidence.
