---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 30
stage_attempt: 1
round: 1
timestamp: "2026-10-17T18:39:22Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3807f3d33248177d365054aa0f86d3745c6515ac4d3f29a8548ca09b8b3f20cd"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-30-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and no-bloat audit.**
  - All 17 changed paths are under `.github/skills/synapseml-pr-loop/`, `.github/skills/synapseml-external-contributor-review/` or `tools/ci/tests/`.
  - Nothing changes in `pipeline.yaml`, workflows, dependency pins, release tooling, product modules, `AGENTS.md` or `CONTRIBUTING.md`.
  - The existing `CIHelpers` job (`pipeline.yaml` 112–125) already collects `tools/ci/tests/`.
  - Every new output field is documented or asserted: `allJobsSucceeded`, `uniqueTestCount`, `timelinePending`, `mergeReady`, `availableRunNames`.
  - No redundant file or speculative abstraction found.
- [x] **Trigger guards in `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.**
  - Line 1 requires PowerShell 7.2; lines 91–95 require 40-hex expected SHAs.
  - Lines 100–105 restrict expected SHAs to exactly one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview`.
  - Lines 534–550 turn SHA mismatches into `revisionErrors`.
  - Lines 608–626 throw on incomplete evidence or a non-OPEN PR. Only then is `/azp run` posted, at most once, and only when the exact `microsoft.SynapseML` aggregate is absent (`-cnotin`).
  - Every documented invocation uses the SHA-bearing form (`SKILL.md` 277–284, `references/ci-evidence.md` 86–90). No other caller exists.
- [x] **Policy evaluation fails closed (`Get-PrReadiness.ps1`).**
  - Lines 237–271: nonzero exit, empty output, `null` pages and invalid JSON all become `collectionErrors`.
  - Lines 335–365: rule and classic-protection allowlists are explicit, so unknown types block. `rulesComplete` is captured before the compare and workflow reads.
  - Lines 562–572: `humanApproval` is `not_required` only when every collected approval rule has an integer count of 0 and no code-owner, last-push or `required_reviewers` requirement.
  - Lines 463–467: SKIPPED/NEUTRAL required checks remain unproven.
- [x] **Workflow attribution (`Get-PrReadiness.ps1` 273–292, 384–430).**
  - A run's PR link counts only when its base-repo URL and head SHA match the run.
  - The fork fallback requires exactly one PR for that `owner:branch` across all states, with the same number and head.
  - Foreign or ambiguous runs become collection errors, so they cannot supersede this PR's failed or approval-gated run.
  - The wait loop (695–736) stays read-only after the single permitted review request (582–600).
- [x] **Collector transport security (`scripts/collect_ci_evidence.py`).**
  - Lines 39–57: tools are resolved through `trusted_executable`; stderr and response bodies are never echoed.
  - Lines 71–137: `AzureClient` has an endpoint allowlist, a fixed project, a `NoRedirect` handler, deadline-bounded timeouts, and an in-memory bearer token. Grep confirms the source contains `******; the masked display is only redaction.
  - Lines 139–164: paging uses `$skip`/`$top` and rejects duplicate IDs.
- [x] **Collector provenance (`collect_ci_evidence.py`).**
  - Lines 167–220: the merge must have ordered `[base, head]` parents. The build must match definition 17563, the GitHub repo id, `refs/pull/N/merge`, the merge `sourceVersion`, and reason `pullRequest` or `manual`.
  - Lines 223–394: each `(type, identifier)` keeps its highest attempt. Only ancestor containers are skipped, and ancestors' issues count against their child jobs.
  - Lines 397–446: unexplained skips and timeline issues block `jobGatesPassed`.
- [x] **Collector test gates (`collect_ci_evidence.py`).**
  - Lines 458–643: only current-attempt rows count, and attempts newer than the timeline are rejected.
  - Job/suite and job/test publications are inferred and enforced; `totalTests` is cross-checked; `--allow-skipped` cannot exempt an `--expected-test`.
  - Lines 691–720: the build is re-collected for `collectionStable`, there is a 600 s deadline, and `mergeReady=False` is hard-coded.
- [x] **Watcher (`scripts/watch_azure_pipeline.py`).**
  - Lines 47–75: build URLs must be HTTPS on an allowlisted host and path, with an int32 build ID.
  - Lines 98–257: the deadline is anchored to kickoff and the merge is verified once. A stale GitHub FAILURE does not end monitoring while Azure is still running.
  - The `azure_completed` and `attention_required` outcomes and their exit codes match `references/ci-triage.md` 24–80.
  - Bytecode writes are disabled only when run as `__main__` (lines 14–16).
- [x] **Verifier (`scripts/verify_trusted_guidance.py` 40–150).**
  - Executable lookup skips relative PATH entries and the cwd subtree, and expands PATHEXT on Windows.
  - Git runs with `GIT_*` stripped, `--no-replace-objects` and `core.fsmonitor=false`.
  - It checks:
    - the checkout root and HEAD;
    - regular-file modes only;
    - that the required files are present;
    - that the stage-0 index equals the tree;
    - `ls-files --others` without an exclude flag, so ignored files are also caught;
    - symlinks in any path component;
    - byte equality, tolerating only CRLF, with exact matching for NUL-containing blobs.
  - The stdlib-only imports fit the `-I -B` bootstrap in `references/trusted-guidance.md`.
- [x] **Documentation.**
  - Every new anchor resolves: `loop-control.md#checkpoint-before-yielding`, `#evidence-invalidation`; `ci-triage.md#waiting-for-azure-pipelines`; `ci-delegation.md#evidence-required-before-triggering`; `ci-evidence.md#github-snapshot`; `SKILL.md#validation-only-handoff`.
  - `references/review-gauntlet.md` keeps every rule removed from `SKILL.md` step 8.
  - Its `-Execute` provenance rule and pre-PR blocker match the `loop-control.md` acceptance rows (219–220).
  - The `readiness-gates.md` and `ci-evidence.md` claims match the helper code.
- [x] **Tests inspected, not executed.** `tools/ci/tests/test_get_pr_readiness.py`, `test_collect_ci_evidence.py`, `test_trusted_guidance.py` and `test_watch_azure_pipeline.py` cover:
  - hidden approvals and poll-time read failures;
  - unreadable versus moved revisions;
  - case-sensitive checks and unevaluated rule types;
  - run-to-PR links and legacy head identity;
  - trigger guards and redirect refusal;
  - deadline expiry and sanitized errors;
  - dirty, ignored, symlinked and shadow-import guidance;
  - watcher outcomes.
- [x] **Nothing for the verifier to miss.** No `__pycache__` exists under `.github/skills`. There are no other agent-instruction sources (`.github/instructions`, `.github/agents`, `.github/prompts`, `CLAUDE.md`, `.github/copilot-instructions.md`), so `GUIDANCE_PATHS` misses none.
- [x] **Considered and rejected as defects** (each fails closed, is documented, or is speculative):
  - workflow runs inserted during pagination;
  - test-run lists ending on a short page;
  - a superseded failed aggregate on the same head blocking;
  - `-RequestReview` posting before a `-RunPipeline` throw;
  - Windows junctions in a trusted, agent-created worktree.
- [ ] **Test or CI execution: not applicable.** This reviewer is read-only and executed no commands. The supplied pytest results are recorded as claims, and no current-candidate Azure CI is claimed.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4363 added and 157 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 30,
  "completedPriorPasses": 29,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
