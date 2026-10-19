---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 65
stage_attempt: 1
round: 6
timestamp: "2026-10-19T08:54:39Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "845f25bdef64782616174903c91a24fd031ce026b172a5405ba3d15b632bd6e7"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-65-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I reviewed all 17 changed paths against their current contents:
  - `.github/skills/synapseml-external-contributor-review/`: `SKILL.md` and `references/ci-delegation.md`.
  - `.github/skills/synapseml-pr-loop/`: `SKILL.md` and the references `ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md` and `trusted-guidance.md`.
  - The four scripts in `.github/skills/synapseml-pr-loop/scripts/` and the four suites in `tools/ci/tests/`.
  - Where the diff hunks were truncated, I read the full files.
- [x] **Watcher performance and observability** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`). The code matches `references/ci-triage.md` L37-68 and the watcher bullets in `SKILL.md`:
  - L29-30: a 600-second poll and a 120-minute cap.
  - L98-102: a monotonic deadline based on kickoff time, so a restart gets only the remaining time.
  - L136-155: each poll's `AzureClient` is bounded by the original deadline, and the merge is verified only once. `timeout_result["azure"]` is set only after the post-collection deadline check.
  - L156-175: one `agent_loss` event per identifier/attempt pair.
  - L245-256: GitHub and Azure results must agree.
  - L357-367: the exit-code map.
- [x] **Collector** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`). The code matches `references/ci-evidence.md` L126-226:
  - L39-68: Windows batch cleanup is bounded at about 15 seconds and keeps the PID diagnostic, matching `ci-evidence.md` L188-192.
  - L71-111: errors use fixed operation labels and do not echo arguments or stderr.
  - L125-217: the token stays in memory, redirects are refused, timeouts are clipped to the deadline, and `$skip`/`$top` pagination detects repeated IDs.
  - L505-688: details are fetched only for requested run names, job-attempt provenance is enforced, and `--allow-skipped` cannot exempt an `--expected-test`.
  - L738-767: a 600-second total deadline, a stability recheck, and `mergeReady=false`.
- [x] **Readiness** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`). The code matches the output table and comment-trigger gate in `references/ci-evidence.md` L73-108:
  - L1 and L99-111: `#Requires -Version 7.2`, native errors handled explicitly, the exact Azure aggregate name, and the `-RunPipeline` argument check.
  - L442-470: failed, pending, missing and unproven checks.
  - L559-578: evidence, engineering and human-approval states are kept separate.
  - L610-648: the complete-collection trigger gate is retained, as instructed. `pipelineRunRequested` records the POST's exit status independently of receipt parsing.
  - L683-700: the readiness object.
  - In `tools/ci/tests/test_get_pr_readiness.py` L127-141, stubbed `Get-Date`/`Start-Sleep` keep the wait tests from sleeping.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` L15-33, L40-70 and L94-158). It checks HEAD, the index, untracked/ignored files and working-file bytes. It runs no hooks or filters and filters `PATH` entries. This matches `references/trusted-guidance.md` L28-53.
- [x] **Cross-references and compatibility.** These anchors resolve:
  - `ci-evidence.md#comment-trigger-gate` (L94) and `#azure-merge-and-named-tests` (L126)
  - `SKILL.md#validation-only-handoff`
  - `readiness-gates.md#user-value-and-scope`
  - `ci-triage.md#waiting-for-azure-pipelines`
  - `loop-control.md#state-and-cost-discipline`
  - `ci-delegation.md#evidence-required-before-triggering`

  The new compatibility requirements (PowerShell 7.2+ and a logged-in Azure CLI) match `#Requires -Version 7.2` and the `az account get-access-token` path at `collect_ci_evidence.py` L150-166.
- [x] **Candidates I considered and rejected as non-defects in this scope:**
  - Azure HTTP/transport errors do not name the endpoint (`collect_ci_evidence.py` L177-186). They are sanitized as documented in `ci-evidence.md` L213-226, and the documented recovery (resume with the original kickoff) does not depend on knowing the endpoint.
  - In `collect_tests`, the local variable `selected` holds every inspected row. The name is internal only; the JSON keys match the docs.
  - The verifier runs `git cat-file` once per file, which is bounded by the small guidance tree.
  - `-RunPipeline` does nothing when the aggregate already exists, which `ci-evidence.md` L110-114 documents.
- [ ] **Not verifiable by a read-only review.** I executed no tests; the supplied results were treated as evidence only. The following need the final CI-qualified passes:
  - **CIHelpers runtime:** the job runs `pytest tools/ci/tests/` under `timeoutInMinutes: 15` (`pipeline.yaml` L112-126). The only timing supplied is a local Linux baseline of 486.94 seconds.
  - **Collector request volume on real builds:** `PublishTestResults@2` sets no `testRunTitle` (`pipeline.yaml` L980-985). If many test runs share a default title, the collector may fetch many of them. `ci-evidence.md` L143-146 already warns that run titles may not be unique.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5379 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 65,
  "completedPriorPasses": 64,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver disposition

This clean lens is invalidated by the subsequent watcher completion fix.
See the [lens 3 disposition](task-unknown-attempt-65-stage-attempt-1-review-3-claude-opus-5.5.md#driver-disposition)
for the resolution, validation and independence audit. The original response
above is unchanged; this lens is not a qualifying final-patch signoff.
