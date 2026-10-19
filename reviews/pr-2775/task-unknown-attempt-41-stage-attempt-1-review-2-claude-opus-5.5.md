---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 41
stage_attempt: 1
round: 2
timestamp: "2026-10-18T04:28:05Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "11ea34873ede9a7781c8a447e382c7d91a5e3e0b3375459f54993385109e17cf"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-41-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced the retained trigger gate in `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`. It matches `references/ci-evidence.md` lines 92-106, and its diagnostics are complete:
  - Line 104 requires both expected SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - Lines 557-558 build `$evidenceComplete` from three sources: revision errors, `collectionErrors`, and truncated thread comments.
  - Lines 612-619 throw and report exactly those three sources.
  - Lines 621-631 require an open PR and post `/azp run` only when the exact-case `microsoft.SynapseML` aggregate is absent.
- [x] Compared every CI-trigger instruction in both skill trees (searched `azp run|unguarded|pinned`). Issue 1 records the inconsistency. Files and lines checked:
  - `ci-evidence.md` 32-34, 52-54, 96-104
  - `ci-delegation.md` 49-54, 69-72, 86-95
  - PR-loop `SKILL.md` 95-109, 216-217, 229-230, 271-279
  - `readiness-gates.md` 94-97
  - `ci-triage.md` 8-17, 52-55
  - contributor `SKILL.md` 41, 64-66
  - `trusted-guidance.md` 68-72
- [x] Checked the watcher/collector layering. Timeline mode in `watch_azure_pipeline.py` reuses the collector's functions instead of a second implementation. Its exit map (lines 357-367) gives both `attention_required` and `azure_completed` code 4. This matches `ci-triage.md` lines 48-51 ("Both exit nonzero") and the rule at line 56 that only success exits zero.
- [x] Checked that policy logic is not duplicated across languages. Only `Get-PrReadiness.ps1` reads rulesets, classic protection, compare, and `actions/runs` (lines 247, 345, 372, 375). The Python helpers read only `statusCheckRollup` from GitHub, plus Azure build, timeline, and test data.
- [x] Checked the `AGENTS.md` conventions:
  - Changed paths stay within the two skill trees and `tools/ci/tests/`. Nothing touches `pipeline.yaml`, workflows, release tooling, dependency pins, `AGENTS.md`, or `CONTRIBUTING.md`.
  - The existing `CIHelpers` job (`pipeline.yaml` lines 111-126) already runs `python3 -m pytest tools/ci/tests/ -q`.
  - The new suites follow the existing `test_watch_azure_pipeline.py` pattern (`unittest` plus `spec_from_file_location`).
  - `#Requires -Version 7.2` matches the new "PowerShell 7.2+" compatibility text.
- [x] Checked that step 8 was moved into `references/review-gauntlet.md` cleanly:
  - The cross-links `#validation-only-handoff`, `#evidence-invalidation`, `#waiting-for-azure-pipelines`, `#azure-merge-and-named-tests`, and `#evidence-required-before-triggering` all resolve.
  - No manual prompt-assembly instruction remains, except the prohibition in the `loop-control.md` triage table.
- [x] Checked the scope of `verify_trusted_guidance.py`. `GUIDANCE_PATHS` and the required files cover the loaded skills. Excluding `.agents/` matches its compatibility-only status in the branch skill. An untracked `.github/copilot-instructions.md` would still be flagged.
- [x] Rejected these candidates as non-defects, given the scope rule against speculative abstractions:
  - `collect_build(client, args)` reads the watcher's Namespace through `getattr(args, "allow_skipped_job", [])`. It works, and "no skipped-job exemptions in the watcher" is documented.
  - `trusted_executable` raises `GuidanceError`, which `command_json` converts to `EvidenceError`.
  - The Azure DevOps resource ID is inline, and the build-URL format is repeated.
  - The no-bloat rule appears in both `SKILL.md` step 4 and `readiness-gates.md`, which follows the existing step/gate-checklist pattern.
  - Artifact retention in `loop-control.md` is compatible with the preservation rule in `AGENTS.md`.
- [ ] Tests were not executed; this reviewer is read-only. The caller's Windows/Linux results (readiness, collector/watcher, and trusted-guidance suites, plus Black 22.3.0) were considered but not reproduced.

## Issues

### Issue 1: The guarded CI trigger can be bypassed through direct `/azp run` routes still documented elsewhere
- **Severity**: Medium
- **File**: `.github/skills/synapseml-pr-loop/SKILL.md`; `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md`; `.github/skills/synapseml-pr-loop/references/readiness-gates.md`; `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Line(s)**: `SKILL.md` 99-100, 216-217, 229-230 (compare 271-279); `ci-delegation.md` 69-72; `readiness-gates.md` 94-97; `ci-evidence.md` 32-34, 52-54, 96-104; related: `ci-triage.md` 52-55
- **Description**: This PR makes the complete-collection gate the only approved way to trigger CI by comment. `ci-evidence.md` says the helper "deliberately requires complete collection before triggering". If the helper blocks, the agent must obtain the evidence or use the separately authorized pinned-queue route. Otherwise it must "leave CI blocked; do not bypass the helper with an unguarded trigger comment." The same file says ambiguous fork identities (32-34) and missing Administration read (52-54) keep the trigger blocked.

  The documents that actually instruct the agent to trigger CI were not updated to match. They still treat a hand-posted `/azp run` as an equal option that needs only authorization and a safety recheck:
  - `ci-delegation.md` step 3 says "If missing, post `/azp run` once". This PR edited step 5 and the scenario table in the same file but left step 3 as is.
  - `SKILL.md` step 7 lists "`/azp run`, `-RunPipeline`, workflow approval, or manual queueing". It says only that the helper "may" post the trigger, not that it must be used.
  - `readiness-gates.md` lists the same options with the same two preconditions.

  The validation-only handoff (`SKILL.md` 99-100) runs "step 7's authorized trigger", not step 9's guarded invocation. So the external-contributor path never reaches the gate, even though reused fork branches are most likely there.
- **Risk**: The retained conservative gate becomes advisory on the path it most needs to protect.
  - An agent following `ci-delegation.md` or step 7 can run fork code in CI while the snapshot is incomplete. That is exactly the state in which `ci-evidence.md` says CI must stay blocked, and the helper's gap diagnostics never run.
  - The helper posts only when the `microsoft.SynapseML` aggregate is absent (`Get-PrReadiness.ps1` 627-631). A target-driven requeue (`loop-control.md` 114-117; `ci-triage.md` 52-55, "before … triggering another build") therefore always needs a route outside the helper. No document says what snapshot state permits that route.
  - One safety precondition now exists in several copies that disagree.
- **Suggested Fix**: Keep the gate and the helper unchanged, and add no new options.
  1. Make the trigger paragraph in the GitHub snapshot section of `ci-evidence.md` the single statement of the comment-trigger precondition.
  2. Add one sentence there for routes the helper cannot serve, such as a requeue when the aggregate already exists. A hand-posted `/azp run` then requires a fresh read-only snapshot at the cleared head/base SHAs with `readiness.evidenceComplete=true`. Otherwise, use the authorized pinned-queue route or stay blocked. A simpler, stricter alternative is to forbid hand-posted comments entirely and use the pinned-queue route for requeues.
  3. In `ci-delegation.md` step 3, `SKILL.md` step 7 (216-217, 229-230), and `readiness-gates.md` 94-97, replace the bare "post `/azp run`" wording with a link to that rule or to the guarded `-RunPipeline -ExpectedHeadSha … -ExpectedBaseSha …` invocation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
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
  "changeSize": "17 source, test, and documentation paths; 4391 added and 173 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 41,
  "completedPriorPasses": 40,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - Issue 1

**Resolved.** `ci-evidence.md#comment-trigger-gate` is now the canonical
comment-trigger rule. The PR-loop CI step, contributor delegation step 3,
readiness checklist, and CI triage reference all link to it.

An absent aggregate uses the guarded helper. An already-present aggregate
that needs a separately authorized requeue requires a fresh read-only snapshot
at the cleared expected head/base pair with `readiness.evidenceComplete=true`
before a hand-posted comment. Existing-build checks, the one-trigger rule, and
the protected-resource contract still apply. Incomplete collection cannot be
bypassed by posting the same comment manually; the independently assessed,
separately authorized pinned route or a blocked outcome remain the alternatives.
No helper behavior, authorization grant, or new option changed.

Read-through acceptance checks covered an absent aggregate, a requeue with
complete evidence, a requeue with incomplete evidence, and a validation-only
handoff. Each route now reaches the same comment gate. The four new links
resolve to one heading.

The follow-up changes documentation only. Existing documented-command tests
passed on both platforms:

- Windows:
  `python -B -m pytest -q tools/ci/tests/test_trusted_guidance.py -k documented -rs --tb=short`
  passed: 2 tests and 6 subtests, 13 deselected, no skips.
- Linux, from `tools/ci/tests`:
  `python3 -B -m pytest -q test_trusted_guidance.py -k documented -rs --tb=short`
  passed: 2 tests and 6 subtests, 13 deselected, no skips.
- `git diff --check` passed.

Pass 41 is invalidated by this documentation change. Lens 1 was clean; lens 2
raised this issue; lenses 3-6 were not started. Both completed transports were
audited for full frozen-patch coverage, actual reviewer identity, and
read-only source-scoped tools without prior/sibling feedback reads.

Original artifact SHA-256 values before this appendix:

- Lens 1: `5736cc4c7db5af41e64e96b11db77c5ec232ce3688560de29dd59a2a889c249b`
- Lens 2: `72cb7eb47bf2373c24a251b38f8d23cff8cd705dec86a1e084399dad3c3d25c3`
