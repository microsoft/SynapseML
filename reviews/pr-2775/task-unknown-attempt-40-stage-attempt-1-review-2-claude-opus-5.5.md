---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 40
stage_attempt: 1
round: 2
timestamp: "2026-10-18T03:13:03Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "841279011c723adf37c6a27c486f97bfce65044f4f61760516ff0b2ceeb01825"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-40-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` in ranges:
  - 1–130: parameters and argument guards.
  - 130–500: REST evidence, rulesets, classic protection, workflow pagination and fork association.
  - 500–745: revision recheck, readiness states, the `-RequestReview` and `-RunPipeline` gates, output, and the wait loop.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, including `monitor()` (98–258) and the exit mapping (363–364). `attention_required` arises only when GitHub and Azure disagree on the result. `jobGatesPassed` and `unresolvedJobs` stay diagnostic, matching `references/ci-triage.md`.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 1–70 and 640–721:
  - `command_json` resolves tools through `trusted_executable`, and errors are sanitized.
  - `main` sets `collectionStable`, `validationPassed` and `mergeReady=False`.
  - The watcher reuses `verify_merge` and `collect_build` rather than duplicating them.
- [x] Read `verify_trusted_guidance.py` `GUIDANCE_PATHS` and `REQUIRED` (15–33). The required `CODEOWNERS` and `pipeline.yaml` exist at the repo root. Both skills' safety references are required.
- [x] Read the pr-loop `SKILL.md` (1–120, 200–300) and its references: `ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md` and `trusted-guidance.md`. Also read the contributor skill's `references/ci-delegation.md`.
  - Every new cross-document link and anchor resolves.
  - `review-gauntlet.md` keeps the AGENTS.md artifact rules: the numbered PR directory once a PR exists, and session-only drafts before then.
- [x] Grepped `.github/skills`, `AGENTS.md`, `CONTRIBUTING.md` and `.agents` for `REVIEW-PROMPTS`, `completeness.complete`, `RequiredCheck` and `-RunPipeline`. No stale reference contradicts the new readiness states or flags.
- [x] Inspected `tools/ci/tests/test_get_pr_readiness.py` (1–60, 215–260, 706–810, 895–1040) and `tools/ci/tests/test_watch_azure_pipeline.py` (1–120).
  - Module loading follows the existing `spec_from_file_location` pattern in `test_website_doctest.py`.
  - The readiness suite fails instead of skipping under `TF_BUILD`, as `ci-triage.md` documents.
- [x] Grepped `.github/workflows/` for `pull_request` triggers. Matches include `pr-validation.yml`, `codeql.yml`, `dependency-review.yml` and `check-dead-links.yml`, so the fork-association path in Issue 1 is reachable.
- [x] Convention checks:
  - All new Python helpers carry the MIT header.
  - The reviewed paths change no product API, dependency pin, release tool, `pipeline.yaml` or workflow definition.
  - The CIHelpers job in `pipeline.yaml` (112–126) still runs `tools/ci/tests/`.
- [x] Considered and dropped as non-defects:
  - `tools/ci/README.md`: its CIHelpers description is still accurate, and `ci-triage.md` documents the pwsh prerequisite.
  - Pruning in `loop-control.md` versus the AGENTS.md preservation rule: originals stay reachable through verified commits plus an index.
  - The watcher passing its `argparse` namespace to the collector: `getattr(..., "allow_skipped_job", [])` fails closed.
- [ ] Test execution: not performed, because this is a read-only review. Pass/fail status relies on the supplied Windows/Linux pytest and Black evidence, which I did not rerun.

## Issues

### Issue 1: `-RunPipeline` stale-request guard depends on unrelated evidence, so the guarded trigger is a dead end for reused fork branches
- **Severity**: Medium
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`; also `.github/skills/synapseml-pr-loop/references/ci-evidence.md` and `tools/ci/tests/test_get_pr_readiness.py`
- **Line(s)**:
  - `Get-PrReadiness.ps1`: 26–28, 386–418, 557–558, 587–591, 613–621.
  - `ci-evidence.md`: 30–31, 51–54, 82–84.
  - `test_get_pr_readiness.py`: 714–762 (`rulesError` subcase at 739–740) and 967–999.
- **Description**:
  - **Two guards for one risk.** Both side effects in `Get-PrSnapshot` guard against acting on a stale or moved revision, but with different preconditions.
    - `-RequestReview` blocks only on `$revisionErrors` or a non-OPEN state (587–591). The script header (26–28) and `ci-evidence.md` (82–84) justify this explicitly: "unrelated policy/workflow collection errors do not suppress it".
    - `-RunPipeline` throws unless `$evidenceComplete` is true (613–614). That flag also requires zero `collectionErrors` and no truncated thread comments (557–558).
  - **The broader gate adds no protection.** The comment at 619 calls this guard a revision check that "catches stale requests". The trigger decision uses only the absence of `microsoft.SynapseML` from the initial rollup and revision/expected-SHA consistency. Rulesets, classic protection, workflow-run association and thread pagination do not affect it.
  - **Concrete dead end.** For a cross-repository PR, `uniqueForkBranch` (386–407) is false whenever the fork branch has had any other PR to the target, including a closed one.
    - Every `pull_request*` run on the head with an empty `pull_requests` array then adds "no unambiguous PR association" (418). Fork runs commonly have an empty array, as `ci-evidence.md` states.
    - `ci-evidence.md` (30–31) says such runs "require manual run association", but the helper has no input to accept one.
    - So `-RunPipeline` can never succeed for that PR, whatever the caller's permissions. A fork's reused `master` branch is a common shape for external contributors, which is the validation-only path this PR targets.
  - **Misleading failure.** The thrown message ("incomplete or stale evidence. Recheck the trusted safety gate.") misstates the cause, and no snapshot is emitted.
  - **Tests pin the coupling.** The `rulesError` subcase expects the trigger to be blocked. `test_shared_fork_branch_or_failed_head_lookup_stays_ambiguous` models the reused-branch shape but never asserts what happens to the trigger.
- **Risk**:
  - The guarded, expected-SHA trigger documented in pr-loop `SKILL.md` step 9 cannot be used for a realistic class of external PRs. The same happens for non-admin maintainers when classic protection is enabled.
  - Operators must either leave validation blocked for a reason unrelated to safety, or post `/azp run` by hand without the helper's head/base check. That is the hand-rolled path `ci-delegation.md` now discourages.
  - The misleading message sends operators back to a safety recheck that cannot clear the condition.
- **Suggested Fix**:
  - **Preferred: align `-RunPipeline` with `-RequestReview`.** Throw only when `$revisionErrors.Count -gt 0` or the state is not OPEN; expected-SHA mismatches already populate `$revisionErrors`. Collection errors still block `evidenceComplete` and `remoteEngineeringGatesClear` in the readiness output. Then:
    - change the `rulesError` subcase to expect a posted trigger;
    - add a reused-fork-branch trigger case;
    - remove the "trigger stays blocked" clause from `ci-evidence.md` 51–54.
  - **If complete evidence is an intentional CI precondition:**
    - record the safety rationale beside the gate;
    - document the reused-fork dead end and its sanctioned alternative in `ci-evidence.md` and `ci-delegation.md`;
    - include the specific `revisionErrors` and `collectionErrors` in the thrown message.

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
  "changeSize": "17 source, test, and documentation paths; 4363 added and 173 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 40,
  "completedPriorPasses": 39,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - Issue 1

**Resolved using the reviewer's conservative alternative.** Kept the existing
complete-evidence trigger precondition. A request to relax it could not be
confirmed, so this change does not narrow the guard, grant CI permission, or
introduce an evidence-override flag.

`ci-evidence.md` now explains why an execution trigger is treated more
conservatively than a review notification, distinguishes complete collection
from passing checks, and explicitly states that manual workflow association
cannot be supplied to this helper. It documents obtaining missing evidence or
using a separately authorized pinned queue after independent assessment under
the existing CI delegation contract. Missing evidence or authorization remains
blocked; an unguarded trigger comment is not an alternative.

`Get-PrReadiness.ps1` now reports the actual revision/collection gaps or
truncated review comments and directs the caller to a read-only diagnostic
snapshot. Existing tests cover the retained no-trigger behavior for reused
fork branches, absent association evidence, and ruleset read failure. Four
new assertions failed against the old generic error.

The first Linux run exposed terminal wrapping in the test harness, not a
different product result. The harness now writes the caught exception's
actual message and exits nonzero, rather than asserting against PowerShell's
ANSI-formatted source frame. Unexpected CI and review-request side effects
remain independently checked.

Validation after the final harness change:

- Windows:
  `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -rs --tb=short`
  passed: 35 tests and 97 subtests, no skips.
- Linux, from `tools/ci/tests`, with isolated PowerShell on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py -rs --tb=short`
  passed: 35 tests and 97 subtests, no skips.
- Linux, from `tools/ci/tests`:
  `python3 -m black --check --required-version 22.3.0 test_get_pr_readiness.py`
  passed.
- `git diff --check` passed.

This change invalidates pass 40. Lens 1 was clean; lens 2 raised this issue;
lenses 3-6 were not started. Both completed invocations were audited for
observed model identity, full frozen-input coverage, and read-only,
source-scoped tools without prior/sibling feedback reads.

Original artifact SHA-256 values before this appendix:

- Lens 1: `b40edcec6aec9ef57943adc8a927d65d65cf6250b4fac3318f07d6a32f77d454`
- Lens 2: `51010ead89e6e657bdbfc534cf48b85bdafa6accdb8818d02bb9af5b47c37016`
