---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 34
stage_attempt: 1
round: 1
timestamp: "2026-10-17T21:14:19Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3e245c340a4c81c4bce50b3a32b20709aefa88f5c70f9f8d043003e319027fcc"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-34-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`Get-PrReadiness.ps1` guards and side effects:** The checks at lines 101-109 throw before any `gh` side effect:
  - Expected SHAs require exactly one PR.
  - `-RequestReview` requires exactly one PR.
  - `-RunPipeline` requires both expected SHAs and cannot be combined with `-WaitForReview` or `-RequestReview`.

  The review request (lines 586-605) refuses a PR that isn't open or has revision errors, before the POST. It records one outcome per PR, so later polls cannot re-POST. The trigger (lines 607-630) requires complete evidence, an open PR, and no existing `microsoft.SynapseML` check (case-sensitive). The source states its race caveat at lines 618-619.
- [x] **`Get-PrReadiness.ps1` recheck and gates (lines 537-576):**
  - The revision recheck covers head OID, head ref, head repository, base ref, state, target-branch movement, and expected-SHA mismatches.
  - `$remoteGatesClear` fails closed when comparison data is missing.
  - The `humanApproval` mapping handles both ruleset and classic field names. It agrees with `references/readiness-gates.md` and `references/ci-evidence.md`.
- [x] **`collect_ci_evidence.py` client, provenance, and pass criteria:**
  - `AzureClient.get` (lines 90-137) restricts endpoints with a numeric-ID regex, refuses redirects, never prints the token, and sanitizes errors.
  - `list` (lines 139-164) rejects repeated IDs. For a fixed result set, that also catches omissions.
  - `verify_merge`/`verify_build` (lines 167-221) enforce ordered `[base, head]` merge parents, definition `17563`, the repository, `refs/pull/<n>/merge`, source version, and queue reason.
  - `main` (line 691 onward) sets the deadline before collection. `validationPassed` requires a completed, succeeded build plus passing job gates, tests, and `collectionStable`.
- [x] **`collect_tests` (lines 458-645) matches `references/ci-evidence.md` lines 95-170:**
  - It cross-checks run, build, and result identities.
  - It matches job attempts by `phaseName.jobName` against timeline Job identifiers, and rejects attempts newer than the timeline.
  - Historical attempts are excluded from current results.
  - Per-job suite and test publications are enforced.
  - `--allow-skipped` cannot exempt a `--expected-test`.
- [x] **`summarize_timeline` and `collect_build` (lines 223-447):**
  - Keeps the highest-attempt record per type and identifier, and detects parent cycles.
  - Jobs inherit issues from their parent Phase/Stage records, and leaf Phase/Stage records are gated too.
  - Warnings and errors deliberately block `jobGatesPassed`. `ci-evidence.md` lines 155-163 document this along with the manual disposition path.
- [x] **`verify_trusted_guidance.py`:**
  - `trusted_executable` uses only absolute PATH entries outside the current directory, and applies PATHEXT on Windows.
  - `git()` strips `GIT_*` variables and uses `--no-replace-objects` and `core.fsmonitor=false`.
  - `verify()` requires that:
    - HEAD equals the expected SHA;
    - tree entries are regular blobs;
    - index equals tree with no conflicts;
    - `ls-files --others` (which includes ignored files) is empty;
    - no parent directory is a symlink;
    - working files match committed bytes, with CRLF-only tolerance for non-binary files.
  - All `REQUIRED` paths exist in this tree. No other agent-instruction sources (`.github/instructions/`, `CLAUDE.md`, prompts, agents) exist outside `GUIDANCE_PATHS`.
- [x] **`watch_azure_pipeline.py`:** `monitor` (lines 98-257) and the exit mapping in `main` (line 317 onward) behave as described in the waiting section of `references/ci-triage.md`:
  - The merge is verified once, and the build is polled each cycle.
  - Agent loss is reported per job and attempt.
  - The kickoff deadline is never extended.
- [x] **Documentation:**
  - These anchors resolve:
    - `ci-triage.md#waiting-for-azure-pipelines`
    - `loop-control.md#checkpoint-before-yielding`
    - `loop-control.md#evidence-invalidation`
    - `SKILL.md#validation-only-handoff`
    - the `ci-evidence.md` sections
    - `trusted-guidance.md`
  - `references/review-gauntlet.md` keeps the gauntlet policy removed from `SKILL.md`: pre-commit and final passes, frozen-patch identity, and artifact rules. It adds the `-Execute` contract without repeating `loop-control.md`.
- [x] **Scope and no-bloat:**
  - Only skill docs, skill scripts, and `tools/ci/tests/` change.
  - There are no changes to `pipeline.yaml`, `.github/workflows/`, dependency pins, release tooling, or product APIs.
  - The existing CIHelpers job (`python3 -m pytest tools/ci/tests/ -q` in `pipeline.yaml`) runs the new suites.
  - The artifact-pruning policy in `loop-control.md` requires reachable archival commits and an index, which is consistent with AGENTS.md preservation.
- [x] **Tests inspected:**
  - `tools/ci/tests/test_get_pr_readiness.py`: a fake-`gh` harness, side-effect markers proving request and trigger refusal, and a `skipUnless(pwsh)` guard. `references/ci-triage.md` says a skipped readiness suite is not a pass.
  - `test_collect_ci_evidence.py`, `test_trusted_guidance.py`, and `test_watch_azure_pipeline.py` load modules with bytecode writing disabled. Their fixtures exercise the paths above.
- [x] **Candidates considered and rejected (fail-closed or documented):**
  - Trigger race with a lagging check: documented in source, and the trigger requires pinned SHAs.
  - An explicit null `warningCount` would raise: fail-closed, and the live build parsed.
  - An older `pwsh` fails rather than skips: fail-closed.
  - The phase/job key format: confirmed by the supplied live SuperpixelSuite evidence.
- [ ] **Not executed:** No tests, black, or scripts were run, and no live GitHub or Azure queries were made, because this reviewer is read-only. The supplied Windows and Linux pytest and black results were relied on, not reproduced.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4398 added and 162 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 34,
  "completedPriorPasses": 33,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
