---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 27
stage_attempt: 1
round: 2
timestamp: "2026-10-17T17:13:26Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "07af510e21ba1aed85c3ddee928b39c810ae7081c19876ab45a339529391a6e7"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-27-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Repository conventions.** I checked the diff against the `AGENTS.md` rules and `.agents/README.md`.
  - The diff makes no change to `pipeline.yaml`, the workflows, release tooling or dependency pins.
  - Skills stay under `.github/skills/`.
  - The regressions are in `tools/ci/tests/`, which the existing `CIHelpers` job already runs (`pipeline.yaml` lines 112-126; `tools/ci/README.md`).
- [x] **Module structure.**
  - The imports run one way: `watch_azure_pipeline.py` → `collect_ci_evidence.py` → `verify_trusted_guidance.py`, with no cycle.
  - Each script sets `sys.dont_write_bytecode` only under `__main__`, before it imports its sibling (watcher lines 15-24, collector lines 18-21).
  - The new tests follow the existing watcher test's pattern of `unittest.TestCase` plus `importlib.util.spec_from_file_location`.
- [x] **Constant ownership.** I compared watcher lines 17-41, 105-108, 139-147 and `parse_args` with collector lines 23-27, 60-61, 182-206, 397-447 and 646-690.
- [x] **How the readiness states are derived.**
  - I traced `Get-PrReadiness.ps1` lines 282-460 and 486-660: collection errors, `$rulesComplete`, the revision recheck, `$evidenceComplete`, `$humanApproval` and the `-RunPipeline` guards.
  - I compared that against `references/ci-evidence.md` lines 36-72.
  - I compared it against `tools/ci/tests/test_get_pr_readiness.py` lines 40-62, 122-150, 259-284, 380-402, 472-484, 630-637 and 962-969.
- [x] **Documentation consistency.**
  - I grepped `.github/skills/` for the "validated without maintainer changes" rule, for `-RunPipeline`/`-ExpectedHeadSha` usage, and for the Python interpreter flags.
  - All new anchors resolve to existing headings: `loop-control.md#checkpoint-before-yielding`, `#state-and-cost-discipline` and `#evidence-invalidation`; `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`; `ci-evidence.md#github-snapshot`; and `ci-delegation.md#evidence-required-before-triggering`.
- [x] **Trusted-guidance design.**
  - `GUIDANCE_PATHS`, `REQUIRED` and `trusted_executable` match `references/trusted-guidance.md`.
  - The documented commands use `-I -B` for the bootstrap (`trusted-guidance.md` line 18).
  - They use `-E -s -B` for the collector (`ci-evidence.md` line 95) and the watcher (`ci-triage.md` line 24).
- [ ] **Test execution: not applicable.** This reviewer is read-only and ran no commands. I did not re-run the supplied Windows or Linux pytest results.

## Issues

### Issue 1: A failed target comparison changes `humanApproval`
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 313-319, 357, 500-513, 538-548
- **Description**:
  - `$rulesComplete` is set to `$script:collectionErrors.Count -eq 0` at line 357. By then, the compare read at line 318 has already added any failure to that shared list.
  - `humanApproval` returns `unknown` whenever `$rulesComplete` is false (line 538). So a failed `compare/<base>...<head>` read turns an `APPROVED` PR into `humanApproval=unknown`.
  - The comparison feeds the behind-by integration check, not approval policy.
  - Failures of the PR recheck and fresh-branch reads happen after line 357 and never affect `humanApproval`. Two kinds of revision-read failure therefore treat the human gate differently.
  - `references/ci-evidence.md` lines 44-47 tie `unknown` to approval policy that cannot be read. Line 70 says `humanApproval` is "independent of engineering".
  - Tests assert `unknown` only for rules and classic-protection failures (lines 472-484, 630-637, 962-969). The compare-failure subtest (lines 259-284) uses a fixture that defaults to `APPROVED` (line 129) but never checks `humanApproval`. The coupling is untested, not a deliberate design.
  - The name `$rulesComplete` also misdescribes what the value covers.
- **Risk**: On a transient comparison failure, the output misreports the human gate that this PR exists to separate out. Maintainers may then chase an approval problem that does not exist. This is not an unsafe pass, because `evidenceComplete` still blocks the merge.
- **Suggested Fix**:
  - Compute `$rulesComplete` from the policy reads only. Either run the comparison after line 357 (leaving the branch-metadata check where it is), or count errors around the policy reads alone.
  - In the existing compare-failure subtest, assert that `humanApproval` stays `approved`.

### Issue 2: The watcher keeps its own copies of constants it now shares with the collector
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Line(s)**: 17-24, 28, 30-41, 105-108, 139-147. Collector: `collect_ci_evidence.py` 23-27, 61, 201, 430.
- **Description**:
  - The watcher now imports its GitHub and Azure plumbing from the collector (lines 17-24).
  - Even so, it still defines its own `MAX_BUILD_ID`, `REPOSITORY` and `AZURE_PROJECT_ID` (lines 28, 30, 31), identical to the collector's (lines 23, 24, 27).
  - It also builds the same build-results URL (lines 105-108 here, collector line 430).
  - In `--azure-timeline` mode, a single run uses both sets of copies:
    - `query_pr` and `parse_args` validate `--repo` and query the PR with the watcher's `REPOSITORY`.
    - `verify_merge`/`github_json` and `verify_build` use the collector's `REPOSITORY` (collector lines 61 and 201).
    - Trusted check URLs are matched with `AZURE_BUILD_PATHS`, which is built from the watcher's project ID. Azure API calls use the collector's `API_ROOT`.
- **Risk**: The repository and Azure project identity used in the provenance checks has two sources of truth. If one copy changes (for example, after a project move), timeline mode would verify the build against one identity while accepting check URLs or PR data for another. Nothing in the code keeps the copies aligned. The duplication also breaks the PR's own no-bloat gate.
- **Suggested Fix**:
  - Add `AZURE_PROJECT_ID`, `MAX_BUILD_ID` and `REPOSITORY` to the existing `from collect_ci_evidence import (...)` list, and delete the watcher's copies.
  - Keep the watcher-only constants: `POLL_SECONDS`, `MAX_TIMEOUT_MINUTES`, `CHECK_NAME` and `AZURE_PROJECTS`.
  - No new module or abstraction is needed.

### Issue 3: The "validated without maintainer changes" rule is restated in three places, against the PR's own no-bloat gate
- **Severity**: Low
- **File**:
  - `.github/skills/synapseml-external-contributor-review/SKILL.md`
  - `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md`
  - `.github/skills/synapseml-pr-loop/SKILL.md`
- **Line(s)**:
  - Contributor `SKILL.md`: 89-94
  - `ci-delegation.md`: 80-81, plus table row 97
  - PR-loop `SKILL.md`: 106-108, plus 99-101 and 269
  - `loop-control.md`: 247
- **Description**:
  - The rule: a correct contribution can finish as "validated without maintainer changes", with no manufactured edit, artifact commit or sign-off request.
  - It appears as prose in the PR-loop validation-only handoff (106-108), in a new contributor-skill paragraph (89-94), and again in `ci-delegation.md` step 5 (80-81). That file's new acceptance row (97) already states the same scenario.
  - The contributor paragraph also repeats two rules that live elsewhere:
    - Validation-only mode skips commit and gauntlet publication. The PR-loop handoff (99-101) and stage 8 (269) already own this.
    - Stop after validation. Contributor step 2 (lines 67-68) already says this.
  - The PR's new no-bloat gate (PR-loop `SKILL.md` 149-154) explicitly says to remove "duplicate documentation".
- **Risk**: Three normative copies across two skills must always be edited together. A partial edit gives agents conflicting instructions. The change set also breaks the gate it introduces, which weakens that gate as a review standard.
- **Suggested Fix**:
  - Keep one normative statement in the PR-loop "Validation-only handoff" (106-108), since that section owns the mode.
  - Replace the contributor paragraph with a one-line link, or drop it, since step 2 already covers it.
  - Delete the sentence at `ci-delegation.md` 80-81.
  - Keep the acceptance rows (97 and 247) as scenario checks.

### Issue 4: An inline comment still describes prefix matching for the Azure aggregate check
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 435-437 (the code is at 441-446)
- **Description**:
  - The comment says presence is tested "by prefix, so one required name covers every leg the pipeline reports".
  - The PR made the default `microsoft.SynapseML` requirement an exact, case-sensitive match (line 444), so that a matrix leg can no longer stand in for the aggregate.
  - The updated parameter help (lines 79-84) and `references/ci-evidence.md` lines 37-39 ("a successful matrix leg does not substitute for it") both describe the new behaviour.
  - The comment now says the opposite of what the default does.
- **Risk**: The file contradicts itself. A maintainer who trusts the comment could restore prefix matching for the aggregate, which would bring back the false green this PR closes, where a matrix leg passes in place of the aggregate.
- **Suggested Fix**: Reword the comment to match the code. For example: "Test presence separately: the Azure aggregate must appear exactly; other caller-supplied names keep legacy prefix matching."

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

### Issue 4
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
  "changeSize": "17 source, test, and documentation paths; 4268 added and 152 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 27,
  "completedPriorPasses": 26,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition after round 2

The original response and verdict above are preserved. Its SHA-256 before this
append was `f5158185b3807d23dadd15c26680c14c94377ec3e9e8964a1b8340af7e2d4266`.
Round 1 was clean; round 2 ended this pre-commit pass. Rounds 3-6 did not run.
A new full pass is required on the corrected patch.

### Issue 1: Fixed; approval policy is independent of comparison errors

The target comparison now runs after approval-policy completeness is captured.
Its failure still blocks evidence and engineering readiness but no longer
changes an otherwise known human-approval state.
The existing polling regression now asserts `humanApproval=approved`; the
comparison-failure case failed before this fix. Rules and classic-protection
permission failures still produce `humanApproval=unknown`.

### Issue 2: Fixed with existing constant imports

The watcher imports `AZURE_PROJECT_ID`, `MAX_BUILD_ID`, and `REPOSITORY` from
its existing collector dependency instead of defining three copies. This
bounded removal of duplication needs no new module, URL wrapper, configurable
identity, or other abstraction. Repository, URL, build-ID, and timeline
behavior remain covered by the watcher tests.

### Issue 3: Fixed; one normative no-change handoff

The contributor skill links to the PR loop's validation-only handoff instead
of repeating its no-change outcome and stage exclusions. The CI-delegation
procedure no longer restates that outcome. Both acceptance-scenario rows and
all edit, discussion, publication, and CI authorization gates remain intact.
The link resolves to the existing `Validation-only handoff` heading.

### Issue 4: Fixed

The check-presence comment now describes the exact Azure aggregate and the
legacy prefix behavior retained only for other caller-supplied names.

### Validation and scope

- Windows:
  `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py -k 'PrReadinessTests or WatchAzurePipelineTests or importing_helpers_preserves' -rs --tb=short`
  — **70 passed, 34 deselected, 147 subtests passed**.
- Linux, from `tools/ci/tests`, with PowerShell 7.6.7 on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py test_watch_azure_pipeline.py test_collect_ci_evidence.py -k 'failed_revision_reads_during_poll or rules_permission_failure or classic_protection_permission_failure or WatchAzurePipelineTests or importing_helpers_preserves' -rs --tb=short`
  — **43 passed, 61 deselected, 72 subtests passed**.
- Black 22.3.0 and whitespace checks passed. The change stays within the
  existing 17 source/test/documentation paths; no product API, CI definition,
  dependency, or release changes were added.
- Both reviewer transports were read-only and did not open prior/sibling
  feedback. Relative paths were resolved against the review worktree during
  the audit. Actual reviewer identity was `claude-opus-5.5`.
- Current-candidate Azure validation remains blocked on authorization.
