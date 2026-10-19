---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 42
stage_attempt: 1
round: 2
timestamp: "2026-10-18T05:14:35Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "de8394289e3a87ab3d945a9a0cecd084a1b8359ad8527e581e9abb43f71b8b7c"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-42-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
Paths are under `.github/skills/synapseml-pr-loop/` unless stated otherwise.

- [x] **Scope and governance.** I reviewed the whole supplied merge-base-to-index diff (17 paths). It does not change `pipeline.yaml`, `.github/workflows/`, dependency pins, release tooling, `AGENTS.md` or `CONTRIBUTING.md`. That matches the AGENTS.md "Ask before changing" rule and the stated scope.
- [x] **Cross-reference integrity.**
  - `references/ci-evidence.md#comment-trigger-gate` is the single canonical gate. It is linked from `SKILL.md` step 7, `references/ci-triage.md`, `references/readiness-gates.md` and `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md` step 3, and every one of those links resolves.
  - These targets also resolve: `SKILL.md#validation-only-handoff`, `references/trusted-guidance.md`, `loop-control.md#evidence-invalidation`, `ci-triage.md#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry` and `ci-evidence.md#azure-merge-and-named-tests`.
- [x] **Helper layering.**
  - `scripts/watch_azure_pipeline.py` reuses `collect_build`, `verify_merge`, `AzureClient` and `command_json` from `scripts/collect_ci_evidence.py`.
  - That script in turn reuses `trusted_executable` and `GuidanceError` from `scripts/verify_trusted_guidance.py`.
  - The fetch, provenance and tool-resolution logic is not duplicated.
  - Each entry point turns off bytecode writing before importing its sibling modules, and `tools/ci/tests` checks this.
- [x] **Verifier scope matches the repository's guidance files.** `GUIDANCE_PATHS` and `REQUIRED` cover `AGENTS.md`, `CONTRIBUTING.md`, `CODEOWNERS`, `pipeline.yaml`, `.github/skills` and the PR template. `.agents/` contains only pointer READMEs, and `.github/copilot-instructions.md` does not exist, so no live guidance source is missed.
- [x] **Readiness contract.**
  - The `scripts/Get-PrReadiness.ps1` output fields (`readiness.engineering`, `readiness.merge`, `humanApproval`, and `completeness.complete` as an alias of `remoteGatesClear`) match `references/ci-evidence.md`.
  - The `-RunPipeline` refusal diagnostics (`revisionErrors`, `collectionErrors`, truncated threads) also match.
  - The conservative complete-collection trigger gate is kept, and its documented limitation and authorized pinned-queue alternative are present.
  - `#Requires -Version 7.2` matches "PowerShell 7.2+" in both skills, `ci-evidence.md`, `ci-triage.md` and the test's skip reason.
- [x] **Test placement and conventions.** The new suites are in `tools/ci/tests/`, which the existing CIHelpers job already runs (`pipeline.yaml` lines 112–126, `python3 -m pytest tools/ci/tests/ -q`), so no pipeline edit is needed. The unittest style follows the existing `tools/ci/tests/test_watch_azure_pipeline.py`.
- [x] **Python 3.10 compatibility scan.** I grepped the helper scripts and `tools/ci/tests` for features newer than Python 3.10: `tomllib`, `contextlib.chdir`, `datetime.UTC`, `except*`, `typing.Self`, `TaskGroup` and `itertools.batched`. There were no matches.
- [x] **Evidence behind the two issues.**
  - `scripts/collect_ci_evidence.py` lines 209–220. The message reaches the user through line 711 and through `scripts/watch_azure_pipeline.py` line 148.
  - `references/loop-control.md` lines 7–32, with section headings at lines 7, 68 and 135.
  - The inbound links are `references/readiness-gates.md` line 25 and `references/review-gauntlet.md` line 27.
  - No test in `tools/ci/tests` asserts the affected message text.
- [ ] **Tests not run by this reviewer.** This lens is read-only. Any claims about runtime behavior rely on the supplied Windows Python 3.14.6 and Linux Python 3.12.3 evidence.
- [ ] **Python 3.10 coverage of the current candidate not verified.** The caller describes the supplied 3.10 run as historical. CIHelpers has no `UsePythonVersion` step, so it would test the image's default `python3` (3.10 on ubuntu-22.04) only once CI is authorized.

## Issues

### Issue 1: Artifact-retention policy is inserted mid-paragraph into the checkpoint-writing rules
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/loop-control.md`
- **Line(s)**:
  - Lines 14–32; the inserted block is lines 15–29.
  - Inbound links: `.github/skills/synapseml-pr-loop/references/readiness-gates.md` line 25 and `.github/skills/synapseml-pr-loop/references/review-gauntlet.md` line 27.
- **Description**:
  - Lines 15–29 add a new retention and pruning policy for review artifacts. It covers superseded drafts, the index in the numbered review directory, reachability of archival commits, `refs/pull/<number>/head` checks and repair after history rewrites.
  - It sits in the middle of the "Checkpoint before yielding" paragraph, between line 14 and the checkpoint's own persistence rules at lines 30–32 ("Write a temporary sibling file and atomically rename it into place. Preserve an unreadable/corrupt checkpoint…").
  - The atomic-write sentence has no explicit subject. It now follows "…a commit hash alone does not prove the hosting service will retain it.", so the nearest thing "it" can refer to is the archive or index, not the JSON checkpoint.
  - The two new links labelled "[retention policy]" and "[artifact retention]" both point to `#checkpoint-before-yielding`, a heading that names neither topic. Elsewhere this reference gives separate concerns their own `##` heading, such as `State and cost discipline` (line 68) and `Evidence invalidation` (line 135).
- **Risk**:
  - Agents read this guidance directly. An agent resuming from this section could apply the atomic-rename and corrupt-file rules to review artifacts, or skip past them as part of the retention text.
  - Readers who follow a "retention" link land on a checkpoint section and have to find the policy mid-paragraph.
  - With both topics in one paragraph, a later edit to one policy is more likely to disturb the other.
- **Suggested Fix**: Only move text; add nothing new.
  1. Keep lines 30–32 directly after line 14.
  2. Move lines 15–29 into a new `## Artifact retention` section, for example just before `## State and cost discipline`.
  3. Point the links at `readiness-gates.md` line 25 and `review-gauntlet.md` line 27 to `loop-control.md#artifact-retention`.

### Issue 2: The `verify_build` status error blames the queue time, which was already validated
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: 209–220
- **Description**:
  - `verify_build` checks `queueTime` first and raises its own message at lines 209–210 ("Azure build is missing a valid queue time.").
  - The status check at lines 213–220 only runs after the queue time has passed. Even so, it raises "Azure build is missing its queue time or valid status."
  - Every other check in the function gives one condition one specific message: definition, provenance, queue time and result. This message looks like a leftover from an earlier combined check.
- **Risk**:
  - The message is shown to users. It becomes the collector's JSON `message` (line 711). In `--azure-timeline` mode it also becomes the watcher's `MonitorError` (`watch_azure_pipeline.py` line 148), because `collect_build` reruns `verify_build` on every poll.
  - If a build has a valid queue time but its `status` is missing or unexpected (for example Azure's `none` value, or a new state), the message sends the operator to the wrong field.
  - That works against this PR's stated goal of specific diagnostics.
- **Suggested Fix**: Make the message about status only, matching the wording of the neighbouring result check, for example `"Azure build has an invalid status."`. No current test asserts the old text. Optionally, add one negative test that asserts the new message.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4412 added and 178 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 42,
  "completedPriorPasses": 41,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution

**Issue 1 resolved:** moved the existing retention paragraph unchanged into
`loop-control.md#artifact-retention` and updated both inbound links.
Checkpoint atomic-write and recovery instructions are contiguous again.
The moved paragraph was compared with the frozen index version: its policy
text is identical, and both new links resolve. No retention rule was added
or removed.

**Issue 2 resolved:** the collector now reports only the invalid build status;
the independently validated queue time is no longer blamed. Two cases in the
existing build-validation test cover a null or unsupported status with a
valid queue time. Both failed against the old diagnostic and pass now.

Validation:

- Windows:
  `python -B -m pytest -q tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 173 subtests, no skips.
- Linux, from `tools/ci/tests`:
  `python3 -B -m pytest -q test_collect_ci_evidence.py test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 173 subtests, no skips.
- Linux:
  `python3 -m black --check --required-version 22.3.0 .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py tools/ci/tests/test_collect_ci_evidence.py`
  passed for both changed Python files.
- `git diff --check` passed.

Pass 42 is invalidated. Lens 1 was clean; lens 2 found these issues; lenses
3-6 were not started. Both completed invocations were audited for observed
model identity, complete frozen-input coverage, and read-only source-scoped
tools without prior/sibling feedback reads.

Original artifact SHA-256 values before this appendix:

- Lens 1: `7a7492b8b8a48fe929cbc9855bd6565a18f763ab421065adbadf16893e3612f4`
- Lens 2: `c6ef0f9dd99ef3ee10a2f8bda768d5ade86108865ad55f61f968e4891db2977e`
