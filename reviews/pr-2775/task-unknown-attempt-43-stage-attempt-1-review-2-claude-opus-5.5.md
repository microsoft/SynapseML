---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 43
stage_attempt: 1
round: 2
timestamp: "2026-10-18T05:51:55Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "48e4644ee6503a1523cc630866ed5970e5ea6c3b815312aaee7a2bc9cd52131d"
---

The review is complete and found three Low issues: consistency and documentation gaps in the validation helpers. Nothing here affects product code or loosens the CI-trigger gate. I didn't run any tests; runtime results come from the validation evidence you supplied.

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-43-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope:** I reviewed the full diff of 17 paths and re-read each changed source on disk. In `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` I checked:
  - parameter guards (L98-106) and `Get-RestEvidence` (L238-272);
  - rules, classic protection and workflow scoping (L321-438);
  - required checks (L450-473), the fresh recheck and readiness states (L518-577);
  - the review-request guard (L587-606) and the `-RunPipeline` gate (L612-637);
  - the output object (L640-703) and the wait loop (L705-744).
- [x] **Module layering:** In `collect_ci_evidence.py` (`command_json`, `AzureClient`, `verify_merge`, `verify_build`, `summarize_timeline`, `collect_build` L397-443, `collect_tests`, `main`) and `watch_azure_pipeline.py` (imports L17-27, `monitor`, `parse_args`, exit-code map), the dependency order is verifier → collector → watcher, with no cycles. Both entry points set `sys.dont_write_bytecode` before importing sibling modules, so the verified guidance tree stays clean.
- [x] **Trusted-guidance verifier:** `verify_trusted_guidance.py` lists `CODEOWNERS`, `pipeline.yaml` and `templates` in `GUIDANCE_PATHS`/`REQUIRED`; all exist at the repository root. `trusted_executable` is shared by the verifier's `git()` and the collector's `command_json`, which matches `trusted-guidance.md`.
- [x] **CI discovery:** `pipeline.yaml` L112-126 runs `python3 -m pytest tools/ci/tests/ -q` on `ubuntu-22.04`, so the new suites run without a pipeline change, consistent with the AGENTS.md rule to ask before changing `pipeline.yaml`.
  - I searched the changed Python files for APIs that need Python 3.11+ (such as `enterContext`, `contextlib.chdir`, `datetime.UTC`, `tomllib`, `except*`, `ExceptionGroup`, `batched`) and found none. That matters because the image's default `python3` is 3.10.
- [x] **Test harness:** `tools/ci/tests/test_get_pr_readiness.py` L233-235 (`skipUnless(pwsh or TF_BUILD)`) matches the `ci-triage.md` statement that a missing `pwsh` fails the suite in Azure Pipelines but skips it locally. The loaders in `test_collect_ci_evidence.py` L21-32 and `test_watch_azure_pipeline.py` L19-35 restore `sys.path` and suppress bytecode.
- [x] **Links:** These anchors exist at their headings:
  - `ci-evidence.md#comment-trigger-gate`, linked from SKILL.md step 7, `ci-triage.md`, `readiness-gates.md` and `ci-delegation.md` step 3, as links rather than copies;
  - `loop-control.md#artifact-retention` and `#evidence-invalidation`;
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`;
  - `ci-delegation.md#evidence-required-before-triggering`;
  - `SKILL.md#validation-only-handoff`.
- [x] **Usage consistency:** Every documented `-RunPipeline` command includes both expected SHAs (SKILL.md step 9). No document shows a multi-PR `-RequestReview`. `contributor-safety.md` mentions `-RunPipeline` only as forbidden while a contribution is blocked. The step-8 extraction links `references/review-gauntlet.md`. A search for leftover manual prompt-assembly text (`REVIEW-PROMPTS`, `manual prompt`, `generated/manual`) in `.github/skills`, AGENTS.md and CONTRIBUTING.md found none.
- [x] **Port branches:** The branch skill references (`branch-spark4-common.md:94`, `branch-spark3p5.md:26-27`) define no other name for the Azure aggregate check. Requiring exactly `microsoft.SynapseML` (case-sensitive) does not conflict with port-branch guidance.
- [x] **Conventions:** `tools/ci/README.md` lists only some suites (L75, L80, L183), so not listing the new helper suites there matches existing practice; their documentation lives in `ci-triage.md`.
- [x] **`allJobsSucceeded` usage:** Searching `.github/skills` and `tools/ci/tests` shows it is produced only at `collect_ci_evidence.py:439-442` and appears only in test assertions (L455, L531) and fixture data (L834, L856). No code reads it and no document describes it.
- [ ] **Tests not executed:** This lens is read-only. Runtime results come from the supplied validation evidence, not from this review.

## Issues

### Issue 1: Undocumented `allJobsSucceeded` output contradicts the documented job gate
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 439-442 (tests: tools/ci/tests/test_collect_ci_evidence.py 455, 531, 834, 856)
- **Description**: `collect_build` emits `allJobsSucceeded`, computed only from `state == "completed"` and `result == "succeeded"`. Unlike `jobGatesPassed` (L405-415, L438), it ignores `issueCount` and `agentLossDetected`.
  - A completed job that Azure reports as `succeeded` but that has timeline warnings or errors, including issues inherited from a Stage or Phase, gives `allJobsSucceeded: true` next to `jobGatesPassed: false`.
  - `ci-evidence.md` L172-178 says warnings, errors and untyped issues "block the automatic job gate even when Azure reports success". It documents the other job-gate outputs (`jobGatesPassed`, `skippedJobs`, `allowedSkippedJobs`, `unresolvedJobs`, `timelinePending`) but never this one.
  - Nothing reads the field: `main()` (L697-707) does not, and the watcher only passes it through in its `azure` payload, including on the exit-0 `success` outcome. `ci-triage.md` L60-62 points watcher users to `jobGatesPassed`/`unresolvedJobs` only.
  - The only extra information it carries, that a waived skip occurred, is already in `skippedJobs`/`allowedSkippedJobs`.
- **Risk**: Collector and watcher JSON contains an undocumented success-looking total that disagrees with the documented gate. A reader could claim "all Azure jobs succeeded" without the required manual review of timeline issues. The tests also lock in an output contract nobody uses, which is the kind of addition the PR's own no-bloat gate rejects.
- **Suggested Fix**: Remove `allJobsSucceeded` and its test assertions and fixture keys, relying on `jobGatesPassed`, `skippedJobs` and `allowedSkippedJobs`. The field is new in this PR, so removing it costs no compatibility. If the "passed without waivers" signal is genuinely required, define it as `jobGatesPassed and not skippedJobs` and document it in `ci-evidence.md` next to `jobGatesPassed`.

### Issue 2: The comment-trigger gate places the helper's error diagnostics after the hand-posted requeue route
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/ci-evidence.md
- **Line(s)**: 95-112
- **Description**: SKILL.md step 7, `ci-triage.md`, `readiness-gates.md` and `ci-delegation.md` all link to this paragraph as the single canonical gate. Its sentence order is:
  1. The `-RunPipeline` helper and why it requires complete collection (L95-102).
  2. The hand-posted `/azp run` requeue route (L102-106).
  3. "Its error identifies the evidence gaps. Run without `-RunPipeline` to inspect the snapshot. Obtain the missing evidence or … use a separately authorized pinned-queue route" (L107-110).

  Grammatically, "Its" now refers to the hand-posted comment, which produces no error. The diagnostic actually comes from the helper's throw (`Get-PrReadiness.ps1` L613-619). It is also unclear whether the obtain-evidence or pinned-queue alternative applies to a requeue whose fresh snapshot has `readiness.evidenceComplete=false`.
- **Risk**: Instructions for two routes are interleaved in the gate that every comment route must follow. Someone on the requeue route could look for an error that never appears, or apply the alternative to the wrong route. That weakens the clarity this PR aimed for by making the section canonical.
- **Suggested Fix**: Reorder the text without changing policy:
  1. Helper description and rationale.
  2. "The helper's error lists the gaps; run without `-RunPipeline` to inspect the snapshot."
  3. The requeue sentences.
  4. One sentence stating which route or routes the obtain-evidence and pinned-queue alternative covers.

  Keep the complete-collection requirement unchanged.

### Issue 3: Script help still presents `-RunPipeline` as an unconditional trigger
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 30-35
- **Description**: The `.DESCRIPTION` says the Azure build "waits for an `/azp run` comment -- so `-RunPipeline` posts one." The script actually refuses `-RunPipeline` when:
  - either expected SHA is missing, or it is combined with `-WaitForReview` or `-RequestReview` (L104-106);
  - evidence is incomplete (L612-620);
  - the PR is not open (L621-623).

  It posts only when the exact, case-sensitive `microsoft.SynapseML` check is absent (L627-631). In the same help block, the `-RequestReview` paragraph (L26-28) states its preconditions and that it does not authorize CI. The `-RunPipeline` sentence states neither, so the main help text disagrees with the parameter comment (L84-85) and with `ci-evidence.md#comment-trigger-gate`.
- **Risk**: `Get-Help` describes the helper's only CI-triggering action, the one that runs PR code against protected resources, as unconditional. Users learn the preconditions only from errors, and the help drifts from the guarded contract this PR introduced.
- **Suggested Fix**: Replace the clause with a short description of the guarded behavior. For example: "`-RunPipeline` posts one only for a single open PR whose expected head/base SHAs match complete evidence and whose exact Azure aggregate is absent; it does not authorize CI." Optionally mention the comment-trigger gate.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending triage by the driving agent
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending triage by the driving agent
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending triage by the driving agent
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
  "changeSize": "17 source, test, and documentation paths; 4420 added and 178 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 43,
  "completedPriorPasses": 42,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution

**Issue 1 resolved:** removed the new, undocumented `allJobsSucceeded` field,
its two redundant assertions, and two fixture entries. Repository search
confirmed it had no consumer outside those tests. The documented
`jobGatesPassed`, unresolved-job and skip evidence remain unchanged, including
the assertions that timeline issues and missing timelines cannot pass.

**Issue 2 resolved:** grouped the helper's diagnostic instructions directly
after its requirements, then the manual requeue route, then the common
incomplete-evidence alternative. The latter explicitly applies to either
comment route. This only clarifies the existing policy.

**Issue 3 resolved:** the script's help now states the single-open-PR,
expected-revision, complete-evidence, and absent-exact-aggregate requirements;
it also states that the flag does not authorize CI and cannot be combined
with review requests or waiting. A blank line after `#Requires` is necessary
for PowerShell to recognize the comment-based help: the original adjacency
produced autogenerated syntax-only help. Isolated layout probes established
the minimal fix and were removed afterward.

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
- PowerShell parsing and
  `Get-Help .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1 -Full`
  passed; the rendered description includes the guarded behavior.
- Comparison with the frozen index confirmed every byte after the PowerShell
  help block is unchanged after line-ending normalization. The passing
  readiness-suite evidence therefore still covers the execution body.
- `git diff --check` passed.

Pass 43 is invalidated. Lens 1 was clean; lens 2 found these issues; lenses
3-6 were not started. Both completed transports were audited for actual
reviewer identity, complete frozen-input coverage, and read-only source-scoped
tools without prior/sibling feedback reads.

Original artifact SHA-256 values before this appendix:

- Lens 1: `114cdb6dc590d043022782cbfe531dd4e5bbdd2277bde4bb54daa7119f4dda16`
- Lens 2: `3a90833b732d4b136b649bcf7d7b0d323ec19aef0bcd4110ba56e6e16e133c8b`
