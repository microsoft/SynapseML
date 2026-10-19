---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 26
stage_attempt: 1
round: 2
timestamp: "2026-10-17T16:19:35Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6e281f1af6757a5e203e1a8e2a4aa239b9fa38077bfa9059d3e1d6913806d747"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-26-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all four changed helpers in full: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py` and `Get-PrReadiness.ps1`. Traced where they call each other:
  - the watcher calls `verify_merge`/`collect_build` (`watch_azure_pipeline.py:138-146`);
  - the collector calls `trusted_executable` (`collect_ci_evidence.py:20`, `38-55`);
  - the readiness object is built at `Get-PrReadiness.ps1:630-659`.
- [x] Compared `references/review-gauntlet.md` with the SKILL.md step-8 text it replaces. It keeps the frozen-patch rule, the diff manifest, `reviews/pr-<number>/` placement, collision and independence rules, invalidation and the bootstrap handoff. The retention/index rule in `references/loop-control.md:14-29` keeps the original artifacts reachable, so it does not conflict with AGENTS.md "Preserve attempt/round/model filenames".
- [x] The readiness contract is consistent across `Get-PrReadiness.ps1:630-659`, `references/ci-evidence.md:65-70`, `references/readiness-gates.md:124` and `SKILL.md:91-92`. `completeness.complete` is treated as an alias only, everywhere.
- [x] Cross-skill links resolve:
  - `synapseml-external-contributor-review/SKILL.md` → `../synapseml-pr-loop/references/trusted-guidance.md`
  - `references/ci-delegation.md` → `../../synapseml-pr-loop/references/ci-evidence.md`
  - the anchors `loop-control.md#checkpoint-before-yielding`, `#evidence-invalidation` and `ci-triage.md#waiting-for-azure-pipelines` exist.
- [x] The new tests follow the existing spec-loading pattern (`tools/ci/tests/test_watch_azure_pipeline.py:18-33`, `test_website_doctest.py:11-15`). The `CIHelpers` job runs them (`pipeline.yaml:112-125`).
- [x] AGENTS.md scope: the diff does not touch `pipeline.yaml`, workflows, release tooling or dependency pins. `AGENTS.md` and `CONTRIBUTING.md` gain no version-specific text.
- [x] Considered and rejected two possible findings:
  - **Moving `trusted_executable` out of the verifier.** The bootstrap runs the verifier under `python -I` (`references/trusted-guidance.md:15-28`), which drops the script directory from imports. The verifier therefore has to stay self-contained.
  - **Exit code 4 shared by `azure_completed` and `attention_required`.** This is documented (`ci-triage.md:46-49`), and the JSON `outcome` field tells them apart.
- [ ] No commands or tests were run; this is a read-only review. The caller-supplied validation results were used as context and not reproduced.

## Issues

### Issue 1: Pipeline and repository identity is duplicated across the coupled helpers
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`; `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: watcher 16-23, 27-30, 104-107; collector 22-26, 429; readiness 78-85, 448, 456, 585, 693
- **Description**:
  - **Python.** The watcher already imports six names from `collect_ci_evidence` and is documented as reusing the collector. Even so, it defines its own `REPOSITORY`, `AZURE_PROJECT_ID` and `MAX_BUILD_ID`, and builds its own build-results URL (lines 104-107, same as collector line 429). The watcher's URL allowlist (`AZURE_BUILD_PATHS`), `--repo` guard and build-ID bound use one copy. The collector's `API_ROOT`, its repository check in `verify_build` and its evidence `url` use the other.
  - **PowerShell.** The Azure aggregate name `"microsoft.SynapseML"` is a bare literal at five sites. How `-RequiredCheck` matches an entry depends on whether it equals that literal: exact and case-sensitive if it does, case-insensitive prefix if not. This behavior is documented at lines 78-84, but it is implemented by repeating the literal at each site.
- **Risk**: The copies can drift apart.
  - A project or repository change applied to only one Python module would make the watcher trust and link a different project from the one the collector queries. This fails closed, but with confusing errors or links.
  - In PowerShell, updating four of the five sites would leave `-RunPipeline` (line 585) or the missing-aggregate warning (line 693) keyed to a stale name. That could cause a spurious or skipped `/azp run`.
- **Suggested Fix**:
  - In the watcher, import `REPOSITORY`, `AZURE_PROJECT_ID` and `MAX_BUILD_ID` from the collector, which is already imported. Add a small `build_results_url(build_id)` in the collector and use it from both scripts. Keep the watcher-only `CHECK_NAME` and the `a365` alias local.
  - In `Get-PrReadiness.ps1`, define one script-scoped `$AzureAggregateCheck = "microsoft.SynapseML"` and use it at all five sites.
  - No new files are needed.

### Issue 2: Shared collector functions take a CLI `argparse.Namespace` as an implicit cross-tool interface
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Line(s)**: collector 166, 181, 396-446 (especially 403 and 435-437), 457-461; watcher 138-150, 271-313, 346
- **Description**:
  - `verify_merge(args)`, `verify_build(build, args)`, `collect_build(client, args)` and `collect_tests(client, args, jobs)` each take a whole parser `Namespace` as input.
  - The watcher passes in a Namespace built by its own, different parser. The only sign of this cross-tool contract is `getattr(args, "allow_skipped_job", [])` (line 403). The sibling `collect_tests` reads `args.allow_skipped` directly (line 461).
  - The watcher cannot accept `--allow-skipped-job`, yet `collect_build` still computes policy fields for it with an empty exemption list: `unresolvedJobs`, `jobGatesPassed` and `allowedSkippedJobs: []`. The watcher then includes that `azure` object in its `success`, `azure_completed`, `attention_required` and `timeout` results.
  - Some jobs are skipped by design, for example the notebook E2E jobs under the selective-E2E rules in `tools/ci/README.md`. For those builds, a watcher `success` can therefore carry `azure.jobGatesPassed: false` and list the intentional skips as unresolved.
- **Risk**:
  - Readers and linters cannot see what the shared functions actually require.
  - If `collect_build` or `verify_build` later reads another collector-only attribute, the watcher raises `AttributeError`. That error bypasses the watcher's `EvidenceError` → `MonitorError` handling (lines 147-150 and 346). The run ends with a traceback, prints no `finished` JSON event, and exits 1, the same code as `failed`/`error`.
  - Combining fact collection with gate evaluation makes the watcher emit gate verdicts that look authoritative but were computed without the policy input. This can mislead triage. The docs partly mitigate this by requiring a fresh collector run after every monitor exit.
- **Suggested Fix**:
  - Give the shared functions explicit parameters: `pull_request`, `build_id`, `head_sha`, `base_sha`, `merge_sha` and `allowed_skipped_jobs=()`. Alternatively, pass a small frozen dataclass that each CLI's `parse_args` builds.
  - Split `collect_build` into a fact collector (build plus summarized jobs) and `evaluate_job_gates(jobs, allowed_skipped_jobs)`.
  - Have the watcher use only the fact collector, or leave the gate fields out of its output.
  - Remove the `getattr` fallback. All of this stays within the existing two files.

### Issue 3: `Get-PrSnapshot` handles failures of comparable evidence reads in two different ways
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`; `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Line(s)**: readiness 98, 237-271, 311-322, 504-508, 664-681; ci-evidence 40-41, 76-77
- **Description**:
  - `Get-RestEvidence` records a failed read in `collectionErrors`, and the snapshot is still emitted (fail-closed). This applies to the target-branch read (line 311), rules, classic protection, workflow runs, the PR-head lookup and the fresh target-branch recheck (line 508).
  - The live-tip `compare` (lines 318-321) and the fresh PR re-read (lines 504-506) `throw` instead. Within the same revision-stability check, the branch recheck is a soft failure and the PR recheck is a hard failure.
  - `-WaitForReview` calls `Get-PrSnapshot` again on every poll for up to 240 minutes (lines 64 and 680) and builds `$results` across all `-PullRequest` numbers (line 664). With `$ErrorActionPreference = "Stop"` (line 98), one transient GitHub failure on either of those reads ends the script with no JSON for any PR.
  - This contradicts the documented model. `ci-evidence.md:40-41` says API failures produce `collectionErrors`, and lines 76-77 say later polls report revision errors in the snapshot "instead of aborting".
- **Risk**: Long waits break on transient GitHub errors, and multi-PR runs lose all the evidence they had collected. Callers have to handle two failure shapes: an exception, or `evidenceComplete=false`. Behavior differs from the documented contract. Because both paths fail closed, this cannot produce a false green.
- **Suggested Fix**:
  - Read the compare through `Get-RestEvidence`. A null `$compare` already fails `behind_by -eq 0` and `evidenceComplete`.
  - On a failed fresh PR re-read, add "Could not recheck PR revisions" to `$revisionErrors` instead of throwing. The existing request guard will still block a review request that has not been attempted yet.
  - Keep `throw` only for the initial `gh pr view` and the deliberate `-RequestReview`/`-RunPipeline` guards.
  - Add one fake-`gh` regression test: a failing compare and recheck during a `-WaitForReview` poll still emits a snapshot.

### Issue 4: Bytecode hygiene relies on an import-time global side effect that the repository's own test path bypasses
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`; `tools/ci/tests/test_collect_ci_evidence.py`; `tools/ci/tests/test_watch_azure_pipeline.py`; `tools/ci/tests/test_trusted_guidance.py`; `.github/skills/synapseml-pr-loop/references/ci-triage.md`; `tools/ci/README.md`
- **Line(s)**: collector 18-20; watcher 14-16; verifier 127-128; test_collect_ci_evidence 21-31, 716-733; test_watch_azure_pipeline 18-33; test_trusted_guidance 15-21; ci-triage 85-87; tools/ci/README 70-73
- **Description**:
  - The verifier rejects any untracked or ignored file in the guidance paths, including `__pycache__` (verifier lines 127-128). The helpers try to avoid creating bytecode by setting interpreter-global state between their imports: `sys.dont_write_bytecode = True`.
  - That protects only the sibling imports of a script run as `__main__`. This is also the only case tested (`test_normal_cli_invocation_does_not_dirty_the_guidance_snapshot`).
  - When a helper is loaded by an import loader instead, the loader compiles and caches it before its body runs. This covers the repository's tests (`spec.loader.exec_module`), `python -m`, and any other importer. The flag then stays set for the rest of that process, including the whole pytest session.
  - The documented test commands (`ci-triage.md:87`, `tools/ci/README.md:72`) omit `-B`, unlike the documented helper invocations (`-I -B`, `-E -s -B`). Running those commands therefore writes `.pyc` files under `.github/skills/synapseml-pr-loop/scripts/__pycache__/`. The reviewed checkout has untracked entries there for all three helpers.
- **Risk**:
  - A maintainer who runs the documented tests in a trusted guidance worktree, a natural check before relying on the helpers, makes `verify_trusted_guidance.py` reject that snapshot. `trusted-guidance.md` treats that mismatch as a blocker and forbids resetting the checkout.
  - A library module silently changing interpreter behavior for its importers is hidden coupling. Future helpers are likely to copy the pattern.
- **Suggested Fix**:
  - Limit the side effect to the process that owns it: `if __name__ == "__main__": sys.dont_write_bytecode = True`, placed before the sibling import.
  - In the test loaders, disable bytecode only while the module executes, for example `with patch.object(sys, "dont_write_bytecode", True): SPEC.loader.exec_module(module)`.
  - Add `-B` to the documented test commands, matching the commands used for validation. `pipeline.yaml` does not need to change because CI agents are ephemeral.
  - Optionally, extend the existing no-`__pycache__` test to cover the import path as well.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
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
  "changeSize": "17 source, test, and documentation paths; 4182 added and 151 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 26,
  "completedPriorPasses": 25,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition after round 2

The original response and verdict above are preserved. Its SHA-256 before this
append was `6084f3715aa3a6a5acfadbe9122b986666f032548ded54b532f4e85fa0421510`.
Round 1 was clean; round 2 ended this pre-commit pass. Rounds 3-6 did not run.
These fixes require a new full pass; this is not qualifying clean evidence.

### Issue 1: Rejected as speculative refactoring

The reviewed copies identify the same fixed repository, project, build-ID
bound, and exact Azure aggregate. No current query, link, allowlist, or trigger
uses a different value. The proposed risk requires a future incomplete
configuration change; neither CLI supports changing these identities today.
Introducing a URL wrapper and reorganizing constants does not fix a demonstrated
behavior defect in this patch. Keep the existing scope rather than expanding
it for hypothetical future drift.

### Issue 2: Rejected; conservative monitor diagnostics are intentional

Both parsers supply the revision and build attributes consumed by the shared
functions. The watcher does not call `collect_tests`; its lack of that
function's selector attributes cannot cause the reported `AttributeError`.
The proposed failure requires adding a new untested collector-only dependency.
The real watcher-to-collector path is covered by the timeline integration test.

The monitor deliberately supplies no skip exemptions. Its output therefore
retains `allowedSkippedJobs: []` and unresolved conditional jobs until a caller
audits and supplies policy to the collector. `success` describes the named
GitHub check, not `jobGatesPassed`, passing tests, or full readiness. CI triage
requires fresh readiness and actual job/test assessment after every monitor
exit. Removing these conservative diagnostics or introducing separate data
transfer and gate-evaluator abstractions would not correct the documented
contract. No gate was weakened.

### Issue 3: Fixed and covered by a failing-before regression

Target comparison now uses `Get-RestEvidence`. A failed, malformed, or empty
final PR read records collection and revision errors instead of terminating an
otherwise usable snapshot. Existing not-yet-attempted review-request and CI
trigger guards still reject incomplete revision evidence.

`test_failed_revision_reads_during_poll_preserve_blocked_snapshot` reproduces
comparison failure and failed, malformed, or empty rechecks on the second poll.
It verifies blocked JSON and the single earlier review request, not a repeated
side effect. All four cases failed before the fix. The evidence documentation
now distinguishes these failures from initial PR or paginated review-query
failures, which cannot construct a snapshot and still terminate collection.

### Issue 4: Fixed without changing CI or adding files

Collector and watcher bytecode suppression now applies only to direct CLI
execution, before sibling imports. Test loaders temporarily suppress bytecode
while loading guidance modules, then restore the caller's setting. The focused
documented test command includes `-B`. The broader documented pytest command
also stays safe because those loaders no longer write guidance caches.

`test_importing_helpers_preserves_callers_bytecode_setting` imports all three
helpers in isolated subprocesses; the collector and watcher cases failed
before the fix. Existing direct-CLI cache-hygiene tests remain passing.
The Linux run below deliberately omitted `-B` and started with no guidance
bytecode files; the guidance cache remained empty afterward.

### Validation and scope

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_trusted_guidance.py -rs --tb=short`
  — **117 passed, 1 POSIX-only skip, 259 subtests passed**.
- Linux, from `tools/ci/tests`, with PowerShell 7.6.7 on PATH:
  `python3 -m pytest -q test_get_pr_readiness.py test_collect_ci_evidence.py test_watch_azure_pipeline.py test_trusted_guidance.py -rs --tb=short`
  — **118 passed, 259 subtests passed, no skips**.
- Black 22.3.0 and whitespace checks passed. No pipeline, workflow, dependency,
  release, or product API changes were made.
- Both reviewer transports contain only source, policy, and test reads.
  No prior/sibling feedback was read. Round 1's empty search failed before
  returning evidence. Actual reviewer identity was `claude-opus-5.5`.
- Current-candidate Azure CI remains unrun pending authorization; local results
  and this disposition do not establish merge readiness.
