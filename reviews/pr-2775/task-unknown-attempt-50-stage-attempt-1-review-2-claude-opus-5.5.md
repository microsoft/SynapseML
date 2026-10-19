---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 50
stage_attempt: 1
round: 2
timestamp: "2026-10-18T15:49:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b2d04124ff91cbaeab2edab78dd53d7c318f714d4478d52204d5f5b9cfa7025b"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-50-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scripts read in full.** `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `verify_trusted_guidance.py` and `watch_azure_pipeline.py`.
- [x] **References and skill docs read.** `ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md` and `trusted-guidance.md`, plus both `SKILL.md` files and `synapseml-external-contributor-review/references/ci-delegation.md`.
- [x] **Module layering is clean.**
  - The verifier has no sibling imports, so it works under `python -I`.
  - The collector imports only `GuidanceError` and `trusted_executable` from it.
  - The watcher imports collector primitives. The import graph has no cycles.
  - `sys.dont_write_bytecode` is set before sibling imports. A glob found no `__pycache__` under `.github/skills/`; bytecode caches exist only under `tools/ci/`.
- [x] **Docs match the code.**
  - The `ci-evidence.md` readiness table matches the `readiness` and `completeness` objects in `Get-PrReadiness.ps1` (`remoteGatesClear` at about lines 561–568).
  - The `-RunPipeline` guards (lines 104–111) and the trigger block (about lines 607–648) match `ci-evidence.md#comment-trigger-gate`.
  - The watcher's exit-code map and its `azure_completed` and `attention_required` outcomes match `ci-triage.md`.
  - The collector's `verify_build` and `collect_tests` match "Azure merge and named tests".
- [x] **Comment-trigger routes are centralized.** A grep for `/azp run|-RunPipeline|comment-trigger` in `.github/skills/` shows every comment route goes through `ci-evidence.md#comment-trigger-gate`:
  - SKILL.md steps 7 and 9
  - `ci-triage.md`
  - `readiness-gates.md`
  - `ci-delegation.md` step 3
  - the requeue route in `ci-evidence.md`

  The conservative complete-collection gate is unchanged.
- [x] **Anchors resolve to real headings.** All link targets below exist:
  - `ci-evidence.md`: `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
  - `loop-control.md`: `#evidence-invalidation`, `#artifact-retention`, `#state-and-cost-discipline`
  - `ci-triage.md`: `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `ci-delegation.md`: `#evidence-required-before-triggering`
  - `SKILL.md`: `#validation-only-handoff`
  - `readiness-gates.md`: `#user-value-and-scope`
- [x] **CI wiring needs no pipeline edit.** The `pipeline.yaml` CIHelpers job (lines 112–126) runs `python3 -m pytest tools/ci/tests/ -q` on `ubuntu-22.04`. The new suites are picked up automatically, which respects the AGENTS.md approval rule for pipeline changes.
- [x] **Test conventions match the repo.**
  - The new suites load scripts with `importlib.util.spec_from_file_location`, the same pattern as `test_website_doctest.py`, and suppress bytecode writes.
  - The readiness harness replaces `gh` with a fail-closed fake that rejects any unexpected GitHub call. CIHelpers therefore makes no network calls and needs no credentials.
  - Under `TF_BUILD`, a missing `pwsh` makes the readiness suite fail rather than skip, as `ci-triage.md` documents.
- [x] **Credential header checked by pattern match.** The tool display masked line 150 of `collect_ci_evidence.py` as `******`. A pattern match confirmed the header is built from `self.token` (`****** and the test asserts the header for the `fixture-token` fixture.
- [x] **Verifier path coverage is consistent.** `GUIDANCE_PATHS` covers AGENTS.md, CONTRIBUTING.md, `.github/skills`, CODEOWNERS, `pipeline.yaml` and `templates`. `.agents/` holds only pointer READMEs, and `trusted-guidance.md` explicitly excludes it.
- [ ] **No tests were run by this reviewer; one documented suite is missing from the current evidence.** The supplied evidence covers three of the four helper suites. `tools/ci/tests/test_trusted_guidance.py` is in `ci-triage.md`'s four-suite command and runs the collector and watcher with their documented flags. It was last run only in the full-directory baseline, which predates the latest readiness and collector changes. Read statically, the three doc command lines that suite parses (verifier, collector and watcher) still match its regular expressions. CIHelpers will run it.
- [ ] **Hosted-agent runtime not observed.** On Linux, the three suites alone took 493.72 s, against CIHelpers' `timeoutInMinutes: 15`. The final CI-qualified pass needs to confirm the whole directory fits on hosted agents. This is a residual risk, not a finding.

## Issues

### Issue 1: The no-bloat gate is written twice with different rules, and the step 4 copy skips the artifact-retention precondition
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/SKILL.md`; `.github/skills/synapseml-pr-loop/references/readiness-gates.md`
- **Line(s)**: SKILL.md 150–155; readiness-gates.md 22–26
- **Description**: The same scope gate is defined in two places, worded independently.
  - **SKILL.md step 4** runs during the fast loop. It requires every hunk to "support an acceptance criterion, fix a demonstrated defect, or satisfy a repository requirement." It says to remove "useless files … duplicate documentation, and verbose filler" from the full PR diff, keeping only "necessary evidence."
  - **readiness-gates.md "User value and scope"** uses a different test: "concrete purpose in the requested change." It is the only copy that routes superseded artifacts through `loop-control.md#artifact-retention`. It drops step 4's demonstrated-defect and repository-requirement allowances and the "do not trade correctness or traceability" clause.

  This breaks the PR's own single-source pattern: step 8 points to `review-gauntlet.md`, and every comment route points to `ci-evidence.md#comment-trigger-gate`. It also violates the gate's own ban on duplicate documentation.
- **Risk**:
  - Agents meet step 4 first, before final target integration. It lets them delete superseded `reviews/pr-<n>/` artifacts as "duplicate documentation" without the retention preconditions. Retention allows pruning only after final integration and after confirming the archival commits are reachable from the pushed head and `refs/pull/<n>/head`.
  - With many superseded passes, this can remove the only tracked copy early or break indexed links.
  - Two different admissibility tests can also disagree on whether a tightly coupled defect fix belongs in the diff, and they will keep drifting apart.
- **Suggested Fix**: Keep a single definition in `readiness-gates.md#user-value-and-scope` and merge step 4's acceptance test and its correctness/traceability clause into it. Replace SKILL.md lines 150–155 with a short pointer: "Apply the [no-bloat gate](references/readiness-gates.md#user-value-and-scope) to the full PR diff. Prune superseded review artifacts only under [artifact retention](references/loop-control.md#artifact-retention). Record the scope audit in the checkpoint." This also reduces the line count.

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
  "changeSize": "17 source, test, and documentation paths; 4988 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 50,
  "completedPriorPasses": 49,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Maintainer disposition

The original feedback is preserved. The pending follow-up consolidates the
no-bloat rule under `readiness-gates.md#user-value-and-scope`: the admissibility
test, correctness/traceability protection, artifact-retention precondition, and
checkpoint requirement now have one definition. SKILL.md step 4 links to it and
explicitly includes the retention preconditions. This removes two net lines and
changes no executable code. Both link targets were checked.

The previously missing current trusted-guidance integration evidence was added:
`python -B -m pytest tools/ci/tests/test_trusted_guidance.py -q --tb=short -rs`
passed on Windows (14 tests, 12 subtests, 60.25 seconds; two intentional POSIX
symlink skips) and Linux (16 tests, 12 subtests, no skips, 12.36 seconds).
This complements, rather than reruns, the other three current helper suites.
Hosted-agent CI and publication remain outstanding. The documentation change
requires a new frozen-patch review; this is not a clean result for that patch.
