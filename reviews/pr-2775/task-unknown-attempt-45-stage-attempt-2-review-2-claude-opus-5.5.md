---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 45
stage_attempt: 2
round: 2
timestamp: "2026-10-18T09:30:12Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d91296794799886996ac241ce9b4550adcd3830698d712ec2128614ee1adda55"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-45-stage-attempt-2-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope and repository rules.** All 17 changed paths are under `.github/skills/synapseml-pr-loop/`, `.github/skills/synapseml-external-contributor-review/` or `tools/ci/tests/`.
  - The diff does not change `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `AGENTS.md` or `target/`. This follows the AGENTS.md "ask before changing" rule.
  - A glob of `.github/skills/synapseml-pr-loop/scripts/` finds only the four helpers. No `__init__.py` or bytecode was added.
- [x] **How the Python helpers import each other.** `scripts/watch_azure_pipeline.py` (L14-27) imports from `scripts/collect_ci_evidence.py`. That file imports `trusted_executable` and `GuidanceError` from `scripts/verify_trusted_guidance.py` (L21).
  - `references/trusted-guidance.md` (L18-30) requires the verifier to start under `python -I`, which blocks imports from its own folder. That makes the verifier the only place `trusted_executable` can live without being copied, so the placement is sound.
  - Both entry scripts set `sys.dont_write_bytecode` before importing sibling modules.
- [x] **Links and anchors.** I checked these targets with heading greps:
  - `ci-evidence.md#comment-trigger-gate` (L93) and `#azure-merge-and-named-tests` (L121)
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry` (L188)
  - `loop-control.md#artifact-retention` (L53), `#state-and-cost-discipline` (L71) and `#evidence-invalidation` (L138)
  - `SKILL.md#validation-only-handoff`
  - `ci-delegation.md#evidence-required-before-triggering`

  The contributor skill links to the PR loop's bootstrap, handoff and trigger gate rather than copying them.
- [x] **Every comment-trigger route uses the same gate.** These all point to `ci-evidence.md#comment-trigger-gate`, including the hand-posted requeue:
  - `SKILL.md` steps 7 and 9
  - `ci-triage.md` L16-17
  - `readiness-gates.md` L99
  - `ci-delegation.md` step 3

  `Get-PrReadiness.ps1` matches the gate:
  - L101-110: one PR, both expected SHAs, and refusal of `-WaitForReview` and `-RequestReview`
  - L617-623: complete evidence collection
  - L625: open PR
  - L631: exact aggregate absent

  The conservative complete-collection gate was not challenged, as the plan directs.
- [x] **Readiness states.** `Get-PrReadiness.ps1` L561-581 and L676-707 emit the fields listed in `ci-evidence.md` L73-80. No state authorizes a merge, and the collector always emits `mergeReady: false`.
- [x] **Watcher and collector agree with the docs.**
  - `watch_azure_pipeline.py` L358-364 map both `attention_required` and `azure_completed` to exit code 4, matching `ci-triage.md` L50-53.
  - Timeline mode reuses the collector's `verify_merge` and `collect_build` instead of a second implementation.
  - The watcher's lack of skipped-job exemptions matches `ci-triage.md` L60-63.
- [x] **Test conventions.** The new suites follow the existing `test_watch_azure_pipeline.py` pattern: MIT header, `spec_from_file_location`, and a `sys.path` insertion removed in `finally`.
  - `test_get_pr_readiness.py` L234-236 skips locally when `pwsh` is missing and fails under `TF_BUILD`, as documented.
  - The Black pin `22.3.0` is confirmed in `environment.yml` L57.
- [x] **Docs that mirror each other on purpose.** The no-bloat rule appears both in `SKILL.md` step 4 and in `readiness-gates.md` L20-25. This follows the existing pattern where each process step has a matching gate, so it is not flagged.
  - `review-gauntlet.md` keeps the old step-8 rules except the deliberate change for runners that cannot write pre-PR session drafts.
- [ ] **Tests were not run by this reviewer** (read-only tools); I relied on the supplied local evidence.
  - One observation, not a finding: `test_get_pr_readiness.py` L238-262 starts a new `pwsh` process for each scenario.
  - The `CIHelpers` job has `timeoutInMinutes: 15` (`pipeline.yaml` L114, L125), so the job has a 15-minute limit. The supplied 486.94 s run is local only; the duration on hosted agents is unproven.

## Issues

### Issue 1: Collector functions read the command-line arguments object of two different CLIs, and a `getattr` default hides the watcher's skip policy
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 167, 182, 397-404 (callers: `watch_azure_pipeline.py` 139-147; collector `main` 688-692)
- **Description**: The watcher reuses `verify_merge(args, ...)`, `verify_build(build, args)` and `collect_build(client, args)`. Their input is any object whose attribute names match the collector's command-line options: `build_id`, `pull_request`, `head_sha`, `base_sha` and `merge_sha`.
  - The watcher's parser has no `--allow-skipped-job` option, so L404 uses `getattr(args, "allow_skipped_job", [])`.
  - The documented rule that the watcher grants no skipped-job exemptions (`ci-triage.md` L60-63) is therefore enforced only because another CLI's arguments object lacks an attribute. The function signatures do not show it.
- **Risk**: The shared functions depend on two independent argument parsers.
  - Renaming an option, or adding a same-named option to the watcher, silently changes collector behavior. For example, the watcher's `jobGatesPassed` diagnostics would start excusing skipped jobs.
  - If a required attribute goes missing, the watcher fails with an unhandled `AttributeError` instead of `EvidenceError` or `MonitorError`.
- **Suggested Fix**: Make the inputs explicit and remove the `getattr`.
  - Change the signature to `collect_build(client, args, allowed_skipped_jobs=())`, or pass a small identity tuple or dataclass for the build ID, PR and SHAs.
  - Pass `args.allow_skipped_job` from the collector's `main`; the watcher uses the default.
  - Update `tools/ci/tests/test_collect_ci_evidence.py` L593-594 to pass the argument instead of setting `self.args.allow_skipped_job`.

### Issue 2: The new retry sentence in loop-control.md repeats the procedure it links to
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/loop-control.md
- **Line(s)**: 125-127 (repeats `ci-triage.md` 190-210; the same rule also appears at 28, 129-131 and 254)
- **Description**: L125 links the targeted retry procedure, and L126-127 immediately restate it: "A retry that retains its build ID also retains its original kickoff/deadline; record advanced job attempts and preserve successful-job evidence."
  - The linked section already covers this: record kickoff/deadline, job IDs and attempts (`ci-triage.md` L190-194); successful jobs and their tests must remain (L204-205); resume with the original kickoff and two-hour deadline (L208-210).
  - The same file repeats the rule three more times: the checkpoint fields (L28), the monitoring contract (L129-131: "Restarting a watcher cannot extend that deadline") and the scenario-table row (L254).
- **Risk**: This breaks the PR's own no-bloat gate, which bans duplicate documentation (SKILL.md step 4). It also goes against AGENTS.md, which prefers a link to the source of truth over copied text. The deadline rule gains another copy that can drift from the procedure.
- **Suggested Fix**: Delete L126-127 and keep the link on L125. The scenario-table row at L254 remains as the acceptance case.

### Issue 3: review-gauntlet.md restates the pre-PR session-draft rule several times in one paragraph
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/review-gauntlet.md
- **Line(s)**: 39-47 (overlaps 21-23 and 66-71)
- **Description**: L22-23 already say that drafts go in the session workspace before publication, and L66-71 repeat the rule for new PRs.
  - The new L39-47 adds two needed points: stop if the runner cannot write session-local review artifacts, and `-RunDir` only moves transport files.
  - It also restates the rule three more times:
    - L39: "Before a PR exists, the repository requires session-only drafts."
    - L45-46 repeats L40-42: "A runner enforcing in-repository direct output is incompatible with this pre-PR path."
    - L46-47 repeats L43-45: "Check the installed runner's capability rather than assuming that accepting a session transport directory means it supports session drafts."
- **Risk**: The PR's scope and its own no-bloat gate exclude verbose filler. Saying one rule several ways in a brand-new reference makes it slower to read and leaves more sentences that must stay consistent.
- **Suggested Fix**: Replace L39-47 with one sentence that keeps both new points:
  > If the installed runner cannot write session-local review artifacts under the direct `-Execute` contract (`-RunDir` only relocates transport files), stop for a trusted compatible process; do not invent a PR number, assemble substitute reviews, or claim the pre-commit gate passed.

  Keep L21-23 as the placement rule.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
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
  "changeSize": "17 source, test, and documentation paths; 4767 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 45,
  "completedPriorPasses": 44,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver dispositions after review

The original feedback above is preserved. No later lenses ran in this pass.

| Issue | Disposition and evidence |
| --- | --- |
| 1 | Not accepted as a current defect. Both in-repository argparse producers require and validate the shared identity attributes. Watcher tests exercise `verify_merge` and `collect_build` with its actual parsed arguments through queued/running/completed builds; collector tests separately cover skipped-job blocking and explicit exemptions. The missing optional attribute intentionally means no exemption. The proposed future option rename/addition is hypothetical, not a demonstrated incompatible caller. Adding an identity DTO or a second argument surface solely for that possibility would conflict with the requested no-bloat scope. No interface or policy change was made. |
| 2 | Addressed. Removed the two duplicate retry sentences from `loop-control.md`. The retained link resolves to `ci-triage.md#targeted-infrastructure-retry`, which still requires preserving successful jobs, advancing only authorized attempts, and using the original kickoff/deadline for the same build ID. |
| 3 | Addressed. Condensed the runner-compatibility paragraph in `review-gauntlet.md` from nine lines to four. It retains the direct `-Execute` session-artifact requirement, the distinction from `-RunDir` transport storage, and all stop/no-substitution safeguards. The placement and publication rules remain intact. |

Only the two documentation files changed. `git diff --check` passed, and the
linked retry procedure and retained artifact-placement requirements were read
and compared. No command, heading, code or test changed, so the existing full
Linux result (370 tests and 338 subtests, no skips) and Windows affected-suite
result remain applicable; the suites were not needlessly rerun.

These documentation edits invalidate pass 45. The next pass must review the
entire updated patch. No current-head Azure CI or merge readiness is claimed.
