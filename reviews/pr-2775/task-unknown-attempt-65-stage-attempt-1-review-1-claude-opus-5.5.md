---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 65
stage_attempt: 1
round: 1
timestamp: "2026-10-19T09:09:46Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "20cbe967cf494410f791c464fe58c9e0bae037644cc979727f85d4eb0fdf7f41"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-65-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I reviewed the whole supplied merge-base-to-index diff across all 17 paths: skill docs, four helpers and four test modules. Each helper's control flow was checked against the documented contract rather than the PR's own claims.

- [x] **`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`** (read in full). These sections match `references/ci-evidence.md`:
  - Ruleset and classic-protection evaluation (about lines 326–369).
  - Revision rechecks and how the evidence, engineering and human-approval states are derived (about lines 520–579).
  - The readiness output (about lines 684–691).

  Human approval falls back to `unknown` in each of these cases:
  - incomplete rules;
  - a non-integer required-approval count;
  - a nonzero count with a null `reviewDecision`;
  - a code-owner, last-push or named-reviewer requirement.

  `merge` is never more than `requires_engineering_evidence`. The complete-collection `-RunPipeline` gate is retained.

- [x] **`collect_ci_evidence.py` transport.**
  - `command_json` uses fixed operation labels and does not echo arguments, stderr or response bodies.
  - Encoding failures are handled before JSON failures.
  - A Windows batch timeout kills the whole process tree, and a cleanup failure keeps the PID in the error.
  - `AzureClient` allowlists endpoints, refuses redirects, clips timeouts to the deadline, and pages with `$skip`/`$top` while detecting duplicate IDs.
  - The `Authorization` header at line 168 really is `****** The viewer masked it; a literal-match count confirmed it.

- [x] **Collector provenance.**
  - `verify_merge` requires the exact ordered `[base, head]` parents.
  - `verify_build` pins definition 17563, the GitHub repository, `refs/pull/<n>/merge`, `sourceVersion` and the build reason.
  - A second `collect_build` after the test reads drives `collectionStable`.
  - `validationPassed` requires a completed, succeeded build plus the job gates, the tests and stability.

- [x] **Job-attempt matching.** Test runs are keyed by `phaseName.jobName` (line ~560) and matched against timeline Job identifiers (line ~519). `pipeline.yaml` has only top-level `jobs:` at line 111, with no explicit stages, so this key shape fits. A mismatch gives "lacks matching job-attempt provenance", which fails closed and never passes. Leaf Phase/Stage containers count toward job gates but never supply test-attempt evidence.

- [x] **`watch_azure_pipeline.py`** (read in full). These all match `references/ci-triage.md`:
  - the deadline is taken from the run's kickoff, not the watcher start;
  - the merge is verified once, with a clipped timeout;
  - `agent_loss` is reported once per job identifier and attempt;
  - the watcher keeps waiting while Azure is still running;
  - `azure_completed` and `attention_required` both exit with code 4;
  - `replaced` and `superseded` outcomes, and the Python 3.10 kickoff parsing.

- [x] **`verify_trusted_guidance.py`.**
  - Git is run with `GIT_*` variables stripped, `--no-replace-objects` and `core.fsmonitor=false`.
  - It compares HEAD, the index, working-file bytes, and untracked and ignored files.
  - It rejects symlinks and submodules.
  - Executable lookup skips the current directory, its descendants and relative `PATH` entries.
  - Every `REQUIRED` path exists: root `CODEOWNERS` (no `.github/CODEOWNERS` takes precedence), `pipeline.yaml` and `references/contributor-safety.md`.

- [x] **CI coverage.**
  - The `CIHelpers` job in `pipeline.yaml` (lines 112–126) runs `python3 -m pytest tools/ci/tests/ -q`, so all four helper suites run there.
  - `test_get_pr_readiness.py` (lines 249–251) fails rather than skips without `pwsh` when `TF_BUILD` is set, as the docs state.
  - The test harness replaces `gh` with an in-process function, so CI makes no network calls and posts no triggers or review requests.

- [x] **Documentation consistency.**
  - Every comment route, including requeues, goes through `ci-evidence.md#comment-trigger-gate`. That covers both steps 7 and 9 of the PR-loop `SKILL.md`, plus `ci-triage.md`, `readiness-gates.md` and step 3 of `ci-delegation.md`.
  - All linked section anchors resolve, including `#evidence-required-before-triggering`, `#evidence-invalidation`, `#waiting-for-azure-pipelines`, `#user-value-and-scope` and `#validation-only-handoff`.

- [x] **No-bloat and scope gate.** The diff changes no product API, dependency pin, release tool, `pipeline.yaml` or workflow definition. Each new file is either referenced by the skills or exercised by `CIHelpers`. I found no redundant files or filler.

- [x] **Candidates checked and dismissed:**
  - Skipping a final poll exactly at the deadline is the deliberately tested boundary.
  - Query errors that end the watcher are documented as a nonzero exit.
  - Timeline warnings blocking the automatic job gate is documented, conservative behaviour.
  - PowerShell command discovery relies on a trusted shell, which `trusted-guidance.md` documents.

- [x] **Supplied validation evidence** (Windows Python 3.14.6 and Linux Python 3.10.20 runs, mutation experiments, Black 22.3.0). I checked it for consistency with the code but did not re-run anything.

- [ ] **Live API shapes are unverified.** I could not check these without network access:
  - Azure timeline identifier format and the nullability of count fields.
  - The test-run `pipelineReference` shape.

  The code fails closed if any of these differ.

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
