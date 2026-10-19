---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 2
round: 2
timestamp: "2026-10-18T16:59:44Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "c5b500a069e52b76fef23916b2435e429c3c779bd8a41a4b28744164821d50d7"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-2-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and repository conventions:** I checked the diff against the repository rules in `AGENTS.md`.
  - All 17 paths are under `.github/skills/synapseml-pr-loop/`, `.github/skills/synapseml-external-contributor-review/`, or `tools/ci/tests/`.
  - Nothing changes in `pipeline.yaml`, workflows, release tooling, dependency pins, `target/`, `AGENTS.md`, `CONTRIBUTING.md`, or the `.agents/` pointers.
  - The `CIHelpers` job is unchanged (`pipeline.yaml` lines 111-126). It runs `python3 -m pytest tools/ci/tests/ -q`, so it picks up the new suites without a pipeline edit.
- [x] **Each contract is defined in one place.**
  - The comment-trigger gate is defined only in `references/ci-evidence.md` under `### Comment-trigger gate`. These files link to it:
    - `SKILL.md` lines 213-214 and 227-228
    - `references/ci-triage.md` lines 16-17
    - `references/readiness-gates.md` lines 100-101
    - `synapseml-external-contributor-review/references/ci-delegation.md` line 70
  - The no-bloat gate is defined only in `references/readiness-gates.md` lines 21-28; `SKILL.md` links to it at line 150. A search of `.github/skills/` for bloat, filler, useless and speculative wording finds no second definition.
  - Step 8 is now a link to `references/review-gauntlet.md`, which keeps the earlier step-8 text and adds the `-Execute` and artifact-retention rules.
- [x] **`Get-PrReadiness.ps1` matches its documentation.**
  - Line 1, `#Requires -Version 7.2`, matches the "PowerShell 7.2+" compatibility field in both skills.
  - The guards at lines 102-111 match the gate's "one PR, both expected SHAs, refuses `-WaitForReview`/`-RequestReview`".
  - The trigger at lines 611-647 matches the documented behavior:
    - It throws when collection is incomplete.
    - It requires an `OPEN` PR.
    - It checks for `microsoft.SynapseML` case-sensitively with `-cnotin`.
    - It returns an `id`/`html_url`/`created_at` receipt.
  - The `readiness` and `completeness` output fields (lines 684-714) match the output table in `ci-evidence.md`.
- [x] **The Python helpers are layered cleanly.**
  - `watch_azure_pipeline.py` (lines 17-27) reuses `AzureClient`, `collect_build`, `verify_merge`, `command_json`, `github_json` and the shared constants from `collect_ci_evidence.py`. It does not duplicate the Azure or GitHub access code.
  - The collector gets `trusted_executable` from `verify_trusted_guidance.py` (line 21).
  - Every `args.*` attribute read in `verify_merge`, `verify_build` and `collect_build` (collector lines 203-475) exists on the watcher's namespace (watcher `parse_args`, lines 277-319).
  - The `getattr` default for `allow_skipped_job` matches the statement in `ci-triage.md` that the watcher has no skipped-job exemptions.
- [x] **Watcher outcomes and exit codes match `ci-triage.md` lines 45-62.**
  - Codes in `main`: success 0, failed/error 1, superseded 2, replaced 3, attention_required/azure_completed 4, timeout 124, interrupted 130.
  - In timeline mode, the watcher keeps waiting while the Azure build is not completed. An empty timeline sets `jobGatesPassed` to false, which matches the claim that an absent timeline never counts as passing.
- [x] **The trusted bootstrap is consistent.**
  - `GUIDANCE_PATHS` and `REQUIRED` (`verify_trusted_guidance.py` lines 15-34) match `references/trusted-guidance.md`, including CODEOWNERS, `pipeline.yaml`, `templates/` and `verifiedPaths`.
  - The verifier imports no sibling modules, so the documented `-I` bootstrap works.
  - The collector and watcher import siblings, which the documented `-E -s -B` run mode supports.
- [x] **All cross-skill links and anchors resolve to existing headings:**
  - `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
  - `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
  - `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `#evidence-required-before-triggering`, `#validation-only-handoff`
- [x] **The tests follow the existing patterns.**
  - The new and modified suites under `tools/ci/tests/` load scripts the same way as the earlier suites: `importlib` with `sys.dont_write_bytecode` patched, adding the scripts directory to `sys.path` only temporarily.
  - They have Microsoft copyright headers and platform-scoped skip decorators.
  - The readiness suite's `pwsh`/`TF_BUILD` skip condition matches `ci-triage.md` lines 101-103.
- [x] **Python formatting and version compatibility.**
  - The only lines over 88 characters are string literals Black cannot split: collector lines 466, 632 and 640; verifier lines 29 and 166; watcher lines 197 and 238.
  - The watcher's `parse_kickoff` handles a Python 3.10 limitation, which suits the ubuntu-22.04 `CIHelpers` job.
- [x] **Independence:** I did not open or search `reviews/`, checkpoints, prompts or session evidence. All searches were limited to `.github/skills/`, `tools/ci/` or named files.
- [ ] **Not verified (not a finding):** whether the hosted agent finishes `CIHelpers` within `timeoutInMinutes: 15`.
  - The only timings are supplied local Linux runs: 493.72 s and 12.36 s for the four helper suites.
  - The earlier 486.94 s full-directory run predates the latest changes.
  - I ran no commands or tests. All runtime results here are the supplied evidence.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4986 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 51,
  "completedPriorPasses": 50,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Subsequent disposition

Round 5 identified two public-path coverage gaps. Extending the existing tests
invalidated this pass. This original clean round is retained as historical
evidence, not counted toward the changed patch's review.
