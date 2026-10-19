---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 44
stage_attempt: 1
round: 2
timestamp: "2026-10-18T06:35:45Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "fa171055aa1435c6e782b044cfa1a8996f26d110011e694c144d22e4484da71e"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-44-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The helpers are layered without cycles and don't duplicate infrastructure.**
  - `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` L15–27 reuses these from the collector: `AzureClient`, `command_json`, `github_json`, `collect_build`, `verify_merge`, and the repository, project and build-ID constants.
  - `collect_ci_evidence.py` L18–21 reuses `trusted_executable` and `GuidanceError` from `verify_trusted_guidance.py`. The verifier imports nothing from its sibling scripts.
  - Only the two scripts that import siblings set the bytecode guard (`sys.dont_write_bytecode`), which keeps the trusted snapshot clean.
  - Tests covering this: `tools/ci/tests/test_collect_ci_evidence.py` L718 and L737, and `tools/ci/tests/test_watch_azure_pipeline.py` L739.
- [x] **The comment-trigger gate has one source document and one enforcement point.**
  - The section `### Comment-trigger gate` in `references/ci-evidence.md` (L93–119) is linked from four places:
    - `SKILL.md` L218
    - `references/ci-triage.md` L17
    - `references/readiness-gates.md` L99
    - `synapseml-external-contributor-review/references/ci-delegation.md` L70
  - Every other `/azp run` mention either routes through the PR loop (external `SKILL.md` L41 and L66) or restricts triggering (`contributor-safety.md` L26 and L76, `ci-delegation.md` L93, `trusted-guidance.md` L68).
  - `Get-PrReadiness.ps1` enforces the gate, and this matches both the doc and the script's help text (L35–39):
    - parameter guards at L102–110
    - expected-SHA checks at L558–559
    - checks at L617–639 for complete evidence, an OPEN PR, and an absent `microsoft.SynapseML` aggregate matched case-sensitively; the error at L623 lists the evidence gaps
  - The conservative rule requiring complete collection before a trigger is retained. The documented alternatives are a pinned-queue route that needs separate authorization, and a hand-posted requeue that needs a fresh snapshot with `evidenceComplete=true`.
  - Tests: `test_get_pr_readiness.py` L377, L384 and L719.
- [x] **The readiness states are kept separate and named consistently.**
  - The output table in `ci-evidence.md` (L73–80) matches the script help (L41–47): `evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval` and `merge`.
  - `completeness.complete` stays as a compatibility alias with stricter meaning, and `SKILL.md` L92–93 tells readers to use the separate states.
  - Tests: `test_get_pr_readiness.py` L453, L470, L579 and L645.
- [x] **Trusted-tool lookup is implemented once, and its platform difference is documented.**
  - `trusted_executable` (`verify_trusted_guidance.py` L40–70) serves all three Python helpers.
  - `references/trusted-guidance.md` documents:
    - excluding the current directory from tool lookup;
    - the resulting limitation when running from a home directory or filesystem root;
    - that the PowerShell helper relies on a trusted shell, because it uses normal PowerShell command lookup.
  - The documented commands use consistent Python flags: `python -I -B` for the verifier (`trusted-guidance.md` L18) and `python -E -s -B` for the other helpers (`ci-evidence.md` L128, `ci-triage.md` L28).
- [x] **The skill keeps detail out of `SKILL.md` and in linked reference files.**
  - `SKILL.md` step 8 is now just a link to `references/review-gauntlet.md`, which holds the moved text.
  - That file hardcodes no model IDs. It leaves reviewer execution and model identity to the installed `/review-code` `-Execute` contract.
  - Which skill owns what is stated explicitly (`trusted-guidance.md` L3–6).
  - All of these anchors resolve: `#comment-trigger-gate`, `#validation-only-handoff`, `#artifact-retention` (`loop-control.md` L53), `#evidence-invalidation` (L138), `#evidence-required-before-triggering` and `#waiting-for-azure-pipelines`.
- [x] **The change follows the AGENTS.md rules.**
  - It doesn't change `pipeline.yaml`, workflows, release tooling, dependency pins, the product API, or JVM signatures.
  - The new test suites run under the existing `CIHelpers` job (`pipeline.yaml` L112–126).
  - Tests check that secrets are never echoed (`test_collect_ci_evidence.py` L699 and L792).
  - Review artifacts go in `reviews/pr-<n>/`, with drafts kept in the session workspace until a PR exists.
  - `loop-control.md` L53–69 never deletes the only copy of an artifact or rewrites feedback. Before pruning, it requires checking that archived commits are still reachable from `refs/pull/<n>/head`.
- [x] **The code runs on the declared runtimes.**
  - `CIHelpers` uses the ubuntu-22.04 system Python, which is 3.10.
  - A grep of the scripts and test suites found no features that need Python 3.11 or later.
  - `parse_kickoff` (`watch_azure_pipeline.py` L259–275) handles Azure's 7-digit fractional seconds, which Python 3.10 can't parse directly.
  - `#Requires -Version 7.2` at the top of `Get-PrReadiness.ps1` matches the "PowerShell 7.2+" requirement in both skills' frontmatter.
  - The readiness suite can't silently skip in CI: at `test_get_pr_readiness.py` L234 it skips only when PowerShell is missing and the run isn't in Azure Pipelines.
- [x] **Output formats and exit codes are consistent across helpers.**
  - JSON keys are camelCase and outcome values are snake_case.
  - The watcher's exit codes and the collector's rule of exiting 0 only when `validationPassed` match `ci-triage.md` and `ci-evidence.md`.
- [x] **No-bloat audit: no unnecessary changes found.**
  - Every changed path maps to the stated scope.
  - No speculative options or abstraction layers were added.
  - `tools/ci/README.md` was left unchanged, which fits its existing partial list of test suites. The new suites are documented in `ci-triage.md` L96.
  - Three trade-offs were judged acceptable rather than defects, since the only cost is to hypothetical future changes:
    - `trusted_executable` lives in the verifier module rather than a new file.
    - The collector's `collect_build` and `verify_merge` read the command-line arguments shared by two scripts.
    - `Get-PrSnapshot` remains one large function, and the PowerShell test harness covers it end to end.
- [ ] **Not done: running tests.** This was a read-only review. The pytest, Black and `git diff --check` results come from the supplied evidence and were not re-run.
- [ ] **Not done: confirming the reviewed files match the frozen patch byte for byte.** That relies on the supplied SHA-256 manifest check (`3084e3ce…`).

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4423 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 44,
  "completedPriorPasses": 43,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
