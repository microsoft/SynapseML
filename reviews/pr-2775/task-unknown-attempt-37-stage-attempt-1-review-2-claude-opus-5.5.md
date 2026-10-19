---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 37
stage_attempt: 1
round: 2
timestamp: "2026-10-17T23:42:09Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "02af6e355eb8e7ffdab1e3e2330aff976e7b10f2f0e035470a058289d8619c28"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-37-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector read in full, and the watcher reuses it.** I read all of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`: `command_json` L39, `verify_merge` L167, `verify_build` L182, `summarize_timeline` L223, `collect_build` L397, `collect_tests` L458, `parse_args` L646, `main` L691.
  - `watch_azure_pipeline.py` L14-27 imports these helpers instead of copying the GitHub/Azure clients.
  - Dependencies run one way with no cycle: watcher → collector → `verify_trusted_guidance.trusted_executable`. This matches the `-E -s -B` sibling-import rule in `references/trusted-guidance.md`.
- [x] **Bytecode and import handling is the same everywhere.**
  - Both importers set `sys.dont_write_bytecode` under `__main__` before their sibling import (collector L18-21, watcher L14-17).
  - The verifier imports nothing from its own directory and is started with `-I -B`.
  - The tests load these modules with the same patch: `tools/ci/tests/test_collect_ci_evidence.py` L21-32, `test_watch_azure_pipeline.py` L18-34, `test_trusted_guidance.py` L15-22.
  - Two tests read the launch flags out of the reference docs and run them (`test_trusted_guidance.py` L108-152 and L161-201), so a doc change that breaks the commands fails a test.
- [x] **Evidence, engineering, human-approval, and merge states are computed separately** in `scripts/Get-PrReadiness.ps1`.
  - `$rulesComplete` (L369) is computed before workflow collection, so later workflow errors cannot change human approval.
  - `$evidenceComplete` (L556), `$remoteGatesClear` (L558), `$humanApproval` (L566), the `readiness` object (L665), and the `completeness.complete` alias (L694) match the table in `references/ci-evidence.md` and the gate text in `references/readiness-gates.md`.
  - The ruleset and classic-protection allowlists (L339, L350) match the documented ignored types and keys.
- [x] **Trigger and review-request guards match the docs.** The guards at L101-109, L589, and L612-631 agree with `SKILL.md` step 9, `references/ci-evidence.md`, and step 3 of the external `ci-delegation.md`.
  - `-RunPipeline` needs one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`.
  - `-RequestReview` needs exactly one PR.
  - The review request ignores unrelated policy errors while the CI trigger requires complete evidence; this difference is intentional and documented.
- [x] **Every new link resolves to an existing heading.**
  - `loop-control.md` L7, L68, L135; `ci-triage.md` L6, L184; `ci-evidence.md` L7, L95; external `ci-delegation.md` L25; pr-loop `SKILL.md` L95 (`#validation-only-handoff`).
  - Searching `.github/skills`, `AGENTS.md`, and `CONTRIBUTING.md` for `REVIEW-PROMPTS|manual prompt|assemble` finds only the prohibition row at `loop-control.md` L220. No leftover manual prompt-assembly instructions contradict the `-Execute` runner contract in `references/review-gauntlet.md`.
- [x] **Every documented flag and output field exists in the code.**
  - Watcher: `parse_args` (L272-314); the `azure_completed` and `attention_required` outcomes both map to exit 4 in `main()`.
  - Collector: flags at L646-688; fields `timelineType`, `issueCounts`, `failureStageHint` (L373-383), `publicationErrors` (L576), and `availableRunNames`, `expectedPublications`, `expectedTestPublications` (L621-640).
  - Verifier: `--checkout` and `--expected-sha` (L155-171); `GUIDANCE_PATHS` and `REQUIRED` (L15-33).
- [x] **The diff follows the AGENTS.md rules.**
  - It changes nothing in `pipeline.yaml`, `.github/workflows/`, release tooling, or dependency pins.
  - The `CIHelpers` job in `pipeline.yaml` (L112-126) already runs `python3 -m pytest tools/ci/tests/`, so the new suites run in CI without pipeline edits.
  - The readiness harness replaces `gh` with an offline stub (`test_get_pr_readiness.py` L20-126). Without `pwsh` it fails under `TF_BUILD` instead of skipping (L225-228).
  - `AGENTS.md` and `CONTRIBUTING.md` are unchanged. The Spark 4.1 sentence at `ci-triage.md` L177 repeats existing text in `synapseml-branches/references/branch-spark3p5.md` L24.
- [x] **No duplicated guidance (no-bloat check).**
  - Step 8 is now a link to `references/review-gauntlet.md` (`SKILL.md` L261-265), following the existing pattern of moving detail from `SKILL.md` into `references/`.
  - The external-contributor skill links to the shared bootstrap, the collectors, and the validation-only handoff instead of copying them (external `SKILL.md` L18-20 and L90-91; `ci-delegation.md` L78-80).
  - The no-bloat rule appears as an action in step 4 and as a gate in readiness-gates, following the existing action/gate pairing. The two versions do not conflict.
- [x] **Considered and not flagged.** None of these is a defect today; fixing them would only guard against hypothetical future edits, which the review scope excludes.
  - `collect_build` reads the collector-only `allow_skipped_job` option with `getattr` (L404). The watcher's arguments therefore give no skip exemptions, which `ci-triage.md` documents and `test_collect_ci_evidence.py` L510-545 tests.
  - The `microsoft.SynapseML` name is hardcoded (L461, L469, L620) next to the configurable `-RequiredCheck` (L86). This enforces the documented exact-aggregate rule. A custom `-RequiredCheck` replacing the default is older behavior, not new in this diff.
  - `Get-PrSnapshot` is long, which affects maintainability only.
- [x] **Supplied validation evidence reviewed but not re-run.** I read the reported Windows and Linux pytest totals for the readiness, guidance, watcher, and collector suites, plus the prerequisite-guard runs, Black, and `git diff --check`. They are consistent with the code above; I executed no commands or tests.
- [ ] **Live GitHub/Azure API behavior was not checked.** This was a read-only review with no command execution. I used the supplied API-format facts only as context.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4258 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 37,
  "completedPriorPasses": 36,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
