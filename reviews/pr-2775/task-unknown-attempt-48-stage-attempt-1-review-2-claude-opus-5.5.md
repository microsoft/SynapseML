---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 48
stage_attempt: 1
round: 2
timestamp: "2026-10-18T12:40:03Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "79753ca37649bd271277459b00d540ef60bab6fd1fb2ae981f5f7c661f2c3774"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-48-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope follows the AGENTS.md rules.** The diff covers 17 paths under `.github/skills/synapseml-pr-loop/`, `.github/skills/synapseml-external-contributor-review/` and `tools/ci/tests/`. It does not change `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `AGENTS.md`, `CONTRIBUTING.md` or `__init__.py`. The existing CIHelpers job (`pipeline.yaml` lines 112–126, `python3 -m pytest tools/ci/tests/ -q`) picks up the four helper suites without any pipeline edit.
- [x] **The helper modules depend on each other in one direction only.**
  - `scripts/verify_trusted_guidance.py` uses only the standard library and imports no sibling script. That is required because it is bootstrapped with `python -I -B`.
  - `scripts/collect_ci_evidence.py` imports only `GuidanceError` and `trusted_executable` from it.
  - `scripts/watch_azure_pipeline.py` (lines 17–27) reuses the collector's `command_json`, `AzureClient`, `verify_merge` and `collect_build` instead of duplicating GitHub or Azure access.
  - No helper imports a script that imports it back.
  - Both command-line entry points set `sys.dont_write_bytecode` before their sibling imports. This keeps the verified guidance snapshot free of bytecode, which `test_ignored_bytecode_cannot_supplement_trusted_guidance` treats as a failure.
- [x] **Every output field the docs name exists in the code.**
  - `references/ci-evidence.md` and the collector agree: `availableRunNames`, `expectedPublications`, `expectedTestPublications`, `issueCounts`, `pipelineDefinition`, `timelinePending`, `skippedJobs`, `allowedSkippedJobs`, `unresolvedJobs`, `jobGatesPassed` (`collect_build`, about lines 398–439; `collect_tests`, lines 614–635), and `collectionStable`, `validationPassed` and `mergeReady=False` (`main`, lines 693–706).
  - `references/ci-triage.md` and the watcher agree on `agent_loss`, `azure_completed`, `attention_required` and `replaced`, with exit codes 4, 4 and 3 (lines 357–366).
  - The readiness output contains the documented `readiness.*` states, `completeness.pipelineRunComment` and the `completeness.complete` alias (`Get-PrReadiness.ps1`, lines 680–712).
- [x] **The CI-trigger gate is intact and every comment route uses it.**
  - `Get-PrReadiness.ps1` checks its arguments before doing anything else (lines 102–110): one PR, both expected SHAs, and no `-WaitForReview` or `-RequestReview`.
  - Before posting, it requires complete evidence and an open PR (lines 612–621). The success flag `pipelineRunRequested` comes from the POST's exit status; parsing the returned comment cannot change it (lines 625–646).
  - The conservative complete-collection gate is preserved.
  - Every documented comment route links `ci-evidence.md#comment-trigger-gate`, including authorized requeues: SKILL.md step 7 (line 218), `ci-triage.md` lines 16–17, `readiness-gates.md` lines 94–99 and `ci-delegation.md` step 3. SKILL.md step 9 uses the guarded helper call with both expected SHAs.
- [x] **All cross-document links resolve.** I checked these anchors against their headings: `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`, `#evidence-invalidation`, `#artifact-retention`, `#state-and-cost-discipline`, `#targeted-infrastructure-retry`, `#waiting-for-azure-pipelines`, `#validation-only-handoff` and `#evidence-required-before-triggering`. The old SKILL.md step-8 text was moved into `references/review-gauntlet.md` rather than copied, and nothing links to the removed text.
- [x] **The new tests follow the existing `tools/ci/tests` style.**
  - They have the copyright header, use `unittest` classes that pytest collects, and load modules from paths relative to the test file.
  - The watcher tests patch the names the watcher itself imported (`watcher.collect_build`, `watcher.verify_merge`, `watcher.query_pr`), which is correct because the watcher binds them at import.
  - The readiness suite uses `skipUnless(pwsh or TF_BUILD)` (lines 244–246), matching the documented rule: fail in CI, skip locally.
  - `test_trusted_guidance.py` reads the documented `-I -B` and `-E -s -B` command lines from the docs, so the docs and the actual flags cannot drift apart.
- [x] **The documentation keeps the existing structure.** SKILL.md stays a summary and points to the reference files (`ci-evidence.md`, `review-gauntlet.md`, `trusted-guidance.md`). The no-bloat rule appears once as a process step (SKILL.md step 4) and once as a gate (`readiness-gates.md`, "User value and scope"). That matches the existing step/gate pairing and is not duplicated prose.
- [x] **I read the supplied validation evidence but did not rerun it.** It includes the pytest results on Windows and Linux, pinned Black, PowerShell parsing and `Get-Help`, `git diff --check`, and the manifest round-trip. I executed no commands or tests.
- [x] **Some candidates were considered and not reported because none is a concrete defect.**
  - `-Repo` still accepts any repository while the Azure check name is fixed. No documented step passes another repository, and the result then reports a missing check, so it fails safe.
  - `collect_build` reads `allow_skipped_job` with a `getattr` default. This intentionally gives the watcher no skip exemptions; refactoring it would be a speculative abstraction.
  - The CIHelpers job has a 15-minute timeout. The largest supplied local timing is 506.56 s, still under 900 s; timing on hosted agents is unverified, so this is speculative.
  - The verifier's `GUIDANCE_PATHS` leaves out `.agents/`. That directory is only a pointer (`.agents/README.md`), and `trusted-guidance.md` lists the verified paths explicitly.
- [x] **Independence was maintained.** I did not open, search or glob `reviews/`, the session state, prompts, checkpoints or other reviewers' output. Searches stayed within `.github/skills/`, `tools/ci/`, `pipeline.yaml`, `pyproject.toml`, `AGENTS.md`, `CONTRIBUTING.md` and `.agents/`.
- [ ] **Not checked: whether the working tree matches the staged index.** Running commands was not permitted. I reviewed the working-tree copies and relied on the supplied path/mode/blob manifest round-trip check (SHA-256 `0c88845c…`).

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4853 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 48,
  "completedPriorPasses": 47,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`0c88845cb68201d5bfd121dfaafcee5192e83a9071eb69aedf817a1f25fed50c`.
This round was clean. A test change after round 5 invalidated the pass;
round 6 did not run. This is not qualifying final review or CI evidence.
