# Review Round 6 — Polish & Hardening

## Summary
- Round: 6
- Theme: Polish & Hardening
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Scope: I read all of the frozen `review.diff`: 17 files and 2,933 lines. Its SHA-256 matches the manifest (`152561506548BA78…6C13`).
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:448-450` — `evidenceComplete` is true only when there are no revision errors and no collection errors. `remoteGatesClear` is built on top of it.
  - At `:496`, `-RunPipeline` throws if evidence is incomplete.
  - At `:573`, `complete` is set to `$remoteGatesClear`.
  - Several other behaviours match the docs:
    - It rechecks the branch's current tip.
    - It pages through rulesets and workflows and checks the `total_count`.
    - It matches the required-check total exactly.
    - It reads classic branch protection only when `protection.enabled != false`.
  - When evidence is missing, the script blocks instead of passing.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:378` — job IDs are built as `phaseName.jobName`.
  - `pipeline.yaml` has a single stage, with top-level `jobs:` and matrix legs, so the `Phase.Leg` keys line up.
  - `PublishTestResults@2` has no `testRunTitle`, and the `Unit Test` and `Generate Codecov report` task names match the hint patterns.
  - Lines `:232-251` detect lost agents and set the phase to `agent_loss`.
  - The f-string at `:101` is correct. The file viewer displays it as `******`, but I checked the actual characters.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:127-182` — lost-agent events are reported once per `(identifier, attempt)` through `reported_agent_losses`.
  - The merge is verified once.
  - Each poll creates a new `AzureClient` with a deadline.
  - An Azure run still in progress takes priority over GitHub status.
  - At `:263` and `:374`, `attention_required` maps to exit code 4.
  - All of this matches `.github/skills/synapseml-pr-loop/references/ci-triage.md:6`.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:84` — it rejects untracked files under `GUIDANCE_PATHS` using `git ls-files --others`. The tree/index, symlink and CRLF checks also work.
- [x] All new cross-reference anchors resolve:
  - `.github/skills/synapseml-pr-loop/references/ci-triage.md:172` (Targeted infrastructure retry)
  - `.github/skills/synapseml-pr-loop/references/loop-control.md:53` (State and cost discipline)
  - `.github/skills/synapseml-pr-loop/references/loop-control.md:120` (Evidence invalidation)
  - `.github/skills/synapseml-pr-loop/references/ci-evidence.md:7` (GitHub snapshot)
  - `.github/skills/synapseml-external-contributor-review/references/ci-delegation.md:25` (Evidence required before triggering)

  `review-gauntlet.md` is step 8 of the old `SKILL.md` copied word for word, with its relative link depths fixed.
- [x] `.github/skills/synapseml-pr-loop/references/ci-triage.md:83-85` — only two places in the skill guidance mention `tools/ci/`:
  - The watcher regression test path and its pytest command, here.
  - A link to documentation at `.github/skills/synapseml-branches/references/branch-spark4-common.md:58`.

  No skill tells agents to run helper scripts from outside the trusted guidance paths.

No high-confidence bugs, security problems, broken links or contradictions between the docs and the scripts were found.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `7a170b130578dbeb1faf1c5838cfb730bd1c99b268ec6afd6683bbed52c20b0a`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
