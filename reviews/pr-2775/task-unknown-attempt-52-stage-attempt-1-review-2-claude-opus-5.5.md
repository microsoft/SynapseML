---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 52
stage_attempt: 1
round: 2
timestamp: "2026-10-18T18:55:30Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b1e4e67a5e46d703e3b47182bf0ffb232f91e5f732ac1cf651381d6c7897d0d4"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-52-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **AGENTS.md conventions:** I checked the diff scope against AGENTS.md.
  - The diff makes no edits to `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `target/`, `AGENTS.md` or `CONTRIBUTING.md`.
  - The artifact placement in `references/review-gauntlet.md` and `references/loop-control.md#artifact-retention` matches the AGENTS.md rule: session drafts first, then `reviews/pr-<number>/`, with no placeholder folder.
- [x] **Helper layering:** the dependency chain is `watch_azure_pipeline.py` → `collect_ci_evidence.py` → `verify_trusted_guidance.trusted_executable`.
  - The verifier imports only the standard library, so it can bootstrap under `-I`.
  - Both CLI entry points set `sys.dont_write_bytecode`, which keeps the verified snapshot free of the ignored files that the verifier rejects.
- [x] **Readiness helper vs. documentation:** `Get-PrReadiness.ps1` matches `ci-evidence.md` (GitHub snapshot, comment-trigger gate) and the SKILL.md step 9 command. I checked:
  - the parameter guards (lines 100–111);
  - the `-RunPipeline` checks for complete evidence, an open PR and an absent exact aggregate (about lines 610–648);
  - the readiness, human-approval and merge states and the `completeness.complete` alias (about lines 680–713).
- [x] **Collector vs. documentation:** `collect_ci_evidence.py` matches "Azure merge and named tests". I checked:
  - `parse_args` requires the head, base and merge SHAs plus an expected suite or test;
  - `verify_merge` checks the parents in order;
  - `verify_build` checks definition 17563, `refs/pull/N/merge`, `sourceVersion` and the queue reason;
  - `main` makes a stability recheck and always sets `mergeReady=false`.
- [x] **Watcher vs. documentation:** `watch_azure_pipeline.py` matches the `ci-triage.md` waiting contract. This covers the `--repo` lock, the `--azure-timeline` base/merge requirement, the outcomes and the exit-code map.
- [x] **Comment-trigger routes:** I searched `.github/skills/` for `/azp run|RunPipeline|requeue`.
  - SKILL.md step 7, `readiness-gates.md`, `ci-triage.md` and `ci-delegation.md` step 3 all link to `ci-evidence.md#comment-trigger-gate`.
  - The external-contributor SKILL.md routes hand off to the PR loop.
- [x] **Link anchors:** all cross-reference anchors resolve: `#comment-trigger-gate`, `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`, `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`, `#github-snapshot`, `#azure-merge-and-named-tests` and `#evidence-required-before-triggering`.
- [x] **Test structure:** the suites in `tools/ci/tests/` load scripts with `spec_from_file_location`, add to `sys.path` only temporarily, and patch bytecode writing off.
  - The readiness suite uses `skipUnless(pwsh or TF_BUILD)`.
  - `test_trusted_guidance.py` runs the documented commands with `--help` only, so it makes no network calls. This fits the credential-free CIHelpers job (`pipeline.yaml` lines 111–126, `tools/ci/README.md`).
- [ ] **Not done: running tests.** Commands are not allowed in this review, so I did not reproduce the supplied pytest or Black results. I also could not check hosted-agent CIHelpers runtime against its 15-minute `timeoutInMinutes`, because local timings are not hosted CI.

## Issues

### Issue 1: Required `--merge-sha` input has no documented source
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/ci-evidence.md; .github/skills/synapseml-pr-loop/references/ci-triage.md
- **Line(s)**: ci-evidence.md 128–134 and 154–156; ci-triage.md 45–48
- **Description**: The collector's `parse_args` and the watcher's `--azure-timeline` mode both require an expected "synthetic merge SHA". `verify_build` compares it with the build's `sourceVersion`, and `verify_merge` checks that its parents are the expected base and head.
  - Every other identity input has a documented trusted source:
    - build ID: the `microsoft.SynapseML` check's details URL;
    - head and base: the readiness snapshot's `headSha` and `baseSha`;
    - kickoff time: `pipelineRunComment.created_at`, or Azure `queueTime` for manual runs.
  - No document and no helper output says where the merge SHA comes from. `Get-PrReadiness.ps1` emits no merge identity.
  - Timeline mode is advertised for when the GitHub aggregate is absent or lagging. In that case the only documented build-ID source, the aggregate's details URL, is also unavailable.
  - The promise that "historical builds remain inspectable" likewise has no documented way to get that build's revision pair.
- **Risk**: Operators have to improvise, and the obvious choice can be wrong.
  - GitHub's `merge_commit_sha` (`refs/pull/<n>/merge`) is recomputed when the target moves, so it can differ from an earlier build's `sourceVersion`.
  - The collector then fails with "Azure build provenance does not match the expected PR merge". That reads as a provenance failure rather than a wrong input, and can prompt a needless requeue, which costs a fix/integration cycle.
  - It also pushes agents towards the ad-hoc lookup scripts that `ci-delegation.md` step 5 tells them to avoid.
  - The tools fail closed, so this cannot produce a false pass.
- **Suggested Fix**: No code change is needed.
  - In `ci-evidence.md`, next to the build-ID sentence, add one sentence saying where `--merge-sha` comes from:
    - the merge revision recorded for the cleared pair, if there is one;
    - otherwise the build's `sourceVersion`, which the collector already proves has the expected base and head parents.
  - In the same sentence, note that GitHub's merge ref is recomputed when the target moves, and that without the aggregate the build ID comes from the Azure run for `refs/pull/<number>/merge`.
  - Have the timeline bullet in `ci-triage.md` link to that sentence rather than repeat it.

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
  "changeSize": "17 source, test, and documentation paths; 5016 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 52,
  "completedPriorPasses": 51,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 accepted and fixed in the existing CI evidence guide: it now identifies
the Azure run when the aggregate is absent, the recorded merge or run
`sourceVersion`, and the ordered parents for historical inspection. It warns
against substituting a recomputed live merge ref. Timeline instructions link
to this single source of guidance. No helper, test, or additional file was needed.
The changed text was checked against the collector's existing ordered-parent
and build-provenance checks; the link resolves to the existing heading.

The original review is preserved above. The executed reviewer reported
`claude-opus-5.5`; its input contained the full frozen source diff, and its
tool calls did not read sibling feedback. The reviewed predecessor was
`a96e0370dab63a22a73f56c24ec7441cd36d3622`, with base
`0543bf7e5266c587207ae8c28c3b696deeed3ed1` and source diff SHA-256
`3c4121d5e82a20a6a9cdb26daed116fdd17511da78f94b0c1d4966106a4fd951`.
The matching source manifest is `pr-validation-skills-attempt-52-manifest.json`.
This pass stopped after round 2; rounds 3-6 did not run. The documentation
correction invalidates this pass. It is not current-patch or Azure CI evidence.
