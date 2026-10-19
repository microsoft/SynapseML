---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 1
round: 2
timestamp: "2026-10-18T16:35:30Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4b20ab3b87a6728efeaa0813486fa531d10faf6775d7b47ea4ff06fff1cae96e"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Entry-point skills checked against the current files.** These are `.github/skills/synapseml-pr-loop/SKILL.md`, `.github/skills/synapseml-external-contributor-review/SKILL.md` and `references/ci-delegation.md`.
  - SKILL.md covers the trusted-guidance bootstrap, the separate readiness states, the validation-only no-change outcome, the no-bloat link (lines 150–151) and the gauntlet extraction (line 261).
  - Every comment route, including requeues, links to `references/ci-evidence.md#comment-trigger-gate`.
  - All 26 relative skill anchors resolve to existing headings.
- [x] **`Get-PrReadiness.ps1` guards and outputs.**
  - The parameter guards (lines 49–111) make `-RunPipeline` require `-ExpectedHeadSha` and `-ExpectedBaseSha`, so it works on one PR only. It also refuses to run with `-WaitForReview` or `-RequestReview`.
  - The output (lines 683–716) keeps `evidenceComplete`, `remoteGatesClear`, `readiness.engineering` and `merge` separate. `readiness.engineering` never reports itself ready.
  - This matches `ci-evidence.md` and SKILL.md step 9. The conservative complete-collection trigger gate is kept as intended.
- [x] **Python module layering is one-directional.**
  - `watch_azure_pipeline.py` (lines 17–27) imports `command_json`, `AzureClient`, `collect_build` and `verify_merge` from `collect_ci_evidence.py`.
  - `collect_ci_evidence.py` (line 21) imports `trusted_executable` from `verify_trusted_guidance.py`. Nothing imports in the reverse direction.
  - Both entry points set `sys.dont_write_bytecode` before the sibling import (watcher lines 15–16, collector lines 18–19). This matches the `-I -B` / `-E -s -B` flags in `trusted-guidance.md` and the verifier's rejection of untracked or ignored files in guidance paths.
- [x] **Collector constants match the repository.** Definition 17563, the project GUID, `refs/pull/N/merge` and `phaseName.jobName` job keys are consistent with:
  - the top-level `jobs:` in `pipeline.yaml` (no stages);
  - the CIHelpers job (`pipeline.yaml` lines 112–126);
  - the watcher's `AZURE_PROJECTS` list of trusted build-URL paths.
- [x] **Watcher exit codes are documented once.** The outcome map (`watch_azure_pipeline.py` lines 357–367) appears only in `ci-triage.md` lines 53–54. Grep found no copy in `ci-evidence.md`.
- [x] **Verifier paths match the real tree.** `GUIDANCE_PATHS` and `REQUIRED` (`verify_trusted_guidance.py` lines 15–33) match the root `CODEOWNERS`, `pipeline.yaml` and `templates/`. The optional `.github/copilot-instructions.md` is absent, and the verifier would detect it if it appeared.
- [x] **AGENTS.md conformance.** The diff does not change:
  - `pipeline.yaml`, workflows, release tooling or dependency pins;
  - any product API;
  - `AGENTS.md` or `CONTRIBUTING.md`.

  It adds no credentials. The `reviews/pr-<n>/` and session-draft placement rules are restated consistently in `review-gauntlet.md` and `loop-control.md`.
- [x] **Tests follow the existing pattern.**
  - The four helper suites under `tools/ci/tests/` reuse the existing watcher test's loader: `spec_from_file_location`, a temporary `sys.path` entry and bytecode suppression.
  - They use unittest style and copyright headers.
  - `test_trusted_guidance.py` runs the flags documented in the references.
  - CIHelpers picks the suites up through `tools/ci/tests/` without a pipeline change.
- [x] **Retention-index classification traced across files (Issue 1).** Checked `loop-control.md` lines 53–67, 101, 140–146, 182–185, 195–199 and 237, `review-gauntlet.md` lines 26–29 and 44–45, and `SKILL.md` lines 150–151.
- [ ] **Tests not run by this reviewer.** Under the read-only mandate, I did not run pytest, Black or the PowerShell checks. The Windows and Linux results in the prompt are context only.
- [ ] **CIHelpers runtime on hosted agents is unverified.** The new suites take about 494 s + 12 s on local Linux, and the job's `timeoutInMinutes` is 15. No Azure run exists for this candidate. The final CI-qualified run must confirm this; it is a residual risk, not a finding.

## Issues

### Issue 1: Artifact-retention index fits no allowed path class under the final path-set gate
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/loop-control.md
- **Line(s)**: 57–58, 64–67, 101, 146, 182–185, 195–198, 237. Related: `review-gauntlet.md` 26–29 and 44–45; `SKILL.md` 150–151.
- **Description**:
  - Artifact retention requires the numbered review directory to keep "the required qualifying review set and a concise index linking superseded evidence by commit" (lines 57–58). `SKILL.md` lines 150–151 make these retention preconditions part of the no-bloat gate.
  - The rest of the contract recognizes only two kinds of changed path: manifest paths (reviewed content) and "allocated review outputs" (lines 146, 183 and 195).
  - "Allocated" covers only reviewer output paths the driver assigns (`review-gauntlet.md` lines 44–45) and moved session drafts (line 101). The index is neither.
  - The final check (lines 182–185) requires the merge-base..HEAD path set to equal the manifest plus allocated outputs. The artifact-only exception (line 195) covers only "adding or pruning only allocated review-output artifacts".
  - `review-gauntlet.md` line 26 and scenario line 237 also limit the final diff to "only required qualifying evidence", which the index is not.
- **Risk**: A long-running PR is exactly where retention applies; this PR has 50 consumed passes. A literal driver then hits one of two problems:
  - The index is an unreviewed extra file, so the final path-set check fails and the driver stops blocked.
  - Or the driver stages the index as reviewed content. Every required link repair after a push, rebase or merge (lines 64–67) then changes the fingerprint. That invalidates the frozen final pass and consumes one of the two final CI-qualified passes. It also forces review of review text, contrary to line 198.

  Either way, the agent must improvise a classification the trusted contract is meant to define.
- **Suggested Fix**: Add one clause to Artifact retention, for example: "Record the index path as an allocated review output in the checkpoint; it follows the artifact-only exception." Optionally amend line 237 and `review-gauntlet.md` line 26 to "required qualifying evidence plus that index". No code change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
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

## Maintainer disposition

The original finding is preserved, but its claimed exclusion of the retention
index is not supported by the contract:

- `loop-control.md#artifact-retention` explicitly requires the index alongside
  qualifying reviews. It is required retention evidence, not an optional summary.
- The path-set gate uses the checkpoint's exact allocated review-output paths.
  It does not restrict their producers to reviewers. The sentence assigning each
  reviewer an unused output path is a collision rule, not an exhaustive definition
  of everything the driver may allocate.
- The existing checkpoint already allocates `reviews/pr-2775/README.md`.
  The frozen patch excludes that exact path, and the complete path-set and
  roundtrip checks pass. Updating its archival links therefore uses the stated
  artifact-only exception without changing the source manifest.
- Retention verification after publication still applies; allocating a path
  neither waives preservation nor permits arbitrary unrelated files.

No source or documentation change is warranted for this finding. The proposed
wording is an optional clarification, not a demonstrated blocked workflow.
This issues-found round is not counted as clean; an independent retry must
review the unchanged frozen patch.
