---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 43
stage_attempt: 1
round: 1
timestamp: "2026-10-18T05:39:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f8f274e5916beead367ad0658625df0a51574a3235113725dd2fb9c33f067553"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-43-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and no-bloat.** I checked all 17 changed paths. None touches `pipeline.yaml`, `.github/workflows/`, dependency pins, release tooling or product APIs. The six-round gauntlet text was moved out of `.github/skills/synapseml-pr-loop/SKILL.md` §8 into `references/review-gauntlet.md`, not copied, and §8 now just links to it. Every changed doc, script and test supports one of the stated goals: reliable evidence, separate engineering and human gates, trusted guidance, or exact CI/test provenance. I found no redundant files.
- [x] **`-RunPipeline` trigger guards in `Get-PrReadiness.ps1`.**
  - L98–L106: requires exactly one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`.
  - L609–L640: before any comment, it throws if evidence is incomplete, and the error lists the revision errors, collection errors and truncation. It also requires an OPEN PR.
  - It posts `/azp run` once, and only when the exact `microsoft.SynapseML` check is absent (case-sensitive `-cnotin`).
  - The conservative complete-collection gate is unchanged, and its error names the specific gaps.
- [x] **Collection in `Get-PrReadiness.ps1` (L238–L480).**
  - `Get-RestEvidence` turns a nonzero exit, empty output, `null` or invalid JSON into a collection error, never an empty policy.
  - `$rulesComplete` is captured before the compare and workflow reads.
  - A workflow run counts as this PR's only if its base repo URL and head SHA match. The fork fallback needs exactly one same-branch PR with a matching number and head; anything ambiguous is a collection error.
  - Required-check names match exactly and case-sensitively. `-RequiredCheck` prefixes can add checks but cannot remove the Azure aggregate. Skipped or neutral required checks stay unproven.
- [x] **Separate readiness states (`Get-PrReadiness.ps1` L518–L577, L672–L702).**
  - A failed recheck reports "Could not recheck", not "moved".
  - `humanApproval` is `unknown` when rules are incomplete. It is `not_required` only when every collected review rule explicitly requires zero approvals and has no code-owner, last-push or named-reviewer requirement.
  - `merge` never authorizes a merge, and `completeness.complete` covers remote gates only.
  - All of this matches the output table in `references/ci-evidence.md`.
- [x] **`collect_ci_evidence.py`.**
  - L71–L136: Azure endpoints are restricted to an allowlist, redirects are refused, and the token comes from a trusted `az` within the deadline. The viewer masks L114, so I confirmed by pattern search that the bearer header is built from the in-memory token.
  - L38–L56 and L121–L130: CLI stderr and HTTP bodies are not echoed.
  - L138–L163: `$skip`/`$top` pagination rejects duplicate IDs.
  - L166–L220: verifies the merge commit's ordered parents, pipeline definition 17563, repository, source branch, source version and queue reason.
  - L455–L637: test selection is attempt-aware, and named tests and suites must be published in each job's current attempt. `--allow-skipped` cannot exempt an `--expected-test`.
  - L683–L717: the build is re-read after tests; `collectionStable` gates `validationPassed`, and `mergeReady` is always false.
  - Every malformed-input path I traced raises `EvidenceError`, which produces a sanitized error and exit code 1.
- [x] **`watch_azure_pipeline.py`.**
  - L47–L75: the build URL must use a trusted host, path and port, with no user info or fragment, and the build ID must fit int32.
  - L98–L103: the deadline is fixed from the kickoff time, so restarts and timeline mode cannot extend it.
  - Timeline mode verifies the merge once. An absent timeline never counts as passing. `azure_completed` and `attention_required` both exit 4 (L359–L368).
  - L259–L274: fractional-second timestamps are truncated, not rounded up.
  - All of this matches `references/ci-triage.md` L19–L93.
- [x] **`verify_trusted_guidance.py`.**
  - Checks HEAD against the pin and accepts only regular files in the tree.
  - The index must equal the tree at stage 0, and `ls-files --others` runs without exclude-standard, so ignored files such as bytecode are caught.
  - Rejects symlinks anywhere in a guidance path, and compares working bytes allowing only CRLF differences.
  - Runs git with `GIT_*` variables stripped, plus `--no-replace-objects` and `core.fsmonitor=false`.
  - Tool lookup skips relative PATH entries and anything under the current directory, and checks resolved symlink targets.
  - All `REQUIRED` resources exist. The documented `-I -B` bootstrap and `-E -s -B` helper commands are the exact flags that `tools/ci/tests/test_trusted_guidance.py` parses and runs.
- [x] **Comment-trigger routes.** Every mention of `/azp run`, `-RunPipeline` or requeue under `.github/skills` goes through `ci-evidence.md#comment-trigger-gate` or the guarded helper:
  - `SKILL.md` L214–L219 and L268–L285
  - `ci-triage.md` L16–L17
  - `readiness-gates.md` L94–L101
  - `ci-delegation.md` step 3
  - the contributor `SKILL.md` handoff

  A hand-posted requeue requires a fresh snapshot with both expected SHAs and `evidenceComplete=true`. The pinned-queue alternative is kept, and nothing relaxes the gate.
- [x] **Links.** All new anchors and cross-skill links resolve to existing headings: `comment-trigger-gate`, `github-snapshot`, `azure-merge-and-named-tests`, `artifact-retention`, `evidence-invalidation`, `state-and-cost-discipline`, `waiting-for-azure-pipelines`, `validation-only-handoff` and `evidence-required-before-triggering`.
- [x] **CI coverage.**
  - The `CIHelpers` job (`pipeline.yaml` L112–L126) runs `python3 -m pytest tools/ci/tests/ -q`, so all four suites run in Azure.
  - The readiness suite skips only when neither `pwsh` nor `TF_BUILD` is present, so it fails rather than skips in Azure, as `ci-triage.md` says.
  - I found no syntax or APIs that need Python 3.11 or later in the changed Python files.
- [ ] **Tests not executed.** I am a read-only reviewer. I inspected the supplied pytest, Black and `git diff --check` results but did not reproduce them. The readiness suite has no current Python 3.10 run, which the author disclosed; `CIHelpers` will run it.
- [ ] **Live API response shapes not verifiable here.** These include the classic-protection key set, `protection.enabled` on ruleset-only branches, and the A365 alias. Every unexpected shape I traced fails closed: it becomes an unassessed gate, a collection error or a `MonitorError`. The remaining risk is a false block, not a false pass.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4420 added and 178 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 43,
  "completedPriorPasses": 42,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
