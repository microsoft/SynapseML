---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 49
stage_attempt: 2
round: 2
timestamp: "2026-10-18T14:30:50Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "0a23e7a4887f737acad152ca54bfc54eb960f6610d3be1bc552d2da915ddff31"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-49-stage-attempt-2-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Readiness helper read in full** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, lines 1-758).
  - Line 1 has `#Requires -Version 7.2`.
  - The guards at lines 102-110 refuse `-RunPipeline` without both expected SHAs, and refuse it alongside `-WaitForReview` or `-RequestReview`.
  - `$owner` and `$name` are derived from `-Repo` at lines 116-121.
  - The trigger at lines 612-647 requires complete evidence, an open PR, and an absent exact `microsoft.SynapseML` aggregate.
  - The readiness object reports evidence, engineering, human approval and merge as separate states. `completeness.complete` is kept as an alias for the remote engineering gates.
  - All of this matches `references/ci-evidence.md` (GitHub snapshot and comment-trigger gate) and `SKILL.md` lines 274-284.
- [x] **Module layering is one-directional and reused rather than duplicated.**
  - Dependencies run `watch_azure_pipeline.py` → `collect_ci_evidence.py` → `verify_trusted_guidance.py`.
  - The watcher imports shared constants, `command_json`, `verify_merge`, `collect_build` and `AzureClient` instead of copying them.
  - Errors are wrapped at each layer: `GuidanceError` → `EvidenceError` (`command_json`) → `MonitorError` (watcher lines 89-92 and 148-151). The watcher's `main` therefore always emits JSON with a mapped exit code (lines 356-367).
- [x] **Watcher CLI matches its documentation** (`references/ci-triage.md`, lines 31-69).
  - The `--azure-timeline --base-sha --merge-sha` coupling is the same in both.
  - The `--repo` restriction to `microsoft/SynapseML` matches.
  - `attention_required` and `azure_completed` both exit nonzero.
  - `replaced`, `superseded`, timeout and interrupt codes match.
- [x] **Trust bootstrap is consistent** (`verify_trusted_guidance.py` read in full).
  - Everything in `REQUIRED` exists. Root `CODEOWNERS` and `pipeline.yaml` are present.
  - `trusted-guidance.md` documents the `-I -B` bootstrap and the `-E -s -B` helper flags. Those flags keep the script directory on the import path, which the collector's sibling import needs.
  - The PowerShell helper's ambient command discovery is documented explicitly. PowerShell does not implicitly run commands from the current directory.
- [x] **Every comment-trigger route goes through the single gate.**
  - Searched `.github/skills/` for `RunPipeline|azp run|completeness\.complete|RequiredCheck|ExpectedHeadSha`.
  - These routes link to `ci-evidence.md#comment-trigger-gate`:
    - `SKILL.md` steps 7 and 9
    - `ci-triage.md` lines 16-17
    - `readiness-gates.md` lines 94-99
    - `ci-delegation.md` step 3
  - The other mentions are not ungated triggers:
    - `contributor-safety.md` only prohibits triggering.
    - `branch-spark4-common.md` line 94 only confirms that a build ran.
    - External `SKILL.md` lines 41 and 66 hand off to the PR loop.
  - The conservative complete-collection gate is retained as instructed.
- [x] **All cross-reference anchors resolve to existing headings.**
  - `ci-evidence.md`: `#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
  - `loop-control.md`: `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
  - `ci-triage.md`: `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `SKILL.md`: `#validation-only-handoff`
  - `ci-delegation.md`: `#evidence-required-before-triggering` (line 25)
- [x] **The step-8 extraction left nothing stale.**
  - `SKILL.md` lines 263-266 now point to `references/review-gauntlet.md`.
  - A search for `six-round|REVIEW-PROMPTS|DiffFile|diff-file|step 8` found no references to the removed text.
  - `review-gauntlet.md` and the `loop-control.md` sections (evidence invalidation, artifact retention, decision rows) fit together. They also agree with AGENTS.md's rule on where `reviews/pr-<number>/` artifacts go.
- [x] **The no-bloat gate follows the skill's existing pattern.**
  - The procedure step is in `SKILL.md` lines 150-155 and the gate checklist item in `readiness-gates.md` lines 22-26.
  - Both send superseded artifacts to `loop-control.md#artifact-retention`. That rule keeps them in reachable archival commits, which is consistent with AGENTS.md's preservation rule.
- [x] **The new tests follow the existing conventions.**
  - The new suites live under `tools/ci/tests/` and load modules the same way the existing watcher test does: `spec_from_file_location` plus a temporary `sys.path` insert and pop (collector test lines 21-32, watcher test lines 19-35).
  - `pipeline.yaml` lines 112-126 show `CIHelpers` running the whole directory with `TF_BUILD` set, so the new suites run in CI without any pipeline change. The readiness suite cannot silently skip there (lines 244-246).
  - The supplied Linux suite time of 506.56 s is within the job's `timeoutInMinutes: 15`. There is no hosted-agent evidence of an overrun, so this is not reported.
- [x] **The diff stays in scope.** None of the 17 paths touch `pipeline.yaml`, workflows, release tooling, dependency pins or product APIs. The new Python files carry the repository license header.
- [x] **Considered and deliberately not reported, as intentional, documented or cosmetic:**
  - `collect_build`'s `getattr(args, "allow_skipped_job", [])`, shared with the watcher. "No skipped-job exemptions in the watcher" is documented.
  - The collector's success JSON has no `outcome` key, but the exit code is the documented contract.
  - The Azure results-URL format is repeated in the collector and the watcher.
  - In `ci-evidence.md`, "collector" also refers to the readiness helper in the GitHub-snapshot section; this is a terminology preference only.
- [ ] Tests were not executed; this was a read-only review. It relies on the supplied Windows and Linux pytest, Black, PowerShell-parse and `git diff --check` evidence.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4861 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 49,
  "completedPriorPasses": 48,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Subsequent disposition

This isolated clean retry belongs to attempt 49's frozen patch. Reproduced
findings in round 3 required source and test changes, invalidating the pass.
It is preserved as historical evidence, not counted toward a later patch's
clean review.
