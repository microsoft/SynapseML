---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 45
stage_attempt: 1
round: 2
timestamp: "2026-10-18T09:02:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "69f71329c9ede3bd0c89a0c90ccee51132c0064bcd47b2c90e8975569095bbdd"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-45-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all four changed helpers in full: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`. The layering is acyclic: watcher (`watch_azure_pipeline.py` 14-27) → collector → verifier. The collector gets `trusted_executable` from the verifier (`collect_ci_evidence.py` 21, 42). The verifier imports only the standard library, so it is a sound trust root.
- [x] Checked the comment-trigger gate in `references/ci-evidence.md` 93-119 against `Get-PrReadiness.ps1`. The argument guards (104-110) require one PR and both expected SHAs, and refuse `-WaitForReview`/`-RequestReview`. The complete-collection refusal (616-624) lists revision errors, collection errors and truncation, and points to the diagnostic snapshot. The helper requires an OPEN PR (625-627). It posts only when the exact case-sensitive aggregate is absent, at most once per invocation (631-642). The race limitation and the authorized pinned-queue alternative are documented. The conservative gate is kept, and this review proposes no relaxation.
- [x] Every documented comment route links `ci-evidence.md#comment-trigger-gate`, including the hand-posted requeue (`ci-evidence.md` 105-109). The routes are pr-loop `SKILL.md` steps 7 and 9, `ci-triage.md`, `readiness-gates.md`, and contributor `references/ci-delegation.md` step 3. The contributor `SKILL.md` hands off to the loop.
- [x] Every new cross-document anchor resolves: `comment-trigger-gate`, `waiting-for-azure-pipelines`, `evidence-invalidation`, `artifact-retention`, `state-and-cost-discipline`, `targeted-infrastructure-retry`, `azure-merge-and-named-tests`, `github-snapshot`, `evidence-required-before-triggering`, `validation-only-handoff` and `who-counts-as-external`.
- [x] The docs and code agree on the readiness outputs: `readiness.{evidenceComplete, remoteEngineeringGatesClear, engineering, humanApproval, merge, unassessedGates}` and the retained `completeness.complete` alias (`Get-PrReadiness.ps1` 644-707). They also agree on the watcher exit codes (`watch_azure_pipeline.py` 355-367) and the documented collector and watcher flags. Contract tests extract the flags from the docs (`tools/ci/tests/test_trusted_guidance.py` 119-125, 185-197).
- [x] The trust model is consistent. Python helpers resolve `git`, `gh` and `az` through `trusted_executable` (`verify_trusted_guidance.py` 40-70). The PowerShell helper uses normal discovery (`Get-PrReadiness.ps1` 112), which is a documented limitation (`references/trusted-guidance.md` 49-50), so it is not reported.
- [x] `SKILL.md` step 8 now links to `references/review-gauntlet.md` instead of keeping a second copy, which removes duplicate policy text. The no-bloat gate is the procedure in `SKILL.md` step 4 plus the checklist in `readiness-gates.md`, which matches how the other gates are documented.
- [x] Test architecture matches `tools/ci/tests` conventions: unittest style that runs under pytest, spec-loaded modules, and in-process fakes for the collector and watcher. A missing `pwsh` fails the readiness suite under `TF_BUILD` (`test_get_pr_readiness.py` 234-236), as `ci-triage.md` 99-101 documents.
- [x] Checked the CIHelpers budget. `pipeline.yaml` 112-126 sets `timeoutInMinutes: 15`, then runs a pip install and `python3 -m pytest tools/ci/tests/ -q`. `tools/ci/tests/test_e2e_impact.py` 395-398 asserts the job has no `dependsOn` or `condition`, and `tools/ci/README.md` 144-148 says it runs on every build and marks CI red on failure. `test_get_pr_readiness.py` has 42 `self.snapshot(` call sites, about 134 pwsh launches per run, set against the supplied timings.
- [x] Checked and rejected these candidates as not defects:
  - `getattr(args, "allow_skipped_job", [])` (`collect_ci_evidence.py` 404) is exercised through watcher-parsed args (`test_watch_azure_pipeline.py` 113-150).
  - Classic-protection items reported only in `unassessedGates` is documented behavior.
  - The overlap between `unprovenRequiredChecks` and failed or pending checks is semantically accurate.
  - The constant fields `mergeReady` and `revisionScope` are intentional contract labels.
- [x] This reviewer executed no commands or tests. All runtime figures come from the validation evidence the user supplied.
- [ ] Hosted-agent CIHelpers duration for this candidate: not available. No current-candidate Azure CI exists, and this reviewer executes nothing.

## Issues

### Issue 1: One pwsh process per scenario risks the CIHelpers 15-minute job timeout
- **Severity**: Medium
- **File**: tools/ci/tests/test_get_pr_readiness.py
- **Line(s)**: 237-287 (the `snapshot` harness). Subtest fan-out at 342, 561, 670, 736, 810, 996 and 1091. Related: `pipeline.yaml` 112-126 and `.github/skills/synapseml-pr-loop/references/ci-triage.md` 93.
- **Description**:
  - Every scenario starts a new `pwsh -NoProfile -NonInteractive -Command HARNESS` process (244-256, `timeout=30`), so each one pays for a cold PowerShell start and re-parses the full readiness script.
  - Most of the 42 call sites are inside `subTest` loops: 8 at 342, 12 at 561, 10 at 670, 7 at 736, 12 at 810, 6 at 996 and 8 at 1091. That is about 134 PowerShell launches per run.
  - The existing CIHelpers job collects this suite. `ci-triage.md` line 93 says the four helper suites run there. The job has a 15-minute cap that also covers checkout, pip install and seven other suites, including the selector tests that use real Git repositories (`tools/ci/README.md` 149-151).
  - The supplied Linux run took 709.634 s for the four changed suites alone, which is 79% of the 900 s cap. The Windows run took 188.93 s, so hosted-agent timing is uncertain. However, the only measurement on the CI operating system is close to the cap, and none exists for this candidate.
  - Runtime is driven by this harness design, not by the test logic.
- **Risk**: CIHelpers runs on every build, and a failure turns the `microsoft.SynapseML` aggregate red. If hosted timing is close to the supplied Linux timing, or varies with agent load, every PR could fail or flake on a timeout unrelated to its change. Diagnosing that would use up budget-limited CI cycles. Raising the cap would need a `pipeline.yaml` change, which this PR's scope excludes and AGENTS.md requires approval for.
- **Suggested Fix**:
  - Keep every failure-boundary case, but share process startup. Run one pwsh session per test method or per class. That session iterates a JSON array of fixtures and calls the script with `&` inside a per-fixture `try/catch`. It resets the mocks and the trigger and review-request files for each fixture, and emits one result or error record per fixture so the existing assertions still apply. (`$script:` state resets on each `&` invocation.)
  - Optionally, merge combinations that test no separate boundary. For example, the five `enabled=False` variants at line 670 could become one fixture with every setting disabled.
  - Record a hosted CIHelpers duration with clear headroom before relying on the job. Do not raise `timeoutInMinutes` without explicit approval.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
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
  "changeSize": "17 source, test, and documentation paths; 4767 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 45,
  "completedPriorPasses": 44,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition after runtime investigation

**Issue 1: resolved by measurement; no harness or pipeline change.**

The earlier Linux measurement loaded PowerShell from a Windows-mounted WSL
`v9fs` filesystem, unlike a native Linux CI installation. Copying the identical
runtime to the native Linux filesystem reduced three cold-start measurements
from 1.472-1.676 seconds to 0.579-0.613 seconds. The executable hashes matched.

The complete existing CIHelpers test directory then passed on the unchanged
frozen patch, using real Python 3.10.20 and native-filesystem PowerShell 7.6.7:

`python -B -m pytest tools/ci/tests/ -q --tb=short -rs`

**370 tests and 338 subtests passed in 486.94 seconds, with no skips.**
This includes all eleven suites, not only the four changed suites. Dependencies
were installed in an isolated environment using the existing CI job's
`pytest pyyaml` dependency set. The test run used about 54% of the 900-second job
budget, leaving about 413 seconds for other steps on this machine.

This is local Linux evidence, not a hosted-agent timing guarantee or this PR's
Azure CI result. Current-head hosted CI remains a separate unfulfilled gate.
The measurements do not establish a timeout defect requiring a stateful
cross-scenario harness. Retaining fresh processes preserves scenario isolation
and avoids adding lifecycle, reset and timeout machinery based on a
non-representative filesystem measurement. No test cases were removed and no
timeout or dependency pin was changed.

The original finding and verdict above remain intact. A new lens-2 invocation
will receive the current runtime evidence and the same frozen source patch,
without prior review feedback. This evidence-only disposition does not
invalidate the source fingerprint or start another fix pass.
