---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 25
stage_attempt: 1
round: 1
timestamp: "2026-10-17T15:29:33Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f753c7b45810df8e015af833cb211f06a4dc6a8f30ea88d86b0cb359e65a2f05"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-25-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`. Every malformed or missing nested field I traced raises a sanitized `EvidenceError`, and I found no path that ends in an uncaught traceback. The areas covered:
  - **CLI calls (`command_json`):** stderr is never echoed.
  - **`AzureClient`:** endpoints are allowlisted and redirects are refused. The token and every request stay within one deadline. Pagination uses `$skip`/`$top` and rejects duplicate IDs.
  - **`verify_merge`:** the merge commit's parents must be exactly `[base, head]`, in that order.
  - **`verify_build`:** pins definition 17563, the GitHub repository, `refs/pull/N/merge`, `sourceVersion`, and the queue reason.
  - **`summarize_timeline`:** picks the latest attempt, applies ancestor issues to each job, and keeps Phase/Stage-only leaves.
  - **`collect_tests`:** current-attempt matching and the per-job republication rules.
  - **`main`:** the build and timeline are re-read after the test runs, so movement fails the stability gate.
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - The deadline comes from the kickoff time and is shared with `AzureClient`.
  - The merge is verified once, and the watcher keeps waiting while Azure is incomplete.
  - `azure_completed` and `attention_required` both exit with code 4.
  - `agent_loss` events are reported once per job and attempt.
  - Build URLs must use a trusted host and project, and build IDs must fit in int32.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (lines 1–710):
  - **`-RunPipeline`** posts `/azp run` at most once per run, and only if the exact-case `microsoft.SynapseML` aggregate is absent. It refuses multiple PRs, missing expected SHAs, `-WaitForReview`, incomplete or stale evidence, and non-open PRs.
  - **`-RequestReview`** sends one request and is guarded by the revision checks.
  - **Workflow scoping:** runs must have a verified PR association; the unique fork-branch fallback is checked.
  - **Policy gates:** unevaluated ruleset and classic-protection rules are reported.
  - **Readiness fields:** `humanApproval` and `merge` are derived as the `ci-evidence.md` output table says.
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - It compares HEAD, tree, index, and working-file bytes, and also checks untracked and ignored files.
  - It rejects symlinks and submodules.
  - It runs git with `--no-replace-objects` and `core.fsmonitor=false`, and strips `GIT_*` environment variables.
  - Every `REQUIRED` path exists at the reviewed revision: root `CODEOWNERS`, `pipeline.yaml`, `templates/`, `contributor-safety.md`, and `ci-delegation.md`. The missing `.github/copilot-instructions.md` is optional, and its absence is tolerated.
- [x] Checked the docs against the code:
  - Documented flags match the argparse and `param` definitions in `ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, and `SKILL.md`.
  - Relative links resolve, as do the anchors `#checkpoint-before-yielding`, `#evidence-invalidation`, `#state-and-cost-discipline`, `#waiting-for-azure-pipelines`, `#evidence-required-before-triggering`, `#github-snapshot`, and `#targeted-infrastructure-retry`.
- [x] Checked `pipeline.yaml` against the collector:
  - The pipeline has one top-level `jobs:` list and no stages, so test-run keys of the form `phaseName.jobName` line up with timeline Job identifiers.
  - Real step names ("Setup repo", "Prepare for tests", "Unit Test", "Publish Test Results") behave correctly under the `test_tasks` filter. That filter only produces a hint and does not gate anything.
- [x] Checked the tests:
  - `tools/ci/tests/test_get_pr_readiness.py` mocks `gh` as a PowerShell function, so it does not depend on the platform.
  - `tools/ci/tests/test_trusted_guidance.py` sets the git identity with `-c`, and runs the helpers only with `--help`, so it makes no network calls.
- [x] Scope: the reviewed paths change nothing in `pipeline.yaml`, `.github/workflows/`, release tooling, or dependency pins.
- [ ] I ran no commands or tests; this lens is read-only, so I did not reproduce the supplied pytest, Black, or live-API results. One gap in that evidence: the `CIHelpers` job (`pipeline.yaml` lines 112–126) runs `tools/ci/tests/` on hosted Ubuntu, where `pwsh` is preinstalled. The readiness tests that were skipped in the cited WSL run will therefore run under Linux PowerShell in CI, but the supplied evidence only covers them on Windows.

## Issues

### Issue 1: Tool lookup runs the symlink's target, which breaks launchers that dispatch on argv[0]
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`
- **Line(s)**: 59–63 (`trusted_executable`). Callers: `git()` (line 73) and `collect_ci_evidence.py` `command_json` (line 40), which also backs `watch_azure_pipeline.py` `query_pr`.
- **Description**: Once `trusted_executable` finds a candidate, it returns `Path(candidate).resolve()`. The helpers therefore run the symlink's final target instead of the file found on PATH. Some tool launchers are symlinks to one shared binary that decides what to run from `argv[0]`:
  - snap: `/snap/bin/<app>` points to `/usr/bin/snap`
  - mise shims point to `mise`
  - aqua links point to `aqua-proxy`

  When the resolved target is run, `argv[0]` is the shared binary's own name. That binary then runs its own command-line interface, receives arguments such as `api …` or `account get-access-token …`, and fails. Only the check that the tool is not under the current directory needs the resolved path; running the tool does not.
- **Risk**: In those environments every collector, watcher, and verifier query fails. The error is generic: "gh/az query failed (exit N)" or "Cannot verify the pinned Git snapshot." The shared helpers become unusable, even though the docs require them ("Reuse this watcher; do not create a temporary replacement"), and the error message gives no clue to the cause. It never produces a false pass.
- **Suggested Fix**: Keep the existing check on the resolved target (lines 61–62), but return the absolute candidate path rather than its resolved target. The candidate's directory is already resolved and checked at lines 55–57. Decoys whose real target is under the current directory are still rejected, and the launcher keeps its basename. Then:
  - Update the resolved-path assertions in `tools/ci/tests/test_trusted_guidance.py` (around lines 264–309) to compare resolved paths.
  - Add a POSIX-only regression test with a symlinked launcher that fails unless `basename "$0"` is the requested tool name.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Kept the resolved-target safety check but return the
  absolute launcher path from trusted tool lookup. Identity assertions compare
  resolved paths, and the bootstrap documentation explains launcher preservation.
- **Why**: Name-based shims must receive the requested tool's basename without
  permitting a symlink to execute code from the current checkout.
- **How verified**: A real POSIX shell dispatcher failed before the fix with
  exit 17 when invoked through its resolved target. It now succeeds through
  the `gh` symlink; a symlink targeting the checkout is still rejected.
  `python3 -B -m pytest -q test_trusted_guidance.py test_collect_ci_evidence.py
  test_watch_azure_pipeline.py -rs --tb=short`, run from `tools/ci/tests` on
  Linux, passed: 87 tests and 177 subtests.
  `python -B -m pytest -q tools/ci/tests/test_trusted_guidance.py
  tools/ci/tests/test_collect_ci_evidence.py
  tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short` on Windows passed:
  86 tests and 177 subtests; the one POSIX-only skip is covered by the Linux run.
  Black 22.3.0 and whitespace checks passed.

The original artifact was retained before this resolution update; SHA-256:
`36f4f4ccef88251ec8e322b549356593891ce375d50f7c01fc9f756fdae98ef7`.
The original `issues_found` verdict is preserved and does not qualify the
revised patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4145 added and 151 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 25,
  "completedPriorPasses": 24,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
