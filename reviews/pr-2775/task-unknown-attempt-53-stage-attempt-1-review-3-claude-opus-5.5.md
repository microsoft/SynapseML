---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 53
stage_attempt: 1
round: 3
timestamp: "2026-10-18T20:01:26Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "705dadb6d61155a4d2017db129bb917341766c7430772181d22f5b99071eb5ab"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-53-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied merge-base-to-index diff (base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`). Re-read the source files on disk for each failure path; they matched the diff.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, all 753 lines:
  - `run_windows_batch` (lines 39–68) and how `command_json` maps errors (71–93).
  - `AzureClient`: the deadline, refused redirects, 404 or empty timeline, and `$skip`/`$top` paging that rejects duplicate IDs.
  - `verify_build` provenance checks.
  - `summarize_timeline`: keeps the latest attempt, rejects parent cycles, and passes ancestor issues down to jobs.
  - `collect_tests`: job-attempt matching and the current-attempt publication rules.
  - The `collectionStable` recheck in `main`.
  - Malformed, retried, partial, or unprovenanced evidence always fails closed. No path produced a false `validationPassed`.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, full file:
  - The deadline comes from the kickoff time, and Azure collection cannot extend it.
  - Each poll reads GitHub before Azure. The superseded and replaced outcomes, duplicate-check rejection, and untrusted-URL rejection all work.
  - `parse_kickoff` truncates fractional seconds, and the exit-code map is correct.
  - The GitHub-lag case for `azure_completed` is documented as a nonzero result that needs triage (`references/ci-triage.md` lines 55–58), so I did not report it.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, full file:
  - Tool lookup skips relative PATH entries and anything under the current directory.
  - Git runs with `GIT_*` variables removed, `--no-replace-objects`, and fsmonitor turned off.
  - It compares the tree, index, untracked and ignored files, and working files. CRLF normalisation applies only to text blobs, and symlink paths are rejected.
  - `main` converts the expected exceptions into a JSON failure.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, lines 1–760:
  - `-RunPipeline` parameter guards (103–111).
  - A partial-page failure in `Get-RestEvidence` (240–274) blocks `evidenceComplete`.
  - Rules, classic protection, workflow paging, fork association, and check classification (331–499).
  - Revision recheck and readiness gates (520–567).
  - The CI trigger (about 607–648) requires complete evidence and an open PR, and survives a bad receipt.
  - Wait-loop warnings (718–760).
  - The conservative complete-collection trigger gate is unchanged.
- [x] Every documented comment-trigger route links to `references/ci-evidence.md#comment-trigger-gate` (heading at line 94): pr-loop `SKILL.md` line 214, `references/ci-triage.md` line 17, `references/readiness-gates.md` line 101, and the external-contributor `references/ci-delegation.md` line 70.
- [x] `pipeline.yaml` (lines 100–130) has no stages; top-level `jobs:` is at line 111. Timeline `phase.job` identifiers therefore match the collector's `phaseName.jobName` key. The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`. A root `CODEOWNERS` file exists, as the verifier's `REQUIRED` list expects.
- [x] Compared the cleanup promise in `references/ci-evidence.md` (lines 189–191) with `tools/ci/tests/test_collect_ci_evidence.py` (lines 896–945). In the mocked cleanup-failure test, `process.wait` always succeeds.
- [ ] Tests were not run. This review is read-only; I relied on the supplied validation evidence without re-running it.

## Issues

### Issue 1: A failed forced kill hides the process ID that the docs promise
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 58–64, with the replacement error at 91–93. Related: `.github/skills/synapseml-pr-loop/references/ci-evidence.md` 189–191 and `tools/ci/tests/test_collect_ci_evidence.py` 929–945.
- **Description**: If `taskkill /T /F` or the following `communicate(timeout=5)` fails, the handler calls `process.kill()` and then `process.wait(timeout=5)`. Only after that does it raise the `EvidenceError` that names the process tree. Neither call is protected:
  - If the killed launcher still hasn't exited, `wait` raises `subprocess.TimeoutExpired`.
  - If Windows denies `TerminateProcess`, `kill` raises `PermissionError`.

  Either exception replaces the intended error. `command_json` then turns it into the generic "`az did not return valid evidence.`" with no PID.

  This is the case where the process most clearly survived cleanup, and `ci-evidence.md` covers it: "A cleanup failure reports the process ID for inspection before retrying." The regression test uses a `Mock` whose `wait()` always returns, so this branch is never tested.
- **Risk**: It still fails closed; nothing falsely passes. But after a confirmed cleanup failure, the operator gets a misleading generic error and no PID to inspect. A leftover `az` child process may keep running while the agent retries. The diagnostic exists to prevent exactly that. It is rare, since it needs a stuck termination or a denied kill.
- **Suggested Fix**: Make the forced kill best-effort, and always raise the error that names the process:
  ```python
  except (GuidanceError, OSError, subprocess.SubprocessError) as error:
      try:
          process.kill()
          process.wait(timeout=5)
      except (OSError, subprocess.SubprocessError):
          pass
      raise EvidenceError(
          f"CLI timeout cleanup could not confirm removal of process tree "
          f"{process.pid}; inspect it before retrying."
      ) from error
  ```
  Add two subcases to `test_failed_batch_timeout_cleanup_reports_the_process_for_inspection`, one with each setting:
  - `process.wait.side_effect = subprocess.TimeoutExpired("az", 5)`
  - `process.kill.side_effect = PermissionError()`

  Each subcase should assert that the `process tree 123; inspect it before retrying` error is still raised. The worst-case cleanup time stays at the documented 15 seconds.

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
  "changeSize": "17 source, test, and documentation paths; 5025 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 53,
  "completedPriorPasses": 52,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 accepted and reproduced. The existing cleanup regression now calls the
public `command_json` path and covers a successful fallback, denied forced kill,
and timed-out wait. Before the fix, both fallback-failure subcases received the
generic error instead of the process ID.

The collector now chains a fallback failure into the existing actionable
`EvidenceError`, preserving the PID without exposing raw diagnostics. It still
fails closed and retains the same timeout bounds. No new helper or file was
needed; the existing documentation promise now holds on both failure paths.

`python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed on Windows (81 tests, 232 subtests, no skips) and Linux (79 tests,
228 subtests, two Windows-only skips that pass on Windows). Black 22.3.0 and
the focused diff check passed.

The executed model was `claude-opus-5.5`; its complete frozen-diff input and
read-only tool calls were audited. No sibling feedback was read.
The reviewed predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`,
base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`, and source diff SHA-256
`ccb6c3ad89d9d290719e0e5bd22f199849937cc5a24d451a7cfe9ca75ae9ba65`.
The matching source manifest is `pr-validation-skills-attempt-53-manifest.json`.
The original review is preserved above. Rounds 4-6 did not run; the fix invalidates
this pass. It is not current-patch or Azure CI evidence.
