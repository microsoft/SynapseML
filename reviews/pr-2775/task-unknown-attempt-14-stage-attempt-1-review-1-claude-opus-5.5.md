---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 1
timestamp: "2026-10-17T09:40:33Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ca4db7ee64bedf2e5179b09a0e07d10eb98428e9651de034a074c1b798f52d30"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-14-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all four changed helpers in full and traced their control flow: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `verify_trusted_guidance.py` and `watch_azure_pipeline.py`. This covered the `-RunPipeline` guards (Get-PrReadiness.ps1 L97-103, L543-560), revision rechecks (L478-495), fork run association (L337-389), readiness-state mapping (L496-507, L596-606), and watcher outcome/exit mapping (watch_azure_pipeline.py L316-357).
- [x] Checked the docs against the code. The output table in `references/ci-evidence.md` matches the `readiness`/`completeness` objects. The watcher flags, outcomes and exit codes in `references/ci-triage.md` L6-85 match `watch_azure_pipeline.py`. The bootstrap flags in `references/trusted-guidance.md` L17-27 match the doc-parsing regression in `tools/ci/tests/test_trusted_guidance.py`.
- [x] Checked the collector's safety and provenance logic in `collect_ci_evidence.py`:
  - endpoint allowlist and redirect refusal (L62-125)
  - ordered `[base, head]` merge parents (L152-164)
  - build repository, branch, version and reason checks (L167-190)
  - leaf container/Job attempt selection (L193-348)
  - current-attempt-only evidence and per-job publication rules (L403-560)
  - `collectionStable` re-read (L607-624)

  `pipeline.yaml` is jobs-only, with no top-level `stages:`. So the `phaseName.jobName` key matches single-stage timeline Job identifiers.
- [x] Verified that every new relative link and anchor resolves. In `loop-control.md`: `#checkpoint-before-yielding`, `#state-and-cost-discipline` and `#evidence-invalidation`. In `ci-triage.md`: `#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`. In `ci-evidence.md`: `#github-snapshot`. In `ci-delegation.md`: `#evidence-required-before-triggering`. Root `CODEOWNERS` exists, which matches `REQUIRED` in `verify_trusted_guidance.py` L23-32.
- [x] Confirmed that `references/review-gauntlet.md` keeps the policy removed from `SKILL.md`: pre-commit vs. final CI-qualified passes, frozen-patch identity, reviewer isolation, budgets, invalidation rules, and the bootstrap/artifact publication rules.
- [x] Checked whether the test fixtures are realistic:
  - `tools/ci/tests/test_get_pr_readiness.py` L109 supplies `headRepository.nameWithOwner`.
  - `tools/ci/tests/test_watch_azure_pipeline.py` L254, L420, L457 and L469 patch `trusted_executable`, so only `test_trusted_guidance.py` L222 exercises real tool lookup.
  - `pipeline.yaml` L112-126 runs CIHelpers on ubuntu-22.04 only.
  - `environment.yml` L6 pins `python=3.11.8`.
- [ ] Executed tests or commands — not applicable. This was a read-only review; I considered the supplied pytest, Black and live-collection results but did not reproduce them.

## Issues

### Issue 1: Trusted tool lookup cannot find Windows executables on Python earlier than 3.12
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py (also affects collect_ci_evidence.py and watch_azure_pipeline.py)
- **Line(s)**: verify_trusted_guidance.py 39-54; collect_ci_evidence.py 33-55; watch_azure_pipeline.py 77-95
- **Description**:
  - `trusted_executable()` calls `shutil.which(str(directory / name))` with the bare names `git`, `gh` and `az`, so the argument always has a directory component.
  - Before CPython 3.12, `shutil.which` returns early in that case and checks only the literal path. It does not apply `PATHEXT`; 3.12 changed this.
  - On Windows the tools are `git.exe`, `gh.exe` and normally `az.cmd`. With Python 3.11 or earlier, every absolute PATH entry misses. That includes the interpreter declared in `environment.yml` (`python=3.11.8`).
  - Results on that interpreter:
    - The mandatory bootstrap verifier always fails with "Cannot resolve git from trusted absolute PATH entries".
    - The collector and the watcher (which existed before this PR and now gets `gh` through `command_json`) also fail.
  - `command_json` rewrites the `GuidanceError` as "gh did not return valid evidence.", which blames the wrong cause.
  - CI cannot catch this: CIHelpers is Linux-only and the watcher tests patch the lookup. `test_cli_lookup_ignores_cwd_and_relative_path_entries` would fail on Windows with Python 3.11.
- **Risk**: The trusted-guidance bootstrap is required on entry and resume. On a platform these helpers explicitly target (CRLF tolerance, Windows decoy rationale), it fails closed, as do the evidence helpers. The misleading error invites the ad hoc replacement scripts that the docs forbid.
- **Suggested Fix**:
  - Make the lookup independent of the Python version. On Windows, expand `PATHEXT` (default `.COM;.EXE;.BAT;.CMD`) per absolute directory, test `directory / (name + ext)` with `is_file()`, and prefer those matches over extensionless files. Keep the current-directory exclusion.
  - Alternatively, enforce and document Python 3.12 or newer with an explicit error.
  - Let `command_json` keep the non-sensitive `GuidanceError` text.
  - Add a regression for the Windows candidate expansion that runs on any host interpreter, for example a pure helper that takes `pathext` as input.

### Issue 2: Fork run association reads `headRepository.nameWithOwner`, which gh does not return
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1; tools/ci/tests/test_get_pr_readiness.py
- **Line(s)**: Get-PrReadiness.ps1 279-282, 349-389, 478-487; test_get_pr_readiness.py 109, 583-660
- **Description**:
  - Line 350 sets `$headRepo = $view.headRepository.nameWithOwner`. `gh pr view --json headRepository` returns only `id` and `name`; the owner comes from `headRepositoryOwner.login`. With StrictMode off, `$headRepo` is `$null`.
  - So the fork lookup at L351-370 never runs and `$uniqueForkHead` stays false.
  - Fork `pull_request*` runs with an empty `pull_requests` array, the case `ci-evidence.md` describes, each add "Workflow run … has no unambiguous PR association". `pr-validation.yml`, `codeql.yml` and `dependency-review.yml` all produce such runs.
  - As a result `evidenceComplete` is false, the run is dropped from `workflowApprovalsRequired`, and `-RunPipeline` is blocked for typical external fork PRs.
  - The recheck at L487 compares `$null -ne $null`, so it never detects a head-repository change.
  - The fixture invents `nameWithOwner`, so the fork tests pass against a response shape that real gh does not produce. The supplied live `evidenceComplete=true` result does not show that this branch ran.
- **Risk**: The failure is fail-closed, but the documented fork path is effectively dead in production. External PRs cannot get complete evidence or use the guarded trigger, and approval gates show up as generic collection errors. Operators may then fall back to an unguarded manual `/azp run`.
- **Suggested Fix**:
  - Build `owner/name` from `$view.headRepositoryOwner.login` and `$view.headRepository.name`, or use the REST `head.repo.full_name`.
  - Add `headRepositoryOwner` to the fresh `--json` list at L479 and compare the composed value.
  - Change the fixture to real gh output (`{"id": "...", "name": "SynapseML"}`) so the fork tests fail before the fix.
  - Confirm with `gh pr view <n> --repo microsoft/SynapseML --json headRepository,headRepositoryOwner`.

### Issue 3: Collector can dirty the trusted snapshot when run without `-B`
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 6-16
- **Description**:
  - The watcher sets `sys.dont_write_bytecode = True` before its sibling import ("including without -B", watch_azure_pipeline.py L14-15), and a regression covers it (test_watch_azure_pipeline.py L686-706).
  - The collector imports `verify_trusted_guidance` at L16 with no such guard.
  - Run without `-B`, it writes `scripts/__pycache__/verify_trusted_guidance.*.pyc` into the trusted snapshot. The required resume recheck then rejects the snapshot (verify_trusted_guidance.py L108-109).
  - `trusted-guidance.md` forbids resetting someone else's checkout to make the check pass.
- **Risk**: Omitting one flag blocks resume until someone cleans up manually; the two sibling helpers defend inconsistently.
- **Suggested Fix**: Add `import sys` and `sys.dont_write_bytecode = True` before the sibling import, and mirror the watcher's no-`-B` `--help` regression for the collector.

### Issue 4: Readiness helper does not implement the documented trusted CLI resolution
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1; .github/skills/synapseml-pr-loop/references/trusted-guidance.md
- **Line(s)**: Get-PrReadiness.ps1 105, 203, 240, 282, 306, 478, 527, 555; trusted-guidance.md 37-42
- **Description**: `trusted-guidance.md` says helpers resolve `git`, `gh` and `az` from absolute PATH entries, excluding the current directory and its descendants. `Get-PrReadiness.ps1` is the helper that posts `/azp run` and review requests, yet it calls bare `gh` through normal PowerShell command discovery. That checks aliases and functions first, then every PATH entry, including absolute entries inside the current checkout. PowerShell does not search the current location implicitly, so the specific Windows decoy is mitigated, but the documented exclusion is not implemented.
- **Risk**: The spec overstates the protection for the only helper that writes to GitHub.
- **Suggested Fix**: Resolve `gh` once with `Get-Command gh -CommandType Application -All`, take the first absolute `Source` outside `$PWD` and its descendants, and invoke it as `& $ghPath` everywhere. Alternatively, scope the doc sentence to the Python helpers.

### Issue 5: Claim that the verifier "runs no repository hooks" ignores `core.fsmonitor`
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py; .github/skills/synapseml-pr-loop/references/trusted-guidance.md
- **Line(s)**: verify_trusted_guidance.py 57-69, 99, 108; trusted-guidance.md 34
- **Description**: `git()` strips `GIT_*` variables and replace objects, but `git ls-files` loads the index. When `core.fsmonitor` names a hook command in repository or global config, Git runs that command during index load. The doc nevertheless says "It runs no repository hooks or Git content filters."
- **Risk**: This is a defense-in-depth gap plus an inaccurate safety claim. A PR cannot set that config, so exploiting it requires prior local compromise.
- **Suggested Fix**: Pass `-c core.fsmonitor=false` in `git()`, or qualify the doc claim.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed locally; publication pending
- **What changed**: Expanded Windows PATHEXT before absolute-path lookup and preserved sanitized tool-resolution errors.
- **Why**: Python before 3.12 does not expand PATHEXT for an absolute command path.
- **How verified**: Host-independent git/gh/az extension cases and missing-tool diagnostics failed before the fix and pass afterward. Existing Windows decoy integration also passes.

### Issue 2
- **Status**: Rejected with live evidence
- **What changed**: No speculative compatibility change.
- **Why**: The assertion that gh does not return `headRepository.nameWithOwner` is false for the installed GitHub CLI 2.102.0.
- **How verified**: `gh pr view 2775 --repo microsoft/SynapseML --json headRepository,headRepositoryOwner,isCrossRepository` returned a populated `headRepository.nameWithOwner` and `headRepositoryOwner.login`. The earlier live fork snapshot therefore exercised the actual field, not an invented fixture shape.

### Issue 3
- **Status**: Fixed locally; publication pending
- **What changed**: Disabled bytecode writes before the collector imports its sibling verifier.
- **Why**: Direct collector invocation must not dirty the verified guidance snapshot.
- **How verified**: A subprocess with no `-B` and no `PYTHONDONTWRITEBYTECODE` created bytecode before the fix and leaves no cache afterward.

### Issue 4
- **Status**: Documentation corrected locally; publication pending
- **What changed**: Scoped absolute CLI resolution to Python helpers and explicitly required a trusted PowerShell shell and PATH for the readiness helper.
- **Why**: Normal PowerShell command discovery was existing behavior; the documentation must not claim an unimplemented guarantee.
- **How verified**: Compared the revised statement with the Python resolver and PowerShell entry point. No extra resolver or behavioral refactor was introduced.

### Issue 5
- **Status**: Fixed locally; publication pending
- **What changed**: Passed `-c core.fsmonitor=false` to every verifier Git command.
- **Why**: Index reads can execute a configured fsmonitor hook.
- **How verified**: A real temporary Git hook wrote a marker before the fix and is not invoked afterward; snapshot verification still succeeds.

## Driver validation and identity

- Reviewed predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
- Target and merge base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed pending diff SHA-256: `524d39e68ac784efc6a3ddc857b513d257fd67c83e35aa704d99aa91f2982c83`.
- Original artifact SHA-256: `668edf6bb7d3e9709adc29575f76512b795ddc10c342f1fb8fd0cc50e90c1526`.
- Windows: `python -B -m pytest -q tools/ci/tests/test_trusted_guidance.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py` passed: 76 tests and 112 subtests.
- Linux: `python3 -B -m pytest -q tools/ci/tests -rs --tb=short` passed: 314 tests and 112 subtests; 20 PowerShell tests skipped and covered by the prior unchanged Windows readiness suite.
- Black 22.3.0 left all four changed Python files unchanged.
- This failed pass is superseded by the fixes. It is not a clean or CI-qualified review.