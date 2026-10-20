---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 71
stage_attempt: 1
round: 2
timestamp: "2026-10-19T22:42:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "375b195b955310d277e977aba39e1ff2d41906fae5166b7877790408a5f71134"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-71-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Every changed path was read from disk, because the diff display was truncated.** Whole-file reads:
  - `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
  - `collect_ci_evidence.py`, `verify_trusted_guidance.py` and `watch_azure_pipeline.py`
  - both `SKILL.md` files
  - `ci-delegation.md`
  - the six pr-loop references: `ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md` and `trusted-guidance.md`
- [x] **Each policy is defined in one place, and every link resolves.**
  - The Azure scope exception lives only in `synapseml-pr-loop/SKILL.md` §7. The external-contributor `SKILL.md` and `ci-delegation.md` link to it.
  - The comment-trigger gate lives only in `references/ci-evidence.md#comment-trigger-gate`. SKILL.md §7 and `ci-delegation.md` step 3 link to it.
  - The gauntlet was moved to `references/review-gauntlet.md`, and Step 8 points to it. The bootstrap lives in `references/trusted-guidance.md`.
  - These anchors all resolve: `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`, `#user-value-and-scope`, `#waiting-for-azure-pipelines`, `#azure-merge-and-named-tests`.
- [x] **The Python helpers import in one direction, and each is invoked in a mode that supports its imports.**
  - `verify_trusted_guidance.py` imports only the standard library (lines 6–13) and is run with `python -I -B` (`trusted-guidance.md` line 18).
  - `collect_ci_evidence.py` imports `trusted_executable` from the verifier (line 22).
  - `watch_azure_pipeline.py` imports the shared Azure/GitHub identity and client from the collector (lines 17–27), so those constants are not duplicated.
  - The collector and watcher are documented with `python -E -s -B` (`ci-evidence.md` line 144, `ci-triage.md` line 31). That mode keeps the script directory on the import path, which `-I` would remove.
- [x] **The HTTPS transport matches the stated contract** (`collect_ci_evidence.py` ~119–166 and ~229–237).
  - `NoRedirect` raises before credentials are forwarded.
  - `DeadlineHTTPSHandler` calls `do_open` with the handler's default context, so standard certificate checks stay in place.
  - `DeadlineReader` wraps the detached original raw stream, applies the remaining budget before every `readinto`, and closes the original stream.
  - On a transport error, the deadline is rechecked before deciding between the timeout message and the generic message.
  - No timer or worker thread was added.
  - Matching tests in `tools/ci/tests/test_collect_ci_evidence.py`: lines 1057–1077, 1087–1168 and 1251–1287.
- [x] **`Get-PrReadiness.ps1` keeps its gates separate.**
  - `#Requires -Version 7.2` (line 1) matches every "PowerShell 7.2+" statement in the docs.
  - Parameter guards (~103–111): expected SHAs and `-RequestReview` require exactly one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - The trigger fires only when evidence is complete, the PR is open, and the exact `microsoft.SynapseML` check is absent.
  - The readiness object (~682–715) reports evidence, engineering, human approval and merge as separate states. It exposes `ruleRequiredChecks` and `classicProtection`, which `readiness-gates.md` uses to interpret exempt diffs.
- [x] **The tests follow the existing `tools/ci/tests` pattern.**
  - All four suites use only the standard library and `unittest`. That fits the CIHelpers job, which installs only `pytest pyyaml` (`pipeline.yaml` lines 111–126).
  - Each suite carries the MIT header and loads scripts with `spec_from_file_location` without writing bytecode.
  - The readiness suite skips only when `pwsh` is missing locally; under `TF_BUILD` it is forced to run. Its PowerShell harness stubs `gh` and fails on any unexpected call.
- [x] **The change follows AGENTS.md conventions.**
  - Nothing changes in `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `AGENTS.md`, `CONTRIBUTING.md` or product APIs.
  - AGENTS.md's "`/azp run` where supported" rule is not contradicted. The exemption applies only within the skill and explicitly leaves GitHub rules unchanged (SKILL.md §7).
- [x] **Candidates I checked and rejected as defects:**
  - **`tools/ci/README.md` test list.** It is not a full inventory and already omits suites that existed before this PR. The new suites and the `pwsh` requirement are documented next to the scripts (`ci-triage.md` lines 108–111).
  - **`collect_build`'s `getattr(args, "allow_skipped_job", [])`.** This hidden argument sharing is documented and would only matter after a hypothetical future edit.
  - **Warnings blocking job gates.** The collector's strict warning gate is documented as a deliberate conservative choice.
  - **HTTP debug logging.** The handler breaks only if global `http.client` debug logging is turned on, which is not a default path.
- [x] **The supplied CI evidence lines up with the source.** The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` (`pipeline.yaml` ~125), the job whose log for build 241706733 is cited. That log shows 383 passed, 2 skipped and 429 subtests passed. The two skips match the Windows batch-launcher guards, which ran unskipped in the supplied local Windows run.
- [ ] **No tests, builds or linters were run by me.** This lens is read-only, so all CI and local results are evidence supplied by the caller.
- [ ] **I did not verify that the on-disk files match the frozen index tree `6b699eac29fb43032d274a41b5c630650ae58422`.** I computed no hashes. The source-diff hash is not the complete input hash.
- [ ] **Test bodies were read selectively.** I read the loaders, harness, fixtures and the transport/timeout cases, not every test case line by line.
- [ ] **The Azure timeline identifier format was not checked against live API data.** The `phase.job` attempt key is plausible only because `pipeline.yaml` has no stages and the supplied build is green.
- [ ] **Human approval is still outstanding and outside this lens.** CLEAN here does not mean engineering, merge or approval readiness.