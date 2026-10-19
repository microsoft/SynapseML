---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 41
stage_attempt: 1
round: 1
timestamp: "2026-10-18T04:06:49Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f090483cc27e73b6cb663f2fa8cef89dd449a5e58dbaa95a08263e9275455ed8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-41-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope and no-bloat.**
  - All 17 changed paths are under `.github/skills/synapseml-pr-loop/**`, `.github/skills/synapseml-external-contributor-review/**` or `tools/ci/tests/**`.
  - Nothing changes in `pipeline.yaml`, `.github/workflows/`, dependency pins, release tooling or the product API. `environment.yml` and `pr-validation.yml` both still pin `black==22.3.0`.
  - There is no stray `__pycache__` or extra file under the skill directories.
  - Every file maps to a stated goal: readiness states, Azure provenance, the watcher timeline, trusted guidance, the gauntlet extraction or the no-bloat gate (`SKILL.md` step 4). I found no redundant file or speculative option.
- [x] **`Get-PrReadiness.ps1` guards (~95–103) and the retained CI-trigger gate (~606–636).**
  - Expected SHAs and `-RequestReview` accept exactly one PR.
  - `-RunPipeline` requires both expected SHAs and refuses `-WaitForReview` and `-RequestReview`, matching `SKILL.md` step 9 and `ci-evidence.md`.
  - The complete-collection gate throws before any comment is posted. Its message lists revision errors, collection errors and truncated threads, and tells the caller to run without `-RunPipeline` to see the diagnostic snapshot.
  - The authorized pinned-queue alternative is documented in `ci-delegation.md` (lines 53, 69–71 and 91).
  - The trigger fires only when the exact, case-sensitive `microsoft.SynapseML` check is absent, and at most once per PR per invocation.
- [x] **`Get-PrReadiness.ps1` evidence collection.**
  - `Get-RestEvidence` (~238–271) turns nonzero exits, empty or `null` bodies and pages, and invalid JSON into `collectionErrors`. A `/protection` failure adds an Administration-permission hint.
  - Workflow runs are attributed through verified associations (base repo URL plus head SHA, ~273–280). The fork fallback requires a unique `pulls?head=owner:branch&state=all` match whose head SHA equals the current head. Ambiguity becomes a collection error before the newest run per workflow/event is chosen.
  - An unreadable recheck reports "Could not recheck…", not movement.
  - `evidenceComplete`, `remoteEngineeringGatesClear`, `humanApproval`, `engineering` and `merge` are reported separately (~556–680), and none of them authorizes a merge.
- [x] **`collect_ci_evidence.py` provenance and safety.**
  - Requests are limited by an endpoint allowlist regex, `NoRedirect` refuses redirects, and the token stays in memory.
  - Grep matches `Bearer` on line 114 and finds no literal asterisks there; the masked display is a tool artifact.
  - Error messages are sanitized `EvidenceError` text, and `list()` pages with `$skip`/`$top=100` and rejects repeated IDs.
  - `verify_merge` requires the ordered `[base, head]` parents. `verify_build` pins definition 17563, the GitHub repo, `refs/pull/N/merge`, the source version, reason, queue time and status.
  - `main()` sets the 600-second deadline before the 60-second-bounded `verify_merge`, so it counts toward the documented ten-minute total.
- [x] **`collect_ci_evidence.py` timeline and test logic** (`summarize_timeline` 234–396, `collect_tests` 456–641).
  - The timeline keeps the highest attempt per `(type, identifier)` and detects parent cycles. Ancestor issues and declared issue counts block the job gate, and leaf containers are gated too.
  - Each test run must match a real Job attempt (`phaseName.jobName`). An attempt newer than the timeline raises an error, and historical runs cannot satisfy a retried job.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - `collectionStable` re-reads the build and timeline, `validationPassed` is strict, and `mergeReady` is always false.
  - Behaviour matches `ci-evidence.md` §Azure merge and named tests.
- [x] **`watch_azure_pipeline.py`.**
  - The deadline is computed once from the kickoff (at most 120 minutes) and never extended. A timeout returns the canonical build link.
  - `parse_build_id` accepts only HTTPS URLs on the trusted hosts for the trusted project (the GUID or `a365`), with no userinfo, port or fragment tricks and an int32 build ID.
  - Timeline mode verifies the merge once, uses a fresh `AzureClient` per poll, and emits `agent_loss` once per job attempt.
  - Exit codes match `ci-triage.md`: 0, 1, 2 (superseded), 3 (replaced), 4 (`attention_required` or `azure_completed`), 124 (timeout) and 130 (interrupted).
  - `parse_kickoff` normalizes fractional seconds for Python 3.10.
- [x] **`verify_trusted_guidance.py`.**
  - It checks that the toplevel matches the checkout and HEAD matches the expected SHA.
  - The tree may contain only regular blobs and must include every `REQUIRED` path, and the index must equal the tree at stage 0.
  - `ls-files --others` runs without exclude-standard, so ignored files such as bytecode are caught.
  - Guidance paths may not traverse symlinks, and file bytes must match the blobs, tolerating only CRLF on text.
  - It strips `GIT_*` variables and uses `--no-replace-objects` and `core.fsmonitor=false`.
  - `trusted_executable` skips relative PATH entries and the current directory and its descendants.
  - The documented `python -I -B` bootstrap excludes the script directory on Python 3.10 as well.
- [x] **Documentation.**
  - All links and anchors resolve: `ci-triage.md#waiting-for-azure-pipelines`, `loop-control.md#checkpoint-before-yielding` and `#evidence-invalidation`, `ci-evidence.md#github-snapshot`, `SKILL.md#validation-only-handoff`, and `ci-delegation.md#evidence-required-before-triggering`.
  - `review-gauntlet.md` carries the removed `SKILL.md` gauntlet rules forward, updated for the installed `-Execute` contract and digit-free `task-unknown` naming. It is consistent with the retention rules in `loop-control.md` and the artifact placement in `AGENTS.md`.
  - `readiness-gates.md` defers to the separate readiness states.
  - The "validated without maintainer changes" outcome is consistent across both skills.
- [x] **CI wiring and test isolation.**
  - `pipeline.yaml` CIHelpers (lines 112–126) already runs `python3 -m pytest tools/ci/tests/ -q` on `ubuntu-22.04` (system Python 3.10, with `pwsh`).
  - A static scan of the four suites and three scripts found no Python 3.11+-only constructs (nested same-quote f-strings, `enterContext`, `tomllib`, `ExceptionGroup`, `datetime.UTC`).
  - The suites import only the standard library. Under `TF_BUILD` without `pwsh`, the readiness suite fails rather than skips.
  - Scripts load with `sys.dont_write_bytecode` patched, and the trusted-guidance tests use temporary repositories.
- [x] **Candidates I evaluated and dismissed, plus one open question.**
  - `failedChecks` omits `STARTUP_FAILURE`, but that list predates this PR. The new `failedWorkflows` includes `startup_failure`, and unproven required checks still block.
  - Workflow pagination counts rows rather than unique IDs. Exploiting this would need more than 100 runs on one head plus concurrent inserts. GitHub's 1,000-row cap fails closed.
  - Warnings blocking `jobGatesPassed` is documented, conservative behaviour that requires a manual triage disposition. It cannot produce a false pass.
  - Unverified: if Azure's Runs-List endpoint capped `$top` below 100, pagination would stop early. Most consequences fail closed (the `totalTests` check and missing-publication errors), and I found no evidence that such a cap exists.
- [ ] **Tests were not executed** because the reviewer tools are read-only. I inspected the driver's pytest, Black 22.3.0 and diff-check results but did not reproduce them. No Python 3.10 run of the readiness suite was supplied, and 3.10 is the CIHelpers interpreter; the static scan above reduces that risk but does not replace a run.
- [x] **Independence:** I did not read or search any prior or sibling review artifact, the `reviews/` directory, or session-state paths.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4391 added and 173 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 41,
  "completedPriorPasses": 40,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
