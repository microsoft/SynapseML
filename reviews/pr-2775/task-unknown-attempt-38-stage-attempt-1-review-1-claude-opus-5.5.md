---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 38
stage_attempt: 1
round: 1
timestamp: "2026-10-18T00:41:04Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "c444cefdc8ac11666beaa5e2dc691c1e26b6b4e03df2b454bd291752fd2b310e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-38-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Readiness helper** — read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.
  - Lines 99–107 refuse `-RunPipeline` with `-WaitForReview` or `-RequestReview`. They also require exactly one PR plus both `-ExpectedHeadSha` and `-ExpectedBaseSha`, and `-RequestReview` is limited to one PR.
  - Lines 611–628 post `/azp run` only after `evidenceComplete` is true, the PR is OPEN, and the exact `microsoft.SynapseML` aggregate is absent. It posts at most once per invocation.
  - `$rulesComplete` (line 367) is computed before the compare and workflow reads, so a later read failure cannot downgrade a known `humanApproval`.
  - Revision rechecks (lines 517–553) report unreadable data as "Could not recheck…", not as movement.
- [x] **Collector** — read all of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.
  - Requests go only to a fixed project API root with an endpoint allowlist. `NoRedirect` refuses redirects.
  - Errors are sanitized: no CLI stderr, response bodies, or issue messages are echoed. The token stays in memory only.
  - `verify_build` checks pipeline definition `17563`, repository, `refs/pull/<n>/merge`, `sourceVersion`, and reason. `verify_merge` requires parents in `[base, head]` order.
  - Job-attempt currency comes from timeline Jobs, so historical attempts cannot satisfy a retried job.
  - The build and timeline are re-read after tests (`collectionStable`). `validationPassed` is a conjunction of all gates, and `mergeReady` is always false.
- [x] **Watcher** — read all of `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.
  - The monotonic deadline is anchored to kickoff (lines 99–103). Every sleep is `min(600, remaining)`, so the deadline is never extended.
  - Timeline mode verifies the merge once and reuses `collect_build`.
  - `azure_completed` and `attention_required` both exit 4.
  - `parse_kickoff` truncates fractions to 6 digits for Python 3.10 `fromisoformat`.
  - Build URLs are restricted to HTTPS, trusted hosts and paths, and int32 build IDs.
- [x] **Verifier** — read all of `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.
  - It strips `GIT_*` variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`.
  - It checks HEAD, the index (stage 0 only), working bytes, and untracked plus ignored files (`ls-files --others` without exclude-standard).
  - It rejects symlinks and submodules and tolerates CRLF only for non-binary blobs.
  - `trusted_executable` skips relative PATH entries and the current directory and its descendants, including resolved symlink targets. No Git filters or hooks are reachable.
- [x] **CI wiring** — `pipeline.yaml` lines 112–126: the existing `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu 22.04.
  - The pipeline is jobs-only, so the collector's `phaseName.jobName` key matches timeline Job identifiers.
  - A grep of the changed Python found no 3.11+ APIs, such as `enterContext`, `datetime.UTC`, or `ExceptionGroup`.
- [x] **Tests**
  - `tools/ci/tests/test_get_pr_readiness.py`: the harness mocks every `gh` route and asserts no trigger or review-request side effects. Its skip guard (lines 225–227) turns missing `pwsh` into a failure under `TF_BUILD`.
  - `tools/ci/tests/test_trusted_guidance.py`: fixtures set an explicit commit identity, and the documented `-I -B` bootstrap is checked against shadow imports.
  - `tools/ci/tests/test_collect_ci_evidence.py`: the 42 listed tests cover provenance, retries, shards, sanitization, redirects, the deadline, and stability.
- [x] **Docs and links**
  - Every changed cross-link anchor resolves to an existing heading: `#validation-only-handoff`, `#waiting-for-azure-pipelines`, `#azure-merge-and-named-tests`, `#github-snapshot`, `#checkpoint-before-yielding`, `#evidence-invalidation`, `#targeted-infrastructure-retry`, and `#evidence-required-before-triggering`.
  - The old step-8 text moved into `references/review-gauntlet.md` with its pass and invalidation rules intact.
  - The claims in `references/ci-evidence.md` and `references/ci-triage.md` match helper behavior. `CODEOWNERS` exists at the repository root, as the verifier's `REQUIRED` list expects.
- [x] **Scope** — the diff does not change `pipeline.yaml`, workflows, dependency pins, release tooling, or product APIs.
- [x] **Considered and dismissed** — each of these fails closed or is documented, so none is a concrete defect:
  - A ruleset `creation` rule is flagged for assessment while classic `block_creations` is ignored. `ci-evidence.md` documents this asymmetry, and it fails closed.
  - Azure `$skip/$top` pagination stops on a short page. Downstream `totalTests` and expected-publication checks still fail closed.
  - The fork head filter could build an empty owner. Both known `gh` JSON shapes supply `headRepositoryOwner.login`, and a lookup miss already blocks.
  - Run names containing `::` would never be fetched, so they report "not published".
- [ ] **Tests not run** — the plan is read-only, so I executed no tests or commands. The supplied Windows, Linux, and Python 3.10 totals and the Black results are cited as provided, not reproduced.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4301 added and 164 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 38,
  "completedPriorPasses": 37,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
