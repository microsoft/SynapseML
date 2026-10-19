---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 35
stage_attempt: 1
round: 1
timestamp: "2026-10-17T22:01:56Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5136c493ca6612a5f6304108685495bdce141e0d3856ebe2b4fc045130c04f8b"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-35-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` in full.
  - The guards at lines 101–109 match the documentation. Expected SHAs and `-RequestReview` each require exactly one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - Paginated REST reads that return empty output become collection errors.
  - The fork fallback for linking a workflow run to its PR needs exactly one PR with a matching SHA, repository and branch.
  - Required checks use exact names. Skipped or neutral checks count as unproven.
  - A failed revision recheck (517–553) is not reported as movement.
  - A known approval is computed before the comparison.
  - The review request (578–604) is attempted once, and only for an OPEN PR with no revision errors.
  - The trigger (606–630) fires once, and only when evidence is complete, the PR is OPEN, and there is no `microsoft.SynapseML` aggregate check. That name test is case-sensitive (`-cnotin`).
- [x] Read `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` in full (724 lines).
  - `verify_merge` and `verify_build` (166–219) tie the build to merge parents `[base, head]`, definition 17563, the repository ID, `refs/pull/N/merge`, `sourceVersion`, and a pullRequest or manual reason.
  - `summarize_timeline` (222–399) keeps the newest attempt for each record, counts ancestor issues and detects cycles.
  - `collect_tests` (458–637) requires published results for every job/suite and job/test pair. An allowed skip cannot excuse a missing expected named test.
  - The client (72–163) refuses redirects, enforces a 600-second deadline and never echoes stderr.
  - `collectionStable` reads the build again after collecting tests.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` in full.
  - `query_pr` (78–96) is read-only, fixed to `microsoft/SynapseML`, and requests only `state,headRefOid,statusCheckRollup`.
  - `monitor()` reads only those fields (124–220).
  - Timeline mode keeps the original deadline, prints `agent_loss` once per job attempt, verifies the merge once, and creates a new Azure client for each poll.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` in full (174 lines).
  - `trusted_executable` (40–66) skips relative PATH entries and anything inside the working directory, and expands PATHEXT on Windows.
  - It is defined only here. The collector imports it at line 21, so there is no duplicate.
  - Git calls remove `GIT_*` environment variables and disable replace objects and fsmonitor.
  - Verification requires regular-file blobs, an index identical to the commit tree, and no untracked or ignored files.
  - The required paths (15–33), including root `CODEOWNERS`, `pipeline.yaml` and `templates/`, exist.
- [x] Read the test suites:
  - `tools/ci/tests/test_get_pr_readiness.py`: harness lines 1–242 and tests 243–830 and 846–1070.
    - The guard at line 200 skips only when neither `pwsh` nor `TF_BUILD` is present, so the suite cannot silently skip on Azure.
    - `snapshot()` checks that trigger and review-request side effects occur only when allowed.
  - `tools/ci/tests/test_collect_ci_evidence.py`: lines 1–120 and 686–880.
  - `tools/ci/tests/test_watch_azure_pipeline.py`: lines 1–130, 455–475 and 540–760.
  - `tools/ci/tests/test_trusted_guidance.py`: in full. Its command regexes match the commands in `references/trusted-guidance.md`, `ci-evidence.md` and `ci-triage.md`.
- [x] The `pipeline.yaml` CIHelpers job (112–126) runs `python3 -m pytest tools/ci/tests/ -q`, which covers all four new suites. This agrees with `tools/ci/README.md` and `references/ci-triage.md`. The diff does not change pipeline or workflow definitions, dependency pins, release tooling or product modules.
- [x] Read these docs: the pr-loop `SKILL.md`, the six pr-loop references (`ci-evidence`, `ci-triage`, `loop-control`, `readiness-gates`, `review-gauntlet`, `trusted-guidance`), and the external-contributor `SKILL.md` and `references/ci-delegation.md`.
  - The anchors `#validation-only-handoff`, `#waiting-for-azure-pipelines` and `#evidence-invalidation` all resolve.
  - A grep of `.github/skills` found no `-RunPipeline` usage without both SHAs.
  - `SKILL.md` lines 272 and 278 match the script guard at `Get-PrReadiness.ps1` lines 107–108.
- [x] Security: the masked `Authorization` value in the diff is display masking, not a committed credential. No tokens, keys or connection strings appear in the reviewed sources.
- [x] The supplied validation counts match the test sources (counted, not run):
  - The readiness file has 33 tests. The `-k` filter `review_request_requires_one_pr` selects 1 and deselects 32. The affected-path selection gives 5 tests, 19 subtests and 28 deselected.
  - The watcher file has 39 tests. With the earlier 32-test readiness file, that gives 42 selected and 29 deselected.
- [ ] Not done: reproducing the supplied pytest, Black and `git diff --check` results, or the live GitHub/Azure API facts. This review was read-only and ran no tests or commands. The supplied results were checked only for consistency with the sources.

## Issues

### Issue 1: Unused `baseRefName` field added to the watcher test fixture
- **Severity**: Low
- **File**: tools/ci/tests/test_watch_azure_pipeline.py
- **Line(s)**: 63
- **Description**: This PR adds `"baseRefName": "master"` to the `snapshot()` fixture (lines 59–73), but the watcher never requests or reads that field.
  - `query_pr` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (78–96) asks `gh` only for `state,headRefOid,statusCheckRollup`, and `monitor()` reads only those fields.
  - No watcher test reads, varies or asserts on `baseRefName`.
  - `test_query_is_read_only_and_bounded` (456–471) passes `snapshot()` through a mocked `subprocess.run`, but checks only argv, timeout and encoding.
  - The same field in `tools/ci/tests/test_get_pr_readiness.py` line 139 is legitimate, because `Get-PrReadiness.ps1` requests and compares `baseRefName` (lines 309 and 518–545).
- **Risk**: There is no runtime defect, but the line is dead fixture data.
  - It breaks the PR's own no-bloat gate (pr-loop `SKILL.md` step 4), which requires every hunk to serve an acceptance criterion, fix a demonstrated defect or meet a repository requirement.
  - It suggests the watcher uses the target branch name, although timeline mode is documented as tied to immutable merge parents. A maintainer could wrongly assume base-branch handling is tested here.
- **Suggested Fix**: Delete line 63. Then re-run `python -B -m pytest -q tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short` and `python -m black --check --required-version 22.3.0 tools/ci/tests/test_watch_azure_pipeline.py`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
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
  "changeSize": "17 source, test, and documentation paths; 4393 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 35,
  "completedPriorPasses": 34,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`da935723537ea5712461ee55ac920f253b108cbf58b049486248cf1f576959e9`.
This finding ended the pass; the artifact does not qualify the revised patch.
The reviewed diff SHA-256 was
`cd56dd01d9ca9be165912b1ecc548f5d284bef3257516e098b7ec3b9f84cd383`;
the accompanying attempt-35 manifest records the complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
The transport used the observed `claude-opus-5.5` model and read-only source,
policy, and test tools, without accessing earlier review feedback.

**Issue 1: Fixed.** The merge-base-to-index diff confirms that this PR added the
unused `baseRefName` fixture field. It was removed; watcher behavior and test
assertions are unchanged.

Validation:

- Windows: `python -B -m pytest -q tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short`
  passed: 39 tests and 65 subtests.
- Linux: `python3 -B -m pytest -q tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short`
  passed: 39 tests and 65 subtests.
- `python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_watch_azure_pipeline.py`
  passed; the file was unchanged.
- `git diff --check` passed.

No current-head Azure validation or final CI-qualified pass is claimed.
