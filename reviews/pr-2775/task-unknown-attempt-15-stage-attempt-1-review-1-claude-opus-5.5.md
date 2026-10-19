---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 1
timestamp: "2026-10-17T10:19:42Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "87760737ca17d88e3e1da671b15917037c8e326122348c52328b86df4f296c0a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-15-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read every changed script and reference in full:
  - Scripts: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (all lines), `verify_trusted_guidance.py`, `watch_azure_pipeline.py`, and `Get-PrReadiness.ps1` (L1-666).
  - Skills: both `SKILL.md` files.
  - References: `trusted-guidance.md`, `ci-evidence.md`, `review-gauntlet.md`, `loop-control.md`, `readiness-gates.md`, `ci-triage.md`, and `ci-delegation.md`.
- [x] Checked the trust controls in `verify_trusted_guidance.py`:
  - It removes `GIT_*` environment variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`.
  - The `ls-tree` check rejects symlinks and submodules. The index must equal the tree, with stage 0 only.
  - It detects untracked and ignored files without exclude rules. It allows only an extra CR before LF and rejects NUL bytes.
  - `trusted_executable` skips relative `PATH` entries and the working directory and its subdirectories.
- [x] Checked provenance and safety in `collect_ci_evidence.py`:
  - Allowed endpoints (L89-92), refusal of redirects (L62-66), and an in-memory-only token (L93-110).
  - `$skip`/`$top` pagination that rejects a repeated or missing ID (L129-150).
  - `verify_merge` checks the merge's parent order and the build's repository, branch, source version, and reason.
  - It keeps only the latest attempt per timeline record.
  - It re-reads the build to confirm it did not change, and `mergeReady` is always `False` (L611-628).
- [x] Checked `watch_azure_pipeline.py`:
  - Bytecode writing is disabled before the sibling import (L15-16).
  - The deadline is measured from the build's start and passed to `AzureClient(deadline=...)` (L146).
  - It exits with code 4 when attention is required, checks the Azure host and project in URLs, and accepts only int32 build IDs.
- [x] Checked `Get-PrReadiness.ps1`:
  - `#Requires -Version 7.2`, and paginated `--jq tojson` parsing.
  - It confirms a fork branch belongs to only one PR via `pulls?head=owner:branch&state=all`.
  - It resets `collectionErrors` for each snapshot and re-reads head, base, and state.
  - `-RunPipeline` requires one PR and both expected SHAs, refuses `-WaitForReview`, needs complete evidence and an OPEN PR, and only requests a missing `microsoft.SynapseML` check.
  - Its readiness states match the `ci-evidence.md` table.
- [x] Checked the pipeline configuration:
  - The `PublishTestResults@2` tasks at `pipeline.yaml` L401, 686, 752, 789, 980, 1546, and 1555, and at `templates/databricks_e2e_steps.yml` L18, set neither `testRunTitle` nor `mergeTestResults`.
  - L1553 and L1560 use `failTaskOnFailedTests: false`.
- [x] Confirmed tests that are permanently skipped in the repository:
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/featurize/VerifyFeaturize.scala:392`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/image/SuperpixelSuite.scala:60,68`
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/speech/SpeechToTextSDKSuite.scala:175,236,431`
  - `deep-learning/src/test/python/synapsemltest/dl/test_deep_text_model.py:17`
- [x] Reviewed the tests:
  - `tools/ci/tests/test_collect_ci_evidence.py` uses made-up fixtures (L48-95, "Suite" and "UnitTests.scala"). The sharded-suite test (L199) contains only passing results.
  - `tools/ci/tests/test_trusted_guidance.py` uses a temporary repository (L26), so CI bytecode cannot dirty the real checkout. Its `PYTHONPATH` attack test is at L103-158.
- [x] Confirmed that every new cross-document link anchor resolves. Checked `loop-control.md`, `ci-triage.md`, `ci-evidence.md`, and `ci-delegation.md`.
- [x] Found no product API, dependency pin, release-tool, or pipeline/workflow definition change in the reviewed paths. `pipeline.yaml` and `templates/` were read only as reference.
- [x] Used the supplied validation results as reported by the caller: Windows pytest 76 passed; WSL 314 passed with 20 PowerShell skips; Black 22.3.0 passed. I did not run any commands.
- [x] Considered and rejected four other concerns:
  - The watcher reporting `azure_completed` while the GitHub check is missing or late is documented triage behavior.
  - Verifying the merge for manually queued builds is described accurately.
  - Excluding the working directory from `PATH` lookup fails closed.
  - Timeline warnings correctly require a recorded manual decision.
- [ ] Not verified: how `collect_ci_evidence.py` and the watcher's `--azure-timeline` behave against a real SynapseML build. That needs live Azure access, and no such run was supplied. See Issue 1.

## Issues

### Issue 1: Naming one expected test makes the collector check every test in the build
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 403-411, 422-424, 457, 503-516, 611
- **Description**:
  - **What the code does.** `collect_tests` adds the run name before `::` in each `--expected-test` to the set of suites (L405-407). It fetches every Azure test run with a matching name (L423, L457). It then requires every current result in that suite to be Completed and Passed, unless the result is on the skip list (L503-516).
  - **Why the scope is wider than documented.** That is only "expected_suites_only" (`ci-evidence.md` L99-100) if run names are narrow. In this repository they are not. No `PublishTestResults@2` task sets `testRunTitle` (`pipeline.yaml` L401, 686, 752, 789, 980, 1546, 1555; `templates/databricks_e2e_steps.yml` L18).
    - `mergeTestResults` defaults to false, so each `TEST-*.xml` or `python-test-*.xml` file becomes its own run.
    - A run without a title gets a default name based on the result format and build, seen in Azure DevOps as `JUnit_TestResults_<buildId>`. It does not use the Scala or Python suite name.
    - The "exact run name" that `ci-evidence.md` L66-68 tells operators to copy is therefore shared by Scala unit tests, Python, R, website, and end-to-end runs.
  - **Effect on real builds.** Some repository tests are permanently ignored, and these publish as NotExecuted (see the checklist). The end-to-end publications at `pipeline.yaml` L1553/L1560 deliberately tolerate failures.
    - On a normal green build, `tests.passed` and `validationPassed` would therefore be false.
    - The only way through is to list every such test with `--allow-skipped` and a recorded reason.
  - **Cost and fragility.** Every run with the shared name, and all its result pages, is fetched one request at a time. `AzureClient()` at L611 has no overall deadline and no retry, so one failed HTTP call aborts the whole collection.
  - **Tests do not catch this.** The fixtures use made-up names ("Suite", "UnitTests.scala"). The sharded test (L199) has only passing results. No run against a real build was supplied.
- **Risk**:
  - The new "exact named regression" check cannot pass on real builds without a long, hand-maintained skip list. That contradicts its documented scope.
  - Operators will learn to treat nonzero exits as routine ("unresolved evidence, not necessarily a product defect", `ci-evidence.md` L114). That defeats the evidence check this PR adds, and collections become slow and fragile.
- **Suggested Fix**:
  - For `--expected-test`, check only the named tests: each must be present, from the current attempt, and Passed in every job that publishes it, and its run must be complete. Do not fail on unrelated tests that share the run name.
  - Apply whole-run checking only to explicit `--expected-suite`, optionally limited to specific jobs.
  - Alternatively, match on the class prefix of `automatedTestName` instead of the run name.
  - Document that SynapseML runs have default names.
  - Add a test fixture with an unrelated NotExecuted result under the same run name.
  - Pass a bounded deadline to `AzureClient`.
  - Before relying on the check, run a read-only collection against one completed SynapseML build to confirm the real run-name and job-identifier formats.

### Issue 2: Helpers that hold credentials run with `-B` only, so `PYTHONPATH` and user-site code can still load
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/trusted-guidance.md
- **Line(s)**: 18, 24-27
- **Description**:
  - **The inconsistency.** The bootstrap uses `python -I -B` specifically to exclude user site-packages and `PYTHONPATH`. It also states "`-B` alone does not prevent shadow imports" (L24-26). `tools/ci/tests/test_trusted_guidance.py` L103-158 treats a malicious `json.py` on `PYTHONPATH` as a real threat.
    - The same paragraph then says to run later helpers with `-B` only (L26-27).
    - The collector (`ci-evidence.md` L63) and watcher (`ci-triage.md` L24) commands follow that advice.
  - **Why verification does not help.** Verifying the checkout does not clean the environment.
    - `PYTHONPATH` entries come before the standard library, so a `json.py`, `re.py`, `subprocess.py`, or `sitecustomize.py` there is loaded first.
    - User-site `.pth` and `usercustomize` files also run.
    - That code runs inside the process that requests an Azure DevOps bearer token (`collect_ci_evidence.py` L93-110).
  - **Why this is plausible.** The PR loop installs PR packages into local Python environments (`SKILL.md` L79-83). A `PYTHONPATH` or user site pointing at contributor code is therefore realistic.
  - **Why `-I` cannot simply be used.** `-I` also removes the script's own directory from the import path. The helpers import their sibling scripts (`collect_ci_evidence.py` L20, `watch_azure_pipeline.py` L16), so they would fail.
- **Risk**: Code from an attacker-influenced import path could forge `validationPassed` or `evidenceComplete` output, or read the Azure token. That undoes the trust the bootstrap establishes.
- **Suggested Fix**:
  - Document `python -E -s -B` for the helpers. This keeps the script directory but ignores `PYTHON*` environment variables and user site-packages.
  - Alternatively, have each helper add its own directory to `sys.path` before importing siblings, then use `-I -B` everywhere.
  - Extend the documented-command test to check the flags on the collector and watcher commands.

### Issue 3: Pruning review artifacts relies on Git history that required rebases and squash merges can lose
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/references/loop-control.md
- **Line(s)**: 15-21
- **Description**:
  - **The new rule.** Superseded review artifacts may be pruned from `reviews/pr-<n>/`. The originals are said to remain in session storage and, "once committed, immutable Git history", with "a concise index linking superseded evidence by commit".
  - **Why the links break.** `AGENTS.md` L155 requires rebasing feature PRs before final validation, which changes every commit SHA. `ci-triage.md` L200-201 also expects squash merges.
    - After a rebase, the indexed commits are no longer in the PR's history and survive only if the hosting service happens to keep them.
    - An interactive rebase or squash can merge the commit that adds artifacts with the commit that prunes them, so the artifacts never exist in any kept commit.
    - Session storage is private and temporary, so it is not a durable backup.
  - **Conflicting rules.** This contradicts `AGENTS.md` L143-144 ("Preserve attempt/round/model filenames, the reviewed commit SHA, and resolution evidence for debugging") and `review-gauntlet.md` L24-25 ("preserve every clean or failed artifact").
- **Risk**: Failed and superseded review attempts, with their resolution evidence, can be lost while the index still claims they are kept. That weakens the audit trail the repository requires.
- **Suggested Fix**:
  - Prune only after the final rebase.
  - Before pruning, confirm each indexed commit is an ancestor of the pushed PR head (`refs/pull/<n>/head`). Check again after any history rewrite.
  - Forbid squashing the add and prune commits together on the PR branch.
  - Alternatively, keep a compact in-tree archive such as `reviews/pr-<n>/superseded/`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Scope defect fixed locally; run-name premise corrected with live evidence
- **What changed**: Named expectations now gate only those tests and their complete current runs. Explicit expected suites still gate all results and each publishing job. Both paths retain retry provenance and full diagnostic outcomes. Collection has a shared ten-minute deadline.
- **Why**: An unrelated ignored test should not become mandatory merely because a named regression shares its run. The claim that all SynapseML runs use one default JUnit name was not correct: live builds publish Scala class names as run names.
- **How verified**: Same-run ignored tests and unrelated incomplete runs failed named-only regressions before the fix and pass afterward; explicit whole-suite requests still fail. Read-only collection of existing build 241234907 for microsoft/SynapseML#2774 selected one passing SuperpixelSuite test while preserving its two ignored neighbors. `tests.passed=true`, selected count 1, no test errors, and `collectionStable=true`. The full collection correctly remained nonpassing because of unrelated unresolved job gates; this is not CI evidence for this PR.

### Issue 2
- **Status**: Fixed locally; publication pending
- **What changed**: Documented `python -E -s -B` for collector and watcher invocations, with a trusted host interpreter separate from PR-package test environments.
- **Why**: Verified sibling files do not make an inherited PYTHONPATH or user-site safe.
- **How verified**: Both documented commands executed a hostile PYTHONPATH `json.py` before the flag change; both now reach `--help` without importing it.

### Issue 3
- **Status**: Retention contract corrected locally; publication checks pending
- **What changed**: Pruning requires final integration, published archival commits, verified reachability from the retained PR head ref, and verification of the linked files. Rewrites require renewed checks and restored files or repaired links.
- **Why**: A commit hash or session-local copy alone is not a durable archive.
- **How verified**: The existing archival commit is the current published `refs/pull/2775/head` and an ancestor of the pending branch. Merged microsoft/SynapseML#2774 retains its original head through `refs/pull/2774/head`, distinct from its merge commit. New safe follow-up drafts will be published before they are pruned; no archival commit will be squashed away on the PR branch.

## Driver validation and identity

- Reviewed predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
- Target and merge base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed pending diff SHA-256: `8e7111e6a96c50a156658472605f16ce9326f18477161150445feb82f7be90b9`.
- Original artifact SHA-256: `62dbaf53e5d541171bb32705d71cd4a92029dcb807a6af88fee9c8f2986ddbd6`.
- Windows: `python -B -m pytest -q tools/ci/tests/test_trusted_guidance.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py --tb=short` passed: 79 tests and 118 subtests.
- Linux: `python3 -B -m pytest -q tools/ci/tests -rs --tb=short` passed: 317 tests and 118 subtests; 20 unchanged PowerShell tests skipped and covered by the preceding Windows readiness run.
- Black 22.3.0 and whitespace checks passed.
- This failed pass is superseded by the fixes. It is not a clean or CI-qualified review.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; approximately 3400 added lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 15,
  "completedPriorPasses": 14,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
