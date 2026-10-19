---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 40
stage_attempt: 1
round: 1
timestamp: "2026-10-18T03:01:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "42a02b7aa9f683d429887219f3b2968dec007fe53a9b4d35ae650a52bf173692"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-40-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`)
  - `trusted_executable` (L40–69) uses only absolute `PATH` entries. It skips the current directory and its descendants, both for the `PATH` directory and for the resolved target, and expands `PATHEXT` on Windows.
  - `git()` (L72–92) strips `GIT_*`, adds `--no-replace-objects` and `core.fsmonitor=false`, and has a 30-second timeout.
  - `verify()` (L95–156):
    - checks that the requested checkout is the repository root and that HEAD equals the expected SHA;
    - requires regular-file modes from `ls-tree -rz`, which rejects symlinks and submodules, and requires every `REQUIRED` path;
    - requires a stage-0 index that equals the tree;
    - runs `ls-files --others` without exclude flags, so ignored files fail too;
    - rejects symlinked path components;
    - compares working bytes to blobs, allowing only CRLF-to-LF differences.
  - I confirmed all eight `REQUIRED` paths exist: root `AGENTS.md`, `CODEOWNERS` and `pipeline.yaml`, plus the pr-loop and external-contributor skill and safety references.
- [x] **Azure client** (`collect_ci_evidence.py`)
  - `AzureClient` (L69–164) restricts requests to a fixed set of endpoints and refuses redirects, so the bearer token is never forwarded.
  - Each request's timeout is capped by the 10-minute deadline.
  - Error messages are sanitized: no CLI stderr or response bodies.
  - Pagination rejects non-dict rows and missing or duplicate IDs.
- [x] **Build and timeline gates** (`collect_ci_evidence.py`)
  - `verify_merge`/`verify_build` (L167–220) check, in order: merge parents (target, then head), definition `17563` as an `int`, repository, build ID, `refs/pull/N/merge`, source version, queue reason, and status.
  - `summarize_timeline`/`collect_build` validate record types, attempts, and issue counts.
  - Phase/Stage leaves are gated like jobs. A missing timeline is allowed only before completion and never passes job gates.
- [x] **Test evidence and final result** (`collect_ci_evidence.py`)
  - `collect_tests` (L455–640) requires job-attempt provenance that matches a timeline Job, and rejects a test-run attempt newer than the timeline.
  - Historical results stay visible but never satisfy a retried job.
  - It checks per-job publication of expected suites and tests.
  - `--allow-skipped` cannot exempt an `--expected-test`.
  - In `main`, `validationPassed` requires a completed and succeeded build, passing job gates, passing tests, and `collectionStable`.
  - I traced every malformed nested-field path; each raises `EvidenceError` and produces sanitized JSON rather than a traceback.
- [x] **Watcher** (`watch_azure_pipeline.py`)
  - `parse_build_id` (L47–75) requires HTTPS on the default port with no userinfo or fragment, a trusted host and path (including the A365 alias), exactly one numeric `buildId`, and a length check before `int()`.
  - `monitor` (from L98) derives its deadline from kickoff and never extends it. It verifies the merge once and emits `agent_loss` once per (job, attempt). It handles superseded, replaced, `azure_completed`, `attention_required`, and timeout outcomes as `ci-triage.md` L18–90 documents.
  - `parse_kickoff` truncates fractional seconds to six digits, which Python 3.10 requires.
  - The exit-code map in `main` covers every outcome.
- [x] **Readiness helper** (`Get-PrReadiness.ps1`)
  - Guards (L98–106) limit expected SHAs and `-RequestReview` to one PR. `-RunPipeline` requires both SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - Paged REST failures and empty responses become `collectionErrors`.
  - For fork runs, a PR association counts only if the base repository URL and head SHA match; otherwise it requires a unique head branch. Ambiguous associations fail closed.
  - The Azure aggregate must match `microsoft.SynapseML` case-sensitively. Required checks that are not SUCCESS remain unproven.
  - The revision recheck feeds `evidenceComplete`, which keeps engineering and human approval separate.
  - The review request and `/azp run` are each attempted at most once. `/azp run` requires complete evidence, an open PR, and an absent aggregate.
- [x] **Documentation**
  - `review-gauntlet.md` keeps every requirement removed from pr-loop `SKILL.md` §8. It adds the `-Execute` contract and a stop rule for pre-PR session drafts.
  - The pruning rule in `loop-control.md` (L7–29) still preserves evidence: prune only after verifying reachability from `refs/pull/<n>/head`.
  - `readiness-gates.md` (L123–124) links to the readiness states in `ci-evidence.md` instead of duplicating them.
  - All cross-skill anchors resolve: `#validation-only-handoff`, `#github-snapshot`, `#azure-merge-and-named-tests`, `#evidence-invalidation`, `#checkpoint-before-yielding`, `#evidence-required-before-triggering`.
- [x] **Tests**
  - `tools/ci/tests/test_collect_ci_evidence.py` (L34–330) asserts negative provenance and definition cases, absence or skip as non-green, current-attempt-only counting, other-job masking, sharding, and named-test scope.
  - `tools/ci/tests/test_trusted_guidance.py` (L160–345) parses the documented `-E -s -B` flags from the reference docs and checks:
    - a shadowed `PYTHONPATH` does not run untrusted imports;
    - a missing safety resource fails verification;
    - edits to `CODEOWNERS`, the pipeline, or templates are not trusted;
    - executable lookup ignores decoys in the current directory and relative `PATH` entries;
    - Windows `PATHEXT` lookup does not depend on the `shutil` version;
    - symlinked launchers are rejected when their target is in the checkout;
    - the fsmonitor hook is never executed.
  - The readiness harness fakes `gh` and skips without `pwsh` only when `TF_BUILD` is unset.
- [x] **Scope and no-bloat**
  - The diff touches only `.github/skills/**` and `tools/ci/tests/**`. It does not change `pipeline.yaml`, workflows, dependency pins, or product APIs.
  - The existing `pipeline.yaml` CIHelpers job (L111–126) already runs `python3 -m pytest tools/ci/tests/ -q`.
- [x] **Candidates checked and rejected**
  - Explicit `null` timeline counts: they fail closed with a sanitized error.
  - Multi-PR workflow associations: documented as ambiguous, so collection is incomplete.
  - `humanApproval=not_required` with zero collected rules: documented.
  - Watcher success independent of `jobGatesPassed`: documented in `ci-triage.md` L56–61.
  - `STARTUP_FAILURE` checks: required checks stay unproven, and Actions runs are counted in `failedWorkflows`.
- [ ] **Not executed.** This review used read-only tools, so I ran no tests or commands. I did not re-run the supplied pytest, unittest, Black or `git diff --check` results. Offline, I could not confirm the real-build mapping of test-run `phaseName.jobName` to timeline `identifier` (build 241234907) or the JUnit `automatedTestName` format. Both fail closed, and the output lists run and result names, so a selector mismatch is visible.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4363 added and 173 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 40,
  "completedPriorPasses": 39,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
