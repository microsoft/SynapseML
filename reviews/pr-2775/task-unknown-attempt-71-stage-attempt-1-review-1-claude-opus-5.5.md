---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 71
stage_attempt: 1
round: 1
timestamp: "2026-10-19T22:29:17Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3817f033a3b0fb3d40d31fd8c8ee2ae8d1293536ec7807241644bb705450796d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-71-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **CI scope and wiring**
  - `.github/skills/synapseml-pr-loop/SKILL.md` §7 exempts only diffs confined entirely to `.github/skills/` and `reviews/`. It covers both sides of a rename and disqualifies any `tools/` change, including tests.
  - The same rule is linked consistently from `references/ci-evidence.md` (comment-trigger gate), `references/readiness-gates.md`, the external-contributor `SKILL.md` and `references/ci-delegation.md`.
  - This PR changes `tools/ci/tests/*`, so it is correctly not exempt.
  - In `pipeline.yaml` (lines 112–127), the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, which picks up all four changed test suites.
  - None of the files I inspected changes a pipeline, workflow or dependency pin.
- [x] **Collector HTTPS transport** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 119–237)
  - `NoRedirect` refuses redirects.
  - `DeadlineHTTPSHandler` passes only `context=self._context`, so standard certificate verification is unchanged.
  - `DeadlineReader` is installed in the `response_class` factory before `HTTPResponse.begin()`. At that point the buffer is empty, so `fp.detach()` cannot lose read-ahead bytes. The status line, headers, chunk framing and body are all time-bounded.
  - Each raw `readinto` call first sets the socket timeout to the remaining budget.
  - `close()` closes the original `SocketIO`, both on `getresponse()`'s error path and when the `with` block exits.
  - No timer or worker thread was added.
  - The error handler rechecks the deadline first. It then reports a `TimeoutError`, or a `URLError` wrapping one, as a sanitized query timeout, and anything else as the sanitized generic error. This matches the behavioral contract.
- [x] **Collector provenance and test gates** (same file, lines 272–823)
  - `verify_merge` checks the merge's parents in order. `verify_build` checks:
    - pipeline definition 17563;
    - the GitHub repository ID, case-insensitively;
    - `refs/pull/N/merge` and `sourceVersion`;
    - that the reason is `pullRequest` or `manual`.
  - The timeline summary keeps the highest attempt per (type, identifier) and counts issues from ancestor records.
  - Test-run keys of the form `phaseName.jobName` match timeline Job identifiers, because `pipeline.yaml` uses top-level `jobs:` (one implicit stage).
  - Only current-attempt results count, and each job that published a suite must republish it.
  - `--allow-skipped` cannot exempt a `--expected-test`.
  - Results also require `collectionStable`, a deadline recheck after collection, and `mergeReady=false`.
  - The endpoint regex limits the client to read-only GET calls on fixed Azure paths.
- [x] **Trusted-guidance verifier** (`scripts/verify_trusted_guidance.py`)
  - Checks:
    - HEAD equals the pinned SHA;
    - the tree contains only regular blobs;
    - the index matches the tree with no conflict stages;
    - `ls-files --others` (without `--exclude-standard`) also catches ignored files and `__pycache__`;
    - no path traverses a symlink, and each path passes `is_file()` before it is read;
    - only CRLF differences are tolerated, and never for binary files.
  - Git runs with `GIT_*` variables stripped, `--no-replace-objects` and `core.fsmonitor=false`.
  - `trusted_executable` skips relative `PATH` entries and the current directory and its subdirectories.
  - The documented launch flags are correct:
    - `-I -B` for the verifier, which imports only the standard library;
    - `-E -s -B` for the collector and watcher, which need their own directory on `sys.path` for sibling imports. Both set `sys.dont_write_bytecode` before those imports when run directly, and `test_normal_cli_invocation_does_not_dirty_the_guidance_snapshot` covers this.
- [x] **Readiness helper** (`scripts/Get-PrReadiness.ps1`)
  - `collectionErrors` is reset for each PR (line 305).
  - `humanApproval` (lines 569–579):
    - `unknown` when rule or classic-protection collection is incomplete;
    - `not_required` only when every approval rule explicitly requires zero approvals, with no code-owner, last-push or named-reviewer requirement.
  - `merge` is never better than `requires_engineering_evidence` (lines 688–691).
  - `-RunPipeline` refuses `-WaitForReview` and `-RequestReview`, and requires one open PR, both expected SHAs and complete evidence.
  - `ruleRequiredChecks` (objects with `.context`) and the raw `classicProtection` are emitted (lines 667–668). These are exactly the fields the exempt-diff guidance in `references/readiness-gates.md` and `SKILL.md` §9 tells readers to use.
  - In `tools/ci/tests/test_get_pr_readiness.py` (read in full), the harness asserts that no trigger or review request happens unless the test allows it.
- [x] **Watcher** (`scripts/watch_azure_pipeline.py` and `tools/ci/tests/test_watch_azure_pipeline.py`, both read in full)
  - The deadline is measured from the run's kickoff on a monotonic clock, with a 600-second poll interval.
  - After Azure completes, one extra GitHub poll is allowed only if it can start strictly before the deadline (the 6599 vs 6600 test cases).
  - Exit codes 0/1/2/3/4/124/130 match `references/ci-triage.md`.
  - Build URLs are checked against a trusted host/project/path allowlist, and build IDs against int32 bounds.
  - Queries always target `microsoft/SynapseML`.
- [x] **Documentation consistency**
  - Every cross-document anchor resolves to a real heading, for example `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`, `#review-and-validation`, `#targeted-infrastructure-retry`, `#state-and-cost-discipline`, `#evidence-invalidation` and `#evidence-required-before-triggering`.
  - The engineering, human-approval and merge states stay separate throughout.
  - No document allows automated approval, rule bypass, merging or unauthorized CI.
  - The other references are consistent: `review-gauntlet.md`, `writing-prs.md`, `spark-performance.md` and `contributor-safety.md`.
- [x] **Candidates considered and rejected as defects**
  - The exempt-diff field list in `readiness-gates.md` does not name `readiness.evidenceComplete`, `collectionErrors`, `revisionErrors` or `suppressedReviewBodiesForHead`. Neighbouring bullets in the same gate list already require them: the separate readiness states, the suppressed-feedback gate, the blocking review-decision gate and the head-SHA match.
  - `HTTPError` responses are left for garbage collection rather than closed explicitly.
  - Issue counts accept `bool` where an `int` is expected.
  - DNS resolution is not bounded by the socket timeout. An overrun still cannot pass, because the deadline is rechecked after collection.
  - None of these is exploitable or changes a gate outcome.
- [ ] **Tests not run:** I executed no tests or linters (read-only mandate). I did not re-fetch the supplied local pytest/Black results or the hosted Azure and GitHub evidence.
- [ ] **Masked values:** The display masks two values as `******`, so I could not confirm their literal text:
  - the `Authorization` header at `collect_ci_evidence.py` line 214;
  - one credential-shaped URL fixture in `test_untrusted_build_urls_fail_instead_of_passing`.

  Assertions covering both exist.
- [ ] **Truncated diff:** The supplied diff stops inside `pr-loop/SKILL.md`, so I reviewed the rest of the files as they currently stand rather than as hunks. Without running git, I could not independently confirm:
  - the exact list of 17 changed paths;
  - the frozen tree and patch hashes;
  - that no other path changed.
- [ ] **Live API shapes not exercised:** I did not test against real GitHub or Azure responses, such as `pipelineReference` or ruleset payloads.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "pinned target PR-loop sequential six-lens policy",
    "bounds": "existing final CI-qualified limit of two passes; this is the first, with cumulative pre-commit attempts preserved"
  },
  "changeSize": "17 source, test, and documentation paths; 5853 added and 209 removed lines",
  "cyclomaticComplexity": "Moderate coupled provenance, pagination, retry, readiness, deadline and filesystem interactions",
  "rationale": "Current-head local, automated-review and Azure gates now qualify the complete frozen validation-tooling patch for the final six-lens pass",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833",
  "reviewAttempt": 71,
  "completedPrecommitPasses": 70,
  "finalCiQualifiedPassNumber": 1,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [1, 2, 3, 4, 5, 6],
  "reviewedHead": "0487209b131885d5b06b7036300acdc4ce37b09d",
  "azureBuildId": 241706733,
  "verifiedTaskId": null
}
