---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 54
stage_attempt: 1
round: 3
timestamp: "2026-10-18T21:03:18Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "34e1496f327ef045b4d12c00354df34eba04b22a2ca923f84d604d32b39cb910"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-54-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Watcher, read in full** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`). `monitor()` (98–256):
  - fixes its deadline from the kickoff time (99–102);
  - limits each query to the remaining time, and reports a timeout instead of a late error (113–123, 148–153);
  - stops if the PR head or open state changes (130–134);
  - checks the merge parents once (138–146);
  - reports each `agent_loss` once per job and attempt (155–174);
  - keeps waiting while Azure is queued or retrying (199–200, 214–216).

  `parse_build_id` (47–75) rejects untrusted hosts, untrusted projects and build IDs outside int32. `parse_args` (277–319) rejects other repositories, kickoff times in the future, and timeline mode without full base/merge SHAs. `main` (322–367) gives every outcome except `success` a nonzero exit code.
- [x] **Watcher read order when Azure completes.** The watcher reads GitHub (117) before Azure (147). If GitHub updates between those two reads, the result is `azure_completed` or `attention_required` (189–198, 232–250). That is the documented nonzero triage outcome for a missing or lagging GitHub check (`references/ci-triage.md` 55–58). The same file requires a recheck after any exit (89–91). Two tests assert this outcome (`tools/ci/tests/test_watch_azure_pipeline.py` 617–653 and 741–778). It can never produce a false `success`, so I don't report it as a defect.
- [x] **Collector, read in full** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - `run_windows_batch` (39–71) re-raises the original timeout or interrupt only after it confirms the process tree is gone. Otherwise it raises an `EvidenceError` that includes the PID and is chained to the last cleanup failure. Cleanup takes at most three 5-second waits, matching `references/ci-evidence.md`.
  - `command_json` (74–96) does not swallow that error.
  - `AzureClient` (110–203) only calls allowed endpoints, refuses redirects and limits each timeout to the deadline. Only a timeline 404 or empty body counts as pending.
  - Offset pagination fails safe. A dropped test run shows up as a missing expected suite, and a dropped result as a `totalTests` mismatch.
- [x] **Collector gates:**
  - `verify_build` (221–259) requires pipeline definition 17563 and the expected repository, merge ref, merge SHA and queue reason.
  - `summarize_timeline` (262–433) validates identities, attempts, task order, issue counts and parent cycles.
  - `collect_tests` (490–676) ties each test run to a timeline Job attempt, and aborts if a run's attempt is newer than the timeline.
  - `main` (723–753) re-reads the build and timeline to set `collectionStable`, and never sets `mergeReady`.

  `pipeline.yaml` uses top-level `jobs:` (111) with no explicit `stages:`. So the `phaseName.jobName` key (545) matches the timeline Job identifiers. Treating warnings as unresolved jobs is the documented strict gate (`references/ci-evidence.md` 193–205), not a defect.
- [x] **Readiness helper** (`Get-PrReadiness.ps1`, reviewed lines 1–760):
  - The parameter guards (103–111) allow `-RunPipeline` only for one PR with both expected SHAs, and not together with `-WaitForReview` or `-RequestReview`. So the wait loop (726–755) can never post `/azp run` again.
  - The review request is attempted only once per PR (582–608).
  - `Get-RestEvidence` (239–273) records a CLI failure, an empty or `null` page, or invalid JSON as a `collectionErrors` entry.
  - Classic branch protection is read only when the branch is protected and protection is not explicitly disabled.
  - Posting the CI trigger requires complete evidence, an open PR and an exactly-named Azure check that is absent (613–626), as `references/ci-evidence.md#comment-trigger-gate` (94–124) specifies.
- [x] **Trusted-guidance verifier, read in full** (`verify_trusted_guidance.py`):
  - `trusted_executable` (40–69) skips relative PATH entries and anything under the current directory, including resolved symlink targets.
  - `git` (72–92) removes `GIT_*` variables, disables replace objects and fsmonitor, and times out after 30 seconds.
  - `verify` (95–156) requires:
    - the worktree root and HEAD to match;
    - only regular files in the tree;
    - the stage-0 index to match the tree;
    - no untracked or ignored files;
    - no symlinks in the path.

    It tolerates CRLF line endings only for files without NUL bytes.
  - Missing or sparse files fail safe through the `OSError` handler in `main` (159–171).
  - The test fixtures (`tools/ci/tests/test_trusted_guidance.py` 26–62) pass the commit identity explicitly to `commit-tree`, so hosted agents don't need a global Git identity.
- [x] **Docs checked against code:**
  - `SKILL.md` step 7, `references/ci-triage.md` (16–17) and the contributor skill's `references/ci-delegation.md` step 3 all route comment triggers through the comment-trigger gate.
  - The watcher outcomes and exit codes in `ci-triage.md` (34–91) match `main`.
  - The `-I -B` bootstrap flags and `-E -s -B` helper flags in `references/trusted-guidance.md` (14–29) match the helpers' `sys.dont_write_bytecode` guards and their import of the sibling `verify_trusted_guidance` module.
- [x] **Independence:** I didn't open or search `reviews/`, prior artifacts, checkpoints or prompts. Searches covered only `.github/skills/`, `tools/ci/` and `pipeline.yaml`.
- [ ] **Tests not executed:** this reviewer is read-only (view, grep and glob only). The conclusions rest on source inspection and the supplied validation evidence, which I did not reproduce.
- [ ] **Hosted CIHelpers runtime not verified:** the CIHelpers job (`pipeline.yaml` 112–126) keeps `timeoutInMinutes: 15` and runs all of `tools/ci/tests/`. The supplied local Linux baseline is 486.94 seconds, and no Azure run exists for the current candidate. Confirm the timeout headroom in the final CI-qualified pass.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5046 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 54,
  "completedPriorPasses": 53,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Historical clean round, superseded by the targeted regression additions from
round 5. The executed `claude-opus-5.5` identity, complete recorded source-patch
input and independent read-only tool calls were audited.
Reviewed predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`;
base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
source diff SHA-256:
`627ac5e78d8f9d1df88ac6b967a3a3b6ec0bf8b8c3a526be59967e24c1881cd7`.
The source manifest is `pr-validation-skills-attempt-54-manifest.json`.
The original review is preserved above, not current-patch or Azure clearance.
