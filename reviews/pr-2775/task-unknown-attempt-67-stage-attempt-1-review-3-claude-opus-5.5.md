---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 67
stage_attempt: 1
round: 3
timestamp: "2026-10-19T10:42:25Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "da2aa63ab1e260bee882f697d4afc09457731b3ea46aefa6c2e6fcefdc27b93f"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-67-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Watcher** — `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `monitor()` (lines ~98–275) and the exit-code map (~375–385).
  - The deadline is anchored to kickoff, and each query gets at most `min(60, remaining)` seconds.
  - Deadline checks before the query, after the query, and after collection return `timeout_result` with the last Azure evidence.
  - At most one confirmation poll runs, only when more than `POLL_SECONDS` remain. The allowance resets only when Azure is not `completed`.
  - `azure_completed` and `attention_required` both exit 4. Each `agent_loss` event is reported once per job and attempt.
  - No path extends the original deadline or turns missing, pending or disagreeing evidence into exit 0. This matches `ci-triage.md#waiting-for-azure-pipelines`.
- [x] **Collector** — `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, read in full.
  - `run_windows_batch`: on timeout it runs taskkill, then falls back to kill and wait. The fallback error is chained, the message keeps the PID, and `KeyboardInterrupt` is re-raised.
  - `command_json`: error labels are sanitized, and `UnicodeError` is caught before `ValueError`.
  - `AzureClient`: endpoints are allowlisted, redirects are refused, and both the token request and HTTP requests respect the deadline. A 404 or empty body returns `None` only for the timeline endpoint. `list()` rejects repeated IDs.
  - `verify_merge` and `verify_build` check provenance. `summarize_timeline` validates types and attempts, rejects parent cycles, and handles leaf Phase/Stage containers. `collect_tests` enforces the attempt, provenance and publication gates. `main()` re-reads the build to check stability.
  - Every malformed nested field I traced raises `EvidenceError`; I found no `KeyError` or `TypeError` path.
  - The viewer masked the Authorization header value. A count-only search confirmed the source is `f"******"` and the test asserts the fixture-token header.
- [x] **Guidance verifier** — `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.
  - `trusted_executable` skips relative `PATH` entries and those inside the current directory, and also checks resolved targets.
  - `git()` strips `GIT_*` variables, uses `--no-replace-objects` and `core.fsmonitor=false`, and has a 30-second timeout.
  - `verify()` checks the worktree root and HEAD, and rejects symlink or submodule entries. It requires the index to match the pinned tree exactly with no conflicts. It catches untracked and ignored files (no `--exclude-standard`) and symlinked parent directories. CRLF tolerance applies only to non-binary blobs.
  - `main()` reports Git, OS, Unicode and timeout failures as `verified:false`.
- [x] **Readiness script** — `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, lines 1–770.
  - Parameter guards are at lines 103–111. `Invoke-PagedQuery` stops if the cursor does not advance. `Get-RestEvidence` parses each page and records collection errors, including the note about classic-protection permission.
  - Workflow runs: `total_count` and identity are checked, and the fork-association fallback queries `state=all`. Supersession is per `workflow_id,event`.
  - The Azure check name is matched case-sensitively, and a null `behind_by` blocks rather than passes.
  - Revisions are rechecked, and `-RequestReview` is attempted only once.
  - `-RunPipeline` requires complete evidence, an open PR and an absent Azure check. Receipt parsing cannot turn a successful POST into a failure.
  - `-Depth 12` covers the deepest nested output.
- [x] **Docs and anchors** — I read `ci-evidence.md`, the relevant sections of `ci-triage.md`, `loop-control.md`, `readiness-gates.md` and `trusted-guidance.md`.
  - These anchors all resolve to existing headings: `comment-trigger-gate`, `azure-merge-and-named-tests`, `waiting-for-azure-pipelines`, `targeted-infrastructure-retry`, `evidence-required-before-triggering`, `evidence-invalidation`, `state-and-cost-discipline`.
  - The documented behavior matches the code: timeline warnings block the job gate, the collector has a ten-minute deadline, `timelinePending` is reported, and the bootstrap uses `-I` then `-E -s -B`. Both scripts set `sys.dont_write_bytecode` before their sibling imports.
- [x] **CI robustness of the tests**
  - The `CIHelpers` job in `pipeline.yaml` runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu with a 15-minute limit.
  - The readiness suite is skipped only when `pwsh` is missing outside `TF_BUILD`. Windows-only real-process tests are marked `skipUnless(win32)`.
  - Guidance fixtures set the Git identity with `-c user.name/user.email`. The readiness harness fakes `Get-Date` and `Start-Sleep` and uses 30-second subprocess timeouts.
- [x] **Edge cases considered but not reported** (judged conservative, intentional, or speculative):
  - An explicit null timeline count raises an error. Azure is not known to send null counts.
  - Errors after the deadline become `timeout` by design, and tests cover just before, at and after the deadline.
  - Workflow-run pagination can race with new runs. This is inherent, and the count check blocks the snapshot.
  - If the process exits during taskkill, the result is still nonzero.
  - Timeline warnings block `jobGatesPassed`, which is documented as needing a manual disposition.
  - The complete-collection CI-trigger gate stays strict, as the plan requires.
- [ ] I did not run any tests; this was a read-only review. The Windows and Linux results above were reported by the caller and not reproduced here.
- [x] **Independence**: I did not read, search or glob `reviews/`, prior or sibling artifacts, checkpoints, or session evidence. Searches were limited to `.github/skills/`, `tools/ci/` and the named file `pipeline.yaml`.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5511 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 67,
  "completedPriorPasses": 66,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}

## Driver completion evidence

The finalized runner receipt reports exit 0 and CLEAN; runtime events confirm
`claude-opus-5.5`. Input, response and decision hashes match the receipt.
The 35 read-only tool calls stayed within named source files and source subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
privately with SHA-256 `be70c9bed3be2b641285cbc04bcdb9e73736461a865fadc973aa85eea6908080`.
This is one lens of the six-clean pre-commit pass, not current-head CI clearance.
