---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 68
stage_attempt: 1
round: 4
timestamp: "2026-10-19T14:35:50Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f0ae0bc31503bfb12821eb13590e2511f2eb28a3d7492398d5eb30b8ea49b942"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-68-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, lines 133–151: for each pinned tree entry, the verifier checks for symlinks first (136), then `Path.is_file()` (141), and only then runs `cat-file` and `read_bytes()` (143–144). A FIFO or directory is therefore rejected before any blocking open. `main` (161–176) turns `GuidanceError`, `OSError`, `UnicodeError` and `TimeoutExpired` into `{"verified": false}` with exit 1. I also rechecked:
  - blob/mode filtering (114–117) and the `REQUIRED` check (119–120);
  - stage-0 index equality (121–130);
  - `ls-files --others` with no exclude rules, so ignored files are included (131);
  - `GIT_*` stripping, `--no-replace-objects` and `core.fsmonitor=false` (72–92);
  - CRLF tolerance, which applies only to blobs without NUL bytes (145–151).
- [x] `tools/ci/tests/test_trusted_guidance.py`, lines 181–219: the new regression runs the real CLI with `-I -B` and `timeout=30`; `subprocess.run` kills the child on expiry. It covers a directory on all platforms and a FIFO where `os.mkfifo` exists. It asserts exit 1, `verified: false` and the "regular files" message, and restores the file in `finally`. In `ls-tree` byte order, the `.github/...` required entries come before `AGENTS.md`, so the loop reaches the replaced path after clean files.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `main` (738–771): the deadline is set once (740), before `verify_merge`, and passed to the single `AzureClient`. `client.remaining_timeout()` (759) runs after the second `collect_build` and before `print` (760). `remaining_timeout` (134–142) raises when `min(60, deadline - now) <= 0`. With deadline 700, time 699 succeeds, while 700 and 701 produce the exact error JSON and exit 1. The test in `tools/ci/tests/test_collect_ci_evidence.py` (1241–1296) patches only `monotonic`, `command_json`, the factory and `client.get`, so the real final deadline check runs.
- [x] Collector data flow, checked line by line:
  - `verify_build` (236–274): definition 17563, GitHub repository, `refs/pull/<n>/merge`, `sourceVersion` and reason.
  - `summarize_timeline` (277–448): keeps the highest attempt per (type, identifier), detects parent cycles, keeps leaf Phase/Stage containers, and uses `max(listed, reported)` issue counts per record.
  - `collect_build` (451–493): accepts only `succeeded` or an explicitly allowed skip.
  - `collect_tests` (505–690):
    - matches `Phase.Job` keys only against Job-type attempts and rejects newer-than-timeline attempts;
    - selects current-attempt results and checks `totalTests` against the paginated count;
    - refuses skip exemptions for named tests and enforces per-job publication.
  - `AzureClient.list` (193–218): `$skip`/`$top` termination and rejection of repeated IDs.
  - `command_json` (74–111): `UnicodeError` is caught before `ValueError`.
  - The `Authorization` header (168) displays masked, but grep confirms it interpolates `******
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, `monitor` (98–274):
  - It allows one confirmation poll, and only when `deadline - now > 600` (116), so the poll starts strictly before expiry.
  - The allowance resets while Azure is not completed.
  - The deadline is rechecked after both the PR query and Azure collection, so a late poll returns the previous `timeout_result["azure"]` (164).
  - Agent-loss reports are de-duplicated per (identifier, attempt).
  - Exit codes (375–385) are 4 for `azure_completed` and `attention_required`, and 124 for timeout.
  - `collect_build` reads `allow_skipped_job` via `getattr`, so the watcher grants no skip exemptions, matching `references/ci-triage.md`.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - Parameter guards (103–111): `-RunPipeline` needs both SHAs and cannot be combined with `-WaitForReview` or `-RequestReview`; expected SHAs force exactly one PR.
  - A malformed base SHA is recorded in `collectionErrors` (327–330).
  - The revision recheck (541–558) uses case-insensitive comparison against the expected SHAs.
  - `-RequestReview` attempts a request once and throws only on revision errors or a non-open PR (589–608).
  - `-RunPipeline` (613–648) blocks incomplete evidence (613), then checks the PR is open, then tests for the exact aggregate case-sensitively (626). A receipt parse failure (639) leaves `pipelineRunRequested` unchanged.
  - The readiness and completeness fields (683–712) match the `references/ci-evidence.md` output table.
- [x] Documentation consistency:
  - Every CLI flag in `references/ci-evidence.md`, `references/ci-triage.md` and `references/trusted-guidance.md` exists in the argparse definitions (collector 693–720, watcher 295–309, verifier 163–164).
  - The documented `-E -s -B` and `-I -B` prefixes are the ones the import-shadowing tests parse.
  - Every new anchor resolves: `#comment-trigger-gate`, `#azure-merge-and-named-tests`, `#github-snapshot`, `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`, `#targeted-infrastructure-retry`, `#waiting-for-azure-pipelines` and `#evidence-required-before-triggering`.
  - The required root guidance paths `CODEOWNERS` and `pipeline.yaml` exist.
  - In `pipeline.yaml` (111–126), top-level `jobs:` supports `Phase.Job` identifiers. The CIHelpers job installs pytest and pyyaml and runs `python3 -m pytest tools/ci/tests/ -q`.
  - `tools/ci/tests/test_get_pr_readiness.py` (249–251) runs the suite under `TF_BUILD` even without `pwsh`, so it fails instead of skipping, as documented.
- [x] Candidates checked and ruled out as defects:
  - A malformed but non-empty initial base SHA reaches only `collectionErrors`, which `-RequestReview` ignores by design. Triggering it would need GitHub to return a non-hex branch SHA, and the only effect is a review notification.
  - A rejected token persisting on `AzureClient` cannot be reused: every `EvidenceError` ends the collection, and the watcher creates a new client each poll.
  - An ancestor Stage/Phase issue can block sibling jobs, but that only blocks; it cannot produce a false pass.
- [ ] Tests were not run under the read-only contract. Pass/fail status relies on the supplied Windows and Linux evidence. Hosted build 241650336 covers only the pre-fix head.
- [ ] Live Azure timeline and test-run payloads were not observed, including record IDs after retries, whether `errorCount` is present, and `$top` limits. The logic was checked against fixtures and documented shapes only.
- [ ] The diff in the prompt was cut off after the stage-9 hunk of the PR-loop `SKILL.md`. I reviewed the remaining hunks from the working-tree copies of the 17 changed files and did not re-hash them against the supplied source-diff SHA-256.

## Driver completion evidence

The finalized receipt, retained file hashes and observed runtime model match.
All 50 tool calls were read-only and limited to named source paths/subtrees
or narrowly named root-file lookups; no prior or sibling feedback was accessed.
The original artifact is preserved with SHA-256
`47255f9bde5472cb54955597bf438bb6ce0871cde99bafc4547d45a10ded491e`.
The retained input contains the complete frozen diff. The reviewer disclosed
its display limit and used named working-tree files for the remaining source.
This clean lens supplies pre-commit evidence, not current-head CI clearance.