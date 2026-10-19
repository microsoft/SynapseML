---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 68
stage_attempt: 2
round: 1
timestamp: "2026-10-19T14:02:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "1f4a01d39ce68eb3d8fd6c6942b332a18eb83b5680a96bd215eaf312117f2728"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-68-stage-attempt-2-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, L95–156). The checks run in this order:
  - HEAD equals the expected SHA.
  - `ls-tree` accepts only regular blobs (100644/100755) and requires every path in `REQUIRED`.
  - Stage-0 index entries equal the tree.
  - `ls-files --others` lists both untracked and ignored files, because `--exclude-standard` is not passed.
  - The symlink check walks the file and every parent.
  - The new `is_file()` rejection (L141–142) runs before `read_bytes()` (L144), so a FIFO is rejected before it can block.
  - CRLF tolerance is disabled for blobs that contain NUL bytes.
  - `git()` strips `GIT_*` variables and passes `--no-replace-objects -c core.fsmonitor=false`.
  - `trusted_executable` skips relative PATH entries and PATH entries at or under the current directory.
  - `main` turns Guidance, OS, Unicode and timeout errors into JSON.
- [x] **Collector deadline and sanitization** (`collect_ci_evidence.py`):
  - `main()` (L738–768) calls `client.remaining_timeout()` (L759) after the second `collect_build` and before printing. Finishing at or after the 600-second deadline therefore produces error JSON, which matches the deadline statement in `references/ci-evidence.md`.
  - `AzureClient` uses an endpoint allowlist regex and a `NoRedirect` handler. That handler raises `EvidenceError`, which none of the transport `except` clauses catch, and the token is never printed.
  - `command_json` converts timeout, OS, Unicode, JSON and Guidance failures into `EvidenceError`.
  - I traced every nested-field access in `verify_build`, `summarize_timeline` and `collect_tests` and found no type assumption that could raise an uncaught traceback.
- [x] **Provenance gates**:
  - `verify_merge` requires the ordered parents `[base, head]`.
  - `verify_build` requires definition 17563, the GitHub repository `microsoft/SynapseML`, `refs/pull/N/merge`, `sourceVersion` equal to the merge SHA, and reason `pullRequest` or `manual`.
  - `collect_tests` ties each run to a timeline Job attempt. It rejects attempts newer than the timeline, checks `totalTests` against the number of results, and does not let `--allow-skipped` exempt an `--expected-test`.
  - `pipeline.yaml` uses top-level `jobs:` (L111 onward), so the stage is implicit. That matches the `phase.job` identifiers in the fixtures (`tools/ci/tests/test_collect_ci_evidence.py` L68, L85–86).
- [x] **Watcher and bytecode** (`watch_azure_pipeline.py`, L16–17): it sets `sys.dont_write_bytecode` before importing its sibling module. The documented invocations use `-E -s -B` or `-I -B` (`ci-evidence.md` L140, `ci-triage.md` L31, `trusted-guidance.md` L18). Helpers therefore leave no `__pycache__` to trip the verifier's untracked/ignored-file check on resume.
- [x] **Readiness helper** (`Get-PrReadiness.ps1`):
  - Expected SHAs require exactly one PR.
  - `-RunPipeline` requires both expected SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - It posts the trigger only when evidence is complete, the PR is open and the exact `microsoft.SynapseML` aggregate is absent. The conservative trigger gate is kept as instructed.
  - Engineering and human states are reported separately, consistent with `SKILL.md` step 9 and `readiness-gates.md`.
- [x] **Documentation**:
  - All new cross-document anchors resolve: `#comment-trigger-gate`, `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#evidence-required-before-triggering` and `#github-snapshot`.
  - `references/review-gauntlet.md` keeps the step 8 content moved out of `SKILL.md`.
  - `ci-evidence.md` L197–205 documents that timeline warnings block the job gate.
- [x] **Scope and CI coverage**: all 17 paths are under `.github/skills/` or `tools/ci/tests/`. The diff changes no pipeline, workflow, dependency pin or product API. The existing CIHelpers job (`pipeline.yaml` L112–126) runs `python3 -m pytest tools/ci/tests/ -q`, so the changed suites run there.
- [x] **Regression tests reviewed**:
  - The deadline test covers clock values 699, 700 and 701 against a deadline of 700, and it does not mock the final check.
  - The FIFO and directory verifier case runs in a subprocess with a 30-second bound.
  - The PowerShell readiness harness covers the approval matrix.
- [ ] I did not run any tests, Black or PowerShell checks; my tools were read-only. I did not reproduce the supplied local results or Azure build 241650336, which ran on the pre-fix head e102faa6 and is baseline evidence only.
- [ ] I could not check live Azure DevOps or GitHub API behavior against a real build. This includes the test-run `$top` page cap, whether `jobReference.attempt` is present, and how `warningCount` is typed. In each of these cases the collector fails closed, and I found no path that would report a false pass.

## Driver completion evidence

The finalized receipt, retained file hashes and observed runtime model match.
All 37 tool calls were read-only and limited to named source paths/subtrees
or narrowly named root-file lookups; no prior or sibling feedback was accessed.
The original artifact is preserved with SHA-256
`3847dcfb4345e8c8c0dd5a90c8c2505dc589e08190c237e9c94006191999eae7`.
This stage-2 retry replaces only the format-invalid stage-1 invocation; the
frozen source did not change. This is pre-commit evidence, not CI clearance.