---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 72
stage_attempt: 1
round: 6
timestamp: "2026-10-20T00:27:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "fee4e9692795686bb2dbed39b40b6c89bd69b9c812482a730bbbbfe098b8adfd"
---

I found one documentation issue, so the verdict is ISSUES_FOUND. In `ci-triage.md`, the CIHelpers local-validation command uses `-q`, which hides passing test names. The contract it links to one line earlier requires those names to be visible. I made no changes and ran no tests; this review only read the files.

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-72-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **CIHelpers really has no test-results publisher.** In `pipeline.yaml` lines 112–126, the `CIHelpers` job only installs pytest and pyyaml, then runs `python3 -m pytest tools/ci/tests/ -q`. It has no `PublishTestResults@2` step; those steps appear only in other jobs. The claim in `ci-evidence.md` line 247 ("aggregate log output only") is accurate.
- [x] **The new nonpublishing-job contract is consistent everywhere except `ci-triage.md`.** I compared these against each other:
  - `ci-evidence.md` "Jobs without published test results" (lines 243–266)
  - `SKILL.md` lines 263–268
  - the `loop-control.md` acceptance rows (lines 254–255)
  - `ci-triage.md` lines 105–116

  They agree on the rules against invented or unrelated selectors, the exclusion of publication failures, the carve-out for CI-only hardware, and "a local skip is not a pass". The exception is Issue 1.
- [x] **The new links point to real headings.** These resolve:
  - `SKILL.md#7-run-and-triage-full-ci` and `#validation-only-handoff`
  - `ci-evidence.md#github-snapshot`, `#comment-trigger-gate`, `#azure-merge-and-named-tests` and `#jobs-without-published-test-results`
  - `loop-control.md#artifact-retention`, `#evidence-invalidation` and `#state-and-cost-discipline`
  - `readiness-gates.md#user-value-and-scope` and `#review-and-validation`
  - `ci-delegation.md#evidence-required-before-triggering`
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`

  `SKILL.md` stays well under 500 lines.
- [x] **The collector's deadline handling matches its documentation** (`collect_ci_evidence.py`).
  - Lines 126–166: the response's raw socket stream is wrapped, and the remaining budget is applied before every raw read. Closing the wrapper closes the original stream.
  - Lines 180–239: transport and JSON failures recheck the original deadline. They report either "deadline expired" or "query timed out", without echoing the underlying error text.
  - Lines 796–812: a second collection checks stability, then the deadline is checked one last time.
  - This matches `ci-evidence.md` lines 195–206.
  - For performance, detailed results are fetched only for the requested run names (lines 576–577).
- [x] **The watcher matches its documentation** (`watch_azure_pipeline.py`).
  - Lines 98–120: the deadline counts from the build's kickoff, not from when the watcher starts. At most one extra 600-second GitHub poll is allowed, and only if it fits before the original deadline.
  - Lines 14–16: bytecode writes are turned off before sibling helpers are imported, so a direct run leaves the verified guidance snapshot clean.
- [x] **`Get-PrReadiness.ps1` enforces the documented trigger gate and keeps the readiness states separate.**
  - Lines 109–111 and 613–627: `-RunPipeline` requires one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`. It also requires complete evidence, an open PR, and an absent `microsoft.SynapseML` check (exact case).
  - Lines 559–582 and 683–713: evidence, engineering, human-approval and merge states stay separate; `complete` is only an alias. This matches the output table in `ci-evidence.md` and `readiness-gates.md` lines 125–146.
- [x] **The trusted-guidance verifier works with this repository.** Its required file list includes a root `CODEOWNERS`, which exists. Its symlink and regular-file checks run before any file contents are read, as `trusted-guidance.md` says. `tools/ci/tests/test_trusted_guidance.py` (lines 225–262) reads the documented collector and watcher flags from the docs and runs them. The `pwsh`/`TF_BUILD` skip guard in `test_get_pr_readiness.py` (line 250) matches `ci-triage.md` lines 114–116.
- [x] **No stray files.** The `tools/ci/tests/__pycache__` bytecode in the worktree is ignored by `.gitignore` line 19 (`*.pyc`) and is not part of the reviewed paths.
- [ ] **Tests not executed.** This was a read-only review. The supplied Windows and Linux pytest results and the earlier version's Azure aggregates are cited as provided, not reproduced.
- [ ] **Hosted state not checked.** I did not query Azure logs, published results or live GitHub state.
- [ ] **Coverage limitation.** The prompt's diff display was cut off after the PR-loop `SKILL.md` hunk, so I read the remaining sources on disk.
  - I could identify 16 of the 17 changed paths:
    - two PR-loop docs: `SKILL.md` and `ci-evidence.md`
    - four more PR-loop references: `ci-triage.md`, `loop-control.md`, `readiness-gates.md` and `trusted-guidance.md`
    - four PR-loop scripts
    - two external-contributor docs: `SKILL.md` and `ci-delegation.md`
    - four `tools/ci/tests` suites
  - The truncated display did not show which file is the 17th.
  - I reviewed the test suites in targeted sections, not line by line.

## Issues

### Issue 1: The CIHelpers local-validation command hides the test names its linked contract requires
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/references/ci-triage.md
- **Line(s)**: 105–112 (command on line 111). The contract it contradicts is in .github/skills/synapseml-pr-loop/references/ci-evidence.md, lines 250–254.
- **Description**: The paragraph says CIHelpers produces only aggregate logs and tells the reader to "Apply the nonpublishing-job evidence contract". It then gives this command under "For local validation:": `python -B -m pytest <four suites> -q -rs`.
  - `-q` hides the node IDs of passing tests; the output is progress characters and a total count.
  - `-rs` adds only skip reasons.
  - The linked contract requires running "the exact named regressions locally ... with names and skip reasons visible, for example `pytest <test-node-ids> -v -rs`".
  - The pending follow-up adds that link, which creates the contradiction. The PR's own retained local proof had to use `-v -rs --tb=short` instead of the documented command.
- **Risk**: The only concrete command for CIHelpers produces count-only output. The new contract says that kind of evidence is not enough; `ci-evidence.md` also notes that "an increased count alone cannot prove that regression was discovered". An agent who copies the code block could record it as named local proof. That weakens the gate this follow-up adds for regressions that run only in a job without a publisher.
- **Suggested Fix**: Change the command to show test names, for example `python -B -m pytest <four suites> -v -rs --tb=short`, or list the exact regression node IDs. Alternatively, label the `-q` form as a quick check that does not count as named evidence, and give the `-v -rs` form as the evidence command. This is a documentation-only change.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver resolution

Accepted. The existing CIHelpers local-validation command now uses `-v -rs`,
so its output directly meets the named-local-evidence contract linked above it.
The four helper suites already passed with verbose names and skip reasons on
Windows: 145 tests and 432 subtests, with two explicit POSIX-only omissions.
Both omitted POSIX cases and the two transport regressions passed by exact
node ID on Linux Python 3.10: four tests and six subtests, no skips.
This one-flag documentation correction does not change tests or runtime code.

Attempt 72 stops here: rounds 5 and 6 are preserved with their original
`ISSUES_FOUND` verdicts, and rounds 1-4 were not run. The final reviewed-content
snapshot must include the correction; earlier pass results are not combined
with it as if the source had stayed frozen.