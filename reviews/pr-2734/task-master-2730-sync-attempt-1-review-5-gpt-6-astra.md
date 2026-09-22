# Round 5 test quality and coverage review

## Review summary

| Field | Evidence |
| --- | --- |
| Target | microsoft/SynapseML#2734, base `spark4.1` |
| Scope | Only the nine-file staged import of microsoft/SynapseML#2730 |
| Baseline / reviewed HEAD | `deb9256109e6877387fafbec10ab9a210767c381` |
| Common merge base | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Master / MERGE_HEAD | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Staged source tree | `4e06ea70fce2a99dfd3789dd4ba2db83d95a83f7` |
| Round / attempt / actual model | 5 / 1 / `gpt-6-astra` |
| Mode | Sequential, explicitly authorized GPT fallback |
| Findings / verdict | **1 Low / ISSUES_FOUND** |
| Artifact | `reviews\pr-2734\task-master-2730-sync-attempt-1-review-5-gpt-6-astra.md` |

Round 4 completed CLEAN and its separate report was written before this
round started. The Gemini slot was unavailable after its reported
pre-execution failure; no probe was retried and no Gemini review is claimed.

Index-manifest SHA-256, from raw `git ls-files --stage -z` output:

`fd155450c0407e04824e89d5198b2ae4a21d5040cf61c4432eb0262b2bf12884`

The source snapshot is unchanged. Retained reads cover the complete incoming
watcher, its 477-line test file, and the seven guidance files listed in the
round-1 inventory. A narrow follow-up inspected
`tools\ci\tests\test_watch_azure_pipeline.py:390-445` in the sibling worktree;
the relevant staged files are byte-identical here.

## Observable contract evidence

- [x] The tests import the actual watcher and exercise its `main`, argument
  parser, monitor, and query adapter. They do not replace the implementation
  with a duplicate selector or timer.
- [x] Fake clocks and explicit query/sleep counts establish the 600-second
  cadence, two-hour kickoff deadline, late-start/restart behavior, expired
  no-query path, clipped sleeps, and query-time consumption. Replacement
  tests distinguish the new run's kickoff from observation time and reject
  using an older successful check to hide a newer pending run.
- [x] CLI-path cases decode the emitted JSON, check startup/finished events,
  outcome/build identity, and exit codes. Legacy `EXPECTED` through `PENDING`
  to `SUCCESS` passes through the real query/JSON adapter with a mocked
  subprocess. Failed, skipped, neutral, cancelled, superseded, and timeout
  paths cannot satisfy the positive success assertions.
- [x] Trusted URL positives accompany negative host/project/path/user-info/
  port/fragment cases. Boundary IDs, leading zeroes, malformed responses,
  duplicate/missing results, changed heads, repository restriction, encoding
  errors, and interruption have targeted assertions. CLI inputs use a fixed
  wall clock rather than depending on the date the suite runs.
- [x] The guidance/script contracts and 19 relative links remain the same as
  earlier rounds. Mocked watcher tests are not authorization to execute
  contributor code or evidence that an external-contributor workflow was
  followed safely in a live environment.
- [ ] A nonzero CLI exit is tested independently of malformed stdout.
  The current negative fixture does not establish that guarantee; see
  R5-2730-1.

## R5-2730-1: Failed-command fixture also fails JSON parsing

- Severity: Low.
- File/lines: `tools\ci\tests\test_watch_azure_pipeline.py:437-444`,
  specifically the nonzero-exit fixture at line 438.
- Affected scope: the same incoming master test on both ports.

`test_cli_errors_are_not_success` supplies
`CompletedProcess([], 1, "", "authentication required")` and accepts any
`MonitorError`. Removing the explicit return-code guard from
`.github\skills\synapseml-pr-loop\scripts\watch_azure_pipeline.py:92-97`
still satisfies that assertion: empty stdout raises the JSON-decoding
`MonitorError` instead. This fixture therefore does not protect the command
failure check it appears to exercise.

A bounded memory-only probe removed only `if process.returncode` from a copied
`query_pr` AST. It ran the single existing test method with original and
mutated query functions, then supplied a failed subprocess with otherwise
valid completed-success JSON through the actual `main` path.

| Observation | Original query function | Return-code guard removed in memory |
| --- | --- | --- |
| Existing `test_cli_errors_are_not_success` | Passes | Passes |
| Return code 1 with valid matching success JSON | Exit 1, `outcome: error` | Exit 0, `outcome: success` |

The production guard itself is correct. The finding is a regression-test gap:
parseable stdout could conceal an ignored CLI failure if that guard changes.
The probe does not claim such a failure occurred in the live service.

Suggested fix: add a nonzero-exit fixture whose stdout is valid matching PR
JSON, preferably a completed-success snapshot. Assert `MonitorError` from
`query_pr` and/or exit 1 with `outcome: error` from `main`; the latter checks the
observable monitoring contract. Keep the malformed-JSON cases separate.
This is a portable test correction, not a port-specific runtime change.

## Resolution and limits

R5-2730-1 is **Open**. Review stopped at this first actionable round-5 finding.
No source fix or later round was attempted. The two ports have identical
watcher and watcher-test blobs, so the bounded probe applies to both.

The requester reports full helper suites at 321 tests plus 63 subtests,
Black 22.3.0 passing, and all 30 watcher unit tests passing on Windows
Python 3.14.6. Those complete suites were not rerun. This round executed only
the single-method mutation check and synthetic CLI cases described above,
with subprocesses/clocks controlled and no real GitHub query or waiting.

Prior-head live CI remains baseline evidence, not proof of this unpublished
staged tree. Only this unstaged round-5 report was added in this round.
No source file, index, commit, ref, or resource was changed. No completed
three-family gauntlet is claimed.

## Resolution: R5-2730-1

Resolved by changing only the nonzero-exit fixture to contain valid
completed-success PR JSON. The malformed-JSON cases remain separate. This
isolates the command-exit guard without altering the production watcher.
The same test-only correction is present on both ports.

A memory-only mutation check on each port confirmed that the old fixture
passed with or without the guard, while the strengthened fixture passes
with the guard and fails when it is removed. No production file was mutated.
Both final watcher suites passed 30 tests and 63 subtests, and Black 22.3.0
left both Python files unchanged. The earlier complete helper runs passed
321 tests and 63 subtests before this fixture-only correction.

The corrected staged source tree is
`c71e2e21ffa66df5af2b4b485689b681defdb97b`.
The original review and its pre-fix fingerprints above are retained as history.
This is test hardening found during integration, not a runtime divergence:
all eight other incoming files remain byte-identical to master.
