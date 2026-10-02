# Default release targets: round 5

## Scope and limitation

This is the coordinator's direct testing and coverage review of the default
target follow-up to `2391aa166ab46238307de9edf9650102cbff950e`.
The Gemini provider failed before reviewing in round 2. It was not repeatedly
relaunched for the same unavailable provider; no independent Gemini coverage
is claimed here.

## Assessment

No outstanding coverage findings for the changed automation.

| Requirement | Evidence |
| --- | --- |
| Default to master and Spark 4.1 | Actual matrix CLI output, derived flags and publication-driver completion for two targets |
| Keep explicit Spark 4.0 support | Three-target producer/notes cases, existing port recovery cases and the unchanged pre-change plan digest |
| Do not require optional refs or policy | Bootstrap uses a real local Git remote with Spark 4.0 branch and candidate refs removed, no candidate checks, and a policy API that must not be called |
| Preserve reviewed source | Real Bash orchestration preserves an existing Spark 4.1 release PR during opt-in and repairs Spark 4.0 only on explicit inclusion |
| Fail closed on selected-target policy | Veto and unreadable-policy cases, immediate pre-submission rechecks, and rejection of a ledger claiming the selected policy is unnecessary |
| Read old ledgers | An older stricter two-target policy record remains usable without a new optional-policy query |
| Show only selected release notes | Actual evidence export enters the notes CLI for both selections; bad approval creates no output, and existing output is preserved |
| Keep optional consumer versions | Real version-bump CLI runs twice over seven source files, in default and pre-updated optional-runtime cases, with no-write previews |
| Preserve production website gating | Mixed-version publication-lock tests accept a retained published port and reject unknown or mismatched locks |
| Preserve historical documentation | The changed-file inspection shows no edits to historical snapshots or sidebars; history replay tests pass |

The post-condition regression covers preexisting destination-version references,
not only the initial all-old-version input. Windows execution also covers the
CRLF path that failed during development.

## Observed validation

- Release suite: 866 passed, with the existing opt-in SBT test skipped.
- Native version-bump and history suite: 272 passed.
- CI helpers and pipeline contracts: 356 passed and 63 subtests passed.
- Website contract suite: 36 passed.
- The real `1.2.0` source-bump preview completes without modifying files.

The tests use synthetic service responses and local Git remotes. They do not
prove hosted signing, publication, a full website build, or compatibility of
the actual Python wheel on Spark 4.1. Those limits remain explicit release
gates. Current-head hosted CI is required after the follow-up is pushed.
