# Round 6 — Polish and hardening — latest-master sync (Spark 4.1 port, PR 2734)

| Field | Value |
| --- | --- |
| Round | 6 of 6 — final polish, performance, observability, docs, naming |
| Model | claude-opus-5 |
| Task | `task-latest-master-20260922`, attempt 1 |
| Branch | `spark4.1` port worktree, in-progress normal merge |
| HEAD | `d6691ae29c20a87139e37ec6d5966f23b3b760dd` (unchanged since round 3) |
| MERGE_HEAD | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Merge base | `714d365e71f6d2db5b7072094a4a3ad22485eb57` |
| Staged tree (round 3) | `7ba0eb1de595bc97ceddccf300362ec574822d42` |
| Staged tree (now) | `7d2e2447128547d784411ed9f9d6a4bb47295fa3` — matches the fingerprint supplied for this round |
| Index state | 0 unmerged entries, 0 unstaged modifications, `git diff --cached --check` clean |
| Verdict | **ISSUES_FOUND (1 Low, optional hardening)** — both prior findings verified fixed; no blocking defect |

## Scope

Bounded to the delta since my round 3: a fingerprint check plus the two files the
round 3 and round 5 fixes touched. No repository rediscovery, no re-run of the
suites, no re-audit of the incoming master content (that was settled in round 3).

Delta since the round 3 staged tree is exactly two files, identical on both ports:

```
tools/ci/README.md                | 15 ++++++++-------
tools/ci/tests/test_e2e_impact.py | 13 +++++--------
2 files changed, 13 insertions(+), 15 deletions(-)
```

`tools/ci/e2e_impact.py` and `pipeline.yaml` are untouched since round 3, so the
selector logic, job conditions, and `fetchDepth` remain as previously reviewed.

## Fix verification

### R3 L1 — `tools/ci/README.md` overstated Fabric E2E — FIXED

The corrected paragraph now reads as five Databricks CPU jobs plus one Databricks
GPU job, drops the "all seven jobs" claim in favour of "all **enabled** notebook
E2E jobs", and adds an explicit sentence that Fabric E2E stays disabled on this
Spark port regardless of selector output. `seven` no longer appears anywhere in
the file, and the only two remaining Fabric mentions are the unrelated credential
retry note and the new disable statement — so no residual contradiction.

The edit also replaced a stale clause about "the Fabric fork credential
restriction" with "Explicit family-disable parameters still apply". That is a
second correction beyond what I filed: on this port the Fabric condition is a
plain `false`, so there is no fork restriction left to describe. Accurate, and it
removes a claim that would otherwise have aged badly.

Consequence worth recording: `tools/ci/README.md` now intentionally diverges from
master and becomes a fifth justified port difference. This is permitted — the
repository guide requires only `AGENTS.md` and `CONTRIBUTING.md` to stay identical
across branches. Both ports carry byte-identical README content, so a future
master merge conflicts the same way in both trees and can be resolved once.

### R5 Low — override test passed through an unrelated fail-open path — FIXED

The replacement builds a real PR fixture and pins a control assertion before
varying the override:

- `make_pr({PYTHON_TEST: "changed\n"})` produces valid merge-ref metadata, so
  `changed_paths` succeeds instead of raising.
- The control asserts `select_suites(...) == frozenset()` with the fixture's
  seeded `SYNAPSEML_FULL_TESTS: "false"`, proving the scenario is genuinely
  skippable.
- Only then are the five override values applied and `ALL_SUITES` asserted.

This is the right shape. Previously the assertion was satisfied by the fail-open
reaction to invalid metadata, so the override guard was never exercised; now the
guard is the only thing that can turn a proven-skippable case into `ALL_SUITES`.
The parent's mutation result — old five assertions all passed with the guard
removed, new five all fail — is consistent with the code I read.

## Findings

### L6-1 (Low, optional hardening) — the sibling guard test retains the weakness round 5 just fixed

`test_non_pr_runs_do_not_even_consult_git`, immediately above the corrected test,
still passes `tmp_path` with no PR metadata. I verified edit-free, by calling the
selector directly with the guard's short-circuit bypassed, that a non-PR run
against a non-repository directory returns `ALL_SUITES` anyway — through the
fail-open path, with the warning reporting that the checkout is not an Azure PR
merge ref. The test's assertion therefore holds whether or not the `BUILD_REASON`
guard exists, which is precisely the defect round 5 repaired next door.

Two honest qualifications:

- This is **not** a current behaviour defect. Nothing is mis-selected today, and
  a real non-PR build cannot pass the merge-ref checks, so removing the guard
  would not cause an incorrect skip in ordinary CI.
- The property it protects is narrow: the documented promise that manual runs are
  always full. That only becomes observable if a manual rerun targets a
  `refs/pull/<number>/merge` checkout while the PR source-commit variable is also
  set.

The test name also claims git is "not even consulted"; the run I observed proves
the selector bails at the ref-shape check before invoking git, but the assertion
itself does not verify that property either.

Mechanical fix, if taken: reuse the round 5 pattern — build the fixture with
`make_pr`, assert the skippable control, then set the non-PR reason and assert
`ALL_SUITES`. Declining is also reasonable; this is polish on a test that guards a
non-failing path, not a repair. Whatever is decided should be applied to both
ports together so the two trees stay byte-identical here.

## Performance

The round 5 fix trades one in-memory call for five real repository constructions,
each running roughly ten git subprocesses. Eight of the file's twenty-two tests
now build fixtures this way. That is the correct trade — a fast test that cannot
fail is worth less than a slower one that can — and the selector helper suite
remains small enough that the cost is not material. No production path changed,
so CI wall-clock is unaffected outside the helper suite.

## Observability

Unchanged and still sound: decisions go to stdout, diagnostics to stderr, and the
fail-open detail is JSON-encoded so captured git stderr cannot break out of the
logging-command line. The only non-fail-open route remains an unexpected exception
type, which surfaces as a failed step rather than a silent skip.

## Documentation and naming

Documentation is now accurate for this port and no stale count survives. One
naming nit: `test_forced_or_unknown_full_test_option_runs_everything` now also
asserts the default-false baseline, so its name describes only the override half
of what it checks. Harmless; a name mentioning the control would read better.

## Evidence and limits

- Fingerprints, HEAD, MERGE_HEAD, merge base, index cleanliness, and the two-file
  delta were verified directly in this worktree.
- Both changed files are byte-identical to the Spark 4.0 port's copies.
- Local gates (aggregate compile, Test/compile, style, Scala suites, the 121
  selector tests, the 291 CI-helper tests, Black) are taken as reported by the
  parent and were not re-run here. The previously recorded git-replay fixture
  mismatch and its isolated-retry equivalence belong to round 3; nothing in this
  two-file delta touches that area.
- A local Black run flags one blank line in the changed test file, but the
  installed Black is 26.5.1 while the repository pins 22.3.0; an **untouched**
  sibling helper test flags identically, and the file's import block is
  byte-identical to master. This is a version artifact of an unpinned local tool,
  not a regression from the fix, and not a finding.
- No Gemini-family review has executed for this task. The backend returned HTTP
  400 with zero turns before execution, so the three-family review gate is
  **unfulfilled**. This is not a full-gauntlet green.
- Azure validation and current-head GitHub review are outside this round.
- This review made no source edits and performed no staging, commits, pushes, or
  cloud calls. The only file written is this artifact.

## Resolution log

L6-1 is fixed and verified. The finding above is preserved as originally written.

`test_non_pr_runs_do_not_even_consult_git` now takes `monkeypatch` and replaces
`tools.ci.e2e_impact.changed_paths` with `unexpected_detection`, which raises
`AssertionError`. The five reasons and the `ALL_SUITES` assertion are unchanged.
Delta since the round 6 tree is one file, 5 insertions and 1 deletion, identical
on both ports; staged tree is now `d5fe94742dcc3bf4d174250430490166c600f523`,
with HEAD and MERGE_HEAD unchanged and no unstaged or unmerged entries. The
request to apply this fix to both ports together was honoured: the file is
byte-identical to the Spark 4.0 port's copy.

I verified four properties independently rather than accepting the pattern:
the test imports from `tools.ci.e2e_impact`, the same dotted path it patches, and
`select_suites.__globals__` is that module's dict, so the patch really intercepts
the call; `AssertionError` is outside the handled `(OSError,
subprocess.SubprocessError, ValueError)` set, so fail-open cannot swallow it;
with the guard present the raiser never fires and `ALL_SUITES` is returned; and
with the guard removed in memory the `AssertionError` escapes `select_suites`.
That reproduces the reported old-five-pass, new-five-fail result. Running the two
guard tests gives 10 passed, 111 deselected — the 121 selector tests as reported.

This is stronger than the pattern I suggested: it proves git is never consulted,
which is what the test name claims, instead of only pinning the returned set.

Validation note, recorded as reported and not re-run: the latest full 291-test
suite passed on this port. The other port hit the same unchanged
scoped-prerequisite Git index fixture mismatch previously seen here, and the
mounted-filesystem flake is not claimed resolved; native CI is still required.

## Metadata-only publication correction

The driving GPT reviewer removed machine-local checkout paths from all four
GPT report headers, covering Copilot comments 4071822345, 4071822456 and
4071822503 plus the same pattern in round 1. This follows
`reviews/pr-2708/README.md`; original findings, resolutions, source references,
reviewed revisions and fingerprints remain intact.

The correction was checked directly across the six review themes: completeness
of all matching headers, consistent generic metadata, absence of residual host
paths, unchanged evidence values, a repository scan and diff check, and no
runtime or performance change. This is artifact-only recovery of the completed
review, not a new multi-model review or a claim that Gemini became available.
