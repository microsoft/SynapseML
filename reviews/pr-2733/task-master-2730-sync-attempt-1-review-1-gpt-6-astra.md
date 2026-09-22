# Round 1 review: master 2730 port integration

## Review summary

| Field | Evidence |
| --- | --- |
| Target | microsoft/SynapseML#2733, base `spark4.0` |
| Scope | Only the nine-file staged import of microsoft/SynapseML#2730 |
| Baseline / reviewed HEAD | `d08aefee223d5fd7024a78dcd5e19a60b323967d` |
| Common merge base | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Master / MERGE_HEAD | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Staged source tree | `ce3d1c2567a6d8def64fb7d926b4b41556302fc8` |
| Fingerprint capture | `git write-tree`, before creating this report |
| Round / attempt / mode / model | 1 / 1 / sequential / `gpt-6-astra` |
| Findings / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2733\task-master-2730-sync-attempt-1-review-1-gpt-6-astra.md` |

Index-manifest SHA-256, from raw `git ls-files --stage -z` output:

`f1ba864e6cd04958efc07fc672b38cc36ad48222b5e2aa023d1000819a7227fa`

## Integration evidence

- [x] The master delta from the common base and the staged delta from HEAD
  contain exactly the same nine paths. Every incoming file's mode and blob
  equal master, and the nine blobs are identical across both ports.
- [x] None of these nine paths had a port-only change between the common base
  and baseline HEAD. Every other tracked path matches baseline HEAD exactly.
  No retained port edit was overwritten or new port-only change introduced.
- [x] Product/runtime sources, dependency pins, pipeline, workflows, templates,
  and existing Fabric/streaming/GPU policy remain unchanged from the baseline.
  `build.sbt:33-36` retains Spark 4.0.1 and Scala 2.13.16;
  `environment.yml:6` retains Python 3.12.11.
- [x] The merge has no unresolved index entries or unstaged tracked changes.
  Previous sync work was not reopened or counted as this round's scope.

## Tooling and guidance review

- [x] `.github\skills\synapseml-pr-loop\scripts\watch_azure_pipeline.py:35-100`
  validates the HTTPS Azure host/project/path and positive int32 build ID,
  and issues a bounded, argument-list `gh pr view` against the fixed upstream
  repository. It does not fetch the returned Azure URL, execute a shell
  command string, dump credentials, or trigger/cancel builds.
- [x] The same file's `monitor`, lines 103-189, binds observations to the
  requested PR head and build. Missing/malformed/duplicate results fail
  explicitly; a changed head or newer build stops with a distinct non-success
  result. Only `SUCCESS` becomes success. The deadline derives from kickoff
  and remaining wall time, then uses a monotonic clock; query time and clipped
  sleeps consume that budget rather than restarting it.
- [x] Lines 192-270 normalize timezone-aware kickoff input, validate identity
  and the 1-120 minute limit, reject future kickoff times, and emit startup
  and terminal JSON with explicit nonzero error/timeout/interruption outcomes.
  Both new Python files parse under Python 3.12 and 3.13 grammar and use only
  standard-library imports. This is syntax/source evidence, not execution on
  both interpreters.
- [x] Read all of `tools\ci\tests\test_watch_azure_pipeline.py`. Its mocked
  clock and CLI cases cover 600-second polling, kickoff-based expiry, late
  starts/restarts, replacement runs, deadline-clipped queries, terminal
  failures, legacy status contexts, malformed/untrusted URLs, int32 limits,
  changed heads, CLI failures, and interruption. Importing the watcher defines
  its functions; the live command is guarded by `__name__ == "__main__"`.
  The tests were inspected, not rerun.
- [x] The contributor skill and safety reference separate trusted pinned-base
  guidance from PR-authored instructions, preserve safeguards when membership
  is unverified, and distinguish edit access from authorization. They require
  review before execution, separate permission for secret-dependent CI, and
  a new check after the head changes. The contributor message and procedure
  preserve authorship, discussions, and contributor sign-off.
- [x] PR-loop and readiness guidance now make missing CI a blocker rather than
  implicit permission to trigger it. The documented read-only waiting command
  omits mutation switches. Necessary context in
  `.github\skills\synapseml-pr-loop\scripts\Get-PrReadiness.ps1:55-82,350-359,421-441`
  confirms `-RunPipeline` is opt-in and separate from waiting.
- [x] The polling guidance matches the watcher's repository/project limits,
  replacement handling, timeout behavior, and read-only role. It requires
  rechecking the current head/build and Azure job/test results after exit.
  The PR-writing guidance keeps risks and validation status visible.
  All 19 relative file links in the incoming Markdown resolve.

## Exact incoming files inspected

```text
.github\skills\synapseml-external-contributor-review\SKILL.md
.github\skills\synapseml-external-contributor-review\assets\contributor-comment.md
.github\skills\synapseml-external-contributor-review\references\contributor-safety.md
.github\skills\synapseml-pr-loop\SKILL.md
.github\skills\synapseml-pr-loop\references\ci-triage.md
.github\skills\synapseml-pr-loop\references\readiness-gates.md
.github\skills\synapseml-pr-loop\references\writing-prs.md
.github\skills\synapseml-pr-loop\scripts\watch_azure_pipeline.py
tools\ci\tests\test_watch_azure_pipeline.py
```

Additional context was limited to `AGENTS.md`, the runtime-version declarations
above, and the cited readiness-helper sections. Proposed guidance was reviewed
as data, not activated to authorize execution.

## Limits and handoff

No concrete correctness, compatibility, or safety defect was found in this
port integration or the inspected incoming tooling/guidance. The caller is
independently running CI-helper tests and pinned Black; neither was rerun and
their results are not claimed here. The live watcher, GitHub/Azure status lag,
and real service responses were not exercised. A watcher success remains a
check result, not a substitute for current-head readiness evidence.

Only this unstaged report was added. No source change, staging, commit, push,
or CI/resource operation was performed. No Gemini probe was made; the earlier
pre-execution failure is not Gemini review coverage. This is round 1 only,
not a completed three-family gauntlet or a full merge-readiness verdict.
