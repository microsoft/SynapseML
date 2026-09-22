# Round 4 detailed correctness review

## Review summary

| Field | Evidence |
| --- | --- |
| Target | microsoft/SynapseML#2734, base `spark4.1` |
| Scope | Only the nine-file staged import of microsoft/SynapseML#2730 |
| Baseline / reviewed HEAD | `deb9256109e6877387fafbec10ab9a210767c381` |
| Common merge base | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Master / MERGE_HEAD | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Staged source tree | `4e06ea70fce2a99dfd3789dd4ba2db83d95a83f7` |
| Round / attempt / mode / model | 4 / 1 / sequential / `gpt-6-astra` |
| Findings / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2734\task-master-2730-sync-attempt-1-review-4-gpt-6-astra.md` |

Rechecked the recorded HEAD, master, source tree, and index-manifest SHA-256:

`fd155450c0407e04824e89d5198b2ae4a21d5040cf61c4432eb0262b2bf12884`

The hash covers raw `git ls-files --stage -z` output. There are no unresolved
entries or unstaged tracked changes. Retained source reads from rounds 1/2
therefore apply to this exact snapshot. The nine incoming blobs remain
master-identical; all other tracked files remain baseline-identical.

## Detailed evidence

- [x] `.github\skills\synapseml-pr-loop\scripts\watch_azure_pipeline.py:35-63`:
  URL parsing validates scheme, host, port, absence of user information and
  fragment, and the trusted project/path before selecting a numeric build ID.
  ASCII-digit validation and leading-zero removal precede the length/range
  checks and integer conversion. IDs become integers before duplicate/latest
  comparisons, avoiding lexical ordering such as `"9"` versus `"10"`.
- [x] The same file's `query_pr`, lines 66-100, passes an argument list to
  `gh pr view`, fixes the upstream repository, and requests state, head SHA,
  and check rollup. Transport errors, nonzero exit status, decoding errors,
  invalid JSON, and non-object JSON do not become successful snapshots.
  UTF-8 decoding is explicit; there is no shell interpolation or Azure URL fetch.
- [x] `monitor`, lines 103-189, checks PR state/head before consuming check
  results. CheckRun and legacy StatusContext fields have explicit alternatives.
  Every matching URL is validated, duplicate build IDs are rejected, and a
  higher build ID wins over an older successful result. Missing/older-only
  results fail rather than being mistaken for the requested run.
- [x] The deadline uses original kickoff plus the selected timeout minus
  current wall time, then transfers that remaining budget to a monotonic
  clock. Nonpositive budgets return before querying. Both query timeout and
  sleep duration are clipped to remaining time. A result arriving at/after
  the deadline is timeout, not success; a restart with the same verified
  kickoff cannot obtain another full window.
- [x] Only a completed `SUCCESS` conclusion or legacy `SUCCESS` yields
  success. Pending states sleep; failed/neutral/skipped/cancelled conclusions
  cannot pass. Changed head/closed PR and replacement build are separate
  non-success handoffs, not automatic monitoring of unverified new inputs.
- [x] Lines 192-270 validate positive identities, full SHA, canonical repository,
  timezone-aware kickoff, future-time rejection, and the 1-120 minute limit.
  JSON context retains the input identity and kickoff-derived deadline.
  Exit codes distinguish success, failure/error, superseded, replaced,
  timeout, and interruption without triggering or cancelling a build.
- [x] `.github\skills\synapseml-pr-loop\references\ci-triage.md` agrees with
  those inputs and outputs: kickoff provenance is verified by the caller;
  timeout remains unresolved; replacement requires its own verified kickoff;
  every exit requires a head/build recheck before readiness. No pipeline or
  product behavior was changed to accommodate the helper.
- [x] The incoming contributor guidance still requires trusted instructions
  and separate execution/CI authorization. No inspected path grants authority
  from the PR's own instructions or edit-access flag. The relative references
  and retained Spark/Python-specific configuration are unchanged from round 1.

Static deadline trace for the default 120-minute limit:

| Observation | Remaining budget / outcome |
| --- | --- |
| Start at original kickoff | 7,200 seconds |
| Start 90 minutes after kickoff | 1,800 seconds |
| Start/restart at or after the deadline | Timeout before a query |
| Query consumes the last available second | Timeout, even if its response reports success |
| Newer build replaces the requested build | Replacement handoff; no clock reset in this monitor |

These are source-level traces, not additional live or full-suite runs.

## Conclusion and limits

No actionable detailed-correctness finding. The requester's green local
helper/Black results, 30 watcher unit tests on Windows Python 3.14.6, and
round-3 pure-function probes were not rerun or represented as executions by
this round. Prior-head live CI remains baseline evidence, not validation of
this unpublished staged tree.

Round 4 is CLEAN, so the requested sequential round-5 coverage review may
proceed separately. Only this unstaged report was written in this worktree.
No source edit, staging, commit, CI operation, or three-family completion
claim was made.
