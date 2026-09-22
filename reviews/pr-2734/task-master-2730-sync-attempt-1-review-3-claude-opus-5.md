# Round 3 — Edge cases and robustness — master #2730 sync (Spark 4.1 port, PR 2734)

| Field | Value |
| --- | --- |
| Round | 3 of 6 — edge cases, robustness, error and timeout contracts |
| Model | claude-opus-5 |
| Task | `task-master-2730-sync`, attempt 1 |
| Branch | `spark4.1` port worktree, in-progress `--no-commit` normal merge |
| Baseline head | `deb9256109e6877387fafbec10ab9a210767c381` |
| MERGE_HEAD | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Source tree | `4e06ea70fce2a99dfd3789dd4ba2db83d95a83f7` — matches the value supplied for this round |
| Index state | 9 staged paths, 0 unmerged, 0 unstaged |
| Verdict | **CLEAN** — no concrete actionable defect found |

## Scope and what I verified independently

Bounded to the nine staged files and the contracts they introduce. I did not
re-run the helper suites, and I did not re-audit earlier sync rounds.

Staged scope, confirmed exactly as described:

```
A  .github/skills/synapseml-external-contributor-review/SKILL.md
A  .github/skills/synapseml-external-contributor-review/assets/contributor-comment.md
A  .github/skills/synapseml-external-contributor-review/references/contributor-safety.md
M  .github/skills/synapseml-pr-loop/SKILL.md
M  .github/skills/synapseml-pr-loop/references/ci-triage.md
M  .github/skills/synapseml-pr-loop/references/readiness-gates.md
A  .github/skills/synapseml-pr-loop/references/writing-prs.md
A  .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py
A  tools/ci/tests/test_watch_azure_pipeline.py
```

I compared every staged blob ID against the same path in master
`681bd96990c421de3b91d2b1bf8f8f470764199d`: **all nine match**. I then compared
every blob against the Spark 4.0 port's index: **all nine identical**. No product,
dependency, runtime, template, or pipeline configuration file is touched, so the
portable master behaviour is preserved and no port-specific edit is warranted.

## Trust boundary — `parse_build_id`

This is the security-critical parser, since a forged details URL is what could
turn an unrelated build into a green verdict. I exercised it directly against 23
inputs (read-only calls to a pure function, not a suite re-run):

- Accepted only the intended forms: project GUID and the `a365` alias, on both
  `dev.azure.com/msdata` and `msdata.visualstudio.com`, mixed-case host and path,
  explicit `:443`, leading-zero build IDs, and exactly `2147483647`.
- Rejected userinfo spoofing (`https://dev.azure.com@evil.com/...`), foreign
  hosts, `http://`, path traversal, a trailing slash, a fragment, duplicate
  `buildId` parameters, `0`, `2147483648`, a percent-encoded path segment, a
  Cyrillic homograph host, port `8443`, an unparsable port, the empty string, and
  `None`.

Every rejection surfaced as a clean `MonitorError`; **no unhandled exception and
no bypass**. The int32 guard checks digit length before `int()`, so an absurdly
long numeric string cannot force a large conversion.

## CLI contract — `parse_args` and `parse_kickoff`

Nineteen probes: the `1..120` timeout window, 40-character SHA with case
normalisation, case-insensitive repository match with all other repositories
rejected before any query, positive-only IDs, the int32 ceiling, naive-timestamp
rejection, `Z` and offset timestamps normalised to UTC, garbage timestamps, and
rejection of a future kickoff. All behaved as documented.

## Timeout and head-change contracts

- The budget is anchored to the immutable kickoff
  (`kickoff + timeout − now`), then waited on `time.monotonic()`. A restart or a
  late start therefore inherits only the remaining time and cannot extend the
  window, which is the property the workflow depends on.
- A non-positive budget yields the timeout result before any query or sleep.
- The deadline is re-checked after each query returns, so a slow query cannot
  cause action on data that arrived past expiry.
- The per-query subprocess timeout is clipped to `min(60, remaining)`.
- The sleep is clipped to the remaining budget, so the loop cannot overshoot.
- A changed `headRefOid` or a non-`OPEN` state returns `superseded` without
  following the new head; a higher build ID returns `replaced` with its URL
  instead of silently watching a different run.

I looked specifically for a false-green path and found none: `success` requires
an exact `SUCCESS`, on a check matching the configured name, whose URL passes the
trust boundary, whose build ID equals the requested one *and* is the highest
present, with the head unchanged and the PR still open. Every other completed
conclusion — including `NEUTRAL`, `SKIPPED`, `CANCELLED`, and `STALE` — maps to
`failed`. That is the correct conservative direction.

## Documentation and workflow boundaries

All 19 relative links across the staged Markdown resolve to tracked files
(checked by normalising each link against its own directory). The documented
constants match the code: 10 minutes / 600 seconds, a 120-minute kickoff-anchored
deadline, `--timeout-minutes` able to shorten but not extend, the int32 range, and
case-insensitive repository matching.

The safety reference carries the boundary that matters most for this tooling: it
tells the reader to load the checklist from trusted guidance, to treat a copy
introduced by the PR as data, and explicitly that a PR cannot supply the
instructions authorising its own execution. The readiness and skill updates align
with that — evidence gates no longer imply CI authorisation, and every
CI-triggering action requires explicit authorisation plus a fresh head-specific
safety check for external PRs.

## Non-blocking observations

1. Any `MonitorError` before the deadline ends the watch with no retry, so one
   transient CLI or network blip costs a relaunch. This is documented ("query
   errors are nonzero results") and the workflow tells the reader to recheck
   after any exit, so it is an intentional fail-loud posture rather than a defect.
2. A check with the watched name but an empty or missing details URL is treated
   as untrusted and aborts the watch. Correct for security; reachable only in a
   narrow registration window, if at all.
3. Two checks sharing the watched name and the same build ID abort as ambiguous
   rather than picking one. Defensible.
4. A requested build that has not registered yet, with only older builds present,
   is an immediate error rather than a wait.
5. `int()` accepts underscore separators, so `--build-id 1_0` parses as `10`. It
   is operator-supplied and still range-validated, and the URL-side parser
   correctly refuses underscores, so the trust boundary is unaffected.
6. A local clock behind the Azure clock could make a genuine kickoff look
   future-dated and be rejected at argument parsing.

None of these change the outcome for a real run, and none justify diverging from
master, so I am not raising them as findings.

## Evidence and limits

- Merge state, staged scope, blob-level master identity, and cross-port parity
  were verified directly in this worktree.
- The helper suites (321 tests plus 63 subtests, 30 standalone watcher tests) and
  Black 22.3.0 are taken as reported and were **not** re-run. My probes were
  read-only calls into pure functions.
- This port's prior CI 237100199 was still pending at its old head, and the other
  port's 237087792 passed. Neither validates this unpublished merge.
- No Gemini-family review has executed for this task; the backend returned HTTP
  400. The three-family review gate is **unfulfilled** and this is not a
  full-gauntlet result. Rounds 1 and 2 were GPT-family.
- This review made no source edits and performed no staging, commits, pushes, or
  cloud calls. The only file written is this artifact.
