# Round 6 — Polish and hardening — master #2730 sync (Spark 4.0 port, PR 2733)

| Field | Value |
| --- | --- |
| Round | 6 of 6 — final polish, performance, observability, docs, naming |
| Model | claude-opus-5 |
| Task | `task-master-2730-sync`, attempt 1 |
| Branch | `spark4.0` port worktree, in-progress `--no-commit` normal merge |
| Baseline head | `d08aefee223d5fd7024a78dcd5e19a60b323967d` (unchanged since round 3) |
| MERGE_HEAD / master | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Source tree (round 3) | `ce3d1c2567a6d8def64fb7d926b4b41556302fc8` |
| Source tree (now) | `02363251a12bfa3329417db08469b252a0b73e31` — matches the corrected value for this round |
| Index state | 9 staged paths, 0 unmerged, 0 unstaged |
| Verdict | **CLEAN** — the round 5 fix is verified; no concrete defect remains |

## Scope

Bounded to the delta since my round 3: one file, `tools/ci/tests/test_watch_azure_pipeline.py`,
6 insertions and 1 deletion for a net **+5 lines**. I did not repeat the round 3
trust-boundary and CLI sweeps, and I did not re-run any suite.

## Master fidelity

Re-checked every staged blob against master `681bd969…`:

- **8 of 9 are byte-identical to master** — the production watcher and all seven
  guidance files. The round 5 fix touched none of them.
- `tools/ci/tests/test_watch_azure_pipeline.py` is the **sole** deviation:
  477 lines in master, 482 staged, exactly +5.
- All nine blobs remain **identical between the two ports**.

So the fix is confined to test scaffolding, and no runtime, dependency, template,
or pipeline configuration is involved.

## Round 5 fix verification

The change replaces only the nonzero-exit fixture's stdout:

```python
subprocess.CompletedProcess([], 1, "", "authentication required")
# becomes
subprocess.CompletedProcess(
    [], 1, json.dumps(snapshot("COMPLETED", "SUCCESS")), "authentication required"
)
```

I reproduced the mutation independently, in memory, by loading the real watcher
source and a copy with the `if process.returncode:` guard deleted, then driving
`query_pr` with each fixture. No file was modified.

| Fixture | Real guard | Guard removed |
| --- | --- | --- |
| Old: exit 1, empty stdout | `MonitorError` (CLI failed) | `MonitorError` (invalid JSON) |
| New: exit 1, valid success JSON | `MonitorError` (CLI failed) | **returns normally → `assertRaises` fails** |
| Retained: exit 0, `invalid json` | `MonitorError` | `MonitorError` |
| Retained: exit 0, `[]` | `MonitorError` | `MonitorError` |

This confirms both halves of the claim. The old fixture raised in *both*
variants, so the assertion could never detect guard removal — round 5's diagnosis
was exact. The new fixture raises only while the guard exists, so the guard is
now the sole cause of the exception, and removing it produces exactly one failing
assertion with no errors.

Keeping the two malformed-stdout cases is correct: they exercise the JSON-decode
and non-object paths, which are genuinely guard-independent, and my table shows
they contribute no false confidence about the exit guard.

The fix is minimal and well targeted — it changes one constructor argument rather
than restructuring the test, and it leaves the three launch-failure cases
(`TimeoutExpired`, `OSError`, `UnicodeDecodeError`) untouched.

## Performance

One extra `json.dumps` of a small dictionary, evaluated once per run. No sleeps,
subprocesses, or I/O are added. Immaterial.

## Observability

Unchanged, and the fix depends on the watcher's existing discipline of emitting a
*distinct* message per failure mode. Because "GitHub CLI failed", "returned
invalid JSON", and "did not return a PR object" are separate strings, the
mutation table above could attribute each outcome to the right branch. That is
the property that made this verification possible.

## Documentation and naming

- `test_cli_errors_are_not_success` still describes what it asserts: CLI-launch
  failures exit nonzero, and malformed or failed query responses raise.
- `json` was already imported and is used ten times, so the fix introduces no new
  dependency or import churn.
- The exploded call with a magic trailing comma is the stable Black form, so the
  formatting is consistent with the pinned formatter by construction.
- No guidance file changed, so the round 3 documentation review still stands and
  no doc drift is possible in this delta.

## Non-blocking observation

The new fixture detects guard removal only because it is a *fully valid success*
snapshot — matching head, `OPEN` state, and the trusted URL whose build ID equals
the `--build-id` in the shared argument vector. If `snapshot()`'s defaults ever
drifted from that vector, the unguarded path would raise for an unrelated reason
and this test would quietly return to passing for the wrong reason, which is the
same failure mode round 5 found. The risk is low, because many other tests pin
those same defaults and would fail loudly first. Recording it as context for
future edits, not as a defect.

## CI evidence for this port

The prior build 237087792 passed for this port, but it ran against the old head
and does not validate this unpublished merge. Fresh CI is required after the
final commit and push. No watcher is currently running.

For context on the sibling port: its prior build 237100199 **failed**, with 64
jobs succeeding and one LightGBM job failing on an Azure OIDC TLS hostname
certificate mismatch before SBT started — zero Scala execution lines and zero
split-2 test runs in the log. That is an old-head infrastructure blocker, not a
source defect and not a pass, and it likewise requires fresh new-head CI. The
monitor correctly reported failure before its deadline.

## Evidence and limits

- Merge state, corrected tree IDs, blob-level master identity, cross-port parity,
  and the +5-line deviation were verified directly in this worktree.
- The watcher suite (30 tests plus 63 subtests) and Black 22.3.0 are taken as
  reported and were **not** re-run; the full 321-test helper sweep predates the
  fixture change. My mutation probe was memory-only.
- The round 5 report retains its original finding with the resolution appended,
  and my round 3 report is unchanged at 140 lines.
- No Gemini-family review has executed for this task; the backend returned HTTP
  400. The three-family review gate is **unfulfilled** and this is not a
  full-gauntlet result. Rounds 1, 2, 4, and 5 were GPT-family.
- This review made no source edits and performed no staging, commits, pushes, or
  cloud calls. The only file written is this artifact.
