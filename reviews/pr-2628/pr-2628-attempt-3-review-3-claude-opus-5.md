# PR 2628 — Attempt 3, Round 3: Edge Cases & Robustness

## Resolution update

All four findings are addressed. Original findings and evidence remain below.

1. The state argument guard now rejects empty or whitespace-only values before
   any probe or store selection. Seven added CLI variants cover preflight,
   status, preview/approved resume, wait and lock inspection. Explicitly omitted
   state remains valid only for the existing preflight mode.
2. Failed plan output is deliberately retained rather than unlinked by pathname.
   This avoids deleting a file replaced concurrently after exclusive creation.
   The error now warns that a partial or unconfirmed file may remain, prohibits
   using it and directs the operator to inspect and choose a new filename. A
   failing-fsync regression proves exit 2, no success output and no overwrite on
   a second attempt. The README documents this recovery.
3. Maven admission now decodes base64 separately from JSON. Duplicate-member
   errors retain their exact cause rather than claiming bad base64. File/stdin
   reader errors include the source path. Existing root/nested regressions now
   assert these diagnostics as well as rejection.
4. The wait guide documents probe errors, exit 2 without final JSON, no implicit
   read retries and continuation with the same plan/ledger after access returns.
   The policy-read failure test now verifies no report and safe continuation
   without duplicating the earlier build.

The revised error-path selection first produced 13 product/diagnostic failures.
A fourteenth failure was an incorrect recovery-test reset that omitted required
fixture policy variables; it was corrected to restore the original variables.
The combined wait/output/Maven selection now passes all 64 cases. Pinned Black
22.3.0 accepts all six changed files. No remote queue or publication occurred.

## Review Summary
- **Round**: 3
- **Theme**: Error handling, boundary conditions, concurrency, failure modes
- **Mode**: sequential (slot 3)
- **Model**: claude-opus-5 (reasoning effort: max)
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-3-review-3-claude-opus-5.md`
- **Reviewed head**: `9dea133c0820d9e27e68ed04661b18d948575e65` (base `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24`)
- **Reviewed delta**: uncommitted working-tree changes on that head —
  `scripts/bump-version.py` (+9/-4), `scripts/release/release_guard.py` (+9/-2),
  `scripts/release/release_matrix.py` (+44/-3), `scripts/release/release_ops.py` (+164/-39),
  `scripts/release/README.md` (+65/-7), and the regression files
  `scripts/release/test_release_ops.py` (+275), `scripts/release/test_release_plan.py` (+50),
  `scripts/release/test_release_guard.py` (+29), `scripts/test_bump_version.py` (+57).
- **Issues Found**: 4 (1 Medium, 3 Low)
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the full working-tree diff for every changed file, plus the surrounding
      implementation the delta depends on: `StateStore._acquire/__enter__/save/__exit__`,
      `_queue`, `_execute`, `_observe`, `_refresh_group`, `_retry`, `_report`,
      `build_actions`, `_probes`, and `AzureRemote._get` in `scripts/release/release_ops.py`.
- [x] Verified the "release locks before sleeping" contract structurally and by test:
      `_reconcile` (`scripts/release/release_ops.py:2973-3021`) returns `_report(...)`
      from inside `with context as store:`, so `StateStore.__exit__` unlinks both locks
      before `main` reaches `sleep(...)` at line 3175. `test_wait_advances_only_approved_dependencies`
      asserts `not list(cli.state.parent.glob("*.lock"))` from inside its sleep callback.
- [x] Verified the deadline rollback restores state **in place** rather than rebinding:
      `_queue` (`scripts/release/release_ops.py:2553-2558`) uses `store.state.clear()` +
      `store.state.update(before_intent)`, so the `state` aliases held by `_execute:2609`
      and `_reconcile:2987` stay valid, and `before_intent["revision"] = store.state["revision"]`
      keeps `StateStore.save` (`:1592`, `revision += 1`) strictly monotonic.
- [x] Verified no scheduling can follow the deadline: guards at `_queue:2520`, `_queue:2553`,
      `_execute:2612`, and `_reconcile:3010-3013`, with `remote.queue` only reachable at
      `_queue:2561`. Confirmed by `test_wait_restores_unsubmitted_intent_when_its_save_exhausts_deadline`
      (parametrised at elapsed 30 and 31 against a 30 s timeout) and
      `test_wait_does_not_queue_if_last_policy_check_consumes_timeout`.
- [x] Verified the wait loop cannot spin or hang: with `wait` false, `deadline is None` is
      never dereferenced because `report is not None` short-circuits at `:3133`; the
      `continue` at `:3158` always reaches the timeout `break` at `:3133-3138`; and
      `sleep(min(poll, remaining))` is skipped when `remaining <= 0`.
- [x] Ran an offline lower-boundary probe (`--poll-seconds 1 --timeout-seconds 1`,
      injected `FakeRemote`, `urlopen` disabled): one build queued, timeout message emitted,
      exit 1, zero `*.lock` files left in the ledger directory.
- [x] Verified interruption safety: `except KeyboardInterrupt` (`:3180-3185`) sits ahead of
      the `(ValueError, RuntimeError, OSError)` handler; the durable intent write at
      `_queue:2545` precedes `remote.queue`, and `_replace` (`:1567-1591`) unlinks its
      `.write-<owner>` temporary in a `finally`. Covered by
      `test_wait_interrupt_preserves_pending_state_and_releases_locks`.
- [x] Verified duplicate-JSON rejection reaches both readers: `parse_plan_json`
      (`scripts/release/release_matrix.py:604-606`) is used by `read_plan:608` for file and
      `-` stdin sources and by `maven_plan` (`scripts/release/release_guard.py:71`);
      covered by `test_read_plan_rejects_duplicate_members` (file/stdin × top-level/nested)
      and `test_maven_payload_rejects_duplicate_members_before_checkout`.
- [x] Verified exclusive output safety: `os.open(..., O_CREAT | O_EXCL | O_WRONLY, 0o600)`
      (`scripts/release/release_matrix.py:820-822`) is symlink- and race-safe, refuses `""`
      and `-`, and requires an existing parent.
- [x] Verified the denylist boundary is exact, not prefix-based: `_denylisted_path`
      (`scripts/bump-version.py:232-233`) matches only whole path components via
      `(rel, *rel.parents)`, so `scripts/release/**` is excluded while `scripts/release_notes.py`,
      `core/release/runtime.py`, and `docs/release/README.md` stay eligible. Both call sites
      pass repo-relative paths (`scripts/bump-version.py:253`, `:641`).
- [x] Reproduced all three code findings below offline with an injected fake remote and no
      network, no publication, and no repository edits.
- [ ] No live Azure polling, real build queueing, or pushed-head CI evidence was produced —
      forbidden by this round's constraints. All wait behaviour below is verified against the
      repository's own fake remote and a monkeypatched clock.

## Issues

### Issue 1: An empty `--state` silently disables the ledger, and crashes an approved resume
- **Severity**: Medium
- **File**: `scripts/release/release_ops.py`
- **Line(s)**: 3118-3125 (argument guard), 2977-2985 (store selection), 3010-3021 (apply branch), 2609 (`_execute`)
- **Description**: `--state` is `required=True` for `status` and `resume`, but every
  downstream check tests it for *truthiness*, not for presence. The guard at `:3118`
  (`if args.state and (args.state == "-" or ...)`) and the store selection at `:2977`
  (`StateStore(...) if args.state else nullcontext(None)`) both treat `--state ""` as
  "no ledger requested". Two distinct failure modes follow:
  1. `status --state ""` and `status --wait --state ""` skip `StateStore` entirely, so the
     `must_exist` guard "Release state does not exist; run preflight or resume first"
     (`:1553-1556`) never runs. The command reconciles against a throwaway `_new_state(plan)`
     and prints a complete, plausible JSON report in which every action is `planned`.
     Reproduced: exit 1, report printed, and the ledger directory still contained only
     `plan.json` — no state file, no claim, no locks. With `--wait` the loop then polls that
     ephemeral state and stops with "Polling stopped for manual action".
  2. `resume --apply --approve-plan <id> --state ""` passes approval, reaches
     `_reconcile:3015`, and dies at `_execute:2609` with
     `AttributeError: 'NoneType' object has no attribute 'state'`. `AttributeError` is not in
     `main`'s `except (ValueError, RuntimeError, OSError)` handler, so the operator gets a raw
     traceback and the interpreter's exit code 1 instead of the `error: ...` message and exit
     code 2 that every other failure path produces.
- **Risk**: `--state "$STATE_FILE"` with an unset or misspelled shell variable expands to
  `--state ""`. An operator monitoring a real in-flight release then reads a report that
  describes an empty in-memory plan rather than the durable ledger, and may conclude that
  recorded work was never queued. The tool's entire safety model rests on that ledger, so
  operating without one — even read-only — should never be reachable by a typo. No
  publication risk was observed: the crash in case 2 happens before the first `_queue`, so
  nothing is submitted and no state is written.
- **Suggested Fix**: Reject an empty `--state` for `status` and `resume` in the same block
  that rejects `-` and the plan path (`:3118-3125`), for example by testing
  `args.state is not None and (args.state in ("", "-") or ...)`. As defence in depth, make
  `_reconcile` require a store before entering the apply branch, so `_execute` can never be
  called with `store is None`.
- **Reproduction** (offline, repository fixtures only; argparse keeps the last `--state`):
  ```python
  # scripts/release/test_release_ops.py fixtures
  plan = release_plan(repositories=["oss"], families=["upack"])
  cli.remote.missing = {("oss", "upack")}
  cli("status", plan=plan, extra=["--state", ""])     # exit 1, ephemeral report, no ledger
  cli(plan=plan, apply=True, extra=["--state", ""])   # AttributeError at _execute
  ```

### Issue 2: A failed `--output` write leaves the file it just created and blocks the retry
- **Severity**: Low
- **File**: `scripts/release/release_matrix.py`
- **Line(s)**: 816-837
- **Description**: The output file is created with `O_CREAT | O_EXCL` and then written,
  flushed, and fsynced. If any step after `os.open` fails, the `except OSError` branch
  reports "Cannot write plan output; inspect the destination before retrying" but leaves the
  newly created file in place. Reproduced by forcing `os.fsync` to raise `OSError(ENOSPC)`:
  `main` returned 2 with that message while a 3543-byte file remained at the target path, and
  the identical second attempt was refused with "Plan output already exists; preserve it and
  choose a new file". The operator is therefore told that nothing could be written, and is
  then told to preserve the artifact that was written.
- **Risk**: Low and fail-loud. A truncated file cannot load as a plan, so it cannot be
  mistaken for an approved one silently. The real cost is operator confusion during recovery:
  the durable-plan path is now occupied by a file of unknown completeness that the no-overwrite
  rule correctly refuses to replace.
- **Suggested Fix**: On the non-`FileExistsError` `OSError` branch only, unlink the descriptor's
  path before raising. The file was created exclusively in the same call, so removing it cannot
  destroy a pre-existing plan or ledger and preserves the no-overwrite contract. Alternatively,
  state in the message that a partial file may exist at the destination.
- **Reproduction**:
  ```python
  monkeypatch.setattr(os, "fsync", lambda fd: (_ for _ in ()).throw(OSError(28, "ENOSPC")))
  assert matrix.main(["--version", "1.1.4", "--output", str(path)]) == 2
  assert path.exists()  # file created by O_EXCL survives the failure
  ```

### Issue 3: Duplicate-member plans are rejected correctly but reported as a base64 error
- **Severity**: Low
- **File**: `scripts/release/release_guard.py` (primary), `scripts/release/release_matrix.py` (secondary)
- **Line(s)**: `release_guard.py:70-73`; `release_matrix.py:608-616`
- **Description**: `_unique_object` raises a plain `ValueError("release plan contains a
  duplicate JSON member")`, which is **not** a `json.JSONDecodeError`. In `maven_plan` the
  `except (binascii.Error, UnicodeError, ValueError)` clause therefore swallows the specific
  message and re-raises `ValueError("invalid base64 release plan")`. Reproduced: a valid
  base64 payload whose JSON carries a duplicate `scope` member raises
  `invalid base64 release plan`, with the real cause visible only as `__cause__`. The same
  class mismatch means `read_plan`'s `except (OSError, UnicodeError, json.JSONDecodeError)`
  wrapper does not add the `cannot read release plan <path>` context for duplicate members,
  so a multi-plan operator loses the filename.
- **Risk**: Admission still fails closed, which is the security-relevant behaviour, and the
  regression tests assert rejection rather than the message. The cost is diagnostic only: an
  operator told the payload is "invalid base64" will re-encode a plan that is actually
  well-formed base64 with a duplicate JSON key, which re-encoding cannot fix.
- **Suggested Fix**: Preserve the distinction — for example widen the guard's message to cover
  both causes, or re-raise the original text when the caught `ValueError` is not a
  `binascii.Error`/`UnicodeError`/`JSONDecodeError`. In `read_plan`, add `ValueError` to the
  wrapped exception tuple so the path context is retained.

### Issue 4: The bounded-wait documentation omits the transient-service failure mode
- **Severity**: Low
- **File**: `scripts/release/README.md`
- **Line(s)**: 191-225 ("Wait for an approved release")
- **Description**: The README documents three ways a wait ends — completion, timeout
  (exit 1), and interruption (exit 130) — plus the manual-action stop for failed, unknown,
  existing, or blocked work. It does not state the fourth, which the implementation and its
  tests make routine: any read failure inside `_probes` (`:871-882` → `AzureRemote._get:307`,
  which raises `ReleaseError` on every HTTP status error and socket timeout, with no retry or
  backoff anywhere in the module) propagates out of `_reconcile`, out of the `while True` loop,
  and into `main`'s handler, ending the wait with `error: ...` and **exit 2 and no JSON
  report**. `test_wait_rechecks_policy_before_queueing_downstream` asserts exactly this
  (exit 2 after a policy read failure on the second pass), so it is deliberate and correctly
  fail-closed — it refuses to queue downstream work under an unverified policy. But the
  README invites unattended windows of up to 86400 seconds, over which a single transient
  service error is likely, and an operator reading the current text will not know that
  re-running the identical approved command is the intended, safe continuation. The related
  read failures inside `_refresh_group` (`:2088-2092`) are already covered by the documented
  "unknown work" wording.
- **Risk**: An unattended wait can end minutes into a twelve-hour window for a reason the
  runbook does not describe, with exit 2 and no report to interpret. Operators may escalate
  to `--adopt` or `--retry` when a plain re-run is correct.
- **Suggested Fix**: Add one sentence to the same section: a transient source, policy, feed,
  or inventory read failure ends polling with exit 2 and no report; recorded builds and the
  ledger are unaffected, and the same approved command may simply be re-run.

## Verified Robustness Properties (no issue)
These were checked explicitly and behave correctly; they are recorded so the next round need
not re-derive them.

- **Locks are never held across a sleep.** Confirmed structurally and by an in-test assertion
  from inside the sleep callback. Lock ownership is re-verified byte-for-byte in `save`
  (`:1593-1602`) and released in `__exit__` (`:1633-1651`), which warns rather than unlinking
  a lock whose bytes changed.
- **Durable-but-unsent intent is rolled back, not stranded.** Verified in place, with the
  revision preserved and the next ordinary `resume` queueing exactly once.
- **Restored state resumes cleanly.** After a deadline rollback the ledger shows
  `operation: None`, `intent_at: None`, `build_id: None`, `attempts: []`, and the action back
  at `planned`.
- **Poll/timeout boundaries.** `0`, `-1`, `3601`, and `86401` are rejected before any probe,
  ledger creation, or queue (`test_wait_options_fail_before_any_probe`); the valid lower
  extreme `1`/`1` was probed directly and terminates cleanly. `--poll-seconds`/`--timeout-seconds`
  without `--wait`, and `--wait` with `--retry`, `--adopt`, `--inspect-lock`, or an unapproved
  `resume`, are all rejected up front.
- **`status` never queues.** The `status` sub-parser defines no `--apply`, so `apply` is
  always `False` and `_reconcile`'s queueing branch is unreachable.
- **Plan drift.** Re-read and compared by `plan_id` on every pass after the first, correctly
  skipped for `-` (stdin cannot be re-read, and the in-memory plan is already immutable).
- **Ledger deletion mid-wait** is refused because passes after the first set `must_exist=True`.
- **Exclusive output** is symlink-safe and race-safe under concurrent generation; the losing
  process receives the "already exists" refusal rather than clobbering a plan.
- **Denylist precision.** `scripts/release_notes.py`, `core/release/runtime.py`, and
  `docs/release/README.md` remain eligible for bumping; only whole-component matches on
  `scripts/release` are excluded, in both the scan and the post-write sweep.

## Scope and Test Limitations
- Round 3 only: edge cases, error handling, concurrency, and failure modes. Correctness,
  architecture, line-by-line data flow, coverage, and polish belong to the other rounds.
- No network, no publication, no build, no package installation, no commits, and no source
  edits were performed. All dynamic evidence comes from the repository's own fake remote
  with `urlopen` disabled and a monkeypatched clock.
- Wait behaviour is proven against a simulated clock and a fake Azure. Real polling latency,
  clock adjustment during long sleeps, and genuine Azure throttling remain unproven here.
- Lock behaviour is directory-local by design; nothing in this delta claims or was tested for
  cross-machine coordination.
- A clean property above means "checked in this round's scope", not "bug-free".

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending
