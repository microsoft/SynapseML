# Round 6 — Polish & Hardening

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening — performance, observability, documentation accuracy, naming clarity
- **Mode**: sequential (slot 3)
- **Model**: claude-opus-5 (reasoning effort: max)
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-3-review-6-claude-opus-5.md`
- **Reviewed head**: `9dea133c0820d9e27e68ed04661b18d948575e65` (`work/release-repeatability-20260915`)
- **Issues Found**: 0
- **Verdict**: CLEAN

## Reviewed delta (uncommitted, against the reviewed head)

| File | + | - |
| --- | --- | --- |
| `scripts/bump-version.py` | 9 | 4 |
| `scripts/release/README.md` | 75 | 7 |
| `scripts/release/release_guard.py` | 13 | 2 |
| `scripts/release/release_matrix.py` | 46 | 4 |
| `scripts/release/release_ops.py` | 167 | 41 |
| `scripts/release/test_release_guard.py` | 29 | 0 |
| `scripts/release/test_release_ops.py` | 288 | 0 |
| `scripts/release/test_release_plan.py` | 70 | 0 |
| `scripts/test_bump_version.py` | 57 | 0 |
| `reviews/pr-2628/README.md` | 3 | 0 |

Untracked: the rounds 1–5 review artifacts in `reviews/pr-2628/`, plus this report.

## Evidence Checklist

- [x] **No default-path performance regression.** Every new cost in
  `release_ops.py` is gated on `deadline is not None`: the two clock guards and
  the `copy.deepcopy` intent snapshot in `_queue` (`release_ops.py:2518-2526`,
  `2556-2561`), and the loop guards in `_execute` (`release_ops.py:2608-2620`).
  Without `--wait`, `deadline` is `None` (`release_ops.py:3131`), so
  `preflight`, `status` and `resume` allocate, sleep and poll exactly as before.
- [x] **Polling and its work are bounded.** Interval and timeout are validated
  to 1..3600 and 1..86400 seconds (`release_ops.py:3110-3117`); each pass sleeps
  `min(poll, remaining)` and never schedules past the deadline
  (`release_ops.py:3168-3175`). Per-pass cost is one plan re-read, one ledger
  open and one probe/refresh cycle in `_reconcile` (`release_ops.py:2973-3025`)
  — the revalidation the contract requires, not an added inner loop. The
  `stop_on_blocker` rescan in `_execute` is over the plan's own action list
  (at most a dozen entries) and is not a hot path.
- [x] **Output is bounded and non-noisy.** Progress is one stderr line per poll
  interval (`release_ops.py:3169-3175`, minimum spacing 1 s), terminal states
  print one explanatory stderr line (`release_ops.py:3134-3138`, `3162-3167`,
  `3178-3183`), and exactly one JSON report goes to stdout at the end
  (`release_ops.py:3176`). Intermediate reports are deliberately not printed, so
  a long wait cannot flood a log or interleave partial JSON documents.
- [x] **No payload, path or credential in any new diagnostic.** `_safe_error`
  still replaces non-`ReleaseError` service failures with a fixed string
  (`release_ops.py:148-151`); every message added this round is a static
  operator instruction. The `--output` failures deliberately omit both the
  destination path and the OS error text (`release_matrix.py:816-838`), and no
  new code prints plan JSON, base64 payloads or queue command arguments.
- [x] **Exit codes documented match the code.** `ReleaseError` extends
  `RuntimeError` (`release_ops.py:75`), so a failed source/policy/feed/inventory
  probe leaves the loop through `except (ValueError, RuntimeError, OSError)`
  and returns `2` with no JSON report; timeout falls through to
  `return 0 if ... else 1` with an incomplete report; `KeyboardInterrupt`
  returns `130` (`release_ops.py:3178-3192`). `scripts/release/README.md:223-232`
  states exactly these three outcomes.
- [x] **Documentation matches behaviour, claim by claim.**
  `README.md:64-70` (`--output` validates inputs first, never overwrites, needs
  an existing parent) matches `release_matrix.py:806-838`;
  `README.md:70` (duplicate JSON members rejected) matches `parse_plan_json`
  (`release_matrix.py:595-605`) used by both `read_plan`
  (`release_matrix.py:607-618`) and Maven admission
  (`release_guard.py:68-80`); `README.md:58-62` (bumps leave `scripts/release/`
  alone, other live paths still update) matches the directory entry
  `bump-version.py:161` with `_denylisted_path` (`bump-version.py:232-234`)
  applied to both the scan (`:239`) and the post-write sweep (`:646`);
  `README.md:192-221` (`--wait` on status or approved resume, locks released
  between polls, no adoption/retry/approval, stops on blockers, changed plan or
  missing ledger ends the run) matches `release_ops.py:3031-3047`, `3103-3113`,
  `3139-3167` and `_reconcile`'s per-pass `StateStore` context.
- [x] **`status --wait` cannot queue.** `--apply` is registered only on
  `resume` (`release_ops.py:3056-3070`), and `_reconcile` executes only when
  `apply` is true (`release_ops.py:3010-3023`), so the README's "monitoring
  without any queueing" claim holds structurally, not just by convention.
- [x] **Naming and dead code.** New names state their role
  (`parse_plan_json`, `_unique_object`, `_denylisted_path`, `_reconcile`,
  `deadline`, `stop_on_blocker`, `before_intent`); no `TODO`/`FIXME`/`HACK`,
  commented-out code or debug prints appear in the added lines; the new imports
  (`os` in `release_matrix.py:38`, `monotonic`/`sleep` in `release_ops.py:37`)
  are both used.
- [x] **Backward compatibility.** `--output`, `--wait`, `--poll-seconds` and
  `--timeout-seconds` are additive optional flags; `--json` still prints the
  same document to stdout (`release_matrix.py:806`, `842`); plan schema, state
  schema 2 and `plan_id` derivation are untouched; `parse_plan_json` only
  narrows previously accepted duplicate-member input, which the README now
  documents.

## Considered and dismissed (no change requested)

- **`before_intent` deepcopy per queued group.** Only taken in `--wait` mode,
  over a small state dict, at most once per action group per poll interval. The
  cost is negligible next to the Azure CLI round trips in the same pass.
- **`retry_snapshot` branch inside the deadline snapshot.** `--wait` is rejected
  with `--retry` (`release_ops.py:3103-3113`) and `_retry` never passes a
  deadline, so that combination is currently unreachable rather than wrong; it
  is defensive, one line, and safe if a future caller supplies both.
- **`--plan -` skips the per-pass plan re-read** (`release_ops.py:3139-3147`).
  Stdin cannot be re-read; the plan identity is still pinned by the approved
  `plan_id` and revalidated ledger each pass.
- **Failed `--output` files are retained, not unlinked.** Reviewed and settled
  in an earlier round: deleting an operator-supplied pathname after a failed
  write is the more dangerous behaviour. The hedged wording ("may remain … Do
  not use it") is accurate for every `OSError`, including a missing parent
  directory where no file was created, and a later attempt at the same path
  fails closed on `O_EXCL`.
- **Read probes fail closed without automatic retry.** Reviewed and settled;
  `README.md:223-227` documents the safe same-ledger rerun.

## Scope and limitations

- Review only: no source, test or configuration file was modified in this round,
  and no test suite, build, publication or remote service call was executed.
- Test-suite results quoted in this PR's earlier artifacts (full public release
  and pipeline suite 619 passed with 1 opt-in native probe skipped;
  `bump-version` suite 228 passed) are prior-round evidence from earlier runs,
  not measurements taken during round 6. The final full rerun is owned by the
  driving agent.
- No performance trace or profile was captured. The performance findings above
  are read from control flow and gating conditions, and cover only the code
  paths in this delta.
- Remote CI for the current pushed head is still pending; nothing here is
  evidence of live publication-path behaviour.

Clean review round: zero issues found.
