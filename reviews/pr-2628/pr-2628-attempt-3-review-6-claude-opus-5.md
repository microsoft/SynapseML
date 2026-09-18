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

---

# Round 6 (follow-up) — Derivative-tag recovery and native Git tag writes

The record above is unchanged. This section reviews only the later follow-up
delta, which was not part of the round covered above.

## Review Summary
- **Round**: 6 (follow-up pass over the derivative-tag recovery change)
- **Theme**: Polish & hardening — documentation accuracy, error reporting,
  observability, compatibility, maintainability
- **Model**: claude-opus-5
- **Reviewed head**: `fcbe55b7875a4cc8e66b5870e93e01d26c510490`
  (`work/release-repeatability-20260915`), uncommitted working tree
- **Issues Found**: 6 (all Low; none blocks merge)
- **Verdict**: APPROVE WITH NITS

## Reviewed delta (uncommitted, against the reviewed head)

| File | + | - |
| --- | --- | --- |
| `.github/workflows/release-prepare.yml` | 10 | 10 |
| `.github/workflows/release-tag-spark.yml` | 15 | 12 |
| `.github/workflows/release-tag.yml` | 91 | 37 |
| `scripts/release/README.md` | 19 | 0 |
| `scripts/release/release_guard.py` | 98 | 16 |
| `scripts/release/test_release_guard.py` | 207 | 3 |
| `scripts/release/test_release_tag_recovery.py` (new) | 531 | 0 |
| `scripts/release/test_release_ops.py` | 19 | 0 |
| `scripts/release/test_release_workflows.py` | 2 | 2 |

## Evidence Checklist

- [x] **The exact-ref switch closes a real resolution hole.**
  `git rev-parse --verify refs/tags/<tag>` still applies the DWIM rule list, so
  a nested `refs/tags/refs/tags/<tag>` satisfies it when the intended tag is
  absent. Every read now uses `git show-ref --verify` and peels the object that
  ref yielded (`release-tag.yml:120-125`, `:192-207`;
  `release-tag-spark.yml:113-121`, `:155-162`; `release-prepare.yml:311-314`;
  `release_guard.py:113-118`). `test_release_tag_recovery.py` covers the nested
  local tag, the nested remote tag, a tag shadowing `origin/master`, a tag
  shadowing `origin/spark4.0`, and the Maven admission path.
- [x] **Remote confirmation is name-exact and peel-exact.** `_remote_refs`
  (`release_guard.py:125-139`) discards any ref outside the requested set,
  rejects a non-40-hex object and rejects a duplicate name; `verify_remote_tag`
  (`:141-148`) requires the direct ref to be present and the peeled object, when
  the remote supplies one, to equal the expected commit. The ten-case
  parametrisation in `test_release_guard.py` pins each accept/reject pair,
  including peeled-without-direct and direct-without-matching-peel.
- [x] **The push path can only create the selected objects.** `push_tags`
  (`release_guard.py:151-196`) validates the expected commit shape, rejects an
  empty or duplicated selection, runs `check-ref-format`, resolves each tag by
  exact ref and requires `<oid>^{commit}` to equal the approved commit, all
  before any ref is written. Staging is one `update-ref --stdin` transaction
  under a fresh `refs/synapseml-release-push/<uuid>/` prefix, and the push is
  `--atomic --no-follow-tags` with a literal pattern mapping. No `--force`, no
  `+` refspec, no follow-tags path.
- [x] **Cleanup is expected-ID transactional and runs on both outcomes.** The
  `finally` block (`release_guard.py:185-195`) deletes with the original object
  IDs, so a changed staging ref aborts the transaction and the refs survive
  instead of being removed. Recovery is safe either way: a push that succeeded
  before a cleanup failure leaves the tag at the approved commit, and a re-run
  takes the "already exists at the same commit" branch and re-verifies.
- [x] **Recovery is bound to recorded merge SHAs, never to a moving tip.**
  `reconcile_target_tags` (`release-tag.yml:173-231`) accepts an authorising
  commit only from the merged PR's recorded merge SHA, which the caller has
  already required to be an ancestor of the exact `TARGET_COMMIT` OID captured
  once per target (`:258`, `:268-272`). With no recorded merge, the legacy path
  refuses to invent one: both tags must already exist, agree with each other and
  remain on the target branch (`:183-196`). `test_rerun_recovers_*` asserts the
  recovered tags land on the merge SHA while the branch tip has moved past it.
- [x] **Existing tags are never moved and open PRs are never touched.**
  `:205-210` fails on any existing tag that disagrees with the authorising
  commit; the open-PR branch returns before any tag work (`:243-251`).
  `test_existing_tags_at_the_wrong_commit_are_never_moved`,
  `test_legacy_pair_must_agree_without_a_merged_pr` and
  `test_open_release_prs_keep_their_reviewed_branches_and_do_not_mint_tags`
  assert byte-identical remote ref maps afterwards.
- [x] **Partial writes cannot report success.** A server rejection
  (`pre-receive` hook), a remote tag already at a different commit, and a remote
  tag deleted behind a present local tag all fail the step with the remote ref
  map unchanged, and the staging namespace is empty afterwards.
- [x] **LF transaction bytes and no stderr echo.** `_git`
  (`release_guard.py:99-111`) encodes stdin as UTF-8 bytes rather than using
  text mode, which is what keeps `update-ref --stdin` correct when the tooling
  is exercised from a Windows host, and it still never interpolates Git's
  stderr into the raised message. Both properties are pinned by
  `test_git_transactions_use_lf_bytes_without_echoing_stderr`.
- [x] **Compatibility.** `verify-tag` and `push-tags` are additive subcommands;
  no existing subcommand, flag, JSON document or exit code changed.
  `full-release --repo` keeps its behaviour and only narrows a lookalike branch
  name that previously satisfied `ls-remote --exit-code`. The workflows call the
  script from the same checkout that carries the change, and
  `release-tag-spark.yml` runs against the rebased release branch, which carries
  the master content, so no port branch needs the script ahead of the release.
- [x] **No debug residue.** No `TODO`/`FIXME`/`HACK`, no commented-out code and
  no debug print appears in the added lines of the workflows or the scripts.

## Findings (all Low; none blocks merge)

1. **Tag recovery leaves no positive record in the run log.**
   `release-tag.yml:214-221` creates the missing tags and pushes them with
   `push-tags … > /dev/null`, and nothing echoes which tags were recovered. Both
   exit lines (`:274`, `:284`) read "both remote tags are verified" whether this
   run recovered a tag or found both already present. Recovery is the headline
   behaviour of the change and is currently invisible to the release engineer
   reading the run. Suggested fix: echo the recovered names, or drop the
   `> /dev/null` so the guard's `{"pushed_tags": […]}` line lands in the log.
2. **`${TO_PUSH[*]}` now prints option flags.** The array holds `--tag`/value
   pairs, so `release-tag-spark.yml:178` prints
   `✅ Pushed: --tag v1.2.3-spark4.0 --tag v1.2.3-python3.12`. Suggested fix:
   keep a separate display array of bare names.
3. **The generated PR body exposes a fully qualified ref path.**
   `release-tag.yml:339` still interpolates `${BASED_ON}`, which is now
   `refs/heads/release/v…` or `refs/remotes/origin/release/v…`, while the merge
   order paragraphs at `:312` and `:317` use the trimmed `${DISPLAY_BASE}`. The
   PR reads "Rebases `spark4.1` onto `refs/heads/release/v1.2.3-spark4.0`",
   where it previously read `release/v1.2.3-spark4.0`. Suggested fix: use
   `${DISPLAY_BASE}` at `:339` as well.
4. **The workflow header no longer describes the job.**
   `release-tag.yml:3-6` still says the job creates the two master derivative
   tags and opens a rebase PR per spark branch. A re-run now also reconciles and
   recovers the port tag pairs. One header line would keep the file's own
   documentation true; `scripts/release/README.md:75-91` already documents the
   behaviour correctly.
5. **`_git`'s failure text is inaccurate for the new write paths.**
   `release_guard.py:108` raises "git push failed while validating the release
   checkout" and "git update-ref failed while validating the release checkout"
   for staging, pushing and cleanup. The cleanup case is the one that matters:
   the message does not say that the staging refs under
   `refs/synapseml-release-push/<id>/` were deliberately retained, so the
   operator is not told what survived or where to look. Suggested fix:
   distinguish validation from write failures and name the retained namespace.
6. **The target-to-Python mapping now has a third hand-maintained copy.**
   `release-tag.yml:177-180` duplicates the mapping already in
   `release-tag-spark.yml:56-59` and `release_matrix.py:82-83`, and no test ties
   them together. Adding a target fails loudly through the `*)` arm, but a
   valid-looking edit to the Python version in only two of the three places
   would make the recovery path create and push a stale python tag name at the
   recorded merge commit. Suggested fix: a small test asserting both workflow
   case blocks agree with `release_matrix.TARGETS`.

## Considered and dismissed (no change requested)

- **The pattern refspec widens the push set in principle.** `<prefix>*` sends
  whatever exists under the prefix at push time, whereas explicit
  `<oid>:refs/tags/<tag>` refspecs name exactly N refs, and the cleanup list is
  the fixed staged set, so a ref *added* under the prefix between staging and
  push would be pushed and then left behind. Reaching that requires concurrent
  write access to the local ref store, which is strictly more power than is
  needed to rewrite `refs/tags/*` directly, and `--atomic` plus the subsequent
  `verify-tag` still bind the intended tags to the approved commit. Recorded as
  residual. A cheap belt-and-braces check, if ever wanted, is to assert
  `for-each-ref <prefix>` equals the staged set immediately before the push.
- **Guard failures are stderr lines, not `::error::` annotations.** The removed
  inline `echo "::error::Remote $TAG does not confirm…"` produced a GitHub
  annotation; `error: Remote … does not confirm the reviewed commit <sha>` does
  not. The step still fails and the message is in the log, and this matches how
  the pre-existing `full-release` call in the same files already behaves, so the
  change is consistent rather than a new deviation.
- **`reconcile_target_tags` aborts the step instead of appending to `FAILED`.**
  It returns non-zero under `set -e`, so the run stops at the first mis-tagged
  target rather than collecting a summary line. For a mis-tagged release,
  stopping is the safer behaviour and the `::error::` annotation is already
  emitted; only the summary style is inconsistent.
- **A local tag present with its remote counterpart deleted is not re-pushed.**
  That case fails at `verify-tag` instead of self-healing. With `fetch-depth: 0`
  the checkout mirrors the remote, so it only arises from a race, and failing
  closed is the correct choice.
- **`MISSING_TAGS` stores `refs/tags/<name>` and strips the prefix twice**
  (`release-tag.yml:211-216`). Harmless; bare names would read better.

## Verification performed this round

- Targeted suite (`test_release_guard.py`, `test_release_tag_recovery.py`,
  `test_release_workflows.py`): **88 passed** on POSIX. On a Windows host the
  same selection is **48 passed, 40 skipped** — the new recovery module carries
  an explicit `sys.platform == "win32" or no bash/git` skip marker, so the
  workflow-execution tests are skipped natively by design, not silently.
- Full `scripts/release` suite on POSIX: **620 passed, 1 skipped**. The single
  skip is the opt-in SBT probe (`test_release_version.py:16`), which requires
  `SYNAPSEML_TEST_RELEASE_SBT=1` and the branch-selected JDK.
- Formatting: the pinned Black (22.3.0) reports all changed Python files
  unchanged. A newer unpinned Black present on the host disagrees on two files;
  that is not the pinned version and was not treated as a finding.
- All three workflow files parse as YAML, and the added lines carry no
  `TODO`/`FIXME`/`HACK`/debug markers.

## Scope and limitations

- Review only. No source, test, workflow or documentation file was modified in
  this pass, and no remote ref was written. The only files this pass changed are
  review artifacts.
- The suite results above were measured during this pass. Earlier figures quoted
  in this PR's other artifacts remain prior-round evidence.
- The recovery paths were exercised against local bare repositories through the
  new test module; no run against the real remote, and no live release, is
  claimed.
- Remote CI for the current head is still pending; nothing here is evidence of
  live publication-path behaviour.

Verdict: approve with nits. Six Low findings, all documentation accuracy,
log clarity or maintainability; none affects the tagging invariants.

### Round 6 dispositions

All six nits are addressed. Push reports now remain in the workflow log; the
Spark workflow no longer renders CLI flags as tag names. Generated PR bodies use
the display ref, and the orchestrator header describes recovery. Git failures
use operation-neutral wording, while cleanup failures explicitly identify the
retained staging prefix and warn that remote tags may already exist. A regression
ties both workflow Python-version maps to `release_matrix.TARGETS`.
The cleanup-message case has a direct CLI regression.
