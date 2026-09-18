# Round 4: detailed correctness

## Review summary

- **Round**: 4
- **Theme**: Detailed correctness, data flow and types
- **Mode**: Direct, sequential; no agents
- **Model / effort**: `gpt-6-astra` / `max`
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-3-review-4-gpt-6-astra.md`
- **Reviewed HEAD**: `9dea133c0820d9e27e68ed04661b18d948575e65`
- **Target**: `master`, resolved from the supplied base and local `upstream/master`
- **Dirty delta**: 10 tracked files, 757 insertions and 58 deletions against HEAD. Four implementation files, four test files, `scripts/release/README.md` and `reviews/pr-2628/README.md`. Three existing untracked Round 1-3 reports were excluded from historical re-review.
- **Delta SHA256**: `a66752b7244dded9e1b83fda48a5ab94c5dc283f7437af8601d724c9d1ea7daa`
- **Issues found**: 0
- **Verdict**: CLEAN

The fingerprint covers `git diff --no-ext-diff --no-textconv --binary HEAD --`, before adding this report.

## Evidence checklist

- [x] Read `AGENTS.md`, the applicable branch rules and the current dirty diff. Traced changed release code into its immediate callers and ledger validation, without auditing unrelated runtime code.
- [x] `scripts/release/release_ops.py:2518-2566,2973-3024`: the timeout rollback preserves the root ledger object through `clear`/`update`, carries forward the saved revision and recomputes its checksum on save. An offline probe using the real `StateStore` observed revisions `1,2,3,4` and statuses `planned -> unknown -> planned -> planned`, with one root object, zero submissions, a matching final report, successful ledger reload and released locks. Selected regressions also exercised equality with the deadline, grouped operations and later continuation.
- [x] `scripts/release/release_ops.py:3097-3197`: 21 explicit empty/whitespace `--state` variants returned exit `2` without probes or JSON output. Omitted state remained valid for preflight without changing an existing ledger. Focused tests confirmed bounded final sleep, retained pending run IDs, and exit `2` with no report after a policy-read failure followed by safe continuation.
- [x] `scripts/release/release_matrix.py:595-617` and `scripts/release/release_guard.py:67-79`: file, stdin and Maven admission share duplicate-member rejection at nested and root levels. Read errors retain the input path. Offline probes distinguished invalid base64, malformed JSON, invalid JSON encoding and duplicate members; a UTF-8 BOM file retained its plan identity.
- [x] `scripts/release/release_matrix.py:815-843`: validation precedes exclusive file creation. Existing bytes survive a second generation attempt. A persistence failure returns exit `2`, emits no success stdout and explicitly marks the retained output unusable. Preserving that file is deliberate; the failure path does not unlink a pathname it cannot safely reclaim.
- [x] `scripts/bump-version.py:232-256,638-654`: both discovery and the post-write sweep use the same repository-relative ancestor exclusion. Two successive invocations of the actual bump script preserved release fixtures byte-for-byte while updating a live `core/release/runtime.py` pin.
- [x] Ran 25 selected cases from `scripts/release/test_release_ops.py`, `scripts/release/test_release_plan.py`, `scripts/release/test_release_guard.py` and `scripts/test_bump_version.py`: 25 passed, with one unknown-`slow`-mark collection warning. Additional offline probes supplied the alias/revision, whitespace-state and encoding traces above.
- [ ] Native pushed-head previews and CI remain pending. No network, live queue, full-suite, installation, commit or push operation was performed in this round.

Clean Round 4: zero confirmed actionable findings. This is not publication-path evidence or a verdict on the remaining review rounds.

## Round 4 follow-up: derivative-tag recovery

- **Theme:** Detailed correctness, exact identities and command-status data flow.
- **Model / effort:** `gpt-6-astra` / `max`.
- **Mode:** Direct, sequential, Round 4 only; no agents.
- **Reviewed base and HEAD:** `fcbe55b7875a4cc8e66b5870e93e01d26c510490`.
- **Scope:** The uncommitted recovery delta in `.github/workflows/release-tag.yml`,
  `scripts/release/test_release_tag_recovery.py`,
  `scripts/release/test_release_ops.py`, and `scripts/release/README.md`.
- **Workflow SHA256:** `3a59289de68c95a1079c254d56783ed64d1fabd17955354676d9bdb5e91f7022`.
- **Issues found in this follow-up:** 1, Medium.
- **Verdict:** ISSUES_FOUND. The earlier review above is preserved as history.

### Evidence checklist

- [x] Traced `TARGET`, `EXPECTED`, `PYTHON_VER`, `TAG` and the local `TO_PUSH`
  array through each helper branch. The unknown-target guard precedes use of
  the runtime mapping, and each call initializes its own push list.
- [x] Traced open-PR preservation, recorded merge ancestry, paired peeled
  commit comparisons, and the legacy verification-only path. The clarified
  diagnostic does not add automatic creation authority.
- [x] Checked ordinary helper invocation under `set -euo pipefail`, separate
  command-substitution assignments, explicit failure returns and atomic pushes.
  The accepted annotated-tag race remains a fail-closed limitation, not an
  additional finding here.
- [x] Tested the actual extracted workflow step through the existing native-Git
  fixture. Four controls rejected absent/wrong exact remote tags; five subsequent
  counterexamples reported success without the required exact tag identity.
- [x] Used `git show-ref --verify` as the exact-ref oracle. A `rev-parse`
  lookup is itself permissive in the case below and cannot establish exact
  ref presence.

### R4-TAG-1: Require exact tag ref names, not suffix/hash matches

- **Severity:** Medium.
- **File:** `.github/workflows/release-tag.yml`.
- **Lines:** `186-201` for local lookups and `218-219` for remote verification.
- **Description:** `git ls-remote` patterns match ref-name suffixes. Querying
  `refs/tags/v1.2.3-python3.12` can also return the legal tag ref
  `refs/tags/refs/tags/v1.2.3-python3.12`. The verifier checks only the advertised
  SHA with `grep`, not the returned ref name, so the other tag can satisfy it.
  In addition, `git rev-parse --verify` can resolve that nested tag when the
  exact local ref is absent, incorrectly keeping it out of `TO_PUSH`.
- **Impact:** Recovery can exit zero and report both remote tags verified while
  the required tag is missing or points at the wrong commit. No tag movement
  was observed; this is false completion and missed recovery, not the accepted
  race that visibly rejects a push.

#### Verified reproduction

Using `scripts/release/test_release_tag_recovery.py`'s temporary bare-origin
fixture and its real extracted workflow step:

1. Publish valid local/remote pairs, then remove the exact remote Python tag
   or point it at the fixture's primary commit instead of the recorded merge.
2. Confirm that the workflow rejects this state without a lookalike ref.
3. Add `refs/tags/refs/tags/v1.2.3-python3.12` on the temporary origin at the
   expected merge commit and rerun the same step.

| Authorization path | Required exact remote tag | Control exit | Exit with nested lookalike |
| --- | --- | --- | --- |
| Recorded merge | Missing | 1 | 0 |
| Recorded merge | Wrong commit | 1 | 0 |
| Legacy existing pair | Missing | 1 | 0 |
| Legacy existing pair | Wrong commit | 1 | 0 |

In all four cases, the exact remote tag remained missing/wrong and the workflow
left remote refs unchanged while printing that both tags were verified.

A fifth check removed the exact local tag too and fetched the nested tag.
`rev-parse` resolved the lookalike, while `show-ref --verify` confirmed the
exact local and remote refs were absent. The workflow again exited zero and
created neither required ref. Thus a fresh local view does not cure this case.

#### Required fix

Use an exact local-ref existence lookup, such as `git show-ref --verify`,
and peel the object obtained from that exact ref. Parse remote advertisements
by their complete ref-name field. Require the exact tag ref, then compare its
exact `^{}` peeled ref when present, or its direct object ID for a lightweight
tag. A matching SHA on another returned ref must not count as evidence.

Add native-Git regressions for suffix-lookalike refs with missing/wrong exact
tags, including the no-exact-local-ref case. Preserve annotated-tag handling,
immutable refs, atomic pushes and the existing fail-closed cases.

### Resolution and limits

**R4-TAG-1 remains Open.** No implementation or test file was changed by this
reviewer. Only this report was updated; the one-off probes were not added to
the committed suite.

All Git writes were confined to temporary local repositories with file-only
transport and a read-only fake `gh`. No real remote refs or GitHub state were
changed. Scoped whitespace checks passed. The supplied 25-case rerun and
658-pass/1-explicit-SBT-skip result were not rerun in this round and do not
cover the demonstrated ref-name ambiguity. Stop at Round 4 pending this fix.

## Round 4 recheck: exact-ref remediation

- **Model / effort:** `gpt-6-astra` / `max`.
- **Mode:** Direct, sequential, Round 4 only. No agents.
- **Reviewed HEAD:** `fcbe55b7875a4cc8e66b5870e93e01d26c510490`.
- **Dirty scope:** The three release workflows, `scripts/release/release_guard.py`,
  `scripts/release/test_release_guard.py`, the new
  `scripts/release/test_release_tag_recovery.py`, and `scripts/release/README.md`.
  Earlier unrelated changes were not re-reviewed.
- **Disposition:** R4-TAG-1 is fixed for the reported tag-lookup and verification
  failures. Two confirmed Medium findings remain below. **ISSUES_FOUND**.

The reviewed snapshots remained unchanged through the local checks:

| File | SHA256 |
| --- | --- |
| `.github/workflows/release-tag.yml` | `765e62a02ae2cebcb28835453654298bf2ca98521994524688fc9ec67ec22618` |
| `.github/workflows/release-tag-spark.yml` | `56762ab6e370e5f5bf4415b85eec2b2568b22d4cba78d70fde311cf3d356c7a9` |
| `.github/workflows/release-prepare.yml` | `6e772476f3bb7b5b00fb28d2a7045e2a829b964e75fb24009c6e724ba0bca857` |
| `scripts/release/release_guard.py` | `a738b2235d1e95c22ec337892a448f45b8c181082773f346c472056d8e0a481b` |
| `scripts/release/test_release_tag_recovery.py` | `7cb366f44a3cbc2868e25a3ef9af30101752a9122b5cbfac0d32dd88a2432e67` |

### Evidence checklist

- [x] Exact local tag existence now uses `show-ref --verify`; peeling uses its
  captured object ID. `release_guard.py:112-147` applies this to Maven admission
  and parses complete remote ref names, requiring the exact direct ref and
  its exact peeled advertisement when present. Matching suffixes no longer
  establish tag identity.
- [x] Traced the orchestrator's captured target OIDs, local helper variables,
  missing-tag array, ordinary invocation under `set -euo pipefail`, and explicit
  stacking refs. Selected native tests preserve the recorded merge source and
  fresh PR chain. Assignments do not hide failures behind `local`.
- [x] Ran 30 selected tests, with 41 deselected and no skips. They cover the
  CLI parser, exact branch and Maven admission, missing/wrong lookalikes, the
  three original tag steps, recorded-merge recovery and new PR chaining.
- [x] Four additional native-Git counterexamples executed the real tag-writing
  steps and compared **all remote refs**, including branches. Each found an
  unintended branch update before an exact-tag verification failure.
- [x] A separate control/counterexample executed the real preparation ancestry
  guard. A shadow tag changed its result from rejection to acceptance without
  changing the exact master branch.
- [x] Bash syntax checks passed for 31 steps across the three workflows.
  Black 22.3.0 passed for the three scoped Python files; scoped whitespace
  checks passed.
- [ ] No full suite, live workflow, publication, external API, worktree commit
  or real remote write was run. Author-supplied broader results were not rerun and do
  not replace the counterexamples below.

Equivalent portable command for the 30 selected tests, from the repository root:

```bash
export PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_TERMINAL_PROMPT=0 GIT_ALLOW_PROTOCOL=file
PYTHONPATH=scripts/release python3 -m pytest -q -p no:cacheprovider \
  scripts/release/test_release_guard.py scripts/release/test_release_tag_recovery.py \
  -k 'remote_tag_cli_requires_exact_refs or remote_tag_rejects_invalid_expected_commit or full_release_cli_checks_actual_source_branches or suffix_lookalikes or nested_local_tags or shadow_tag or maven_checkout_requires_the_exact_local_tag or all_tagging_workflows_require_exact_remote_refs or full_release_requires_exact_remote_branch_names or new_release_prs_keep_the_explicit_branch_chain or rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips'
```

### R4-TAG-2: A tag push can update a lookalike branch before verification fails

- **Severity / status:** Medium / Open.
- **Locations:** `.github/workflows/release-tag.yml:134-143,215-224`,
  `.github/workflows/release-tag-spark.yml:166-182`, and
  `.github/workflows/release-prepare.yml:318-324`.
- **Cause:** `OID:refs/tags/<tag>` still permits Git to resolve a missing
  destination to an advertised `refs/heads/refs/tags/<tag>` branch. If that
  branch is at an ancestor of the approved source, a non-forced push can
  fast-forward it. `--atomic` applies to the resolved destinations, not the
  intended names. The read-only postcheck detects the missing tag only after
  the wrong branch has changed.

Using the existing temporary bare-origin fixture with native Git 2.43.0, leave
the exact local and remote tag absent, create the lookalike branch at the
approved commit's parent, fetch, then execute the actual workflow step.
For primary tagging, remove the fixture's existing primary tag first.

| Actual step | Unintentionally updated branch | Other tag created by the same push |
| --- | --- | --- |
| Recorded-merge reconciliation | `refs/heads/refs/tags/v1.2.3-spark4.0` | `v1.2.3-python3.12` |
| Primary tag creation | `refs/heads/refs/tags/v1.2.3` | None |
| Master derivative tags | `refs/heads/refs/tags/v1.2.3-python3.11` | `v1.2.3-spark3.5` |
| Merged port tags | `refs/heads/refs/tags/v1.2.3-spark4.0` | `v1.2.3-python3.12` |

All four steps exited `2` with the exact missing-tag diagnostic, but the
lookalike branch had advanced to the approved commit and the required tag
remained absent. This is not the accepted same-OID no-op or annotated-tag
rejection: these executions made unintended remote writes.

**Required fix:** Check Git's resolved destination names against the exact
allowed tag refs before permitting the server to update anything. Retain
postverification, but do not use it as write authorization. Add different-OID,
fast-forwardable lookalike-branch cases for every writer and assert that the
entire remote ref map stays unchanged on rejection. The tag-only snapshots
in `test_release_tag_recovery.py:339-364,401-431` cannot establish that branches
were untouched.

### R4-TAG-3: Preparation ancestry still accepts a tag shadowing master

- **Severity / status:** Medium / Open.
- **Location:** `.github/workflows/release-prepare.yml:277-278`.
- **Cause:** After fetching the exact branch, the guard still passes the short
  name `origin/master` to `merge-base`. A local `refs/tags/origin/master` can
  take precedence over `refs/remotes/origin/master`. Git prints an ambiguity
  warning but returns success.
- **Verified result:** A recorded release commit with valid versioned-doc
  files, absent from the exact master history, made the real guard exit `1`.
  Adding `refs/tags/origin/master` at that same commit made it exit `0`.
  The exact remote-tracking master OID and all remote refs remained unchanged.
  Thus the version checks do not repair the ancestry authorization failure.

This unchanged guard directly authorizes the newly hardened primary tag step,
so it is a remaining exact-ref gap in the companion workflow, not a new defect
introduced by `show-ref`.

**Required fix:** Capture `refs/remotes/origin/master` with exact `show-ref`
and pass its object ID to `merge-base`, as the orchestrator now does at
`.github/workflows/release-tag.yml:100-101`. Add the native shadow-tag rejection
case to the preparation workflow coverage.

### Resolution and limits

R4-TAG-1's exact tag reads and false-completion checks are locally verified
fixed. R4-TAG-2 and R4-TAG-3 remain open, so Round 4 does not pass yet.
All probe writes were confined to disposable file-only repositories; `gh`
was replaced by the fixture. Only this report was edited by the reviewer.
No implementation changes or later review rounds were performed.

## Final Round 4 recheck: literal tag mapping and exact ancestry

- **Model / effort:** `gpt-6-astra` / `max`.
- **Mode:** Direct, sequential, bounded remediation verification only.
- **Reviewed HEAD:** `fcbe55b7875a4cc8e66b5870e93e01d26c510490`, unchanged.
- **Dirty scope:** The new `push_tags` helper and its byte-oriented Git input,
  four workflow callers, preparation ancestry, and their tests/documentation.
- **Remaining findings:** 0 in this reviewed scope.
- **Verdict:** **CLEAN_FOR_NEXT_ROUND**. R4-TAG-2 and R4-TAG-3 are verified
  fixed. The earlier R4-TAG-1 resolution and all original reproductions remain
  preserved above.

| Reviewed file | SHA256 |
| --- | --- |
| `scripts/release/release_guard.py` | `bf60bc73f321071ed2952634b59f6aaa325f23ff79371ca2fe6fabfa105529b2` |
| `.github/workflows/release-prepare.yml` | `cfab56b10c0012400641c1e070e846dfc5ae05ec86b0a3f0fc05db840785d049` |
| `scripts/release/test_release_tag_recovery.py` | `36e2cac71d471f35d96c63c79d9ade581da81fbc11bc9035a8c9f73e1e914b80` |

### Resolution and evidence

- [x] **R4-TAG-2 fixed.** `scripts/release/release_guard.py:151-189` captures
  only selected exact tag object IDs, creates refs under a fresh UUID prefix in a guarded
  transaction, and pushes one literal pattern mapping to `refs/tags/*`.
  This removes single-ref destination guessing rather than checking for it
  before a later ambiguous push. The command retains atomicity and disables
  implicit follow-tags without force or hook overrides.
- [x] All four writers call that helper:
  `.github/workflows/release-tag.yml:139,218`,
  `.github/workflows/release-tag-spark.yml:175`, and
  `.github/workflows/release-prepare.yml:321`. Their exact direct/peeled
  postchecks remain. Bash array expansion preserves each `--tag` argument,
  and a failed helper remains fatal before completion is reported.
- [x] The four actual-workflow branch-lookalike regressions now succeed with
  the intended exact tag present and every existing remote ref unchanged.
  The nested-tag recovery case now requires success, not a conditional
  accepted failure.
- [x] An additional native helper probe selected a lightweight and an
  annotated tag while both a nested tag and a fast-forwardable branch
  lookalike existed. The complete remote map changed only by the two exact
  selected tags, retaining their object IDs. An unselected annotated tag did
  not follow even with `push.followTags=true`.
- [x] `finally` cleanup deletes only the staging refs with their captured
  old IDs. Success and rejected atomic pushes leave no owned staging refs.
  Separate native fault probes confirmed that a creation collision changes
  nothing, and a changed staging OID makes cleanup reject atomically rather
  than delete a ref it no longer owns. Release tags and an unrelated staging
  prefix remain untouched; cleanup failures are explicit.
- [x] **R4-TAG-3 fixed.** `.github/workflows/release-prepare.yml:277-279`
  captures the exact remote-tracking master OID before `merge-base`.
  `scripts/release/test_release_tag_recovery.py:503-530` rejects the shadow-tag case with
  the expected ancestry diagnostic, not an unrelated missing-file failure.
- [x] Ran 16 selected tests: 16 passed, 28 deselected, no skips.
  They include the four writers, ancestry rejection, nested-tag recovery,
  recorded-merge source identity, annotated partial pairs, idempotence and
  atomic rejection. The fixture commits the actual guard and its imports
  before executing the extracted workflow steps.
- [x] Bash syntax passed for all 31 steps in the three workflows.
  Black 22.3.0 passed for the four scoped Python files, and scoped whitespace
  checks passed. The companion documentation describes the implemented
  staging and literal-mapping behavior.
- [ ] The author's 80-test targeted result and ongoing full run were not
  substituted for these checks. No full suite, live workflow, publication
  or external API was run by this reviewer.

Portable command for the selected tests, from the repository root:

```bash
export PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_TERMINAL_PROMPT=0 GIT_ALLOW_PROTOCOL=file
PYTHONPATH=scripts/release python3 -m pytest -q -p no:cacheprovider \
  scripts/release/test_release_tag_recovery.py scripts/release/test_release_workflows.py \
  -k 'tag_writers_never_update_a_lookalike_branch or preparation_ancestry_ignores_a_tag_shadowing_master or nested_local_tags_are_not_the_required_exact_ref or remote_tag_rejection_cannot_report_success or remote_conflict_rejects_the_entire_pair_atomically or rerun_recovers_only_missing_member_of_a_pair or rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips or full_release_policy_precedes_primary_and_derivative_tags'
```

Clean for the bounded Round 4 scope, not a guarantee of bug-free behavior or
live release readiness. Only this report was edited by the reviewer.
All Git probe writes were confined to disposable file-only repositories.
No implementation, real remote or reviewed worktree history was changed.
