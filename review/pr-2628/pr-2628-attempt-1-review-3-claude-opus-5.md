## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: `review/pr-2628/pr-2628-attempt-1-review-3-claude-opus-5.md`
- **Issues Found**: 0 (9 prior findings, all resolved)
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Focused preflight suite re-run on the current worktree: `python -m pytest scripts/test_bump_version.py scripts/release/test_release_matrix.py scripts/release/test_verify_release.py scripts/release/test_bump_bbcvhd.py scripts/release/test_release_workflows.py -q` -> `305 passed, 2 warnings in 87.94s`.
- [x] No-write preview still works without generated docs: `Test-Path website\docs` is `False`, and `python scripts\bump-version.py --to 1.1.4 --dry-run` exits 0, prints `[DRY RUN] Would run: sbt convertNotebooks` and `[DRY RUN] Would run: npm exec -- docusaurus docs:version 1.1.4`, reports `21 files, 143 context-anchored replacements`, emits 0 stdout lines mentioning `reviews`, and writes no `ERROR` to stderr. The write-path guard survives the reorder: `scripts/bump-version.py:406-419` returns early only for `dry_run`, and the missing-`website/docs` failure still precedes the subprocess call.
- [x] Issue 6 (case folding) fixed and probed under Git Bash: the documented gate stops on `true`, `True`, `TRUE`, and `tRuE` (exit 1, `SKIP_SPARK40 is true; stop.`) and passes on `false` and unset (`GATE-PASSED SKIP_SPARK40=false` / `=unset`). That now matches `release-tag.yml:41` (`vars.SKIP_SPARK40 == 'true'`), whose comparison GitHub performs case-insensitively. `SKILL.md:44-45` and `SKILL.md:53-56` carry the same "any casing" wording.
- [x] Issue 2 (fail-closed lookup) still holds: running the `preflight.md:27-36` snippet verbatim in this environment prints `failed to get variables: HTTP 403: You must have repository read permissions...` followed by `Cannot read SKIP_SPARK40; stop the release.` and exits 1.
- [x] Issue 7 (zero counters) fixed: `python scripts\release\verify_release.py --version 1.1.1 --internal-patch 0 --targets spark4.0 --upack-iteration spark4.0=0 --skip ...` -> exit 2, `error: iteration for 'spark4.0' must be a positive integer, got '0'`. `preflight.md:71-72` and `preflight.md:104-113` now say to pass a counter only for a rebuilt family. The single surviving "including zero" sentence is `preflight.md:137`, which governs `bump_bbcvhd.py`, whose `--upack-iteration` is `type=int, default=0` and forwards `None` when falsy (`bump_bbcvhd.py:102-131`) - so zero is correct there and nowhere else.
- [x] Issue 8 (missing `--targets`) fixed and verified live: `python scripts\release\verify_release.py --version 1.1.1 --internal-patch 0 --targets spark4.0 --upack-iteration spark4.0=1 --internal-upack-iteration spark4.0=1 --skip github,public,pip,internal` -> exit 0, `18 checks, 0 missing -> COMPLETE`, with `PRESENT upack spark4.0 synapseml 1.1.1-spark4-0-1`. The same command without `--targets` still exits 2 (`OSS UPack rebuild counters must cover every selected target`), so the added flag is load-bearing. The documented `--internal-patch <N>` shape is also valid, because `verify_release.run()` derives the scope itself: `"internal-only" if internal_patch != "0" else "full"` (`verify_release.py:358-364`).
- [x] Issue 9 (orchestrator dispatch) fixed and traced: `release-tag.yml:1` is `name: Release Tag Orchestrator`, it exposes `workflow_dispatch` (`release-tag.yml:21`), and its "Extract version" step requires a `vX.Y.Z` tag selected as the dispatch ref (`release-tag.yml:53-70`) - exactly the fallback `recovery-and-rollout.md:36-44` now prescribes and the same call `release-prepare.yml:306-312` makes after its own tag push.
- [x] The `reviews` exclusion protects real committed evidence, not just this worktree: `git ls-files` lists 19 tracked `reviews/pr-2666/*` files; `_skip_dir("reviews")` and `_skip_file(Path("reviews/pr-2666/README.md"))` both return `True`; and analyzing the `reviews/` tree with the denylist bypassed yields 6 **unanchored** matches (the `sys.exit(1)` FATAL path at `scripts/bump-version.py:539-546`) and 0 anchored ones.
- [x] Every "Existing release work" claim re-traced to its guard: `release-prepare.yml:72-82` (existing tag and existing `release/prepare-v*` branch), `release-tag.yml:214-231` (open PR left untouched; merged PR reused only while its recorded result remains), `release-tag-spark.yml:63-89, 157, 189` (empty `merge_commit_sha`, tag-commit mismatch, head branch moved after merge), `release-notes.yml:75-77, 98-106` (`verify_release.py --skip ado,internal` gate, existing Release left untouched), `bump_bbcvhd.py:167-177` (identical package versions rejected without a rebuild counter or `--force-revision`).

### Earlier evidence (pass 1)

- [x] `python -m pytest scripts/test_bump_version.py scripts/release/test_release_matrix.py scripts/release/test_verify_release.py scripts/release/test_bump_bbcvhd.py scripts/release/test_release_workflows.py -q` -> `305 passed, 2 warnings in 63.76s`.
- [x] `python scripts\bump-version.py --to 1.1.4 --dry-run` in the worktree (where `Test-Path website\docs` is `False`) exits 0 and prints both `[DRY RUN] Would run: sbt convertNotebooks (in ...)` and `[DRY RUN] Would run: npm exec -- docusaurus docs:version 1.1.4 (in ...\website)` with no `ERROR`. The write-path guard survives the reorder: `scripts/test_bump_version.py:522` still asserts `not bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False)` when `website/docs` is absent.
- [x] The review-artifact denylist is load-bearing, not cosmetic. Loading `scripts/bump-version.py` read-only and calling `analyze()` on `review/pr-2628/pr-2628-attempt-1-review-1-gpt-5.6-sol.md` returns 0 anchored matches and 2 **unanchored** matches (lines 159-160, `bump-version.py --to 1.1.3 --dry-run`), which is the `sys.exit(1)` FATAL path at `scripts/bump-version.py:539-546`. With the change, `_skip_dir("review")`, `_skip_dir("reviews")`, and their nested `_skip_file(...)` paths all return `True`; both the historical `reviews/pr-2666/` evidence and the new `review/pr-2628/` evidence are excluded from version replacement.
- [x] Live artifact probe (read-only, ADO-authenticated): `python scripts\release\verify_release.py --version 1.1.1 --targets spark4.0 --skip github,internal,public,pip` reports `PRESENT upack spark4.0 synapseml 1.1.1-spark4-0` -> `18 checks, 0 missing -> COMPLETE`; the same command with `--upack-iteration spark4.0=1` reports `PRESENT ... 1.1.1-spark4-0-1 -> COMPLETE`. Both versions exist in `BBC-VHD_PublicPackages`, so a counter-free verify passes against the superseded package (Issue 1).
- [x] `gh variable list --repo microsoft/SynapseML --json name,value --jq '.[] | select(.name == "SKIP_SPARK40")'` -> `failed to get variables: HTTP 403: You must have repository read permissions or have the repository variables fine-grained permission`, empty stdout, exit 1 (Issue 2).
- [x] `python scripts\release\release_matrix.py --version 1.1.4 --internal-patch 1` -> exit 2, `error: a nonzero Internal patch is an Internal-only hotfix; use --scope internal-only` (Issue 4); `python scripts\bump-version.py --to 1.1.3 --dry-run` -> exit 1, `Error: current version is already 1.1.3.` (Issue 3, proves Release Prepare cannot be re-run for an already-bumped version).
- [x] Documentation traced into automation: `release-prepare.yml` (`Guard against an already-released version`; `finalize` requires `website/versioned_docs/version-X`, `versioned_sidebars`, and a `versions.json` entry before tagging), `release-tag.yml` (`SKIP_SPARK40` env expression, existing-tag mismatch refusal, open/merged PR reuse and chaining), `release-tag-spark.yml` (`merge_commit_sha` guard, tag mismatch refusal, `--force-with-lease` branch cleanup), `release-notes.yml` (`verify_release.py --skip ado,internal` gate, existing-Release skip). Every claim in `references/automation-boundaries.md` and in `references/recovery-and-rollout.md:5-40` matches the workflows; the exceptions are the gaps filed below.

### Re-review evidence (pass 1)

- [x] Re-ran the documented preflight suite in the worktree: `python -m pytest scripts/test_bump_version.py scripts/release/test_release_matrix.py scripts/release/test_verify_release.py scripts/release/test_bump_bbcvhd.py scripts/release/test_release_workflows.py -q` -> `305 passed, 2 warnings in 60.90s`. `python scripts\bump-version.py --to 1.1.4 --dry-run` still exits 0 with `Test-Path website\docs` = `False`, prints `[DRY RUN] Would run: sbt convertNotebooks` and `[DRY RUN] Would run: npm exec -- docusaurus docs:version 1.1.4`, and lists 21 files / 143 anchored replacements with **0** lines mentioning `reviews`.
- [x] Prior Issues 1-5 are present in the current files: `recovery-and-rollout.md:20-22` (counter warning), `preflight.md:27-35` (fail-closed `if !` around `gh variable list`) plus the `compatibility` frontmatter naming repository-variable read access, `recovery-and-rollout.md:36-41` (merged `skip_docs` recovery), `preflight.md:12-17` (`--internal-patch 0`), `preflight.md:74-79` and `scripts/release/README.md` (`scripts/test_bump_version.py` in the preflight pytest command). Probes: `release_matrix.py --version 1.1.4 --internal-patch 0` -> exit 0 (`scope=full`); adding `--scope internal-only` -> exit 2 `--scope internal-only requires a nonzero --internal-patch`.
- [x] New Issue 6: GitHub documents that `==` ignores case (<https://docs.github.com/en/actions/reference/workflows-and-actions/expressions>, "GitHub ignores case when comparing strings"), and `release-tag.yml:41` is `SKIP_SPARK40: ${{ inputs.skip_spark40 == true || vars.SKIP_SPARK40 == 'true' }}`. Running the documented gate under Git Bash with `value="True"` prints `GATE-PASSED-as-safe` and `SKIP_SPARK40=True`, i.e. the release proceeds while `release-tag.yml:148-151` would drop the spark4.0 rebase PR.
- [x] New Issue 7: `python scripts\release\verify_release.py --version 1.1.1 --internal-patch 0 --targets spark4.0 --upack-iteration spark4.0=0 --skip github,ado,upack,pip,internal,public` -> exit 2, `error: iteration for 'spark4.0' must be a positive integer, got '0'`; the same rejection from `release_matrix.py`. Source: `release_matrix.py:218-223` (`value < 1` is invalid). Zero is valid only for `bump_bbcvhd.py:102-108` (`type=int, default=0`), which is why `preflight.md:126` is correct and `preflight.md:101` is not.
- [x] New Issue 8: the step-4 command shape with counters but no `--targets` fails: `verify_release.py --version 1.1.1 --internal-patch 0 --upack-iteration spark4.0=1 --internal-upack-iteration spark4.0=1 --skip ...` -> exit 2, `error: OSS UPack rebuild counters must cover every selected target...` (`release_matrix.py:229-233`). Adding `--targets spark4.0` makes the identical command exit 0 (`18 checks, 0 missing -> COMPLETE`, rows shown as `1.1.1-spark4-0-1` / `1.1.1-0-spark4.0-1`).
- [x] New Issue 9: `release-tag.yml:13-20` does trigger on `push: tags: v[0-9]*.[0-9]*.[0-9]*`, but `release-prepare.yml:306-312` deliberately does not depend on that: "Pushes made with GITHUB_TOKEN do not emit another workflow run" followed by `gh workflow run release-tag.yml --ref "v${VERSION}"`. `finalize` also runs "Verify merged version and ancestry" (`release-prepare.yml:275-283`) before tagging, which the manual recovery reproduces, and "Dispatch derivative tag orchestration", which it does not.

## Prior Findings

Issues 1-5 are the original Round 3 findings and Issues 6-9 came from the first
re-review. All nine are fixed in the current diff, each Resolution Log entry is
closed, and the descriptions below are kept verbatim as the historical record of
what was wrong. They are not open findings against the current diff.

### 1. Documented verification omits the UPack rebuild counters, so recovery verifies the superseded package
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 55-59, 79-83), `.github/skills/synapseml-release/references/recovery-and-rollout.md` (lines 13-20), `.github/skills/synapseml-release/SKILL.md` (line 129)
- **Description**: The skill treats UPack rebuild counters as first-class for the matrix (`recovery-and-rollout.md:15-17`) and for BBC-VHD (`preflight.md:100-108` passes both counters and requires them to match the reviewed row), but every documented `verify_release.py` invocation omits `--upack-iteration` and `--internal-upack-iteration`. `verify_release.run()` derives the rows from `build_plan(...)`, so without the counters it queries the counter-free version string, which is a *different, superseded* package after a rebuild. `scripts/release/README.md` shows the counter form; the skill, which agents follow, does not.
- **Risk**: After the exact recovery this skill prescribes (immutable UPack rebuild), `verify_release.py` returns `COMPLETE` against the pre-rebuild artifact. Proven live: `1.1.1-spark4-0` and `1.1.1-spark4-0-1` are both `PRESENT`, and the counter-free run reports `0 missing`. The Completion gate ("every selected Maven, pip, and UPack artifact exists", `SKILL.md:139-140`) is then satisfied by the bad package that the rebuild was meant to replace, and BBC-VHD can be pinned to a rebuilt version that was never actually verified.
- **Suggested fix**: Carry the reviewed counters into the verify commands. In `preflight.md:55-59` (selected-target recovery) and `preflight.md:79-83` (known-release replay), show `--upack-iteration <KEY=N>` / `--internal-upack-iteration <KEY=N>`; add a sentence to `recovery-and-rollout.md` "Immutable packages" such as "Pass the same counters to `verify_release.py`; without them it checks the pre-rebuild version, which usually still exists and reports PRESENT."

### 2. The `SKIP_SPARK40` probe reads a failed lookup as "unset"
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 24-31)
- **Description**: `preflight.md:30` states "No output means the variable is unset." `gh variable list` writes its failure to stderr and leaves stdout empty, so a permission, token, or network failure is indistinguishable from an unset variable when the documented command is used as written. Reading repository variables needs a permission a release engineer may not hold; in this environment the documented command returned `HTTP 403`, empty stdout, exit 1.
- **Risk**: This is the one preflight check that gates a state-changing step. `SKILL.md:53-56` says the version PR must not be merged while `SKIP_SPARK40` is true, because the opt-out omits a required target *after* the primary tag exists. A failed lookup silently reads as "safe to merge": `release-tag.yml` then skips the spark4.0 rebase PR, `release-tag-spark.yml` never mints the spark4.0 tags, and `release-notes.yml` (which runs `verify_release.py` across all three targets) blocks the GitHub Release. Recovery requires the manual spark4.0/spark4.1 chaining that `release-tag.yml:150-176` warns permanently breaks ancestry if done wrong.
- **Suggested fix**: Make the check fail closed: use `gh api repos/microsoft/SynapseML/actions/variables/SKIP_SPARK40 --jq .value`, treat exit 0 as the value, HTTP 404 as unset, and *any other* non-zero exit as unknown -> stop. State the required permission alongside the command (and in the `compatibility` line of `SKILL.md`), so a 403 is diagnosed instead of being read as an answer.

### 3. No recovery path for a merged `skip_docs` version PR
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/SKILL.md` (lines 25, 99-102), `.github/skills/synapseml-release/references/recovery-and-rollout.md` (lines 31-40)
- **Description**: The skill repeatedly warns not to merge a `skip_docs` PR but never says what to do once one is merged, and that failure mode is one workflow-dispatch checkbox away. `release-prepare.yml`'s `finalize` job hard-fails at "Verify merged version and ancestry" when `website/versioned_docs/version-X`, the matching sidebars file, or the `versions.json` entry is missing, so the primary tag is never created. The job checks out `pull_request.merge_commit_sha`, so landing the snapshot on master afterwards does not make a re-run pass. Re-running **Release Prepare** for the same version is also blocked: master already carries the bumped version and `bump-version.py` exits 1 with `Error: current version is already 1.1.3.` (verified). `recovery-and-rollout.md`'s "Existing release work" list covers Release Prepare, the derivative workflow, merged Spark PRs, Release Notes, and `bump_bbcvhd.py` - but not this state.
- **Risk**: The release is stuck half-applied: master carries the new version, no primary tag exists, no derivative tags, no Spark release PRs, and the documented entry points all refuse to run. An operator under time pressure is left to improvise a manual tag push, which is precisely the unreviewed direct-to-master behavior `release-prepare.yml` was written to eliminate.
- **Suggested fix**: Add an "Existing release work" entry describing the state and the one supported exit: land the missing documentation snapshot on master through a normal reviewed PR, then create `vX.Y.Z` on the resulting master commit as an explicit human gate (or dispatch `release-tag.yml` on that tag), record why the automated path was bypassed, and do not re-run **Release Prepare** for that version.

### 4. The full-release matrix command shows a placeholder the script rejects
- **Severity**: Low
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 12-17)
- **Description**: Step 1 presents `--version <X.Y.Z> --internal-patch <N>` as the plan command for every track, but `build_plan` rejects a nonzero Internal patch at the default `full` scope: `python scripts/release/release_matrix.py --version 1.1.4 --internal-patch 1` exits 2 with `a nonzero Internal patch is an Internal-only hotfix; use --scope internal-only`. Only `0` is valid in this block; `<N>` is meaningful only in the Internal-only form described at line 35.
- **Risk**: Low. The failure is loud and the script is the authority, but the first documented command of the release fails for a plausible substitution, and an operator who reads `<N>` as free choice is nudged toward the exact hand-editing that lines 39-40 forbid.
- **Suggested fix**: Write `--internal-patch 0` in the full-release block and keep `--internal-patch <N> --scope internal-only` as the separate Internal-only form.

### 5. Preflight tests omit the suite for the only script that rewrites the repository
- **Severity**: Low
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 62-70)
- **Description**: Step 3 runs the four `scripts/release/` suites plus `test_prev_tag.sh`, but not `scripts/test_bump_version.py`. `bump-version.py` is the only release-path script that edits the repository, it is executed unattended by `release-prepare.yml` ("Bump version strings"), and step 2 of this same preflight tells the operator to run it. Both regressions fixed in this PR - the dry-run documentation preview and the `reviews` exclusion - are covered exclusively by that suite.
- **Risk**: The documented preflight cannot catch a regression in the script that performs the actual version rewrite. A defect there surfaces only after **Release Prepare** has already pushed a branch and opened a PR, which is past the first repository write named in `automation-boundaries.md`.
- **Suggested fix**: Add `scripts/test_bump_version.py` to the `pytest` invocation in step 3; the combined run completes in about a minute (305 tests).

### 6. The `SKIP_SPARK40` gate compares case-sensitively, but GitHub does not
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 27-42), `.github/skills/synapseml-release/SKILL.md` (lines 44-45, 53-56)
- **Description**: The fail-closed rewrite added for Issue 2 handles a failed lookup but still decides the value with `[ "$value" != "true" ]` (`preflight.md:34`), which is byte-exact in POSIX shell. The consumer is a GitHub expression: `SKIP_SPARK40: ${{ inputs.skip_spark40 == true || vars.SKIP_SPARK40 == 'true' }}` (`release-tag.yml:41`), and GitHub documents that `==` ignores case when comparing strings. A repository variable set to `True` or `TRUE` - the spelling a human is most likely to type into the Actions variable UI - is therefore *true* for the workflow and *not true* for the documented gate. Verified: running the documented gate with `value="True"` prints `GATE-PASSED-as-safe` and `SKIP_SPARK40=True`. `SKILL.md:44-45` ("unset or false") inherits the same blind spot.
- **Risk**: Identical to Issue 2's failure mode, which this text was written to close, and it is reached without any lookup error. The gate reports safe, the version PR merges, `release-tag.yml:148-151` skips the spark4.0 rebase PR *after* the primary tag exists, `release-tag.yml:185` warns that spark4.0 -> spark4.1 ancestry is broken until a release runs without the opt-out, and `release-notes.yml` then blocks the GitHub Release on missing spark4.0 rows. Recovery is the manual chaining the workflow comments describe as permanently damaging if done wrong.
- **Suggested fix**: Case-fold before comparing, e.g. `value=$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')` immediately after the successful lookup, keeping `[ "$value" != "true" ] || { ...; exit 1; }`. State in the surrounding prose that GitHub's `==` is case-insensitive, so any casing of `true` stops the release, and align `SKILL.md:44-45` with that wording.

### 7. Preflight tells the operator to pass zero UPack counters, which both scripts reject
- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 66-68, 101-103)
- **Description**: The Issue 1 fix added "Use the exact counters from the known plan, including zero" (`preflight.md:101`) and "run `verify_release.py` with the reviewed `--targets`, both reviewed UPack counter arguments" (`preflight.md:66-68`). `verify_release.py` parses those flags with `release_matrix.parse_iterations` and builds the plan with `build_plan`, which rejects any counter below 1 (`release_matrix.py:218-223`). Verified: `--upack-iteration spark4.0=0` exits 2 with `error: iteration for 'spark4.0' must be a positive integer, got '0'`, from both `verify_release.py` and `release_matrix.py`. There is no "zero counter" row in a plan - a target with no rebuild simply has no counter. Zero is meaningful only for `bump_bbcvhd.py`, whose `--upack-iteration` is `type=int, default=0` (`bump_bbcvhd.py:102-108`), so the same phrase at `preflight.md:126` is correct while line 101 is not.
- **Risk**: The verification gate this section exists to strengthen exits 2 for a plan with no rebuild, or with a rebuild in only one package family. The obvious workaround - drop the counter flags - is precisely the Issue 1 defect: `verify_release.py` then checks the pre-rebuild UPack version, which is still `PRESENT`, and reports `COMPLETE` for the artifact the rebuild was meant to replace.
- **Suggested fix**: In step 4 replace "including zero" with the actual rule: pass a counter only for a family that was rebuilt, and omit the flag otherwise. In step 2 change "both reviewed UPack counter arguments" to "the reviewed counter argument for each family that was rebuilt". Leave `preflight.md:126` unchanged, since `bump_bbcvhd.py` does accept zero.

### 8. The replay command shape is rejected whenever only some targets were rebuilt
- **Severity**: Low
- **File**: `.github/skills/synapseml-release/references/preflight.md` (lines 92-95)
- **Description**: The step-4 block passes `--upack-iteration <TARGET=N,...>` and `--internal-upack-iteration <TARGET=N,...>` but no `--targets`, so every target is selected. `build_plan` requires the counter key set to equal the selected target set and to carry a single value (`release_matrix.py:229-238`). Verified: `verify_release.py --version 1.1.1 --internal-patch 0 --upack-iteration spark4.0=1 --internal-upack-iteration spark4.0=1 --skip ...` exits 2 with `error: OSS UPack rebuild counters must cover every selected target. Pipeline 35879 accepts one global counter...`, while the same command plus `--targets spark4.0` exits 0 and reports the `-1` rows (`1.1.1-spark4-0-1`, `1.1.1-0-spark4.0-1`). Step 2 mentions `--targets`; the copy-pasteable command in step 4 does not.
- **Risk**: The one preflight step whose purpose is to prove live credentials and endpoints fails on the exact case the counters were added for - a single-target rebuild - before it reaches any package store. Under time pressure the counters get deleted rather than paired with `--targets`, which re-opens Issue 1.
- **Suggested fix**: Show `--targets <TARGET>` in the step-4 command block next to the counter flags, and state the rule: counters must cover every selected target and share one value, so a per-target rebuild is verified as a separate run.

### 9. The `skip_docs` recovery relies on a tag push starting the derivative workflow, with no check or fallback
- **Severity**: Low
- **File**: `.github/skills/synapseml-release/references/recovery-and-rollout.md` (lines 36-41)
- **Description**: The Issue 3 fix ends with "create the primary tag on that commit; the tag push starts the derivative workflow" (`recovery-and-rollout.md:39-40`). That holds only for a tag pushed with human credentials. `release-tag.yml:13-20` does subscribe to `push: tags:`, but `release-prepare.yml:306-312` documents the exception and refuses to depend on it - "Pushes made with GITHUB_TOKEN do not emit another workflow run" - and instead runs `gh workflow run release-tag.yml --ref "v${VERSION}"`. The recovery text reproduces `finalize`'s snapshot and ancestry verification but drops the dispatch step, and the accepted Issue 3 fix explicitly offered it ("or dispatch `release-tag.yml` on that tag").
- **Risk**: A recovery that is already off the automated path stalls silently right after the riskiest state change. The primary tag exists, no `-python3.11` or `-spark3.5` tags are created, no Spark rebase PRs open, and the operator has been told to expect them - so the missing run is diagnosed only by noticing the absence, exactly the "do not infer success" failure this skill warns about elsewhere.
- **Suggested fix**: Append one sentence: confirm the Release Tag Orchestrator run started for the new tag, and if it did not - for example because the tag was pushed with an Actions token - dispatch `release-tag.yml` with that tag selected as the ref, as `release-prepare.yml` does.

## Resolution Log

### Issue 1

- **Status**: Resolved
- **What changed**: `preflight.md` now passes both reviewed UPack counter
  arguments to `verify_release.py` for recovery and known-release replay.
  `recovery-and-rollout.md` warns that omitting them checks the original
  package, which can remain present after a rebuild.
- **Why**: Verification now targets the same immutable package versions chosen
  by the reviewed matrix and BBC-VHD preview.
- **How verified**: The documented flags match `verify_release.py`, and the
  combined release suite passed all 305 tests.

### Issue 2

- **Status**: Resolved
- **What changed**: The `SKIP_SPARK40` shell check now captures the value only
  after a successful `gh variable list`. Any lookup failure exits before
  approval. The skill compatibility field names repository-variable read
  access as a prerequisite.
- **Why**: Empty output can mean "unset" only after a successful request; a
  permission or network failure leaves the release blocked.
- **How verified**: The current account receives HTTP 403 and exit code 1 from
  the lookup, which the documented `if !` branch treats as a stop condition.

### Issue 3

- **Status**: Resolved
- **What changed**: `recovery-and-rollout.md` now documents the merged
  `skip_docs` state. It requires a reviewed documentation-snapshot PR, checks
  the exact resulting `master` commit, and allows the primary tag only after
  explicit human approval.
- **Why**: This gives operators a supported recovery path when the original
  merged commit cannot pass the finalize job and Release Prepare cannot rerun
  the same version.
- **How verified**: The sequence matches `release-prepare.yml`'s merged-commit
  snapshot checks and the tag-push trigger in `release-tag.yml`.

### Issue 4

- **Status**: Resolved
- **What changed**: The full-release matrix examples now use
  `--internal-patch 0`. A separate instruction pairs a nonzero patch with
  `--scope internal-only`.
- **Why**: Each example now represents a release scope accepted by the matrix
  parser.
- **How verified**: Both the full and Internal-only documented argument shapes
  exit successfully against `release_matrix.py`.

### Issue 5

- **Status**: Resolved
- **What changed**: `scripts/test_bump_version.py` is now part of the preflight
  pytest command in both the skill reference and `scripts/release/README.md`.
- **Why**: The no-write gate now tests the only release helper that rewrites
  the repository.
- **How verified**: The exact combined command passed all 305 tests.

### Issue 6

- **Status**: Resolved
- **What changed**: The shell gate lowercases `SKIP_SPARK40` before comparing
  it. `SKILL.md` and `preflight.md` now state that any casing of `true` blocks
  the release because GitHub string comparisons ignore case.
- **Why**: The preflight and workflow now interpret the repository variable
  the same way.
- **How verified**: A Git Bash probe accepted `false` and unset values while
  classifying `True`, `TRUE`, and `true` as true.

### Issue 7

- **Status**: Resolved
- **What changed**: The `verify_release.py` guidance now adds a counter flag
  only for each UPack family that was rebuilt and omits the flag for an
  unreconstructed family. The BBC-VHD guidance still passes zero because that
  helper accepts it.
- **Why**: The instructions now match the matrix rule that rebuild counters
  are positive integers rather than explicit zero values.
- **How verified**: A positive rebuilt-target plan exits 0, while a zero
  counter exits 2 as the documentation now predicts.

### Issue 8

- **Status**: Resolved
- **What changed**: The known-release command now requires `--targets`.
  Adjacent guidance says each supplied counter must cover every selected target
  with one positive value and directs different counters to separate runs.
- **Why**: A single-target rebuild no longer inherits the default three-target
  set and fail before verification.
- **How verified**: The selected `spark4.0` plan with both rebuild counters
  exits 0, and 67 matrix and verification tests pass.

### Issue 9

- **Status**: Resolved
- **What changed**: The merged-`skip_docs` recovery now requires confirmation
  that **Release Tag Orchestrator** started. If not, it directs the maintainer
  to dispatch `release-tag.yml` with the primary tag selected as the ref.
- **Why**: Recovery no longer assumes every credential used to push the tag
  emits a second workflow run.
- **How verified**: The fallback matches the explicit dispatch in
  `release-prepare.yml` after its primary-tag push.

## Re-review Result (pass 1)

**ISSUES_FOUND.** All five original findings are fixed in the current diff and
independently re-verified: the counter warning in `recovery-and-rollout.md`,
the fail-closed `SKIP_SPARK40` lookup plus the `compatibility` prerequisite,
the merged `skip_docs` recovery entry, `--internal-patch 0` in the full-release
matrix examples, and `scripts/test_bump_version.py` in both preflight test
commands. The focused suite still passes (305 tests) and the no-write preview
prints both documentation commands without `website/docs`.

Four new robustness findings come from the fixes themselves. Issue 6 leaves the
`SKIP_SPARK40` gate case-sensitive against a case-insensitive GitHub
comparison. Issues 7 and 8 make the new `verify_release.py` counter guidance
unrunnable - zero counters and counters without `--targets` both exit 2 - and
the obvious workaround re-opens the original Issue 1 defect. Issue 9 leaves the
new manual recovery depending on a tag-push trigger that the repository's own
release automation deliberately does not rely on. All four are documentation
edits inside `.github/skills/synapseml-release/`; no implementation file was
modified during this review.

## Re-review Result

**CLEAN.** All nine prior Round 3 findings are fixed in the current diff and
each fix was re-verified against the scripts and workflows rather than read
back from the text. The `SKIP_SPARK40` gate now case-folds before comparing, so
it stops on every casing GitHub's `==` treats as true and still fails closed on
a lookup error (HTTP 403 -> exit 1). The `verify_release.py` guidance now
matches `build_plan`'s actual rules: counters are added only for a rebuilt
family, the step-4 command carries `--targets`, and the documented shape runs
live to `18 checks, 0 missing -> COMPLETE` against the rebuilt
`1.1.1-spark4-0-1` UPack. "Including zero" survives only where
`bump_bbcvhd.py` accepts zero. The merged-`skip_docs` recovery now tells the
operator to confirm the **Release Tag Orchestrator** run and to dispatch
`release-tag.yml` on the primary tag when it did not start, which matches both
the workflow's dispatch-ref requirement and `release-prepare.yml`'s own call.

No new robustness findings. The re-check covered the areas the fixes touched
and the boundaries around them: dry-run behavior with `website/docs` absent
(preview succeeds, write path still refuses), the `reviews` denylist against
the 19 committed `reviews/pr-2666/*` files (6 unanchored matches, i.e. the
FATAL path, without it), scope and target combinations
(`--internal-patch`/`--scope` validation, and `verify_release.run()` deriving
the scope itself so the documented Internal replay shape is valid), immutable
artifact and retry rules, and every "Existing release work" claim traced to its
guard in `release-prepare.yml`, `release-tag.yml`, `release-tag-spark.yml`,
`release-notes.yml`, and `bump_bbcvhd.py`. The focused preflight suite passes
(305 tests). Only this review artifact was modified.
