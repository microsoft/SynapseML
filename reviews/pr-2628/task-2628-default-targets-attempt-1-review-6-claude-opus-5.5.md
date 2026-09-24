# PR 2628 default-target follow-up: attempt 1, review round 6

- Theme: R6 final polish and hardening of the default-target delta, plus
  independent closure of the round 3 findings.
- Model: Claude Opus 5.5.
- Reviewed HEAD: `2391aa166ab46238307de9edf9650102cbff950e` with the current
  dirty tree. Merge base: `681bd96990c421de3b91d2b1bf8f8f470764199d`.
- Scope: the current tracked diff (25 files, +635/-147) plus the untracked
  `scripts/release/test_release_defaults.py`. The review concentrated on the
  files changed after round 3:
  - `scripts/bump-version.py`
  - `scripts/test_bump_version.py`
  - `scripts/release/README.md`
  - `scripts/release/test_release_tag_recovery.py`
  - `.github/workflows/release-tag.yml`
  - `.github/workflows/release-prepare.yml`

  Older, unrelated PR code was not re-reviewed.
- Method: source reading, bounded local tests, and reproductions on temporary
  copies. Nothing touched the network or a remote API. There were no workflow
  dispatches, tags, packages, commits or pushes. The only repository file this
  review wrote is this report.

## Result

Both round 3 findings are closed. There are two new Low findings. Both are in
the manual Spark 4.0 opt-in documentation path, and both leave the default
master/Spark 4.1 path unchanged and safe. Neither blocks the default-target
automation.

The 1.2 consumer-wheel distribution gate was already open before this delta.
It is still unresolved and is outside this delta. This report makes no claim
that the release is ready for production.

## Round 3 closure

### DT-R3-1 (Medium): closed

What changed:

- **Post-condition baseline.** `scripts/bump-version.py:779-780` counts
  destination-version references that already exist in each changed file.
  The expected count is now replacements plus those existing references.
- **Stale-reference check.** This still re-runs `analyze` with the unchanged
  skip rules.
- **Failure messages.** The write-failure and post-condition messages
  (`:771-772`, `:800`, `:809`) now say to inspect partial changes and keep any
  edits made before the bump. No `git checkout .` advice remains.
- **Operator guide.** The opt-in section of `scripts/release/README.md`
  (`:116`) now gives this order:
  1. Bump with `--skip-docs`, or use `skip_docs=true`.
  2. Update `spark40Version` and every source guide, including Deep Learning
     Getting Started, ONNX and R Setup.
  3. Run `sbt convertNotebooks`, `docs:version`, `--finalize-docs`, and a
     preview-mode website test and build.
  4. Commit before approval.

Independent re-run of my round 3 reproduction on a temporary copy:

- **Pre-updated optional references, then a bump to 1.2.0.** This now exits 0
  and reports "0 stale selected versions, 55 replacements confirmed" and "21
  already-current references preserved". Before the fix, it exited 1 with
  "expected 55, found 76".
- **Repeated default bumps on CRLF files (1.2.0, then 1.3.0).** The retained
  1.1.3 Spark 4.0 coordinates and pins are kept, and no lone LF appears.
- **Bounded pytest.** This selection passed 9 tests:
  `scripts/test_bump_version.py -k "repeated_default_release_bumps or
  TestPostCondition or explicit_errors_preserve or test_crlf_preserved"`.

### DT-R3-2 (Low): closed

The header of `.github/workflows/release-tag.yml` (lines 3-8) and the operator
guide (`scripts/release/README.md:103` and `:113`) now separate two cases:

- a fresh opt-in chain;
- existing Spark 4.1 PRs or merged results, which are preserved and not
  restacked.

Both also state that every Spark 4.0 tag recovery needs
`include_spark40=true`.

I ran the new tests from `scripts/release/test_release_tag_recovery.py` locally
with a Windows Bash harness. The module is POSIX-only, so the harness lifted
that skip and passed each workflow script by file.

| Test | Result |
| --- | --- |
| `test_orchestration_summary_names_only_selected_ports` (2 cases) | pass |
| `test_optin_preserves_default_spark41_pr_and_requires_explicit_tag_recovery` | pass |
| `test_default_orchestration_does_not_require_or_release_spark40` (2 cases) | pass |
| Existing `test_new_release_prs_keep_the_explicit_branch_chain` | pass |
| Existing `test_open_release_prs_keep_their_reviewed_branches_and_do_not_mint_tags` | pass |

The opt-in test proves four things:

- The reviewed default Spark 4.1 PR branch stays unchanged.
- A default rerun leaves a deleted Spark 4.0 Python tag missing.
- An explicit rerun restores that tag at the recorded merge.
- No Spark 4.1 tag is created early.

## New findings

### DT-R6-1 (Low): partial manual Spark 4.0 opt-in edits pass the website checks

**Where.** The operator guide says `website/test/installDocs.test.js` "checks
the coupled guides against the runtime metadata"
(`scripts/release/README.md:141`). That test checks Spark 4.0 coordinates,
tags and packages only with `includes()`:

- the install guides (around lines 210-222);
- the notebook links (lines 128-141);
- Deep Learning, ONNX and R Setup (lines 321-349).

`scripts/bump-version.py` deliberately skips every Spark 4.0 reference, so the
manual edit is the only thing that writes them.

**Reproduction.** Run on a temporary copy; the worktree was only read.

1. Copy these files:
   - `README.md`
   - the source Install guide
   - `website/src/installArtifacts.js`
   - `website/docusaurus.config.js`
   - `website/versions.json`
   - `website/test/installDocs.test.js`
   - `website/test/published-spark-ports.lock`
2. Run `python scripts/bump-version.py --repo-root COPY --to 1.2.0
   --skip-docs`. It exits 0.
3. Set `spark40Version` to 1.2.0 and prepend 1.2.0 to `versions.json`. This
   stands in for `docs:version`.
4. Control: with README untouched, run `node --test
   --test-name-pattern="installation examples are concrete in README.md"
   website/test/installDocs.test.js`. It fails 1 test, as expected.
5. Update only two README lines: the Spark 4.0 runtime-table row and the
   Spark 4.0 notebook link. Rerun the test: 1 pass, 0 fail.

After step 5, README still has:

- four `1.1.3-spark4.0` dependency references: `README.md:201`, `:245` and
  `:275` (Maven) and `:301` (sbt);
- the Spark 4.0 pip example `synapseml==1.1.3` at `README.md:230`.

These sit beside `1.2.0-spark4.0` elsewhere in the same guide.

**Impact.** A reviewed but incomplete opt-in can publish mixed Spark 4.0 JVM
and Python versions. That contradicts the guides' instruction to choose
exactly one complete runtime variant. Default bumps are not affected: they
keep all Spark 4.0 references, and the bump regression covers that.

**Suggested hardening.** Add an exclusivity check to `installDocs.test.js`
covering:

- README;
- the source and current-version install guides;
- Deep Learning Getting Started, ONNX and R Setup.

In those files, every `X.Y.Z-spark4.0` match should equal `spark40Version`,
and the Spark 4.0 pip pin (the one paired with `pyspark>=4.0.1,<4.1`) should
use it too. Every current match uses a single version (1.1.3), so this check
passes on today's tree.

A weaker alternative: reword `README.md:141` to say the test checks that the
selected references are present, and require the discovery `git grep` to show
no remaining earlier Spark 4.0 version.

### DT-R6-2 (Low): no documented pre-check or back-out when an opted-in Spark 4.0 is not published

**Where:**

- the opt-in section and the lock follow-up in `scripts/release/README.md`
  (`:116` and `:392-398`);
- `.github/skills/synapseml-release/SKILL.md:61`;
- the veto check in `scripts/release/release_guard.py:46`, which the normal
  post-merge flow first reaches at the explicit tag dispatch;
- `.github/workflows/release-prepare.yml`, which never selects Spark 4.0 and
  so never checks whether it can be released.

**Sequence.**

1. In the normal post-merge flow, the opt-in docs are committed on the
   preparation branch and merge to master.
2. Only afterwards does the explicit `include_spark40=true` dispatch check
   `SKIP_SPARK40`, rebase the port, or start its build.
3. Suppose that dispatch is refused, the port rebase conflicts, or Spark 4.0 is
   dropped. Master and the new snapshot still promise `1.2.0-spark4.0`.
4. The strict lock check then rejects the whole version, so the lock follow-up
   cannot deploy the default pair's documentation. The repository's own case
   at `website/test/installDocs.test.js:115-119` covers this: `defaultRelease`
   with the default optional version throws. I ran that test here and it
   passes.

The guide describes only the success path. SKILL line 61 ("keep the unselected
Spark 4.0 entry") assumes the docs were never opted in.

Pre-merge bootstrap is not affected: its candidates are not merged before
publication.

**Impact.** The failure is safe: no wrong promise deploys, and the site stays
on the previous release. However, the operator has no documented way to
recover.

**Suggested hardening (docs).**

- Before committing opt-in docs, confirm that `SKIP_SPARK40` is not true and
  that the Spark 4.0 port is ready to release.
- State that opted-in docs hold the version's production site until Spark
  4.0's lock can be updated.
- Add a back-out: if Spark 4.0 is dropped after the primary merge, the
  reviewed lock follow-up restores `spark40Version` and the coupled source and
  current-snapshot guides to the retained published version. The strict test
  then passes with the retained lock entry. If a three-target plan was already
  approved, use a new two-target plan and approval.

## Optional polish (non-blocking)

- P-1: `README.md:91` and `docs/Get Started/Install SynapseML.md:27` tell
  consumers "Spark 4.0 is opt-in". To a consumer, that is release-process
  jargon. Suggested wording: "Spark 4.0 builds are published only for
  releases that include them; its row and examples show the latest published
  Spark 4.0 version."

## Verification

Run here:

- The round 3 reproduction script (`scripts/bump-version.py` on temporary
  copies) exits 0 in both scenarios.
- The bump pytest subset: 9 passed.
- The Bash tag-recovery subset: 5 plus 2 passed, using the Windows harness.
- The website Node lock tests (`published Spark port versions are explicitly
  locked` and `unpublished documentation can be previewed but cannot be
  deployed`): 2 passed in strict mode on the current tree.
- The DT-R6-1 reproduction: the control fails and the partial edit passes, as
  described above.
- Afterwards, HEAD was unchanged and the tracked and untracked file set
  matched the reviewed delta. Harness temporary scripts were removed.

Reported by the coordinator and not re-run here:

- release tests: 866 pass, with one unchanged opt-in SBT skip;
- bump and history tests: 272 pass;
- CI-helper tests: 356 pass, plus 63 subtests;
- Node website tests: 36 pass.

## Limits

- Not run here, so all actual CI and artifact status is unverified:
  - hosted CI, Azure validation and Pages deployment;
  - GitHub API behavior;
  - `sbt convertNotebooks`, `docs:version` and the full website build;
  - signing, uploads and consumer installs.
- The Bash results come from a Windows msys harness, not the Linux runners.
  The coordinator and round 4 report Linux execution.
- Gemini remains unavailable, so this is not three-family coverage.
- The consumer-wheel distribution gate was already open before this delta and
  is still unresolved.
- No production-readiness claim is made.

Review stops after round 6.

## Coordinator resolutions

**DT-R6-1: addressed.** The website contract now checks every numeric Spark 4.0
artifact/tag reference in the source and current-version installation guides
and the coupled Deep Learning, ONNX and R guides. Its Python command and table
pins must also match the independently selected version. Regression cases
reject the reproduced two-line partial update and an artifact-only update
that leaves an old Python pin. Complete updates and restoration to the
retained version pass.

**DT-R6-2: addressed.** The guide requires an early optional-policy and runtime
readiness check, while retaining the final policy rechecks. It documents
restoring all optional references and the in-flight snapshot to the verified
retained version before tags/submissions, with a new plan and approval.
After any tag or submission it instead requires reconciliation using the
original records and a separate reviewed documentation correction. It does
not permit editing tagged candidates, concealing partial publication or
weakening the publication lock. The release skill points to that procedure.

**P-1: addressed.** Consumer guides now describe maintainer release selection
and the last published Spark 4.0 build, rather than telling consumers to opt in.

Coordinator validation after these changes: 38 website tests passed, and
23 focused source-bump/public-documentation checks passed. The original
review findings and independent verification above are retained.

## Independent closure (round 6 follow-up)

This is a bounded check of the current tree only; no other issues were
searched for. Reviewed HEAD is still
`2391aa166ab46238307de9edf9650102cbff950e`, and the tracked diff is now 25
files (+719/-147).

Changed since the round 6 report:

- `website/test/installDocs.test.js`
- `scripts/release/README.md`
- `.github/skills/synapseml-release/SKILL.md`
- `README.md`
- `docs/Get Started/Install SynapseML.md`

### DT-R6-1: closed

`validateSpark40References` (`website/test/installDocs.test.js:68-86`)
enforces two rules:

- every numeric `X.Y.Z-spark4.0` reference must equal `spark40Version`;
- the Python pin on every Spark 4.0 table row and every `pyspark>=4.0`
  command line must match it too.

It is applied to:

- the three install guides: README, the source guide and the current snapshot
  (`:264`);
- the Deep Learning and R Setup guides, source and current copies, plus the
  ONNX source guide (`:371`).

New cases at `:88` and `:105` cover:

- the reproduced two-line update;
- an update that changes artifacts but leaves a stale Python pin;
- a complete update, and a restoration to the retained version.

I re-ran my temporary-copy reproduction against the current test:

| Scenario | Round 6 | Now |
| --- | --- | --- |
| Two-line partial README opt-in | 1 pass | 1 fail: "mixed Spark 4.0 artifact versions" |
| Metadata only, README untouched | 1 fail | 1 fail |
| Complete README opt-in (all references and pins) | not run | 1 pass |
| Retained version restored (back-out) | not run | 1 pass |

`node --test test/installDocs.test.js` passes 11 of 11 in both strict and
preview mode.

Non-blocking notes (these do not reopen the finding):

- **Boundary gap in the pattern.** The `(?![\w.])` boundary skips a reference
  that is immediately followed by `.` or a word character. Examples are
  sentence-final prose outside backticks, or a `.jar` file name. Today the
  pattern matches all 28 references in the eight validated files and skips
  none. Backticked references followed by a period are still checked.
- **ONNX snapshot copy not read.** The test does not read the ONNX
  current-version snapshot copy. The retained 1.1.3 snapshot has no Spark 4.0
  ONNX rows, so a mandatory check would fail today. The new snapshot is
  generated from the checked source guide.

### Coordinator follow-up on closure notes

The reference boundary now accepts sentence-final punctuation and `.jar`
suffixes. A regression rejects a stale reference with either suffix even
beside a matching reference.

The current ONNX snapshot is now read. Its references are checked whenever
it contains Spark 4.0 examples, and are mandatory for the new snapshot format
identified by the existing source-built R marker. The older published
snapshot has neither and remains unchanged.

### DT-R6-2: closed

- **Early check.** `scripts/release/README.md:116-121` now requires two things
  before opt-in documentation is prepared:
  - `SKIP_SPARK40` is absent or false (an unreadable policy does not count as
    absent);
  - the Spark 4.0 source and consumer-validation prerequisites can be met.

  The later guard rechecks are kept.
- **Back-out before tags or submissions** (`:157-163`):
  - restore the source guides and `spark40Version` to the retained version in
    the lock;
  - correct only the new, unpublished snapshot;
  - preserve the master and Spark 4.1 references;
  - rerun the website checks;
  - regenerate a two-target plan and get new approval;
  - retain the abandoned plan and ledger.
- **Back-out after any tag or submission** (`:165-171`). This is the case from
  the finding:
  - stop and reconcile using the original records;
  - never edit a tagged candidate, move tags or raise the lock;
  - restore the retained references in a separately reviewed documentation
    correction;
  - keep website deployment blocked until that correction lands.
- **Skill.** `.github/skills/synapseml-release/SKILL.md:19-21` now points to
  this procedure.
- **Tests.** The restoration scenario above passes the strict README
  contract. In strict mode, the lock test (`installDocs.test.js:118-138`)
  still fails whenever `spark40Version` names a Spark 4.0 build that the lock
  does not record as published. The unit cases in the test at `:140` cover
  this mismatch.

### P-1: addressed

`README.md:91` and `docs/Get Started/Install SynapseML.md:27` now say three
things:

- new releases update the master and Spark 4.1 builds by default;
- maintainers may also release Spark 4.0;
- Spark 4.0 examples keep the last published version until then.

### Closure verification and limits

- **Full website suite here.** Running `node --test` in `website/` gave 35
  passes and 1 failure. The failure is `quantileRegressionScalaDocs.test.js`,
  which is unchanged: it cannot load because website dependencies are not
  installed in this worktree (the `react-router` module is missing). It is
  not part of this change. The coordinator reports 38 passes with
  dependencies installed.
- **Not re-run here.** The coordinator's 23 Python and public-documentation
  checks.
- **Unverified.** Hosted CI, Pages deployment and real publication.
- **Still open.** The consumer-wheel gate remains unresolved.
- No production-readiness claim is made.

Round 6 closure is complete, and this review stops here.

## Independent closure of follow-up notes

This is a bounded check of the two coordinator follow-ups only. HEAD is
unchanged. The `installDocs.test.js` line numbers cited in the closure above
refer to the previous revision; current positions are given here.

### Boundary note: closed

- **Pattern.** The pattern at `website/test/installDocs.test.js:70` now ends
  with `(?!\w|\.\d)`. A reference followed by a sentence-final period, `..`
  or `.jar` is now checked. It still skips a reference followed by a word
  character or by a dot and a digit, but neither forms a valid artifact name.
- **Regression.** The new regression at `:118-128` passes. Against the
  previous pattern it would fail in two ways:
  - its valid cases find no reference;
  - its stale `999.8.7` references are missed.
- **Current guides.** All 28 numeric Spark 4.0 references in the nine checked
  guide files (including both ONNX copies) still match.

### ONNX snapshot note: closed

- **Coverage.** The current-version ONNX snapshot is read at `:366-373`. At
  `:393-397` it is checked whenever either condition holds:
  - it contains `-spark4.0`;
  - the current R Setup snapshot carries the source-built
    `r_installation: source` marker.
- **Marker enforcement.** The source guide must carry that marker
  (`:206-207`). The release-guidance test also requires it in the current
  snapshot whenever the lock's Spark 4.0 entry is not at the current release
  version (`:216-219`).
- **Legacy snapshot.** The retained 1.1.3 ONNX snapshot has no `-spark4.0`
  text, and its R Setup snapshot has no marker. The legacy ONNX snapshot
  therefore stays unchanged and unchecked.

Copied-tree cases. Only the test's inputs were copied, and the name filter
ran exactly one test per case:

| Case | Result |
| --- | --- |
| Legacy snapshot unchanged | pass |
| Legacy format plus a stale backticked reference | fail: mixed versions |
| Marker added, ONNX references absent | fail: missing references |
| Same, with CRLF line endings in R Setup | fail: missing references |
| Marker added, ONNX copied from the checked source | pass |
| Marker added, stale sentence-final reference | fail: mixed versions |
| Marker added, stale `.jar` reference | fail: mixed versions |

Residual, non-blocking: the check is skipped only when the ONNX snapshot has
no Spark 4.0 text and the R snapshot has no marker. For a new release, that
would require hand-editing both generated snapshots.

### Follow-up verification and limits

- `node --test test/installDocs.test.js` passes 12 of 12 in strict mode and 12
  of 12 in preview mode.
- The full website suite was not re-run. The missing website dependencies
  noted above still apply in this worktree.
- These points are unchanged:
  - hosted CI, Pages deployment and real publication are unverified;
  - the consumer-wheel gate remains unresolved;
  - no production-readiness claim is made.

Follow-up closure is complete, and this review stops here.
