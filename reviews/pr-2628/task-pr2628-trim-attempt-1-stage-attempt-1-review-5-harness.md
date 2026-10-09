# Release harness and compatibility trim audit

## Driver context

- Reviewed microsoft/SynapseML#2628, target `master`, base `861c3a1e14a9511b5604563ff1e3976cefa82e90`, head `800589ac8dfe4bd23596cb174c9ec997e574b6ae`.
- Confirmed 0 behind and 10 ahead. Read the complete base-to-head changes and every current owned file; five files are entirely new.
- Effective Medium comes from the human's six disjoint component audits. This is component 5, attempt 1, stage attempt 1, using the requested harness default without model inference.
- Moderate complexity: Git ancestry recovery, two successive version bumps, subprocess failure handling, receipt-backed rehearsal and Python image conversion.
- Audit only. No source edits, commits, pushes, production operations or nested reviewers. Parent owns aggregation, authorization and final CI.
- Verdict: one introduced correctness defect; bounded consolidation is available. Do not delete the compatibility fix or the offline runner.

## Complete owned-file coverage

All ranges below refer to reviewed head. Paths are repository-relative.

| File | Current lines | Coverage and disposition |
| --- | ---: | --- |
| `scripts\bump-version.py` | 871 | All patterns, filtering, replacement, postconditions, docs generation and recovery; B1, R3. |
| `scripts\test_bump_version.py` | 1956 | All unit, fuzz, real-file, repeated-bump, Git-history and CLI tests; R1, R2, R4. |
| `scripts\release\release_dry_run.py` | 152 | Entire fixed pytest runner, JUnit handling, environment allowlist, report creation and failures; retain. |
| `scripts\release\test_release_dry_run.py` | 149 | Entire runner fixture and success/error/empty/skipped/credentials matrix; retain. |
| `scripts\release\test_release_rehearsal.py` | 201 | Entire offline guard, imported fixtures, plans, approval, resume, receipts, ledger and local bootstrap; C1. |
| `scripts\release\test_public_release_docs.py` | 54 | Entire privacy scan and ordering/text checks; retain as documentation checks, not runtime proof. |
| `opencv\src\main\python\synapse\ml\opencv\ImageTransformer.py` | 201 | Entire handwritten wrapper, both conversion helpers and JVM convenience methods; retain bytes correction. |
| `opencv\src\test\python\synapsemltest\opencv\test_image_conversion.py` | 122 | Entire wheel/JAR provenance, container matrix, invalid sizes, round trips, transform and persistence tests; retain. |

## Correctness finding

**B1, high priority: the newly generic R-archive anchor can rewrite an unrelated archive version.**
`scripts\bump-version.py:82-84,330-343` accepts any `-{V}.zip` on a line containing `synapseml`; it does not bind the matched version to that archive name.
Offline base/head reproduction used `synapseml-future-1.1.0.zip other-1.1.0.zip`.
Base reports zero matches and two unanchored occurrences, which makes the CLI refuse before writing.
Head reports two matches and zero unanchored occurrences; `apply` produces `synapseml-future-2.0.0.zip other-2.0.0.zip`.
Bounded remedy: remove this new broad rule or match the version span inside the intended SynapseML archive token. Retain support for actual module archives.
Add a mixed-archive negative regression. Current R-archive tests at `test_bump_version.py:315-329` exercise isolated known names and miss this case.
The older 200-character self-anchor also accepts unrelated nearby versions, and adversarial tests explicitly encode that behavior. That predates this PR; do not expand this audit into a general matcher rewrite.

## Ranked bounded reductions

Estimates count removed physical lines minus replacement assertions. They are proposals, not applied savings; totals exclude B1's eventual regression/fix.

1. **R1: consolidate duplicated snapshot safety tests, approximately 54 net lines.**
   In `test_bump_version.py`, remove helper duplicates at 674-702, 729-731 and 747-765.
   Their stronger CLI equivalents at 1059-1088, 1218-1238 and 1240-1263 preserve exact output, unchanged historical bytes, idempotence, missing-guide rejection and linked-target protection.
   Remove the five-line staged-only case at 1090-1094 only while keeping the staged/untracked sibling-history matrix at 1096-1160.
   Add an explicit finalized-content assertion to `test_uses_local_npm_binary` at 610-644, so the normal generation path still proves finalization.
   Keep duplicate-heading rejection at 704-727 and subprocess-success-without-snapshot rejection at 733-745; neither is redundant.

2. **R2: merge output-only launches and remove tautologies, approximately 44 net lines.**
   Move the three postcondition-message assertions from `TestPostCondition`, 1916-1956, into the existing successful and dry-run integration cases. Save approximately 38 lines and two child Python launches.
   Delete `test_no_zero_count_files`, 1791-1793: its fixture only inserts files when `r.matches` is nonempty.
   Delete the historical `extra` assertion at 1906-1908: `script_files` is populated only while iterating `expected`, so the difference is necessarily empty.
   These are mostly pre-existing redundancies, not new release scope. Preserve historical missing-file detection, the real-file expected manifest and the historical replay cases themselves.

3. **R3: remove shadowed configuration, 15 net lines.**
   In `bump-version.py:99-105`, the comma-specific `version` rule is subsumed by the newly broadened rule at 89 for the same R-codegen file. Keep the distinct `pythonizedVersion` and `rVersion` rules.
   In 145-152, eight basename exclusions are redundant with `scripts/release` exclusion or name files absent from tracked source.
   Proof: in-memory removal of those eight names changed filtering for zero tracked paths. Removing only the comma-specific rule preserved match spans for all three R-codegen fields.
   Keep repository-relative exclusion and its negative examples. Do not replace it with exclusion of every directory named `release`.

4. **R4: reuse the existing snapshot-path helper, approximately 14 net lines.**
   Replace the repeated guide-path construction inside `test_uses_local_npm_binary`, 610-644, and `test_unrecognized_moving_snapshot_refuses_without_editing`, 704-727, with `_recovery_guide(tmp_path)`.
   The existing helper at 1029-1037 already names exactly that new version's guide; no additional abstraction or fixture is needed. This does not overlap R1's removed tests.

**Combined bounded opportunity: approximately 127 net lines**, plus minor separator changes. Retain descriptive test names and independent assertions; no giant parameter table.

## Rehearsal and assertion-quality decisions

- **C1, coordinate with the ops owner, no savings counted:** rehearsal approval checks at 82-96 repeat `test_release_ops.py:1857-1873`; ledger mismatch at 152-163 repeats 2923-2933. Bootstrap preview/idempotence at 166-183 overlaps `test_release_bootstrap.py:181-196`, and pending CI at 186-192 overlaps that file's failure matrix.
- Reuse canonical scenario functions only if the fixed rehearsal still collects approval, plan-change and bootstrap rejection cases under its own offline fixture. Preserve default and optional-target public-plan coverage. Blindly importing every ops test or dropping these checks is not a safe trim.
- There is no second release engine here: rehearsal calls the real driver/bootstrap with existing simulated-service fixtures. The combined receipt-backed multi-target sequence at 99-134 adds integration evidence; keep it.
- The repeated real-guide bump at `test_bump_version.py:42-134` is worthwhile despite its length: it preserves optional pins across two bumps, including an optional pin already equal to the next release, and checks dry-run bytes. Do not replace it with isolated regex tests.
- The copied script in `recovery_repo:1001-1026` is needed by the printed-command recovery test, not every recovery case. Moving that copy to its sole consumer is a possible fixture-cost cleanup, not a meaningful net-line reduction.
- Public-doc token and ordering assertions describe a human gate; they cannot establish wheel qualification, approval enforcement or immutable publication safety. Keep the privacy scan and do not cite prose checks as behavioral evidence.
- Keep runner environment allowlisting, disabled plugin autoload, fixed test path, no production-input flags, exclusive report creation, nonempty JUnit results, skip rejection and timeout/error reporting. A shorter generic subprocess wrapper would weaken the boundary.

## OpenCV scope and evidence

The bytes correction is release-critical for the retained immutable-byte compatibility contract, not removable because its path is outside release tooling.
An offline probe executed the exact base/head `toNDArray` function bodies with NumPy 2.5.1. Base raised `ValueError` for immutable bytes in both grayscale and RGB cases; head returned the correct shape, uint8 values and RGB order.
Bytearray, list, NumPy-array and memoryview cases succeeded before and after. The probe did not mutate input bytes. Head's grayscale bytes result is read-only, consistent with `frombuffer`; no new copy requirement is justified.
This is function-level failure evidence, not installed-wheel or Spark proof. PySpark and candidate wheel/JAR artifacts were unavailable here; the local Python/NumPy versions also differ from master's pinned environment.
The fix may ship in a separate prerequisite PR, but the exact primary candidate wheel must include it before selected-runtime qualification. Do not remove artifact-byte binding, Spark BinaryType, JVM transform or save/load tests to make that gate cheaper.

## Executed evidence and minimal commands

- Parent-supplied current-head baseline: 1,778 Linux regression passes plus 11 native Git cases; 1,669 hosted release tests including SBT/history; green Azure validation with 22 unchanged skips and cache-only warnings. These are shared results, not reruns by this reviewer.
- Environment distinction: local Windows Python is 3.14, the available pinned-tool WSL environment is 3.12, and hosted branch validation uses 3.11. The Windows rehearsal timeout below is a local observation, not evidence that the shared hosted baseline failed.
- No further broad audit reruns are needed. For authorized edits, use existing Black 22.3.0, targeted affected tests, Linux temporary storage in shared memory, disabled bytecode/cache writes, and native Windows Git for worktree/history-dependent cases.
- `python -m pytest scripts\test_bump_version.py scripts\release\test_release_dry_run.py scripts\release\test_public_release_docs.py -q -p no:cacheprovider -o addopts= -k "not HistoricalReplay" --tb=short`: **326 passed, 10 deselected**. Plugin autoload and bytecode writes were disabled.
- `python scripts\release\release_dry_run.py`: **failed closed, exit 2**, its fixed child exceeded 300 seconds; no passing rehearsal claim.
- `python -m pytest scripts\release\test_release_rehearsal.py -q -p no:cacheprovider -o addopts= -k "not bootstrap" --tb=short`: **10 passed, 2 deselected**. This isolates simulated-driver coverage but does not replace the failed complete runner.
- `python -m black --check` with the eight owned Python paths: **unchanged**, using installed Black 26.5.1, not the repository-required 22.3.0. This is not the pinned formatting gate.
- `python -m pytest scripts\release\test_public_release_docs.py -q -p no:cacheprovider -o addopts= --tb=short`: **56 passed**, including this new report.
- Historical replay, actual candidate-wheel/PySpark tests and final CI were not run by this reviewer. After authorized matcher/history edits, use `python -m pytest scripts\test_bump_version.py -q -k "MustAnchor or MustNotAnchor or HistoricalReplay"` plus the new mixed-archive regression; use the affected Docusaurus/recovery/integration selectors for R1/R2/R4.
- For OpenCV qualification, retain the documented candidate-wheel/JAR setup and run `python -m pytest opencv\src\test\python\synapsemltest\opencv\test_image_conversion.py -q` on every selected runtime. Parent owns this gate.

## Authorized resolution, 2026-10-09
- B1 resolved with complete SynapseML archive-token matching and exact version spans; legacy and future modules remain supported. R1-R4 consolidation retains unique snapshot, staged sibling-history and executable printed-recovery coverage.
- Simplified runner XML fixtures, asserted the exact 300-second timeout, and reused bootstrap's Git helper plus its concurrently added `website_run` fixture. OpenCV, production runner and reassigned public-doc tests were not edited.
- Exact code delta: `bump-version.py` -15, `test_bump_version.py` -73, `test_release_dry_run.py` -11, `test_release_rehearsal.py` -8; **92 additions, 199 deletions, net -107 lines**. This resolution adds 12 report lines.
- Commands below use `python -m pytest`, `-q -p no:cacheprovider -o addopts= --tb=short`, disabled bytecode/plugin autoload, and shared-memory temporary storage for WSL cases.
- B1: `scripts\test_bump_version.py -k "archive_versions_bind_only or mixed_archives_refuse"`: **10 failed before, 10 passed after**; includes whole-CLI refusal with unchanged file bytes.
- WSL: `scripts\test_bump_version.py -k "MustAnchor or MustNotAnchor or SkipDir or SkipFile or Integration or RunDocusaurus or SnapshotRegression or RoundTrip or repeated_default_release"`: **158 passed, 112 deselected**.
- Native Windows: `scripts\test_bump_version.py -k "finalizes_only_new_snapshot or dry_run_validates or linked_guide or explicit_errors_preserve or sibling_runtime_snapshot or printed_conversion_recovery"`: **13 passed, 257 deselected**.
- WSL: `scripts\release\test_release_dry_run.py scripts\release\test_release_rehearsal.py -k "runner or bootstrap or rejects_network"`: **16 passed, 9 deselected** after wiring the new shared fixture; initial two fixture-resolution errors are resolved.
- `python -m black --check scripts\bump-version.py scripts\test_bump_version.py scripts\release\test_release_dry_run.py scripts\release\test_release_rehearsal.py`: **4 unchanged with Black 22.3.0**; owned `git diff --check` passed.
- No unresolved owned finding. The older non-archive window behavior remains outside this bounded fix; full 300-second runner, historical fleet and candidate-wheel qualification remain parent freeze gates. No commits, pushes, live calls or nested agents.
