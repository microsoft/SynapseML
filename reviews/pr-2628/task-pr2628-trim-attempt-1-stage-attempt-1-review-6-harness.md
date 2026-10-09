# Documentation and website bloat review

**Verdict: consolidate the operator documentation; retain the audit evidence.** The six release-guidance files total 1,218 lines. The normal numbered procedure starts at line 286 of the operator guide, then detours through optional-target and bootstrap instructions before source binding. Readers must also reconcile a second procedure in the skill and a third in the agent runbook.

Reviewed microsoft/SynapseML#2628 at `800589ac8dfe4bd23596cb174c9ec997e574b6ae`, against master `861c3a1e14a9511b5604563ff1e3976cefa82e90`; local ancestry confirms 0 behind, 10 ahead. Human-assigned Medium, six disjoint audits, lens 6, harness-default reviewer, no model override or nested agents. This report covers the assigned documentation/website scope, not the aggregate fleet verdict.

## Ranked actions for approval

Line ranges refer to the reviewed head. Savings below are replacement budgets, not completed edits. They include relocated text and do not count deleting historical evidence.

1. **One operator guide, one short skill entry point: approximately 365 lines saved.** Keep `scripts\release\README.md` as the only command/procedure authority. Replace `.github\skills\synapseml-release\SKILL.md:1-96` with at most 24 lines of frontmatter, discovery links and non-negotiable authorization/qualification rules. Consolidate and remove its four reference files: `agent-runbook.md:1-176`, `automation-boundaries.md:1-52`, `preflight.md:1-32`, `recovery-and-rollout.md:1-49`. Most content already appears in the guide. Reserve 16 guide lines for the agent-specific authorization, checkpoint and resumption requirements from runbook lines 9-29 and 164-176. Update every incoming link; do not leave a skill pointing at deleted references. Accounting: 405 old lines minus 24 skill lines minus 16 relocated lines.
2. **Put the normal path first and shorten reference material: approximately 238 lines saved.** Keep a concise numbered default procedure, with qualification before tag creation, explicit approvals beside writes, and links to optional/bootstrap/recovery sections in the same file. Keep those sections as references, not competing procedures. Use the following non-overlapping budgets in `scripts\release\README.md`.

| Current range | Old -> replacement lines | Keep; remove |
| --- | ---: | --- |
| 29-77 | 49 -> 35 | Same candidate wheel, runtime/JAR bindings, exact command and retained results; shorten repeated explanations of what CI does not prove |
| 108-142 | 35 -> 12 | Rehearsal command, dependencies/new output, zero-skip success and no live authority; drop the runner implementation tour and second procedure |
| 144-220 | 77 -> 26 | DBC permissions, candidate check command, non-execution limit, pre-upload validation, retained handoff, immutable recovery and schema distinction; condense implementation narration |
| 242-284 | 43 -> 15 | Original ledger/status, task-level warning admission, preserved raw outcome, Windows file transport and refusal recovery; remove test-fixture and compression-design history |
| 332-412 | 81 -> 50 | Explicit Spark 4.0 selection, policy/readiness check, no restacking, complete doc commands and pre/post-tag back-out distinction; state each invariant once |
| 413-498 | 86 -> 55 | Bootstrap branch names, current-head gates, protection check, preview/apply separation, frozen heads, merge order and reconciliation; shorten repeated guard internals |
| 499-533 | 35 -> 20 | Finalization commands, full history, preserved snapshots, launcher checksum maintenance; link rather than repeat retained-runtime/lock policy |
| 596-615 | 20 -> 8 | Primary-only API docs before Maven, approved-source admission and mutable-doc limitation; link to the guard for endpoint/bounds implementation details |
| 641-680 | 40 -> 16 | Destination-specific receipts, non-yanked exact wheel, fresh public bytes, no fabricated provenance/republication and historical diagnostic limits; remove repeated receipt walkthrough |
| 726-754 | 29 -> 20 | Final payload/metadata comparison, RECORD validation, every selected runtime, owner sign-off and immutable-version recovery; avoid restating pre-tag qualification |

3. **Merge duplicate static checks, not their safety assertions: approximately 20 lines saved.** `website\test\installDocs.test.js:219-262` rereads installation guides and the homepage already covered at 301-339 and 341-380. Move DBC/tag-link assertions into those existing loops; retain one short prepared-versus-historical R-marker admission check. Budget 44 lines to at most 24 total replacement lines across these locations. Keep the conditional historical snapshot exemption. `rSetupDocs.test.js:29-79` should remain the owner of R installation-form/archive checks; it is not redundant coverage of Python/JVM pin consistency.
4. **Remove obsolete migration narration: 3 lines saved.** Delete `scripts\release\README.md:238-240` about the removed Spark 4.1 replay. The single procedure must still require selected-target CI and independent wheel qualification. The skill's equivalent lines 94-96 disappear under action 1, so they are not counted twice.

The budget is about **626 net lines removed before this required report**, with all 43 existing evidence files retained. It leaves roughly 588 lines in the single operator guide, including exceptional procedures, and a 24-line skill. Aim for a default-path entry section of at most 120 lines. Do not achieve a smaller number by burying a pre-write gate in an appendix.

## Complete owned-file coverage

All 16 non-review files were read in full and compared with the base where changed. Unchanged consumer-guide material is not a reason to broaden this release PR.

| Owned file | Disposition |
| --- | --- |
| `.github\skills\synapseml-release\SKILL.md` | Consolidate under action 1; retain a visible consumer-before-bootstrap boundary |
| `.github\skills\synapseml-release\references\agent-runbook.md` | Consolidate; retain unique authorization/checkpoint/resume requirements |
| `.github\skills\synapseml-release\references\automation-boundaries.md` | Consolidate; preserve human signing, qualification and merge boundaries |
| `.github\skills\synapseml-release\references\preflight.md` | Consolidate; preserve read-only/local-state distinction and bootstrap ordering |
| `.github\skills\synapseml-release\references\recovery-and-rollout.md` | Consolidate; preserve dead-owner checks, claim/ledger recovery and immutable publication limits |
| `AGENTS.md` | Keep the one added release-skill discovery link at 18; no new release procedure here |
| `README.md` | Keep changed runtime-retention and notebook-link guidance at 91-93 and 209-217 |
| `scripts\release\README.md` | Actions 1, 2 and 4; preserve exact useful commands rather than six prose copies |
| `docs\Explore Algorithms\Deep Learning\Getting Started.md` | Keep 21-40: aggregate wheel and three matching Python/PySpark pins; no return to an unpublished module wheel |
| `docs\Explore Algorithms\LightGBM\LightGBM - Quantile Regression for Drug Discovery (Scala).md` | Keep only the two coordinate changes at 45 and 509; they are required version-bump anchors, not unrelated tutorial expansion |
| `docs\Get Started\Install SynapseML.md` | Keep changed 27-29 and 260-268; web and repository entry points need their own working examples |
| `docs\Reference\R Setup.md` | Keep source-install marker, package-root build/install, explicit JVM resolver and historical distinction; the PR adds only two net lines |
| `website\src\installArtifacts.js` | Keep independent Spark 4.0 version/package/tag binding; do not collapse it into the primary version |
| `website\src\pages\index.js` | Keep per-row Python pins and runtime-matched notebook links; no rendering abstraction justified by this small delta |
| `website\test\installDocs.test.js` | Action 3 only; preserve preview/production lock checks, partial-edit failures and retained-runtime negative cases |
| `website\test\rSetupDocs.test.js` | Keep source versus historical archive coverage and all six components; do not skip historical guides to shorten test output |

All **43 pre-existing files under `reviews\pr-2628`**, totaling **5,233 lines**, were inspected for retention/scope at the head above. The following exhaustive groups identify that committed inventory; new sibling trim reports are excluded.

| Filename group | Files | Lines |
| --- | ---: | ---: |
| `README.md` | 1 | 89 |
| `pr-2628-attempt-4-review-*` | 6 | 872 |
| `pr-2628-attempt-5-review-*` | 1 | 50 |
| `task-2628-attempt-6-review-*` | 6 | 415 |
| `task-2628-dbc-path-attempt-*-review-*` | 10 | 462 |
| `task-2628-default-targets-attempt-1-review-*` | 6 | 949 |
| `task-2628-release-lookup-attempt-1-review-*` | 6 | 101 |
| `task-pr2628-blockers-attempt-1-stage-attempt-1-review-*` | 1 | 201 |
| `task-pr2628-fleet-attempt-1-stage-attempt-1-review-*` | 6 | 2,094 |

These files contain original findings, later resolutions, unavailable-reviewer dispositions and scoped test claims. Their historical verdicts are not current-head correctness proof. The index also contains historical cleanup/rebase narrative; that does not authorize another purge.

## Retention constraint and rejected cuts

- Existing policy requires preserving original findings/verdicts and committing all reports. The bloat request does not waive it. Do not delete, rewrite, squash together, relocate outside the numbered directory or silently mark these files hidden/generated.
- For easier browsing, the parent can put links to this evidence directory in a clearly labeled historical-evidence `<details>` section of its live PR body, while keeping current findings and release holds visible. This changes navigation, not evidence. No extra index or report is needed.
- Do not revert the LightGBM coordinate edits. An independent in-memory call to the current bump analyzer found unanchored base lines 45 and 509, versus none at head. Removing those edits would restore the preparation refusal.
- Do not remove runtime pins, source-built R instructions, historical snapshots, publication-lock negatives or actual shared-wheel qualification. Do not equate offline rehearsal, a DBC round-trip or static website tests with live service/consumer qualification.
- Preserve explicit maintainer approval, exact plan/source binding, manual signing/merges, tag freeze, ambiguous-submission recovery, schema-2 compatibility, API-doc mutability and the final-wheel human gate. Consolidation changes their location, not their force.

## Minimal documentation checks and limits

Parent-supplied shared baseline: 1,778 Linux regression passes, 11 native Git cases and 1,669 hosted release tests including SBT/history. Azure was reported passing with 22 unchanged skips and cache-only warnings. These results were not rerun or claimed as independent audit evidence. No broader bug review is needed to evaluate the proposed reductions.

- `node --test --test-reporter=spec website\test\installDocs.test.js website\test\rSetupDocs.test.js`: **35 passed**, zero skips, preview variable unset.
- `$env:SYNAPSEML_DOCS_PREVIEW = 'true'; node --test --test-reporter=dot website\test\installDocs.test.js website\test\rSetupDocs.test.js`: **35 passed**, zero skips.
- `python -B -m pytest scripts\release\test_public_release_docs.py -q -p no:cacheprovider --tb=short`: **54 passed**, including this report and concurrent trim reports. Recheck revised links after approved edits.
- The public-doc tests currently depend on guide/skill marker order. Coordinate their update with the test owner, preserving consumer qualification and protection checks before approved bootstrap dispatch. Do not merely delete failing assertions.
- After consolidation, rerun these checks and the existing website build for changed Markdown links; no new test framework is needed. No full website build, live calls, source changes, commits or pushes were made in this audit. The parent owns revision approval, the live PR body and committing this required report.

## Approved consolidation and evidence
- Actions 1, 2 and 4 implemented in `scripts\release\README.md` and the 22-line skill; entry starts at line 21. Deleted the four redundant references after repairing links; no incoming Markdown links remain.
- Kept authorization, qualification/signing/merge gates, optional-target commands, bootstrap, recovery and exact-path/checkpoint/resume requirements in the single runbook.
- Eight tracked owned paths remove **609 net lines**; release guidance alone falls from 1,218 to 605 lines. Action 3 saves 33 website-test lines; public-doc assertions add 37.
- `python -B -m pytest scripts\release\test_public_release_docs.py -q -p no:cacheprovider --tb=short`: **56 passed**, including local links and retained approval/handoff boundaries.
- `node --test --test-reporter=spec website\test\installDocs.test.js website\test\rSetupDocs.test.js`: **34 passed**, zero skips; the same files also pass with `SYNAPSEML_DOCS_PREVIEW=true`.
- `python -B -m black --check scripts/release/test_public_release_docs.py` with existing WSL **Black 22.3.0**: passed, unchanged.
- Eight in-memory negative fixtures were rejected by the relocated DBC/tag-link/R assertions; the unmodified baseline passed. No fixture files were written.
- Full `npm test -- --test-reporter=spec` in `website`: **36 passed, one module-load failure** because unchanged `quantileRegressionScalaDocs.test.js` cannot load missing `react-router`; preview also fails for that dependency.
- `npm ci --offline --ignore-scripts --no-audit --no-fund` failed on an uncached tarball; `npm run build` lacks `docusaurus`. Removed failed-restore residue; manifests/pins unchanged. Full website validation remains blocked.
- All 43 historical reports remain unchanged and visible. No live service calls, production writes, broad release-suite reruns, nested agents, commits or pushes; parent owns aggregate validation and publication.

## Driver decision and final outcome

| Field | Effective decision |
| --- | --- |
| Tier/source | Medium, primary estimate. The human requested a fresh fleet covering every changed file, not a specific tier or model. |
| Change size | 108 files, 31,275 additions and 413 deletions before revision; 43 historical reports account for 5,233 added lines. |
| Complexity | High branching across approval, submission recovery, immutable destinations, artifact provenance and saved-plan compatibility. |
| Rationale | Six disjoint component/lifecycle audits before edits, then bounded same-task repairs. Preserve safety and compatibility; remove duplicate implementations, tests and instructions. |
| Primary model | `gpt-6-astra`. |
| Reviewers/override | Six fresh runtime-selected agents, reports 1-6. The human did not select models or reasoning settings; no complementary or three-model claim. |
| Pre-dispatch record | Session `release_audit.trim_review_contract`; rebase was performed first and left all ten commits unchanged against master `861c3a1e14`. |

All six audits and owned repairs completed. Targeted checks are recorded in their reports; no owned findings remain unresolved.
The combined tracked patch removes 1,248 net lines and five duplicate files. Historical review reports remain unchanged.
Combined Linux tests: 1,807 passed, one opt-in SBT skip; native Git complement: 11 passed; native runner/runbook tests: 81 passed. Both actual offline runners passed all 12 scenarios without skips.
Black 22.3.0 passed for all 238 files. After lockfile dependency restoration and normal docgen preparation, all 39 website tests passed normally and in preview, and the preview build passed.
Exact-head CI and PR publication remain pending at commit time; the live PR will record their results. No production release is authorized by these checks.
