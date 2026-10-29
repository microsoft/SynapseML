# PR 2628, attempt 4, round 4

**Result: changes required.** Two medium-priority correctness findings. This is not a gauntlet-pass declaration.

- Theme: Detailed correctness. Model: `gpt-6-astra`.
- Reviewed base HEAD: `a617374f54c7629da3216112ce8c1196bd735224`, with the current dirty delta.
- Bounded scope: changes since round-1 closure in website preview/deployment, installation tests, version-bump snapshot finalization and the disabled-Fabric test expectation. Traced affected installation links to planned publication outputs. Previously closed runtime findings were not broadly reaudited.

## R4-1. Medium: prepared documentation advertises auxiliary assets the release does not produce

Evidence: `scripts\bump-version.py:40-48,74-77,705-715`; `pipeline.yaml:280-290`; `docs\Reference\R Setup.md:24-34`; `README.md:207`; `docs\Get Started\Install SynapseML.md:219-221`; `website\src\pages\index.js:423-429`.

The bump rewrites R archive names and the Databricks DBC name to the new release version. It then generates and snapshots the modified documentation. Release publication intentionally omits `publishR` and notebook uploads; those remain in the non-release pipeline branch. The new snapshot finalizer removes the moving-master section, not these download instructions.

Reproduced without changing files by running the real `analyze` and `apply` functions against the four source files above, using the detected source version and target `1.2.0`. The result contains six module R archive links ending in `-1.2.0.zip`, plus `SynapseMLExamplesv1.2.0.dbc` in the README, installation guide and landing page. All four inputs passed the anchoring check. These instructions promise downloads outside the approved Maven/primary-PyPI output set. A successful website build does not establish their availability.

Minimal scope-preserving remedy: omit these unsupported release-specific download instructions and state that this release does not publish those auxiliary assets. For Databricks examples, link to the reviewed source notebooks instead of inventing a new DBC. Alternatively, use separately verified historical asset versions with explicit compatibility guidance; do not silently imply compatibility with the new release. Add prepared-output assertions covering both source docs and the new snapshot. Do not expand publication destinations without approval.

No remote availability probe was performed. The finding is the confirmed mismatch between planned links and planned outputs, not a claim of an observed HTTP error.

Resolution follow-up: removed new-release R archive downloads and DBC archive links from the active source guidance. R instructions now build wrappers from the matching source tag with `sbt packageR` and install the generated package roots containing `DESCRIPTION`. Those paths match `CodegenPlugin.scala` and `RCodegen.scala`. Notebook links select the reviewed source tag for each runtime; the homepage derives them from `installArtifacts` rather than a literal version.

Website assertions now require this contract in source documentation and new prepared snapshots, while keeping the existing archive assertions for historical guides. The primary `1.2.0` snapshot was regenerated through `WebsiteChannel.process` and the shared finalizer. Its 37 website checks passed, and hashes confirmed that all 42 older installation/R guides stayed unchanged. Publication destinations were not expanded. Cross-port propagation and independent closure remain pending.

## R4-2. Medium: printed recovery commands bypass the new required finalization

Evidence: `scripts\bump-version.py:406-432,457-466,709-723`; `website\test\installDocs.test.js:165-172`.

After documentation conversion or versioning fails, `main` still tells the operator to run raw `docusaurus docs:version`. The new moving-snapshot cleanup runs only inside the Python `_run_docusaurus` wrapper. Following the printed recovery sequence therefore does not perform that cleanup.

Reproduction by source trace: fail `convertNotebooks` after the version replacements, repair conversion, then follow the printed SBT and npm commands. Docusaurus snapshots the source installation guide, including its moving-master section; no printed command invokes `_finalize_versioned_docs`. The resulting guide violates the newly enforced versioned-guide assertion. If failure instead occurs during finalization, the snapshot already exists, so repeating version creation is also the wrong recovery step.

Provide an idempotent documentation recovery entry point, or print a complete sequence that finalizes the exact new snapshot and handles an already-created version. Cover the printed recovery path, not only direct calls to the finalizer. Preserve historical snapshots and the source guide.

Resolution follow-up: added `python scripts\bump-version.py --finalize-docs --to VERSION`, with a nonwriting `--dry-run`. Recovery requires the current source version and complete local Git history. With the correction below, it rejects snapshots or sidebars found in current HEAD ancestry, including deleted snapshots; newly staged snapshots remain eligible. Unrelated refs and reflogs do not block another runtime's recovery. It calls the same finalizer, refuses missing, unrecognized, symlinked or hardlinked guides, and rejects `--from`, `--skip-docs` and `--verbose`. Failure output includes finalization and never recommends recreating an existing snapshot. Normal bump validation remains intact. The expected-file manifest now names the homepage's actual version source, `website/src/installArtifacts.js`, rather than its former literal-version consumer.

Evidence from this implementation:

- Red: `python -m pytest scripts\test_bump_version.py -q -k TestDocsRecoveryCLI --tb=short`: **24 failed, 1 passed** before production changes.
- Green: `python -m pytest scripts\test_bump_version.py -q -k "TestDocsRecoveryCLI or TestRunDocusaurus" --tb=short`: **39 passed**. Local Git fixtures exercise the actual CLI and execute the printed finalization command, with historical/source preservation, dry-run, admission failures and stage-specific recovery.
- Full native run, with native Git on PATH: `python -m pytest scripts\test_bump_version.py -q --tb=short`: **267 passed, no skips**. Existing unknown-marker and class-fixture deprecation warnings remain.
- Black **22.3.0** checks and scoped `git diff --check` passed. No repository commit, push, publication, CI invocation or remote API call was performed. Temporary fixture commits are local test data.

Multi-runtime correction: the original `--all --reflog` scan incorrectly treated a sibling runtime's committed `1.2.0` snapshot as this branch's history. Recovery now uses `git log HEAD --full-history` for the exact snapshot and sidebar paths. Only commits reachable from current HEAD count, including merged ancestors and subsequently deleted snapshots. Other branches, remote-tracking refs and unrelated reflogs are excluded. Shallow repositories still fail closed; source-version binding, finalizer validation and nonwriting dry-run are unchanged.

Real local Git tests commit the primary `1.2.0` snapshot, retain a matching remote-tracking ref, and create Spark 4.0/4.1 branches from the earlier baseline. Both staged and untracked port snapshots now pass preview and finalization without changing the primary commit or historical guides. The obsolete unrelated-ref rejection case was replaced, not retained as an invariant. `python -m pytest scripts\test_bump_version.py -q -k "sibling_runtime_snapshot or refuses_committed_snapshot_even_after_deletion or refuses_historical_sidebar or shallow_history" --tb=short` changed from **4 failed, 4 passed** to **8 passed**. The full native command above now passes **270 tests, no skips**; Black **22.3.0** and scoped whitespace checks pass. No parent-owned docs/JS files, R4-1 notes, runtime modules or repository refs were changed.

## Checks and limits

The preview setting and both Pages conditions are mutually exclusive in the current workflow. Nine event/ref combinations confirmed that preview cannot select Pages upload or deployment. The build still runs `npm test` and `npm run build` without a failure override; deployment depends on that build. Mocked bootstrap admission rejected pending, failed, cancelled and skipped website runs and accepted a matching successful run. No preview-success bypass was found.

Reviewer checks: **15 targeted Python tests passed**, covering the new finalizer, fixture exclusions and disabled-Fabric expectation; **one targeted Node test passed**, covering future-version preview versus the exact production publication lock. The targeted Python run emitted an unknown `slow` marker warning. No build, publication or external API call was made, and no source code was edited.

Round 3 and the full site build were pending when this review was requested. Parent validation is not claimed as reviewer evidence. This single-model round does not replace missing model coverage or prove live publication, package contents or consumer compatibility.

## Independent reviewer closure, 2026-09-24

**Zero remaining findings in this bounded recheck. R4-1 and R4-2 are verified closed.** This supersedes the original changes-required result for these two findings, while preserving their original text and resolution history. Reviewed the stable current tree over the same base HEAD using `gpt-6-astra`.

R4-1: replayed the original four-file failure shape through the real version-bump `analyze`/`apply` functions for `1.2.0`. The prepared README, installation guide, R guide and homepage contain no invented R archive or DBC download links. All three runtime-specific notebook tags are retained. The actual shared finalizer removed the moving-master section from a temporary prepared installation snapshot without removing its pinned notebook links. R guidance now names local source builds and package roots matching `project\CodegenPlugin.scala:232-241,344-355` and `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\RCodegen.scala:33-35`. Historical versioned docs and sidebars in the reviewed worktree still match HEAD.

R4-2: verified the supported recovery admission and stage-specific commands in `scripts\bump-version.py:417-557,640-656`. Tests executed the printed finalization command after the original conversion-failure shape, checked that existing snapshots do not trigger duplicate version creation, and confirmed dry-run/idempotent behavior. Real temporary Git fixtures covered both port branches with staged and untracked snapshots, unrelated primary/remote-tracking refs, committed/deleted snapshots, retained sidebars and shallow history. No additional cross-branch or historical-preservation defect was found.

Independent commands:

```text
python -m pytest scripts\test_bump_version.py -q --tb=short -p no:cacheprovider -k "TestDocsRecoveryCLI or TestRunDocusaurus"
node --test website\test\installDocs.test.js website\test\rSetupDocs.test.js
```

Results: **42 Python tests passed, 228 deselected; 31 Node tests passed; no skips.** Python reported the existing unknown `slow` marker warning. The Node run checks current source guidance and retains historical archive checks; prospective `1.2.0` output was checked separately as described above.

Only this artifact was edited in the reviewed worktree. Git commits used by the regression suite were confined to disposable local fixtures. No repository commit, push, external API call, package build or full website build was performed. Candidate propagation, all three candidate snapshots and full site/CI results remain parent-owned evidence, not independent claims from this recheck. This closure makes no full-gauntlet, model-diversity or production-readiness claim.
