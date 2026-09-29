## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: gpt-6-astra
- **Artifact**: `reviews\pr-2628\task-2628-attempt-6-review-1-gpt-6-astra.md`
- **Reviewed base SHA**: `ae45761b61ccffe06f4516b97a24042665212cd1`
- **Scope**: The 18 staged DBC integration files against that HEAD for microsoft/SynapseML#2628, not the existing release-automation PR.
- **Diff SHA-256**: `25e5a7fd40d28fd3bc01fdac5ad3630c9954cc115af217c3aa11e67edad88a3a`, computed over `git diff --binary ae45761b61`.
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `AGENTS.md`, the supplied round-1 prompt, the branch context, and `reviews\pr-2628\README.md`. GitHub reports the PR base as `master`; the reviewed local HEAD is the SHA above.
- [x] Inspected the complete integration diff and followed approval, native build/reimport, publication, receipt, driver, verification, and release-notes consumers. Consulted the supplied prior review records without reopening unrelated resolved findings.
- [x] Loaded the actual baseline `release_matrix.py` from Git into an isolated in-memory module. Its one-, two-, and three-target schema-2 plans retain identical documents and approval IDs under the new loader, with zero DBC obligations. Corresponding schema-4 plans have different approval IDs and one DBC obligation per target.
- [x] Independently checked the supplied native proof against its local archive and source commit `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f`. All 57 notebooks, 309,559 bytes, the source digest, and archive SHA-256 `6660ce0286ca904244fa189aae0a71c43d07f1239b352a11440ca051dcd046e0` match. All 1,155 nonempty stored commands match the sanitized source. This is synthetic `0.0.0` evidence, not production publication or notebook execution.
- [x] Used an offline producer fixture and mocked public downloads to execute the actual notes and inventory CLI entry points. Both returned zero despite current DBC bytes differing from valid producer evidence. Joining those same fresh inventory rows to the producer report correctly raises `Public DBC hash differs from producer evidence`.
- [x] Injected a changed Markdown indentation through `roundtrip_archive` using the existing fake workspace. The validator accepted the changed cell and completed cleanup.
- [x] Ran only `python -B -m pytest scripts\release\test_release_ops.py::test_public_maven_receipt_uses_the_actual_guard_producer -q -p no:cacheprovider`. It fails with the DBC path error described below. Broad suites were not rerun.
- [x] Confirmed no source edits or unstaged source changes during review. No agents, commits, tag creation, publication, notebook execution, or live Azure/Databricks write operations were performed. Temporary reproduction data was removed.
- [ ] Production publisher and hosted CI were not exercised. Existing federation and inherited storage permissions were supplied as verified context, not independently revalidated here.

## Issues

### Issue 1: Compare the final live DBC download with producer evidence before publishing notes
- **Severity**: High
- **File**: `scripts\release\verify_release.py`
- **Line(s)**: 355-364; related new comparison at `scripts\release\release_ops.py:2149-2171`
- **Description**: The new public download check validates the bytes against the blob's own hash metadata and checks its claimed source commit. The producer comparison uses the rows in the supplied evidence report. At the release-notes boundary, these checks are disconnected: `.github\workflows\release-notes.yml:85-96` first runs `release_guard.py notes` against saved producer evidence, then independently runs `verify_release.py --inventory-only`. The latter collects fresh DBC hashes but never compares them with the saved producer hashes. In an offline reproduction with valid producer evidence for archive A and a valid archive B at the same URL, both real CLI entry points returned `0`. B had different bytes and self-consistent metadata naming the approved source. Passing the fresh rows to `validate_evidence` instead correctly rejected the mismatch.
- **Risk**: If a public blob is replaced after evidence collection, the final live check can succeed and release notes can advertise bytes that already differ from the approved producer. The publisher's no-overwrite option does not establish storage-wide immutability.
- **Suggested Fix**: Connect fresh DBC verification to the already validated producer evidence in the notes path. Compare each downloaded archive's SHA-256 and size with its producer manifest before emitting installation links or allowing GitHub release creation. Preserve the schema-2 path. Add a regression executing both notes gates with a changed archive that retains the approved version/source metadata.

### Issue 2: The round-trip comparator discards meaningful Markdown indentation
- **Severity**: Medium
- **File**: `scripts\release\release_dbc.py`
- **Line(s)**: 91-96; consumed at 299-302
- **Description**: `cells()` applies `.strip()` to the entire cell source before comparing it. This treats Markdown `    print(1)\n`, an indented code block, as identical to `print(1)\n`, ordinary paragraph text. An offline fault injection changed precisely this indentation in the reimported Jupyter response, and `roundtrip_archive` still returned the archive successfully. The supplied 57-notebook archive is intact; this finding concerns the negative content-preservation gate.
- **Risk**: The builder can claim successful cell preservation after an import/export conversion changes notebook rendering or other whitespace-sensitive content.
- **Suggested Fix**: Preserve leading indentation and significant trailing whitespace in comparisons. Normalize only empirically necessary, harmless transport differences such as line endings. Add a round-trip regression that rejects a Markdown code block changed into paragraph text.

### Issue 3: The producer-receipt regression reads the new DBC from the wrong directory
- **Severity**: Medium
- **File**: `scripts\release\test_release_ops.py`
- **Line(s)**: 2105-2109 and 2208-2213
- **Description**: `produced_maven_receipt` now writes the archive to `root.parent / "dbc"`, while its receipt path is `dbcs/<name>`. The test's byte-verification loop special-cases only `pypi/`, so it attempts to read `root / "dbcs" / <name>`. The targeted test fails at line 2211 with `FileNotFoundError` for `maven-artifacts\dbcs\SynapseMLExamplesv1.1.4.dbc`.
- **Risk**: The release regression suite is deterministically broken, and this test never verifies the newly added DBC producer hash against the actual fixture bytes.
- **Suggested Fix**: Resolve the DBC receipt identity to its actual staging location, `root.parent / "dbc" / <name>`, in the assertion loop, or return an explicit receipt-path-to-file mapping from the fixture. Keep DBC files outside the Maven staging directory's module-only layout.

### Issue 4: The release procedure still promises not to upload notebooks
- **Severity**: Low
- **File**: `scripts\release\README.md`
- **Line(s)**: 394-397
- **Description**: The execution procedure still says release mode "does not upload notebooks." That statement was accurate at the reviewed base, but now contradicts the new schema-4 publication section and the added pipeline steps.
- **Risk**: Operators reviewing the publication scope encounter conflicting descriptions of the side effects they are approving.
- **Suggested Fix**: State that schema-4 release mode publishes the approved runtime-specific DBCs, while saved schema-2 plans retain their original no-notebook scope. Keep the exclusions for generated docs, R packages, module wheels, and badges.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending. This review changed no source.
- **Why**: Fresh public bytes must match the approved producer before release-note publication.
- **How verified**: Offline reproduction returned `notes_gate_exit=0` and `fresh_inventory_gate_exit=0` with mismatching live bytes; explicitly comparing fresh rows with producer evidence rejects them.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: Markdown indentation changes cell meaning.
- **How verified**: The public round-trip helper accepted the injected dedentation; the native supplied archive independently matched its source.

### Issue 3
- **Status**: Open
- **What changed**: Pending.
- **Why**: The fixture's physical DBC directory differs from its public receipt prefix.
- **How verified**: The single targeted producer-receipt test failed at the documented file read.

### Issue 4
- **Status**: Open
- **What changed**: Pending.
- **Why**: The publication-scope statement must distinguish schema 2 from schema 4.
- **How verified**: Compared the baseline disclaimer, current execution procedure, new DBC documentation, and pipeline publication step.

## Resolution verification

All four findings are resolved in the subsequent staged integration diff.

1. `release_guard.py notes` now downloads each schema-4 DBC again and compares
   its verified hash and size with the validated producer evidence before
   writing installation links. CLI regressions reject missing, changed-hash,
   and changed-size downloads; the matching case succeeds. Schema 2 retains
   its no-DBC behavior.
2. `release_dbc.cells` now preserves leading and trailing whitespace, normalizing
   only line endings. The Markdown dedentation fault is rejected. All 57 real
   notebooks also passed native Databricks reimport with this stricter comparison.
3. The producer regression maps `dbcs/` receipt paths to the separate `dbc`
   staging directory and checks the actual archive bytes.
4. The operator procedure now distinguishes schema-4 notebook publication from
   the unchanged schema-2 scope.

The focused archive, contract, plan, and producer-regression run passed 76
tests. The native recheck reused the validated synthetic archive, removed its
temporary workspace folder, and performed no publication or notebook execution.
