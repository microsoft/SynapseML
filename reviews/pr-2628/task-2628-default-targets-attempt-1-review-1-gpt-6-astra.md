# Default release targets: round 1 broad sweep

Reviewer: GPT-6 Astra (`gpt-6-astra`).

Reviewed HEAD: `2391aa166ab46238307de9edf9650102cbff950e`.
Supplied master base: `681bd96990c421de3b91d2b1bf8f8f470764199d`.

Scope: the current 24-file tracked unstaged delta plus untracked
`scripts/release/test_release_defaults.py`. This covers target selection,
guards, bootstrap, executor policy and ledger compatibility, release workflows,
consumer-version preparation, website publication checks, associated tests and
operator documentation. The committed branch-to-master delta was not reviewed.

## Result

No outstanding concrete correctness or specification findings in the final
inspected delta.

An interim observation was corrected during review. The default selector in
`.github/workflows/release-tag.yml:217-222` selects only Spark 4.1, but the
earlier summary instructed operators to merge a Spark 4.0 PR first. The current
summary at `.github/workflows/release-tag.yml:443-447` branches on
`INCLUDE_SPARK40` and gives the appropriate next step. The corrected YAML and
shell conditions were inspected and checked statically.

## Verification

- 30 focused Python tests passed across default selection, explicit inclusion,
  policy failures, legacy ledgers, evidence-gated release-note output, workflow
  contracts and repeated consumer-version bumps.
- 31 Node installation and R-documentation checks passed, including the
  independently pinned Spark 4.0 metadata and mixed-version publication lock.
- Independently loaded the matrix implementation from HEAD and compared its
  default three-target document with the current explicit three-target
  document. Both retained digest
  `2872f6280e4022bb5c86cf46c0eaae88b1fd4b3c96f231d31ed7a6bdc8f14443`,
  and the current loader accepted the unchanged document.
- A synthetic older two-target ledger with `policy.required=true` resumed
  without an optional-target policy query or build submission. Selected
  Spark 4.0 plans still reject a ledger that waives their required policy.
- Synthetic bootstrap preview requested only the two selected candidates and
  proposed five tags. `INCLUDE_SPARK40=true` did not expand the approved plan;
  explicitly selected Spark 4.0 still encountered its veto.
- In-memory prospective bumps of the specialized Deep Learning, ONNX and R
  guides retained known Spark 4.0 coordinates while updating Spark 4.1.
  Release-note rows likewise followed the approved target selection.

## Candidate update

Rechecked the current workflow and inspected
`scripts/release/test_release_tag_recovery.py`'s
`test_orchestration_summary_names_only_selected_ports`. It executes the actual
Summary step in Bash for both selections and checks that Spark 4.0 appears
only when included. This complements the corrected summary above.

Independently reran the four ledger and notes cases in
`scripts/release/test_release_ops.py`: **4 passed**. They cover accepting an
older two-target ledger with `required=true`, rejecting a selected Spark 4.0
ledger with `required=false`, and evidence-backed notes for both selections.
The notes cases also reject bad approval without creating an output file and
preserve existing output on an exclusive-create failure. No new finding.

Coordinator-reported broader checks, not independently rerun here: 857 release
tests passed with one unchanged opt-in SBT skip, 271 native bump/history tests
passed, and 36 Node website checks passed.

## Limits

No production tags, packages or builds were created. No live service APIs,
commits, pushes or source edits were performed. Only this report was added.
No full website build or Actions run was performed. The local Bash launcher
failed, so workflow shell execution was not independently completed; the
workflow assessment uses source inspection and offline checks.

These local results are not production proof. This report covers round 1 only,
not the complete six-round review or release authorization.
