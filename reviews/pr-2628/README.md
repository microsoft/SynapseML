# PR 2628 review evidence

Public review evidence for
[the release automation PR](https://github.com/microsoft/SynapseML/pull/2628)
is limited to public behavior, findings, source commits and validation results.
Private integration procedures and operator records are not release artifacts.

The prior reports were retained in private operator storage before the
confidentiality cleanup. They are not reproduced here because some contained
private operational context. Removing them from this tree does not retract
previously hosted commits or cached copies.

The last pre-cleanup head was `a617374f54c7629da3216112ce8c1196bd735224`.
Its PR validation completed with 65 successful jobs and one explicitly advisory
Spark 4.1 replay failure. That run included successful snapshot publication;
it did not publish production `1.2.0`.

The confidentiality audit found no confirmed live credentials or conclusively
copied proprietary source. It did find private operational metadata in public
plans and documentation. The cleanup must be reviewed and validated separately;
the earlier green checks do not validate the changed source.

The clean feature history beginning at `a38bc5398659f99c0da72c63b9e9063d33b3c114`
was separately reviewed. Its source/archive audit excluded all 28 old feature
commits and found no new confidentiality blocker. Review artifacts record
current-head follow-ups and validation limits. Actual package contents and
cross-runtime Python distribution remain release gates, not completed checks.

## Default-target follow-up

The `task-2628-default-targets-attempt-1-review-*` reports cover the follow-up
to `2391aa166ab46238307de9edf9650102cbff950e`. It defaults new releases to
`master` and `spark4.1`, keeps Spark 4.0 explicitly selectable, and preserves
saved three-target plan identities. Consumer guidance retains the optional
runtime's last published artifacts instead of advancing it automatically.

The reports distinguish independent reviews from direct fallback reviews.
The Gemini provider rejected the requested review before returning findings;
no three-family coverage is claimed. Local checks and previous-head CI are
not current-head hosted validation or production approval. The Python-wheel
distribution gate remains unresolved, and no production tags or packages
were created for this follow-up.

## Release lookup follow-up

The `task-2628-release-lookup-attempt-1-review-*` reports cover the confirmed
release lookup error-handling finding. The small fix received six themed
coordinator passes, not independent multi-model coverage. Its workflow
regression demonstrates the original failure before checking the correction.

## Automatic notebook archive publication

The `task-2628-attempt-6-review-*` reports cover schema-4 public DBC publication.
New plans require one archive per selected runtime, built from approved Git
source and validated through native Databricks export and reimport. Uploads
cannot overwrite, and release completion and notes require matching public
bytes and producer evidence. Existing schema-2 approvals retain their exact
identity and publication scope.

The four available independent review rounds found 13 issues, all resolved
with regression coverage and recorded resolution notes. Gemini rounds 2 and 5
were skipped after dispatch failures, as the maintainer requested. There is
no claim of six completed independent rounds or three-family coverage.
Absolute local artifact paths were replaced with repository-relative paths
before publication; findings and resolution history are preserved.

The final rebase target is `bccec7e73d102bea655b527246e5575e81c983c9`;
the prior PR tip became `47e945b22402eb117583a28057738b9c45bb4432`.
`git range-diff` shows all four original PR commits unchanged by this rebase.
The new upstream changes affect only LightGBM ranker code, not notebooks.

Validation:

- 954 Linux release-tooling tests passed. One live SBT check was intentionally
  skipped. The one deselected current-checkout notebook check passed separately
  using native Windows Git because WSL Git cannot resolve Windows worktree
  pointers.
- 12 website installation tests passed, and Black 22.3.0 checked the changed
  Python. The earlier pipeline-only run passed 124 checks; the final release
  suite also checks the new pipeline ordering and approval gates.
- Real native Databricks export, reimport and content comparison passed for
  all 56 notebooks and 1,108 nonempty commands from source commit
  `ae45761b61ccffe06f4516b97a24042665212cd1`. Its notebook tree is unchanged by
  the final rebase. The synthetic `0.0.0` archive is 304,258 bytes, SHA-256
  `83b5d57d50b19881b6ef972fcc407974ec7d9a2aa18cf0c9beae448d8d642903`.

The native check did not execute notebook code, upload a new public blob,
or create production tags. Current-head hosted CI, human approval and the
existing cross-runtime consumer-wheel gate remain separate requirements.
