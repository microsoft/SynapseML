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
