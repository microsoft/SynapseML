# PR 2628, attempt 5, current-head follow-up

Direct bounded review by `gpt-6-astra`, based on
`a38bc5398659f99c0da72c63b9e9063d33b3c114`. This small workflow-step change was
reviewed directly across the six checklist themes, not delegated or presented
as another multi-model gauntlet.

## Finding and resolution

[The current-head Copilot review](https://github.com/microsoft/SynapseML/pull/2628#discussion_r4094613915)
found that preparation checked only the primary tag. An interrupted release
could leave Spark or Python tags while that tag was absent.

The guard now reads the canonical remote primary, Spark and Python tag family.
Any member stops new preparation and directs the operator to reviewed recovery.
It also reads the preparation branch without treating a failed lookup as an
absent branch. No refs are created, deleted or moved.

## Direct review

- Correctness: the lookup uses the whole requested version boundary, so a
  derivative tag is sufficient to refuse preparation.
- Architecture: the change stays in the existing early workflow guard, before
  version edits and PR creation. Publication and recovery protocols are unchanged.
- Failure handling: both remote reads fail closed. Missing refs return an empty
  successful response; transport failure is not converted to absence.
- Detailed logic: quoted patterns admit the exact primary and hyphen-delimited
  Spark/Python family, not a nearby version such as `v1.2.01`.
- Coverage: real local bare-repository cases cover primary, derivative-only,
  annotated, future-family, unrelated-version, existing-branch and unavailable
  remote cases. Every case checks that remote refs remain unchanged. A separate
  shell case checks failure of the second, branch lookup.
- Documentation and safety: the runbook states the recovery boundary, uses no
  private context, and does not suggest deleting an existing preparation branch.

The new family regressions initially had nine failures and four passes.
After the fix, all 91 affected workflow/recovery/documentation tests passed,
including the second-lookup regression. Black 22.3.0 and whitespace checks passed.

## Hosted validation and release hold

Azure build `237445262` used the exact PR merge commit
`1464f57183e3815c5328912eac6f6f8fcdbf155c`. Its Spark 4.1 replay conflict is the
explicitly advisory job. The exploratory unit job failed in Azure CLI task
startup with a TLS hostname/certificate mismatch, before SBT or tests started;
that is not evidence of a product-test failure or a passing test.

The next pushed head requires its own hosted validation. Production `1.2.0`
remains blocked by the separately recorded consumer-wheel distribution issue.
No production tags or packages were created.
