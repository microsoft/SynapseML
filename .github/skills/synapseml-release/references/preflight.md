# Public release preflight

Use [the operator guide](../../../../scripts/release/README.md).

Before any public write:

1. Check canonical source refs, existing versions and related release PRs.
   A failed prior run may have published partial immutable artifacts.
2. Inspect all source, generated assets, plans and evidence for public
   suitability. Do not copy private profiles or bindings into GitHub inputs.
3. Generate the public plan. Bind final reviewed commits for every target;
   changes require regeneration and new approval.
4. Run `release_ops.py preflight --plan FILE --state FILE`.
5. Run `resume` without `--apply` and inspect the exact pending operations.

Both commands queue nothing, although they may save local state. Keep the
authoritative plan and ledger outside source. A second filename or copied
directory is not a supported way to escape recorded work.

Full releases include every supported target and reject `SKIP_SPARK40`.
Unknown policy state must stop the release. Do not weaken it to bypass a
failed read or use scoped recovery to omit a required runtime.

The normal workflow requires the primary source on `master`. Before the
automation merges, verify a separately reviewed bootstrap entry point exists.
Do not experiment with canonical release tags to discover workflow behavior.

Inventory can report absent artifacts before publication, but cannot approve
completion. Approval evidence needs fresh, complete producer runs, source
bindings, requests and receipts. No-run previews cannot simulate signing
approval, actual upload or consumer installation.
