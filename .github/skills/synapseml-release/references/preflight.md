# Release preflight

Use the recipes in [the command guide](../../../../scripts/release/README.md).
Keep all release-run files outside source checkouts.

## Before any write

1. Identify the existing or new OSS base, selected repositories/targets/families,
   Internal patch, counters, and production or rehearsal destinations.
2. Read existing source tags, package versions, and related PRs. Do not assume
   that an earlier failed pipeline wrote nothing.
3. Generate the plan with `release_matrix.py`. Drafts may describe coordinates
   but cannot authorize tagging or publication.
4. Bind reviewed commits. After any preparation edit or PR merge, regenerate
   the publication plan with the final SHA.
5. Run `release_ops.py preflight --plan FILE --state FILE`. Stop on source,
   policy, destination, dependency, or authentication errors.
6. Run `resume` without `--apply` and inspect the exact pending actions.

The ledger may be created or refreshed locally; these commands queue nothing.
Possessing a digest does not approve it.

Use one authoritative state filename and directory for the approved plan. A
persistent `.release-plan-<plan_id>.json` claim prevents accidentally selecting
a second ledger in that directory. Keep this directory limited to release
records; do not replace its state, delete its claim, or move to another
directory to bypass prior work. Valid older ledgers migrate with their history
preserved. A lock conflict can be inspected with `status --inspect-lock`
without contacting Azure or changing files.

## Full-release policy

Only a new full OSS release uses **Release Prepare**. It includes every
supported target. The workflow checks `SKIP_SPARK40` and the source branches
before creating primary or derivative tags. A legacy skip setting now fails
closed rather than silently omitting a runtime.

Inspect the repository variable before requesting a release:

```bash
gh variable list --repo microsoft/SynapseML --json name,value \
  --jq '.[] | select(.name == "SKIP_SPARK40") | .value'
python scripts/bump-version.py --to <X.Y.Z> --dry-run
```

If the variable cannot be read, its value is unknown. Stop rather than assuming
it is false. Do not use `skip_docs` or a scoped recovery plan to bypass the full
release policy.

## Independent Internal publication

An OSS-only plan may publish before Internal has a release commit. Once that
base is available, initial Internal patch `0` uses `full` with
`repositories=internal`. Later patches use `internal-only`.

Both Internal tracks must reuse a bound, existing OSS dependency without
queueing public publication. The Internal preparation helper also checks its
runtime tuple and pins. For a bare legacy pip pin, require the explicit
verified-base policy; a wrong Python-local suffix is still an error.

Before approving an Internal UPack plan, copy any OSS UPack rebuild counter
from its original OSS plan using `--upack-iteration TARGET=N`. Initial
full-scope Internal plans need this binding when their rollout pairs the two
plans. Counter changes after publication cannot be applied to old receipts.

## Inventory versus release evidence

`verify_release.py --plan FILE --inventory-only` checks exact tags and selected
coordinates without producer approval. A new unpublished plan should report
missing artifacts. A historical `--version` check may expose old release gaps.

For approval, use `verify_release.py --plan FILE --state FILE --json`.
Required skips, missing receipts, all-skipped jobs, mismatched sources/requests,
or stale evidence must leave the release incomplete.

## Rehearsal boundaries

Rehearsal is separate from a no-write preview. Approved rehearsal runs compile
and publish to the explicit nonproduction pip/UPack feeds. It cannot select
Maven, create tags, publish a GitHub Release, or update BBC-VHD.

Named feed selectors are mandatory. Runtime admission resolves their project
and feed IDs and rejects production aliases or identities. A package-name
override does not isolate a destination.

## BBC-VHD preview

Use `bump_bbcvhd.py --repo ROOT --plan FILE --target TARGET`.
Without `--apply`, it previews only. Internal-only plans must retain the
existing OSS version and counter exactly. A conflict requires a corrected,
newly approved plan, not an implicit counter reset.

A no-write preflight cannot simulate human approvals, actual package upload,
image-build success, or deployment to a release train.
