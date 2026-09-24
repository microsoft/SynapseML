# Public release recovery

Published tags and package versions are immutable. A tag mismatch is an error,
not permission to force-push. Bad or incomplete Maven/PyPI publication needs a
new patch version, a new source-bound plan and fresh approval.

## Existing work

Release preparation rejects an existing primary tag or preparation branch.
Tag recovery preserves open release PRs and uses recorded merged commits,
not later branch tips. Notes creation leaves an existing release unchanged.

Read every failed, canceled or skipped required job. A failure before upload
is useful evidence but does not itself authorize a raw queue command. The
driver refuses same-coordinate Maven retries because required-file inventory
cannot establish complete namespace absence.

Run status with the original plan and ledger. For a genuinely ambiguous
submission, inspect the service and explicitly adopt a matching build ID.
Adoption validates request and source and does not queue unrelated work.
Never erase an intent record or ledger to manufacture a fresh attempt.

## Interrupted operation

Use `status --inspect-lock` to read bounded local lock metadata without remote
calls or deletion.

1. Confirm the owner is gone on the reported host and coordinate exclusive
   recovery. Age or a locally absent PID alone is insufficient.
2. Inspect known and ambiguous submissions. A returned build ID is not a
   successful publication receipt.
3. Preserve state and claims. Recheck lock metadata and remove only the exact,
   unchanged locks of a confirmed dead owner.
4. Run normal status and explicitly adopt an exact matching run if needed.

Do not remove the persistent plan claim or replace a missing ledger with empty
state. Restore the original records from trusted backup and reconcile the
service. Locks are directory-local, not global across machines.

## Evidence and approvals

Regenerate expired evidence from authoritative reads rather than editing its
timestamp. Keep credentials and private operator records out of public
artifacts. Human signing approvals and PR merges remain manual.

A source revert does not retract a released package. For a faulty release,
publish a corrected version and communicate the affected coordinates.
Completion requires verified public artifacts and consumer checks on every
target runtime. Private downstream deployment is a separate process.
