# Release recovery and rollout

Read this before retrying a failed release step.

## Recovery rules

### Tags

Published tags do not move. The GitHub workflows accept an existing tag only
when it already points to the expected commit. A mismatch is a release error,
not a reason to force-push the tag.

### Immutable packages

UPack versions are immutable. Generate their rebuild suffixes with
`--upack-iteration` and `--internal-upack-iteration`; do not edit a queue
command by hand. OSS and Internal UPacks use separate counters. Split targets
into separate plans when one pipeline run cannot represent their counters.

The UPack counters do not change Maven or pip versions.
Keep counters in the sealed plan consumed by the driver and verifier.
Re-entering or omitting a counter can otherwise select an older package that
still exists.

An initial `scope=full, repositories=internal` plan that will be paired with
an OSS plan must bind the same existing OSS UPack counter before approval and
publication. Use `--upack-iteration TARGET=N` when generating it. If that
binding was wrong on an already published Internal UPack, create an approved
UPack-only recovery plan with the correct OSS counter and a new Internal
UPack counter. Keep the old receipts unchanged; they cannot authorize the new
plan.

- Explicit same-coordinate retry supports pip/UPack only, after complete
  fresh absence evidence. Maven inventory cannot prove a whole namespace is
  empty, so the driver refuses Maven retries even when every row is `MISSING`.
- If an OSS Maven or pip version already exists with bad or incomplete content,
  cut a new OSS patch version.
- If an Internal Maven or pip version already exists with bad or incomplete
  content, use a new Internal patch.
- Regenerate and approve a new plan after changing a version or counter. Never
  edit a coordinate, runtime flag, or state record by hand.

### Existing release work

- **Release Prepare** rejects an existing primary tag or preparation branch.
- If a `skip_docs` preparation PR merged, the finalize job cannot tag it.
  Add the complete versioned-docs snapshot through a normal reviewed PR. After
  it merges, verify the version and snapshot on its exact `master` commit. With
  explicit human approval, create the primary tag on that commit; the tag push
  should start the derivative workflow. Confirm the **Release Tag Orchestrator**
  run started. If it did not, dispatch `release-tag.yml` with the primary tag
  selected as the ref, as **Release Prepare** does. Record why this manual
  recovery was needed. Do not rerun **Release Prepare** for a version already
  present on `master`.
- The derivative workflow leaves an open release PR untouched.
- A merged Spark release PR is reusable only while its recorded merge commit
  remains on the target branch.
- **Release Notes** leaves an existing GitHub Release untouched.
- `bump_bbcvhd.py` rejects an identical package update unless an intentional
  rebuild counter or component-only revision is supplied.

Inspect the existing state before deciding whether to resume, repair, or create
a new release version.

### Failed builds

Read every failed, canceled, and skipped job. Retry only when the log proves the
failure happened before the release behavior ran or came from unrelated
infrastructure. Fix product, script, tag, or package errors before rerunning.

The hardened publisher fails upload errors. Still require authoritative
successful producer jobs, matching source/request identities, producer hash
receipts, and current artifact visibility. Old runs without receipts and
success-shaped summaries cannot approve a new rollout.

Use `release_ops.py status` before deciding to retry. A saved pending, failed,
or unknown operation is not automatically requeued. For an ambiguous submission,
inspect Azure and explicitly adopt a matching build ID. Never delete the ledger
to manufacture a fresh attempt against an immutable coordinate.

For a recorded failed pip/UPack action, use
`resume --retry ACTION_ID --apply --approve-plan PLAN_ID` with the original
`--plan` and `--state`. One member selects the whole original per-target,
per-repository group. The driver requires a matching terminal failed build,
terminal jobs, available dependencies, and complete fresh absence for every
selected coordinate, including deleted versions. Partial publication and
query uncertainty block retry. It preserves all previous attempts and queues
no unrelated work. Do not combine `--retry` with `--adopt`.

For failed Maven work, use a new OSS version or Internal patch and a newly
approved plan. The lack of whole-namespace absence evidence is not permission
to erase its history or issue a raw pipeline queue command.

`verify_release.py --plan FILE --state FILE --json` exports approval evidence.
Evidence expires after one hour for BBC-VHD writes. Regenerate it through live
reads rather than changing its timestamp.

### Interrupted driver run

Use the original authoritative directory and state filename. The persistent
`.release-plan-<plan_id>.json` claim binds that filename; a new name in the
same directory is rejected. This is directory-local protection, not a global
lock across independent machines or copied directories.

Run `release_ops.py status --plan FILE --state FILE --inspect-lock`. It
reports the exact state-lock and plan-lock paths plus bounded safe metadata.
It does not acquire locks, expose their private ownership values, modify
files, contact Azure, or decide whether a process is alive.

1. Confirm the original process is gone on its reported host and coordinate
   exclusive recovery with other operators. Neither an old timestamp nor a
   missing local PID proves that a remote owner is dead.
2. Inspect Azure for the approved plan and its known or ambiguous submissions.
   Keep any build ID acknowledged on stderr before a failed local save.
3. Preserve the records and re-inspect the locks. Only after confirming their
   metadata is unchanged, remove the exact dead lock files. Never remove the
   state or persistent claim to restart the plan.
4. Run ordinary status and explicitly adopt an exact matching run when needed.
   An acknowledged build ID is not a success receipt; adoption still validates
   the authoritative source and request.

Back up the state and persistent claim in trusted storage. New ledgers use
schema 2; valid schema-1 ledgers migrate without dropping run history. An
initialized claim whose ledger disappeared requires restoring the original
ledger and reconciling Azure, not creating an empty replacement.

## Credentials and approvals

- Keep ADO, ESRP, SAW, Internal, and BBC-VHD credentials out of GitHub.
- Use only approved service connections and release workstations.
- Do not automate approval clicks, pull-request merges, White-Glove sign-off,
  or release-train selection.
- Record build IDs, approved commits, tags, artifact identifiers, and skips in
  the release evidence.

## Rollout source

The detailed human process lives in the
[SynapseML Fabric Release Guide v2](https://msdata.visualstudio.com/A365/_wiki/wikis/Osmos%20Team%20Wiki/130638/SynapseML-Fabric-Release-Guide-v2).
Use its BBC-VHD, White-Glove, release-train, and deployment-monitoring steps.

The wiki page
`Engineering-Knowledgebase/Synapse-ML-(OSS-Library)/Release-Guide/Step-6-Monitor-Deployment`
contains the train dashboard, official schedule, Fabric notebook check, and
announcement steps.

## Rollout completion

Do not close the release when packages publish. Close it after:

1. BBC-VHD CI and White-Glove approval complete;
2. the chosen train deploys;
3. a Fabric notebook reports the expected package version;
4. smoke imports succeed for the released modules;
5. the release announcement is sent; and
6. work items and release boards record the deployed version.
