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
Pass the same counters to `verify_release.py`; omitting them checks the
original UPack versions, which usually remain present after a rebuild.

- Retry the same Maven or pip coordinate only when the failed attempt created
  no immutable artifact and the release service permits a retry.
- If an OSS Maven or pip version already exists with bad or incomplete content,
  cut a new OSS patch version.
- If an Internal Maven or pip version already exists with bad or incomplete
  content, use a new Internal patch.
- Regenerate the complete matrix after changing either version. Never edit the
  coordinate in a queue command.

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

Never use a green pipeline as proof of publication. Publish tasks may continue
after an error. `verify_release.py` is the package gate.

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
