# Release tooling

Use a reviewed JSON plan to prepare, publish, recover, and hand off a release.
The commands below use Bash. The Python CLIs also run on Windows.
Agents should load the [release skill](../../.github/skills/synapseml-release/SKILL.md).

| Command | Purpose |
| --- | --- |
| `release_matrix.py` | Derive tags, versions, destinations, source bindings, and the approval ID |
| `release_ops.py preflight` | Read source, policy, destination, and dependency state without queueing |
| `release_ops.py status` | Reconcile saved build IDs with Azure and current artifact visibility |
| `release_ops.py resume` | Preview work, or queue missing work with explicit plan approval |
| `verify_release.py` | Export current inventory or producer-backed release evidence |
| `bump_bbcvhd.py` | Preview or apply an evidence-backed component update |

## Choose the smallest release track

| Track | Plan selection | What it does not do |
| --- | --- | --- |
| New OSS release | `--scope full --repositories oss` | Does not require an Internal commit or publication |
| Initial Internal release on that OSS base | `--scope full --repositories internal --internal-patch 0` | Does not rebuild or republish OSS |
| Later Internal hotfix | `--scope internal-only --internal-patch N` | Does not change OSS pins, tags, or packages |
| OSS UPack recovery | `--repositories oss --families upack` plus an OSS counter | Does not publish wheels, Maven, or Internal |
| Internal UPack recovery | Internal track plus `--families upack` and an Internal counter | Does not republish its Maven or wheel coordinate |
| Rehearsal | `--mode rehearsal --families pip,upack` plus explicit isolated feeds | Does not publish Maven, create tags, or change BBC-VHD |

A new OSS base often needs two approved plans. Publish its OSS Maven artifacts
first so Internal CI can resolve them. Then prepare and review Internal against
that base. Requiring the Internal merge before publishing the OSS dependency
would create a dependency cycle.

Full GitHub release preparation includes every supported target. It rejects
`SKIP_SPARK40` before creating tags. Scoped recovery uses existing release tags,
not the full-release GitHub workflow.

## 1. Create a plan

Keep plans and state outside source checkouts. Release builds reject dirty
source, including untracked files.

```bash
mkdir -p ../release-run
python scripts/release/release_matrix.py \
  --version 1.1.4 --repositories oss --families maven --json \
  > ../release-run/draft.json
python scripts/bump-version.py --to 1.1.4 --dry-run
```

The first command is a coordinate preview. Its unbound source commits make it
non-executable. The version-bump preview does not edit files or open a PR.
`skip_docs` is not a dry run.

For a full release, run **Release Prepare** on `master`, review the generated
version/docs PR, and merge it only after its required gates pass. The merge
authorizes primary tagging. The derivative workflow opens the port PRs, whose
merges authorize their tags. Do not rebase or force-push shared port branches.
Request `/azp run` only after a maintainer reviews PR execution safety.

After the reviewed source commits are known, bind them:

```bash
python scripts/release/release_matrix.py \
  --version 1.1.4 --repositories oss --families maven \
  --oss-commit "master=$MASTER_SHA,spark4.0=$SPARK40_SHA,spark4.1=$SPARK41_SHA" \
  --json > ../release-run/plan.json
```

Use the actual merged commits, not a feature-branch SHA that a merge may rewrite.
The generator requires lowercase full commit IDs. A plan has `schema_version=1`
and a SHA-256 `plan_id` covering its source commits, scope, targets, families,
destinations, coordinates, and publication flags. Do not hand-edit it.
Any change requires regeneration and approval of the new ID.

## 2. Prepare an independent Internal release

For a hotfix, start from reviewed Internal code and an existing OSS base:

```bash
python scripts/release/release_matrix.py \
  --version 1.1.3 --scope internal-only --internal-patch 1 --targets master \
  --oss-commit "master=$OSS_BASE_SHA" \
  --internal-commit "master=$INTERNAL_SHA" \
  --json > ../release-run/plan.json
```

For initial patch `0` against a newly published base, use
`--scope full --repositories internal --internal-patch 0` instead.

Before approving any Internal plan or creating its publication ledger, bind
the OSS UPack counter it will retain or pair with. Copy the counter from the
original OSS plan using `--upack-iteration TARGET=N`; this applies to full-scope
initial Internal releases as well as hotfixes. Omit it only when the OSS
coordinate has no counter. The [paired rollout example](#5-publish-notes-and-update-bbc-vhd)
shows a full-scope plan with an OSS counter.

Use the companion helper from the Internal checkout:

```bash
python ../SynapseML-Internal/scripts/release/prepare_release.py \
  --plan ../release-run/plan.json --target master \
  --repo ../SynapseML-Internal --oss-repo . --oss-remote upstream \
  --legacy-pip-policy verified-base
```

Replace `upstream` with the local remote that identifies
`microsoft/SynapseML`, not a fork. The explicit legacy policy accepts a bare
`synapseml==X.Y.Z` pin only after verifying the bound historical OSS source and
matching Python, PySpark, Spark, and Scala runtimes. It does not make bare and
Python-local pins interchangeable.

An initial release may need local pin changes through the helper's `--write`
option. Review those changes in an Internal PR, then regenerate the publication
plan with its final merged SHA. Later Internal-only patches preserve OSS pins.
See the Internal repository's `scripts/release/README.md` for preparation rules.

Preview tagging, then apply only after approval of that exact final plan:

```bash
python ../SynapseML-Internal/scripts/release/prepare_release.py tag \
  --plan ../release-run/plan.json --target master \
  --repo ../SynapseML-Internal --oss-repo . --oss-remote upstream \
  --legacy-pip-policy verified-base

# A maintainer supplies REVIEWED_PLAN_ID after reviewing the plan.
python ../SynapseML-Internal/scripts/release/prepare_release.py tag \
  --plan ../release-run/plan.json --target master \
  --repo ../SynapseML-Internal --oss-repo . --oss-remote upstream \
  --legacy-pip-policy verified-base --apply --approve-plan "$REVIEWED_PLAN_ID"
```

The helper verifies clean source and existing refs, pushes the tag family
atomically, and confirms remote refs. It never queues publication.

## 3. Preflight, publish, and resume

Use an authorized Azure CLI login for private source/feed reads and `GH_TOKEN`
when GitHub authentication is needed. Never print credentials or copy them into
plans, state, PRs, or GitHub workflow inputs.

```bash
python scripts/release/release_ops.py preflight \
  --plan ../release-run/plan.json --state ../release-run/state.json
python scripts/release/release_ops.py resume \
  --plan ../release-run/plan.json --state ../release-run/state.json
```

Both commands queue nothing. Preflight checks bindings, release policy,
destinations, and prerequisites. Resume without `--apply` previews pending work.
Artifact absence is expected before a new publication; invalid source or
destination identity is not.

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-run/plan.json --state ../release-run/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID"

python scripts/release/release_ops.py status \
  --plan ../release-run/plan.json --state ../release-run/state.json
```

The driver queues only selected missing work. Maven uses public pipeline
`17563` or Internal pipeline `18453`; official pip/UPack publication uses
`35879`. It preserves the sealed plan and narrows only separate per-run flags.
Unselected Maven work is a dependency blocker when needed, not permission to
expand the plan.

The local ledger is locked, checksummed, and written atomically. It records
submission intent before queueing and the returned build ID afterward.
Pending, failed, or ambiguous submissions are never blindly repeated.
ESRP/SAW approvals remain manual.

Use one authoritative directory and state filename per approved plan. The
driver reserves that filename with `.release-plan-<plan_id>.json` and holds
both `.release-plan-<plan_id>.lock` and `<state filename>.lock` while operating.
A different state filename in the same directory is rejected. This is local
serialization, not cross-machine coordination. Do not change directories,
delete the persistent claim, or replace the ledger to escape recorded work.
Keep the directory limited to the plan and its release records; unrelated or
unreadable files can prevent safe initial ledger discovery.

New ledgers use state schema 2. Valid schema-1 ledgers migrate on a successful
save without losing their existing run IDs or outcomes. New retry history is
never discarded by downgrading a ledger. Back up the ledger and its persistent
claim in trusted storage and restore them to their original paths.

Public PyPI uploads reject existing coordinates, including partial-release
collisions. They do not use `--skip-existing` to turn an unverified wheel into
a successful upload. Preserve the original run evidence or choose a new
release version; do not overwrite the wheel.

If a submission outcome is unknown, inspect Azure first. Adopt an exact matching
run explicitly:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-run/plan.json --state ../release-run/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" \
  --adopt "publisher.internal.master.upack=$KNOWN_BUILD_ID"
```

Use the action ID printed for this plan. Adoption validates the actual run and
does not queue other jobs in that invocation. Do not edit the ledger to erase a
failed or unknown action.

### Retry a failed publisher operation

First run `status`. Set `FAILED_ACTION_ID` to the recorded failed pip or UPack
action, then request one approved retry:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-run/plan.json --state ../release-run/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" --retry "$FAILED_ACTION_ID"
```

The driver requires the same authoritative terminal failed build, terminal
jobs, available dependencies, and a fresh complete package-version lookup
proving every coordinate in that original operation is absent. Deleted
versions, partial publication, missing permissions, or uncertain responses
block retry. A grouped pip/UPack operation retries both families together.
The driver retains the full failed attempt in the ledger and queues no unrelated work.
Status output shows compact attempt summaries with build and operation IDs,
outcomes, retry times, and absence-check times. The complete request and proof
remain in the ledger rather than being repeated in each status report.
It accepts one `--retry` and cannot be combined with `--adopt`.

The lookup follows all package pages, including offset-only pagination.
Its five-minute limit starts at the oldest observation and is checked again
after policy reads and local persistence. Expiry before submission sends no
queue request and leaves the original failed attempt available for a fresh retry.

Same-coordinate Maven retries are intentionally unsupported. A `MISSING`
Maven inventory row does not prove that every JAR, POM, signature, or checksum
is absent. For a failed Maven publication, retain its ledger and use a new
OSS version or Internal patch with a newly approved plan. Do not use a second
ledger or a raw queue command to bypass this limit.

### Recover an interrupted driver

Inspect lock metadata even when ordinary status is blocked:

```bash
python scripts/release/release_ops.py status \
  --plan ../release-run/plan.json --state ../release-run/state.json --inspect-lock
```

This reads bounded local metadata only. It does not contact Azure, acquire or
remove locks, change the ledger, or decide whether an owner is alive.

1. Confirm the original process is gone on the reported host and agree an
   exclusive recovery window with the other release operators. Age or PID
   alone is not proof.
2. Inspect Azure for the plan's recorded or acknowledged build IDs and any
   ambiguous submission. An accepted build ID printed on stderr is not proof
   that publication succeeded.
3. Preserve the records, re-inspect the locks, and remove only the exact dead
   lock files reported by inspection after confirming their metadata has not
   changed. Never remove the ledger or persistent `.json` claim.
4. Run ordinary `status`, then explicitly `--adopt` a matching known run if
   needed. Adoption rechecks its request and source. Do not infer that a
   missing local result means Azure received no submission.

An initialized claim with a missing ledger fails closed. Restore the original
ledger from trusted backup and reconcile Azure; do not manufacture empty state.

Exit `0` means preflight passed, selected work is complete, or a read-only lock
inspection report was returned. Inspection does not report release completion.
Exit `1` means valid but incomplete work. Exit `2` means invalid approval,
plan, state, source, policy, destination, or transport data.

## 4. Export evidence

```bash
python scripts/release/verify_release.py \
  --plan ../release-run/plan.json --state ../release-run/state.json --json \
  > ../release-run/evidence.json
```

Approval evidence requires fresh tag/artifact visibility, matching reviewed
commits, and successful authoritative Azure producer runs with matching
requests and artifact-hash receipts. Publisher `sourceVersion` identifies the
publishing repository, not the released code; its receipt identifies the latter.
The primary public Maven receipt also records the uploaded PyPI wheel's exact
name, version, size, and SHA-256. A Maven-only file list cannot approve it.
Its Maven file records come from the actual ESRP publish directory after the
ESRP step, including present signatures and checksums, not the original Ivy
cache. Missing required modules, POMs, or the Core tests JAR prevent a receipt.

Keep evidence in trusted operator storage and generate it through the driver.
Its JSON records authenticated service reads; it is not a cryptographic
signature on an arbitrary file received from someone else.

An inventory check is useful but cannot approve rollout:

```bash
python scripts/release/verify_release.py \
  --plan ../release-run/plan.json --inventory-only --json
```

Inventory reports always have `complete=false`. `inventory_complete` describes
their selected rows. A skipped required row, missing producer receipt,
unmatched source, or all-skipped run cannot become release approval.
Legacy `--version` checks remain read-only historical diagnostics.

## 5. Publish notes and update BBC-VHD

**Release Notes** requires an approved public-only Maven plan covering every
supported target, plus its exported producer evidence. Do not pass an Internal
or combined plan/evidence document to GitHub.

```bash
python scripts/release/verify_release.py \
  --plan ../release-run/plan.json --state ../release-run/state.json \
  --github-evidence > ../release-run/public-evidence.base64
gh workflow run release-notes.yml --repo microsoft/SynapseML --ref v1.1.4 \
  -f plan_json="$(cat ../release-run/plan.json)" \
  -f evidence_base64="$(cat ../release-run/public-evidence.base64)" \
  -f approve_plan="$REVIEWED_PLAN_ID"
```

This publishes only the primary GitHub Release. It is not an Internal/Fabric
completion signal. Evidence is compressed and size-bounded for GitHub's
workflow input limit; the workflow validates it and repeats live public
inventory checks without receiving Azure credentials.

For BBC-VHD, use the plan containing the selected UPacks and its evidence:

```bash
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD \
  --plan ../release-run/plan.json --target spark4.0
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD \
  --plan ../release-run/plan.json --target spark4.0 \
  --evidence ../release-run/evidence.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID"
```

The first command previews. The second requires complete producer evidence
from the last hour. Internal-only updates preserve the exact existing OSS
UPack pin, including a rebuild suffix; a conflicting plan is rejected.
The updater preserves line endings and rolls back a failed two-file update.
An identical package update is rejected unless `--force-revision` explicitly
requests an image-only rebuild.

For a new base published through separate OSS and Internal UPack plans, pair
their original plans and evidence. Do not create a new combined plan or change
the plan IDs inside existing receipts.

Set the matching OSS counter when generating the Internal plan, before its
approval or publication. For example, if the original OSS plan selects
`1.1.4-spark4-0-1`, generate the initial Internal UPack plan with the same
`--upack-iteration spark4.0=1`:

```bash
mkdir -p ../internal-run
python scripts/release/release_matrix.py --version 1.1.4 \
  --scope full --repositories internal --internal-patch 0 \
  --targets spark4.0 --families upack --upack-iteration spark4.0=1 \
  --oss-commit "spark4.0=$OSS_BASE_SHA" \
  --internal-commit "spark4.0=$INTERNAL_SHA" --json > ../internal-run/plan.json
```

Use the reviewed Spark-specific merge commits for both variables. Both original
plans must now derive `oss_upack_version=1.1.4-spark4-0-1`. Review the Internal
plan ID, then use its own preflight, publication, and evidence steps.
Changing a counter changes the plan ID and requires new approval. If the
Internal UPack was already published with the wrong binding, do not relabel its
receipt. Approve an Internal-UPack recovery plan with the correct OSS counter
and a new `--internal-upack-iteration`, then publish that new immutable
coordinate before pairing the plans.

```bash
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD --target spark4.0 \
  --plan ../internal-run/plan.json --oss-plan ../oss-upack-run/plan.json
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD --target spark4.0 \
  --plan ../internal-run/plan.json --evidence ../internal-run/evidence.json \
  --approve-plan "$REVIEWED_INTERNAL_PLAN_ID" \
  --oss-plan ../oss-upack-run/plan.json \
  --oss-evidence ../oss-upack-run/evidence.json \
  --approve-oss-plan "$REVIEWED_OSS_PLAN_ID" --apply
```

Set both IDs only after their plans are approved. Both plans must use production
mode, full scope, and include UPacks for the selected target. The primary plan
selects only Internal; the companion selects only OSS. Their OSS base, bound
source, runtime, counter, and destination must agree. Each entire original plan
needs fresh complete producer evidence. The paired update changes both pins
and one component revision; an `internal-only` hotfix cannot use it to change
its OSS base.

Review and merge the BBC-VHD PR, complete its CI and White-Glove approval, and
monitor the selected release train using the
[canonical release guide](https://msdata.visualstudio.com/A365/_wiki/wikis/Osmos%20Team%20Wiki/130638/SynapseML-Fabric-Release-Guide-v2).
Package publication alone does not complete rollout.

## Recovery and rehearsal

UPack counters change only UPack coordinates:

```bash
python scripts/release/release_matrix.py --version 1.1.3 \
  --targets spark4.0 --repositories oss --families upack \
  --upack-iteration spark4.0=1 --oss-commit "spark4.0=$OSS_SHA" --json
```

Use `--internal-upack-iteration` independently for Internal rebuilds. Counters
must cover every selected target and have one value per plan. Split targets
when counters differ. Include the existing OSS counter in every
Internal-repository plan that must retain or pair with that OSS UPack,
including `scope=full` initial releases. Bind it before approval and ledger
creation, not at rollout time.

Maven and pip versions cannot be repaired with a UPack counter. If a failed
publication left bad or incomplete immutable artifacts, use a new OSS version
or Internal patch. Existing coordinates, including deleted reserved versions,
are not overwritten. The explicit retry command supports only failed
pip/UPack groups with definitive absence evidence; Maven requires a new
coordinate and plan.

Rehearsal needs named, separate nonproduction pip and UPack feeds:

```bash
python scripts/release/release_matrix.py --version 1.1.3 \
  --scope internal-only --internal-patch 1 --targets master \
  --families pip,upack --mode rehearsal \
  --pip-feed release-rehearsal-pip --upack-feed release-rehearsal-upack \
  --oss-commit "master=$OSS_BASE_SHA" --internal-commit "master=$INTERNAL_SHA" --json
```

Save and review this plan before using the same preflight/resume commands.
Default resume still queues nothing. Approved rehearsal builds and publishes
only to its resolved isolated feeds. It uses existing source tags and cannot
select Maven or roll out BBC-VHD. Feed aliases and IDs are resolved and checked
against production identities; changing a package name is not feed isolation.

## Tests and compatibility

```bash
python -m pytest scripts/release tools/ci/tests/test_pipeline_yaml.py -q
SYNAPSEML_TEST_RELEASE_SBT=1 python -m pytest \
  scripts/release/test_release_version.py -q
```

Use the branch-selected JDK for the SBT regression. The tests use fake
transports and isolated Git repositories, not real publication.
The producer repositories must contain the companion automation changes before
these commands can publish. Legacy plans, old runs without producer receipts,
and inventory-only JSON cannot bypass the new write gates.
