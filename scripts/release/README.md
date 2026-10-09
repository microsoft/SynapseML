# Public SynapseML releases

This is the operator and agent runbook. Use the guarded scripts with one
source-bound plan and one durable ledger, not a separate publisher.

**Consumer-wheel gate, before any production tag:** the release owner must
[qualify the same primary candidate wheel on every selected runtime](#qualify-the-primary-wheel-on-each-runtime).
Resolve distribution and approved outputs before tag approval. Port CI with
different wrappers and green producer CI do not establish this human gate.

Default releases select `master` and `spark4.1`: Maven CDN and Maven Central
artifacts, the primary PyPI wheel, and one source-bound DBC per runtime.
For `1.2.0`, these are Maven `1.2.0`/`1.2.0-spark4.1`, `synapseml==1.2.0`,
and `SynapseMLExamplesv1.2.0.dbc`/`SynapseMLExamplesv1.2.0-spark4.1.dbc`.
[Spark 4.0 is opt-in](#explicitly-include-spark-40); default operations require
none of its refs, CI or artifacts. Private integrations are separate.

The commands below use Bash from the repository root. Python CLIs also run
on Windows; use native path separators and shell syntax there.

## 1. Preview and prepare source

Record the version, selected targets, normal or pre-merge procedure, release
owner and signing approver. Read the [branch policy](../../.github/skills/synapseml-branches/SKILL.md)
and each target's runtime versions. Obtain explicit authorization before
creating branches/PRs or dispatching live validation. Permission to edit
automation or rehearse offline does not authorize tags, packages, notes or merges.
Confirm [App setup](#one-time-github-app-setup) and publisher, Databricks and
storage access separately before live preparation. Read the
[public-information rules](#safety-and-public-information).

Choose one protected directory outside the checkout for plans, the ledger,
its persistent claim and operator evidence. Keep one active operator and follow
the [handoff rules](#handoff-at-every-stop) whenever work stops.

Run the credential-free rehearsal with Python, pytest, PyYAML and Git installed:

```bash
python scripts/release/release_dry_run.py --report ../release-rehearsal.json
```

The parent directory must exist and the report path must be new. Require
`status: passed`, nonzero test counts and zero failures/skips. Simulated services
and local Git fixtures never validate live services or authorize publication;
the report says `live_services_validated: false` and `publication_authorized: false`.
Missing tools, timeout or incomplete execution are errors. Retain the source
revision and any uncommitted diff with the result.

```bash
mkdir -p ../release-runs/v1.2.0/oss
python scripts/release/release_matrix.py \
  --version 1.2.0 --repositories oss --families maven \
  --output ../release-runs/v1.2.0/oss/draft.json
python scripts/bump-version.py --to 1.2.0 --dry-run
```

The draft has no approved commit bindings; neither command authorizes publication.
Keep failed/partial outputs and use new filenames rather than overwriting records.
For publication before the automation merges, use the
[bootstrap procedure](#before-the-automation-pr-is-merged) instead of normal preparation.
Otherwise, after authorization:

```bash
gh workflow run release-prepare.yml --repo github.com/microsoft/SynapseML \
  --ref master -f version=1.2.0 -F skip_docs=false
```

This creates a version/docs PR, not a dry run. Preparation refuses existing
primary, Spark or Python tags and an existing preparation branch; failed remote
reads are not proof of absence. Use [recovery](#recovery-and-limits), not deletion.
Review current-head validation and complete versioned docs before merging.
`skip_docs` only defers generation; it does not waive docs. Do not bump the
published-artifact lock during preparation.

Before any tag-creating merge, complete the consumer-wheel gate above and the
[candidate notebook check](#notebook-archive-publication). The normal workflow
requires primary source on `master`, creates derivative tags and opens port PRs.
Review and merge those PRs in order, preserving each target's Spark, Scala,
Python, JDK and staging/pipeline settings. Never rebase or force-push shared ports.
A maintainer merging an exact same-repository port release PR authorizes tagging
its recorded merge SHA, including a reviewed conflict-resolution PR. Fork PRs
do not authorize callbacks. Tags do not authorize package publication.

## 2. Bind the final source

Fetch canonical tags from `microsoft/SynapseML`. Set `MASTER_SHA` and `SPARK41_SHA`
to the peeled commits of `v1.2.0` and `v1.2.0-spark4.1`. Bootstrap instead binds
final reviewed candidate commits before tags exist; do not use feature SHAs
that a later merge may rewrite.

```bash
python scripts/release/release_matrix.py \
  --version 1.2.0 --repositories oss --families maven \
  --oss-commit "master=$MASTER_SHA,spark4.1=$SPARK41_SHA" \
  --output ../release-runs/v1.2.0/oss/plan.json
```

Present the owner with the plan ID, version, targets, exact commits, canonical
tag set, destinations, candidate results and remaining service/signing gates.
A digest is not approval; the agent cannot approve its own request.
Changing source, coordinates or scope requires a new plan and fresh approval.

New public plans use schema 4. Saved schema-2 plans retain their identity and
original scope without DBCs; never hand-edit them to add or drop targets.
Schema-1 plans are local read-only records: regenerate and approve a new plan
and ledger before execution. Relabeling private metadata as OSS does not make
it safe. Private operations need separate plans, ledgers and explicit external
configuration; no private destination is selected by default.

## 3. Preflight and publish

Use existing authorized CLI identities, never pasted credentials. Canonical tags
must exist before publication preflight; bootstrap uses its preview first.

```bash
python scripts/release/release_ops.py preflight \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json
```

Both queue nothing but can save local state. Inspect source, policy, destination
inventory and exact pending operations. Missing new artifacts are expected;
mismatched tags, unknown source or failed service reads block progress.
Only after the maintainer approves the exact plan and requested publication,
and supplies `REVIEWED_PLAN_ID`, run:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" \
  --wait --poll-seconds 60 --timeout-seconds 3600
```

Retain returned build IDs and complete human signing approvals. The driver
records intent before submitting selected missing work against exact source.
Required style, unit, Python and artifact gates stay enabled; selected optional
tests must pass. Do not substitute raw pipeline queue commands.
For monitoring without queueing:

```bash
python scripts/release/release_ops.py status \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json --wait
```

Timeout/interruption does not cancel builds or discard IDs. Restore failed
service access and continue with the original ledger, not a blind retry.

## 4. Evidence and release notes

After all selected targets complete, collect fresh producer and public-byte proof:

```bash
python scripts/release/verify_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json --json \
  > ../release-runs/v1.2.0/oss/evidence.json
```

Before notes or a readiness claim, the release owner must complete
[final-wheel comparison and consumer qualification](#python-distribution-readiness).
Automated evidence does not enforce this human sign-off.
For bootstrap, request the human merge of the unchanged primary candidate
first, before other automation changes; follow its reconciliation rules if blocked.
Notes require the workflow on the default branch and either master ancestry
or canonical merged-PR provenance binding the tagged head and a merge result on
master. Squash/rebase merges are supported; open/fork PRs are not proof.

With explicit authorization for notes, run the read-only integration check and
regenerate public evidence immediately before dispatch. Evidence expires after
one hour; do not edit its timestamps.

```bash
git fetch origin tag v1.2.0
python scripts/release/release_guard.py verify-primary-integration \
  --tag v1.2.0 --commit "$(git rev-parse 'v1.2.0^{commit}')"
python scripts/release/verify_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json \
  --github-evidence > ../release-runs/v1.2.0/oss/public-evidence.base64
gh workflow run release-notes.yml --repo github.com/microsoft/SynapseML --ref v1.2.0 \
  -f plan_json="$(cat ../release-runs/v1.2.0/oss/plan.json)" \
  -f evidence_base64="$(cat ../release-runs/v1.2.0/oss/public-evidence.base64)" \
  -f approve_plan="$REVIEWED_PLAN_ID"
```

Only allowlisted public plans/evidence may enter these inputs. The workflow
rechecks public artifacts and leaves an existing release unchanged. Only a
completed HTTP 404 lookup permits creation; authentication, rate-limit, server
and transport failures stop it without generating notes.

After verified publication and primary versioned-doc integration on master,
land a reviewed `website/test/published-spark-ports.lock` follow-up with verified
port versions. Keep unselected Spark 4.0 at its retained published version.
Do not open it against older master metadata. Until it lands, strict master
Website Deploy fails and the existing public site remains. Confirm actual
deployment before reporting the website updated. Candidate preview uses
`SYNAPSEML_DOCS_PREVIEW=true npm test`; unset means strict production checks.

## Qualify the primary wheel on each runtime

Build from the reviewed primary candidate with `sbt packageSynapseML`.
Before tags exist, set the intended local version explicitly:

```bash
sbt 'set ThisBuild / version := "1.2.0"' packageSynapseML
```

Retain that wheel, its SHA-256, source commit and version. An uncommitted patch
is provisional: qualify the final committed candidate before tagging.
Use the **same wheel file** in environments matching every selected branch's
`environment.yml`, with that runtime's own JVM packages and branch-selected JDK.
Export its normal classpath with the corresponding local version override:

```bash
sbt 'set ThisBuild / version := "1.2.0"' \
  'export opencv / Runtime / fullClasspathAsJars'
```

For ports, use their intended Maven version rather than `1.2.0`; never reuse
primary Scala JARs. Set `SYNAPSEML_CONSUMER_JARS` to the comma-separated core,
OpenCV and dependency JARs from that classpath, omitting JARs supplied by PySpark.
Run from the reviewed primary checkout, without generated sources on `PYTHONPATH`:

```bash
export SYNAPSEML_CONSUMER_WHEEL=/absolute/path/to/synapseml-1.2.0-py2.py3-none-any.whl
export SYNAPSEML_CONSUMER_JAR=/absolute/path/to/matching-synapseml-opencv.jar
export PYSPARK_SUBMIT_ARGS="--jars \"${SYNAPSEML_CONSUMER_JARS}\" pyspark-shell"
python -m pip install --no-deps --force-reinstall "$SYNAPSEML_CONSUMER_WHEEL"
python -m pytest \
  opencv/src/test/python/synapsemltest/opencv/test_image_conversion.py -q
sha256sum "$SYNAPSEML_CONSUMER_WHEEL" "$SYNAPSEML_CONSUMER_JAR"
```

The tests bind installed Python files to the wheel and the loaded JVM class to
the selected JAR, covering image conversion, transformation and save/load.
Retain each runtime's commit, environment, JAR hashes and actual results.
This focused image gate does not replace full candidate CI or service tests.
If a port needs different Python code, resolve distribution and approved outputs
before tagging. Production rebuilds the wheel, requiring the final comparison below.

## Python distribution readiness

Download the exact receipt-bound PyPI wheel and compare it with the retained
qualified candidate. Installable paths and uncompressed payload bytes must match,
as must package name/version, Python requirements, dependencies, compatibility
tags and entry points. Validate each wheel's `RECORD` against its own contents;
compare metadata semantically where serialization order differs.
Retain both archive SHA-256 values and comparison evidence. ZIP timestamps,
ordering and compression can change a hash without changing payload; a different
hash alone proves neither a mismatch nor equivalence.

Verify the published wheel's installation and consumer behavior on every selected
runtime, and retain the release owner's sign-off before notes or readiness.
Missing candidate evidence, payload/metadata mismatch or a failed runtime check
blocks both. Producer CI, preinstalled wrappers and a source checkout are not
substitutes. Investigate with the owner; an incorrect immutable release needs
a new version, qualification and approval, not an overwritten package.

## One-time GitHub App setup

An organization owner must approve and install the App on `microsoft/SynapseML`
with **Contents: read** and **Pull requests: write**. Preparation and tag workflows
use it to open PRs, not approve/merge them or publish packages. Branch/tag writes
and validation dispatches use `GITHUB_TOKEN`. This does not bypass protection or
organization restrictions on `GITHUB_TOKEN` PR creation.

Configure Actions variable `RELEASE_APP_CLIENT_ID` and secret
`RELEASE_APP_PRIVATE_KEY` through approved secret management. Check presence,
never print values. Unreadable settings mean unknown, not absent or configured.
Missing settings block App preparation, not offline rehearsal. The pinned
`actions/create-github-app-token` action creates a repository-scoped short-lived
token and revokes it when the job ends; no personal-token fallback is allowed.
An installed App grants no publisher, Databricks or storage access, and merging
automation configures none of these external prerequisites.

## Notebook archive publication

Before release, authorize the **SynapseML Build** service connection to create
and delete its temporary folders in the configured Databricks workspace.
Grant **Storage Blob Data Contributor** on the `mmlspark` account's `dbcs`
container. Prefer workload identity federation. Jobs use the existing login,
not storage keys or automatic role assignment; missing access blocks publication.

Before merging preparation and each port PR, check out its head with canonical
`origin` and run this read-only check, adding `--include-spark40 true` if selected:

```bash
python scripts/release/release_guard.py full-release --version 1.2.0 --repo .
```

It reads remote refs and committed notebooks. Preparation and `push-tags`
also enforce admissibility before tags. Notebook-only PRs run credential-free
Release Notebook Validation; Markdown-only PRs do not. These checks and native
Databricks round-trips do not execute notebook code.

For schema 4, `Publish` builds each selected source's archive, strips saved
outputs/metadata, validates paths and preserves cell content through native
export/reimport. Unsupported content fails rather than dropping examples.
It retains the archive and `dbc-provenance.json` before Maven upload.
`Release` downloads that exact producer-attempt artifact, validates its binding
and uploads without overwrite before PyPI/ESRP. Retries reuse it, not new ZIP
bytes; different job-attempt numbers do not justify rebuilding.
Verification binds public source/hash/size to the producer receipt. Missing or
mismatched archives block completion and notes. Schema-2 plans authorize no DBCs.

If upload fails with no public archive, check blob write access and retry only
the failed `Release` job: Maven Blob is already published, PyPI/ESRP have not run.
Do not rerun the successful Maven publisher. Conflicting public bytes require
investigation, never overwrite. Preserve artifacts and the ledger after later
failures. Same-plan/source archive reuse requires another content round-trip
when building; an incomplete release's archive must remain unadvertised.

## Publication and evidence contract

Release mode publishes the selected Maven artifacts, primary PyPI and schema-4
DBCs. It excludes R packages, module wheels, badges and ordinary snapshot
notebook uploads; snapshot CI is unchanged.
Primary `Publish` installs graphviz/doxygen after `packagePython`, then runs
`sbt -DskipCodegen=true publishDocs` and `release_guard.py api-docs` before
`publishBlob`. The guard revalidates plan/source/target and checks bounded,
version-specific public Python/Scala entry points; see [the guard](release_guard.py).
Ports skip this step. API docs are mutable, not an immutable receipt or separate lock.

Evidence needs fresh tags, authoritative successful producer runs, matching
requests/commits and destination-specific artifact hashes/sizes. CDN Maven and
Maven Central are checked separately; their bytes need not match each other.
Each module needs its JAR/POM and Core its tests JAR. Preserve the exact
`release-maven-blob-<jobAttempt>/blob-provenance.json` handoff from `Publish`.
Receipt schema 2 stores CDN bytes in `blob_artifacts` and ESRP/PyPI/DBC bytes
in `artifacts`; receipt schemas do not change approved plan schemas.
The exact non-yanked primary wheel must remain publicly downloadable.

Inventory-only reports, skipped required jobs, stale evidence and old receipts
without CDN hashes cannot approve completion. Preserve rejected records and
use recovery; never invent provenance or republish to satisfy a missing receipt.
Public evidence is limited to 60,000 encoded characters within GitHub's 65,535
combined-input budget. Keep generated or compact plan JSON. On Windows, decode
with `verify_release.decode_evidence` into an external JSON file and use
`release_guard.py notes --evidence <file>`; the workflow's encoded input runs on Linux.
For historical public inventory only:
`python scripts/release/verify_release.py --version 1.1.4 --skip ado,internal`.
It needs no private profile or historical DBCs and grants no approval.

## Explicitly include Spark 4.0

Before preparing opt-in docs, confirm canonical `SKIP_SPARK40` is absent/false
and source/consumer prerequisites are achievable. Unreadable policy is not
absence. Resolve vetoes/runtime blockers before promising new artifacts.
The veto is checked again when selected; it does not block the default pair.

Prepare with `skip_docs=true` or bump locally with `--skip-docs`. Then update
Spark 4.0 coordinates, notebook tags and Python examples in `README.md`,
`docs/Get Started/Install SynapseML.md`,
`docs/Explore Algorithms/Deep Learning/Getting Started.md`,
`docs/Explore Algorithms/Deep Learning/ONNX.md`, `docs/Reference/R Setup.md`
and `spark40Version` in `website/src/installArtifacts.js`. Find additional references:

```bash
git grep -n -E 'spark4[.]0|pyspark>=4[.]0|spark40Version' \
  -- README.md docs website/src/installArtifacts.js
sbt convertNotebooks
(cd website && npm exec -- docusaurus docs:version 1.2.0)
python scripts/bump-version.py --finalize-docs --to 1.2.0
(cd website && SYNAPSEML_DOCS_PREVIEW=true npm test && npm run build)
```

Run generation in the branch's configured build environment. Do not repeat
the version bump or change old snapshots. Review and commit the new source/docs
before candidate approval. Default bumps retain Spark 4.0's last published pins;
its lock advances only after artifact verification.

Normal preparation selects the default pair. After preparing reviewed optional
source and finishing any running default workflow, dispatch against the same tag:

```bash
gh workflow run release-tag.yml --repo github.com/microsoft/SynapseML \
  --ref v1.2.0 -F include_spark40=true
```

Spark 4.0 is processed first. New PRs form `master -> spark4.0 -> spark4.1`
only if neither port PR exists. Existing Spark 4.1 PRs/results are preserved,
not restacked. Every dispatch intended to check/repair Spark 4.0 tags needs
`include_spark40=true`; a default rerun does not recover them.
Generate the plan with `--targets master,spark4.0,spark4.1` and all three final
commit bindings. Bootstrap uses that plan alone, not the workflow opt-in input.
Saved three-target identities stay unchanged. Serialized `base_branch` is
historical lineage, not another required target or ref.

To drop Spark 4.0 **before tags or submissions exist**, restore its source
guides and metadata to the verified retained version in the publication lock.
Correct only the new unpublished snapshot, preserve new master/Spark 4.1 pins,
rerun website checks, generate a two-target plan and obtain new approval.
Retain abandoned records. If any tag/submission exists, stop and reconcile
the original records through recovery; never edit tagged candidates, drop
targets silently or move tags. A separately reviewed doc correction may restore
retained pins after unchanged primary integration. Until reconciled, keep
deployment blocked rather than weakening the lock.

## Before the automation PR is merged

Normal tagging requires primary source on master; new preparation/notes
workflows must exist on the default branch to dispatch. Do not disable guards,
experiment with production tags or treat a fork workflow as production proof.
Use only the reviewed bootstrap mode of the already-registered tag workflow.

Before bootstrap, read master's classic protection and active rulesets.
The unchanged candidate must remain mergeable when master advances. Missing
classic protection does not imply no rulesets. Unreadable rules or requirements
to update the candidate head block tagging; resolve the procedure with the
maintainer, not by changing protection.

Prepare same-repository `release-candidate/v1.2.0-master` and
`release-candidate/v1.2.0-spark4.1` branches, plus
`release-candidate/v1.2.0-spark4.0` only if selected. Each needs automation and
version/docs changes on its own current target baseline, with an open PR to that
target. Require successful current-head Azure and Compile & Style Check;
primary also requires Website Deploy to validate snapshots/sidebar references.
PR/non-master website builds preview without advancing the publication lock
and cannot deploy Pages. Complete the consumer-wheel gate before tag approval.

Generate the source-bound plan as in step 2 with final candidate SHAs. From a
clean primary-candidate checkout whose `origin` is canonical, preview locally:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json
```

With authorization to dispatch validation, preview on GitHub without tags:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json --dispatch-preview
```

Inspect the completed preview, then obtain exact-plan authorization for tagging.
After the maintainer supplies `REVIEWED_PLAN_ID`:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --approve-plan "$REVIEWED_PLAN_ID" --dispatch
```

Manual dispatch defaults to preview; tag creation requires `bootstrap_apply=true`.
The dispatcher generates public inputs after local validation; never pass raw
private files. Verify the completed run and canonical refs, not just queueing.
The job atomically pushes missing family tags, preserves matching tags and
refuses conflicts. It merges no PRs, starts no port-PR chain and queues no packages.
`GITHUB_TOKEN` tag writes do not trigger recursive push workflows.
Return to step 3 with the same approved plan after canonical tags exist.
Required tests rerun on tagged source before uploads.

Freeze tagged candidates: never use **Update branch**, add commits, amend,
rebase or force-push. Ordinary PR-loop rebase/not-behind gates no longer apply.
After publication, merge the unchanged primary candidate first, promptly, before
other automation changes. Resolve conflicts through a separate reconciliation
PR on master that makes the unchanged candidate mergeable. Reconcile any remaining
automation PR afterward; inclusion in a candidate does not prove it merged.
Until integration is proved and notes workflow registration completes, keep
notes unpublished and the ledger intact. Escalate to the owner; do not move
tags or republish coordinates to make notes pass.

## Version and launcher maintenance

Bumps preserve release protocol code, fixtures and older versioned docs.
Only the new snapshot loses the moving master command; source keeps it.
After a generation failure, follow the stage-specific recovery output.
For an existing new snapshot, repair the problem and finalize without repeating
the bump or `docs:version`:

```bash
python scripts/bump-version.py --finalize-docs --to 1.2.0 --dry-run
python scripts/bump-version.py --finalize-docs --to 1.2.0
```

Recovery needs full history, the current source version and a snapshot not
committed in this candidate's ancestry. Sibling-runtime snapshots do not count.
Use `git fetch --unshallow origin` for a shallow canonical clone. Dry run writes
nothing; finalization cannot recreate missing conversion/Docusaurus inputs.
When changing `project/build.properties`, review launcher bytes and update
`project/sbt-launch.sha256` together. Missing/mismatched checksums stop bootstrap.

## Safety and public information

- Use only OSS/Maven allowlisted public plans and evidence in public workflows.
  Encoding or compressing a document does not redact it.
- Keep credentials, private bindings/destinations, profiles, raw ledgers and
  operator records out of source, inputs, comments, logs and release attachments.
  Keep state outside clean source checkouts.
- Never move published tags, overwrite artifacts, discard ambiguous submissions
  or change a plan under existing approval. Keep signing and PR merges manual;
  do not change permissions or bypass branch protection.

## Handoff at every stop

Record locally the source revision/diff, plan ID, exact paths and host, last
completed checkpoint, unchanged candidate SHAs, build/run IDs, test/artifact
evidence, outstanding approvals and next permitted command. Retain the candidate
wheel, source/version bindings, final payload/metadata comparison and owner sign-off.
Distinguish offline rehearsal, live preflight, candidate qualification and
published-artifact verification; the first three are not release completion.

On resume, reread the handoff and authoritative ledger, check for another operator,
refresh service state and recheck approval scope before acting.
Conversation history alone is not the release ledger.

## Recovery and limits

Keep one directory and state filename per approved plan; `.release-plan-<plan_id>.json`
claims that filename. Back up ledger and claim in trusted storage.
Locks are local, not global across copied directories or machines.
Inspect failed/canceled/skipped required jobs before choosing recovery.
For an ambiguous submission, inspect Azure and obtain explicit approval to adopt
an exact matching run; the agent cannot self-adopt:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" \
  --adopt "maven.oss.master=$KNOWN_BUILD_ID"
```

Adoption checks source/request identity and queues no unrelated work.
Never delete intent or ledger records to manufacture a new attempt.
Same-coordinate Maven retries are unsupported. Missing required files do not
prove absence of signatures, checksums or optional JARs. Partial/bad publication
needs a new patch version and approval; a source revert cannot retract packages.
Communicate the affected coordinates.

For a port merge event without `merge_commit_sha`, fetch the canonical target,
independently verify the reviewed merge commit and check it out detached.
Do not substitute the current branch tip. Run
`release_guard.py full-release --repo . --version <version>`, adding
`--include-spark40 true` if selected. Create only missing local tags with
`git tag <tag> <sha>`, confirming existing tags match, then run
`release_guard.py push-tags --repo . --commit <sha>` with both
`--tag v<version>-<target>` and `--tag v<version>-python<python-version>`.
The guard rechecks notebooks and atomically pushes, refusing conflicts.

Use `status --inspect-lock` for bounded metadata without remote reads/deletion.
Confirm the owner is gone on the reported host; age or a missing local PID alone
is insufficient. Coordinate exclusive recovery, inspect known/ambiguous
submissions, preserve records and recheck locks before removing only exact,
unchanged dead-owner locks. Never remove the persistent claim or replace a
missing ledger with empty state; restore trusted backups and reconcile Azure.

Exit `0` means that operation succeeded, not necessarily release completion;
`1` means incomplete work; `2` means invalid approval/source/state/policy or
transport data. Inspect the result rather than blindly retrying.

### Recover a warning-only Azure release build

Cache/Codecov outages can leave a fully published public release
`partiallySucceeded`. With the original plan/ledger, run `release_ops.py status`
or collect verified evidence, never rerun immutable publishers merely to get green.
The driver accepts warnings only with complete task-level proof of allowlisted
cache/Codecov identities, retry counters and execution windows, plus successful
publication/provenance tasks. Missing, skipped, canceled or unexplained failures
remain blockers. Existing source, hash, DBC and freshness checks still apply.
Raw Azure outcomes and sanitized task records stay in the ledger; exported
evidence retains outcomes and a digest binding the complete records.
This can reconcile an older failed classification without queueing a new build.
It does not authorize edited ledgers or forged success. Private builds still
require strict success. If proof fails, inspect the original build and recover.

## Validation

```bash
python -m pytest scripts/release tools/ci/tests/test_pipeline_yaml.py -q
SYNAPSEML_TEST_RELEASE_SBT=1 python -m pytest \
  scripts/release/test_release_version.py -q
```

Select the branch's JDK for SBT. Local tests and no-run previews do not prove
live permissions, signing or publication. Report completed coordinates/source
commits and remaining gates, not skipped tests as proof.
