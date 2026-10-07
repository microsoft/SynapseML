# Public SynapseML releases

Prepare, approve, publish and recover public SynapseML releases with one
source-bound plan and one durable ledger. Downstream private integrations are
not prerequisites and their deployment procedures do not belong in this guide.

**Consumer-wheel gate, before any production tag:** validate the planned Python
wheel packaging and consumer behavior from the candidate sources on every
advertised runtime. Port CI using different wrappers does not prove the primary
wheel works there. Resolve the distribution strategy and approved outputs before
requesting approval or running any tag-creating operation below.

The default public release contains Maven CDN and Maven Central artifacts for
`master` and `spark4.1`, plus the primary public PyPI wheel. For `1.2.0`, the
Maven versions are `1.2.0` and `1.2.0-spark4.1`; PyPI receives
`synapseml==1.2.0`. New schema-4 plans also require the public notebook archives
`SynapseMLExamplesv1.2.0.dbc` and `SynapseMLExamplesv1.2.0-spark4.1.dbc`
in `https://mmlspark.blob.core.windows.net/dbcs`.

Spark 4.0 remains available as an explicit opt-in, not a prerequisite.
The default plan, bootstrap, builds, evidence and notes do not require its
branch, candidate PR, CI, tags or packages. Removing it does not resolve a
Spark 4.1 Python-wheel compatibility gap; the consumer-wheel gate still applies.

Merging the automation does not publish a release. Workflow dispatch, reviewed
source, exact-plan approval and signing approvals are separate steps.

## Qualify the primary wheel on each runtime

Build the aggregate wheel from the reviewed primary candidate with
`sbt packageSynapseML`. Before tags exist, use an explicit local SBT version
override for the intended version, for example
`sbt 'set ThisBuild / version := "1.2.0"' packageSynapseML`.
This packages locally; it does not grant publication permission. Record the
candidate commit, any pending patch and the wheel's SHA-256.

Use that **same wheel file**, not a port-generated wheel, in separate
environments matching each selected branch's `environment.yml`. Package and list
the matching candidate's normal JVM artifacts with
`sbt 'export opencv / Runtime / fullClasspathAsJars'`, using that branch's JDK
and the corresponding local version override. A fat assembly is unnecessary.
Do not reuse primary Scala JARs on a port runtime.

For each environment, set absolute paths to the primary wheel and that
runtime's OpenCV JAR. Set `SYNAPSEML_CONSUMER_JARS` to the comma-separated core,
OpenCV and additional dependency JARs from the exported classpath. Omit JARs
already supplied by that environment's PySpark installation, rather than
adding Spark itself again. Then run:

```bash
export SYNAPSEML_CONSUMER_WHEEL=/absolute/path/to/synapseml-1.2.0-py2.py3-none-any.whl
export SYNAPSEML_CONSUMER_JAR=/absolute/path/to/matching-synapseml-opencv.jar
export PYSPARK_SUBMIT_ARGS="--jars \"${SYNAPSEML_CONSUMER_JARS}\" pyspark-shell"
python -m pip install --no-deps --force-reinstall "$SYNAPSEML_CONSUMER_WHEEL"
python -m pytest \
  opencv/src/test/python/synapsemltest/opencv/test_image_conversion.py -q
sha256sum "$SYNAPSEML_CONSUMER_WHEEL" "$SYNAPSEML_CONSUMER_JAR"
```

Run from the reviewed primary checkout, with no generated-source directory on
`PYTHONPATH`. The artifact bindings check the installed Python files against
the wheel and the loaded JVM class against the selected JAR. The tests cover
bytes, bytearrays, array inputs, grayscale/RGB conversion, Spark `BinaryType`
rows, JVM image transformation and save/load. Keep both runtime results and
hashes with the release record. This is a focused image-compatibility gate,
not a replacement for the selected candidates' full CI or service tests.

## One-time GitHub App setup

The preparation and tag workflows open PRs with an approved GitHub App, not
`GITHUB_TOKEN`. This works when organization policy disables PR creation by
`GITHUB_TOKEN`; it does not change that policy or bypass branch protection.
An organization owner must approve and install the App on `microsoft/SynapseML`.
Give it repository **Contents: read** and **Pull requests: write** permissions.
GitHub supplies metadata read access automatically. No Actions or administration
permission is requested. These workflows do not use the App to approve or merge
PRs, sign artifacts or publish packages.

Configure these repository settings through the approved secret-management
process:

- Actions variable `RELEASE_APP_CLIENT_ID`: the installed App's client ID.
- Actions secret `RELEASE_APP_PRIVATE_KEY`: its PEM private key.

Do not paste the key into a workflow input, PR, log or command argument. Do not
reuse a personal token as an implicit fallback. Both workflows stop with a
configuration error when either setting is missing. The pinned official
`actions/create-github-app-token` action requests a short-lived token scoped
only to this repository and those permissions, and revokes it when the job
ends. Preparation mints it after docs generation but before pushing the branch;
the tag workflow mints it before creating derivative tags.

Branch/tag writes and explicit validation dispatches still use `GITHUB_TOKEN`.
The App token only reads and opens PRs. Review, current-head Azure validation,
plan approval and signing remain human gates. App installation and credentials
are external prerequisites; merging this code does not configure them.

## Notebook archive publication

New public plans use schema 4. Each selected runtime's existing Azure release
build produces a DBC from its exact approved source commit, strips saved
outputs and execution metadata, and uses the Databricks Workspace API to
export and reimport every notebook. Cell contents must survive the round-trip.
Unsupported notebook formats or languages fail the build rather than silently
dropping examples. This archive check does not execute the examples; the
runtime's normal CI and service prerequisites still apply.
Archive validation checks both notebook files and directory entries. Paths
outside the versioned archive root, traversal components and backslash
separators are rejected before Databricks import or publication.

Before a release, authorize the **SynapseML Build** service connection identity
to create and delete its own temporary folders in the configured build
Databricks workspace. Grant it **Storage Blob Data Contributor** scoped to
the `mmlspark` storage account's `dbcs` container. Azure CLI uses the service
connection's login; the job neither reads storage keys nor assigns roles.
Use workload identity federation for the service connection where supported.
Missing access blocks publication.

The `Publish` job validates the archive and retains it with
`dbc-provenance.json` before the first Maven upload. A native archive validation
failure therefore stops package publication. The `Release` job downloads that
exact producer-attempt artifact, checks its plan/source binding, and uploads
and verifies it before PyPI or ESRP publication. A missing or invalid artifact
handoff fails rather than downloading an unselected artifact or rebuilding.
Uploads forbid overwriting. The final release
receipt includes the DBC hash and size, and the release verifier downloads the
archive anonymously, checks its source binding and content hash, and matches
it to producer evidence. Missing or mismatched archives block release
completion and release-note publication. Only verified notes advertise the
new archive links; source documentation retains notebook links as a fallback.

If an upload succeeds but a later step fails, retain the build artifacts and
ledger. Retrying `Release` reuses the validated archive from `Publish`, even when
the two jobs have different attempt numbers; it does not export new ZIP bytes.
A subsequent archive build can reuse a public archive only when it is
bound to the same approved plan and source; it reimports and compares all cells
again. A conflicting version is an error, not permission to overwrite it.
Follow the existing interrupted-release recovery procedure rather than
blindly rerunning Maven or PyPI publication.
An archive may therefore exist for an incomplete release. Do not advertise it
until release evidence is complete. Do not replace the approved plan or reuse
the version for different sources.
If upload reports that no public archive exists, check the service connection's
blob write access and retry the failed `Release` job. Maven Blob publication
has already succeeded, but no PyPI or ESRP publication has run at that point.
Do not rerun the successful Maven publisher. A conflicting public archive
requires investigation; never overwrite it.

The `full-release --repo` and `push-tags` guards check notebook admissibility
from Git before publishing any tags, including port tags. Before merging the
release-prepare PR and each port release PR, check out its head in a clone
whose `origin` is `microsoft/SynapseML` and run:

```bash
python scripts/release/release_guard.py full-release --version 1.2.0 --repo .
```

Use the intended release version and add `--include-spark40 true` when selecting
that optional runtime. The command reads remote branch refs and local committed
notebooks without creating tags or uploading anything. Release preparation and
tag workflows also run these guards automatically. Native Databricks validation
still runs in the release build; neither check proves notebook code execution.

Notebook-only PRs run **Release Notebook Validation**, including the actual
committed `docs/**/*.ipynb` inputs and the archive regression suite. This
read-only job needs no service credentials, JVM build or Databricks access.
Ordinary Markdown-only changes do not trigger it. The separate native archive
round-trip remains mandatory during a release.

Saved schema-2 plans retain their exact identity and original artifact scope;
they do not gain permission to upload DBCs. Generate and explicitly approve a
new schema-4 plan to select archive publication. This is required for 1.2.0
if its earlier draft predates this integration. Do not edit a saved plan or
move existing tags to retrofit the new behavior.

## Safety and public information

- Use the public OSS-only Maven plan for public workflow inputs and evidence.
  Encoding or compressing a document does not redact it.
- Keep credentials, private-source bindings, private destinations, operator
  records and local configuration out of public source, workflow inputs,
  comments, logs and release attachments.
- Keep plans and state outside source checkouts. Builds require clean source.
  Do not commit a release ledger or a local configuration profile.
- A plan ID identifies a document; it is not permission to publish. A maintainer
  must approve its exact source commits, version, targets and destinations.
- Never move a published tag, overwrite a release artifact, discard unknown
  submissions or change the plan under an existing approval.
- Keep signing approvals and PR merges manual. Do not change repository
  permissions or bypass branch protection to release.

The old Spark 4.1 PR compatibility replay job has been removed. Every selected
release target still requires its own build, tests and verified producer
evidence. Removing that job does not qualify the primary wheel for Spark 4.1.

## Recover a warning-only Azure release build

Optional cache or Codecov outages can leave a fully published release marked
`partiallySucceeded` in Azure. Do not rerun immutable publishers just to turn
that status green. Run `release_ops.py status` or collect `verified_evidence`
using the original approved plan and ledger. An older ledger that recorded
that same build as failed can be reconciled without queueing another build.

For public releases only, the driver checks the complete task timeline before
accepting this result. Every failed or warning task must match the exact
allowlisted cache/Codecov label, official Azure task ID and supported major
version. Job and task retry counters are checked separately, and task execution
times must fall within the current job attempt. Maven preparation, ESRP publication,
provenance recording and provenance upload must all have succeeded. Missing,
skipped, canceled, unexplained or unapproved publication failures remain
blockers. All existing source, approval, artifact-hash, DBC and freshness checks
still apply.

The ledger and exported evidence retain Azure's actual `partiallySucceeded`
result. Warning receipts and producer evidence use version 2. The local ledger
retains every sanitized task record. Exported evidence carries per-job outcome
counts, grouped advisory failures, successful publication checks and a SHA-256
link per build to its complete retained job and task records. The one digest
binds every job ID, attempt, execution window and task; it avoids exporting
hundreds of separate high-entropy hashes. This keeps production-sized builds within
GitHub's dispatch-input budget without skipping full timeline validation.
The digest is an audit link, not an approval or a signature.
Export permits up to 60,000 encoded characters and also checks the plan,
approval and request envelope against GitHub's 65,535-character combined input
budget. Production-sized regressions use distinct job/task execution windows
and cover both the default two-runtime release and the optional three-runtime
release, including widespread advisory outages. Keep the generated plan's formatting
or use compact JSON when dispatching; unnecessary whitespace still consumes
GitHub's input budget. Both encode and decode enforce these limits.
Clean builds retain the existing version-1 format.
Consumers of warning evidence must use this updated verifier. Private
publication workflows still require strict success. If the proof is rejected,
inspect the original build and follow interrupted-release recovery below.
Never edit the ledger, forge success, discard the build ID or change the plan.

On Windows, large evidence cannot fit in one environment variable. Decode it
with `verify_release.decode_evidence`, save the resulting JSON outside the
checkout, and use `release_guard.py notes --evidence <file>` for a local guard
check. The GitHub workflow runs on Linux and uses the encoded environment input.

## 1. Preview and prepare source

The examples use Bash. Python CLIs also run on Windows.

```bash
mkdir -p ../release-runs/v1.2.0/oss
python scripts/release/release_matrix.py \
  --version 1.2.0 --repositories oss --families maven \
  --output ../release-runs/v1.2.0/oss/draft.json
python scripts/bump-version.py --to 1.2.0 --dry-run
```

The draft has no approved commit bindings and cannot authorize publication.
The version-bump dry run edits no files. `--output` refuses to replace an
existing file. Inspect and keep a failed or partial output; generate to a new
filename rather than overwriting a release record.

For the normal post-merge workflow:

```bash
gh workflow run release-prepare.yml --repo github.com/microsoft/SynapseML \
  --ref master -f version=1.2.0 -F skip_docs=false
```

Dispatch creates a version/docs PR, not a dry run. Review its changes and
current-head validation before merging. `skip_docs` is a preparation aid, not
permission to finalize a release without its versioned documentation.
Preparation refuses any existing primary, Spark or Python tag for that version,
including an interrupted family with no primary tag. Use reviewed recovery
instead; missing remote access is an error, not proof that the tags are absent.

The primary release workflow checks that the release commit is on `master`,
creates the primary derivative tags, and opens port release PRs. Review and
merge the port PRs in order. Preserve the target's Spark, Scala, Python and JDK
settings and resolve staging/pipeline differences together.

Do not rebase or force-push a shared port branch. A maintainer merging an exact
same-repository port release PR authorizes tagging its recorded merge SHA.
Manual conflict-resolution PRs use the same boundary. Fork PRs do not authorize
the callback. Tags alone do not approve package publication.

Full releases require `master` and `spark4.1`. By default, Spark 4.1's release
PR starts from the primary release directly. A configured `SKIP_SPARK40` does
not block these two targets. It remains a veto when Spark 4.0 is selected;
in that case a failed policy read stops the operation.

### Explicitly include Spark 4.0

Normal preparation always selects the default pair. To include Spark 4.0,
dispatch the tag orchestrator explicitly against the same primary tag after
preparing its reviewed source:

```bash
gh workflow run release-tag.yml --repo github.com/microsoft/SynapseML \
  --ref v1.2.0 -F include_spark40=true
```

This processes Spark 4.0 first. If neither port PR exists, newly created PRs
form the chain `master -> spark4.0 -> spark4.1`. Normal preparation has already
dispatched Spark 4.1, so its existing PR or merged result is preserved, not
restacked. Finish any running default-pair workflow before the explicit
dispatch; never rewrite reviewed branches or released sources.
Include `--targets master,spark4.0,spark4.1` when generating the plan and supply
all three reviewed commit bindings. For pre-merge bootstrap, that explicit
plan is the selection; the workflow's `include_spark40` input never changes it.
There is no repository-wide positive opt-in that can silently alter a plan.

Every new dispatch that should check or repair Spark 4.0 tags needs
`include_spark40=true`. A default dispatch does not check or recover them.

Before preparing opt-in documentation, confirm that the canonical repository's
`SKIP_SPARK40` policy is absent or false and that the Spark 4.0 source and
consumer-validation prerequisites can be met. An unreadable policy is not an
absent policy. Resolve a veto or a known runtime blocker before committing
documentation that promises its new artifacts. The tag and publication guards
still recheck policy later; this preliminary check does not replace them.

For opt-in documentation, prepare with `skip_docs=true` or run the local bump
with `--skip-docs`. On the resulting preparation/candidate branch, update
Spark 4.0's retained coordinates, notebook tags and Python examples in all
source guides, plus `spark40Version` in `website/src/installArtifacts.js`.
This includes `README.md`, `docs/Get Started/Install SynapseML.md`,
`docs/Explore Algorithms/Deep Learning/Getting Started.md`,
`docs/Explore Algorithms/Deep Learning/ONNX.md` and `docs/Reference/R Setup.md`.
Find additional references with:

```bash
git grep -n -E 'spark4[.]0|pyspark>=4[.]0|spark40Version' \
  -- README.md docs website/src/installArtifacts.js
```

Then run the skipped documentation steps from that branch's configured build
environment. Do not repeat the version bump or alter older snapshots:

```bash
sbt convertNotebooks
(cd website && npm exec -- docusaurus docs:version 1.2.0)
python scripts/bump-version.py --finalize-docs --to 1.2.0
(cd website && SYNAPSEML_DOCS_PREVIEW=true npm test && npm run build)
```

Review and commit the completed source and snapshot before approving the
candidate. `website/test/installDocs.test.js` checks the coupled guides against
the runtime metadata. Update Spark 4.0's publication lock only after verifying
its artifacts. Default version bumps retain its previous references; they do
not promise an optional build. Existing saved three-target schema-2 plans
retain their identity and selection; changing to two targets requires a new
plan and approval.
The serialized `base_branch` records historical port lineage, not an additional
release target or a requirement to fetch that branch.

If Spark 4.0 is dropped **before tags or submissions exist**, restore its source
guides and `spark40Version` to the verified retained version recorded in
`website/test/published-spark-ports.lock`. Correct only the new, unpublished
snapshot if it has already been generated; leave older published snapshots
unchanged. Preserve the new master and Spark 4.1 references, rerun website
checks, regenerate a two-target plan and obtain new approval. Retain the
abandoned plan and any ledger rather than overwriting them.

If any tag or submission already exists, stop and follow recovery with the
original records. Do not edit a tagged candidate, silently drop its target,
move tags, or raise the lock to an unpublished version. A separately reviewed
documentation correction can restore the retained Spark 4.0 references after
the unchanged primary source is integrated. Release selection and any partial
publication must be reconciled explicitly before proceeding. Until corrected,
keep website deployment blocked rather than weakening its publication checks.

### Before the automation PR is merged

The normal workflow deliberately requires a primary commit contained in
`master`. New preparation and notes workflows must also exist on the default
branch before GitHub accepts their dispatches. Do not remove these safeguards,
create production tags experimentally, or pretend a fork workflow validated
the production release.

Before bootstrap, read master's classic protection and active rulesets. Confirm
that a candidate can merge without updating its head when master advances.
An absent classic-protection rule does not mean there are no rulesets. If rules
require an up-to-date head, or cannot be read, stop before tagging and resolve the
release procedure with the maintainer. Do not change protection to bypass it.

Use the explicit bootstrap mode of the already-registered tag workflow.
Prepare same-repository branches named `release-candidate/v1.2.0-master`
and `release-candidate/v1.2.0-spark4.1`. Only an explicitly selected Spark 4.0
release also needs `release-candidate/v1.2.0-spark4.0`.
Each must include the automation and version/docs changes on its own current
target baseline. Open a PR to the corresponding target without merging it.
Require successful current-head Azure validation and Compile & Style Check.
The primary candidate also needs a successful current-head Website Deploy build.
That build validates the versioned documentation and sidebar references.
PR and non-master website builds preview the prepared version without changing
the last-published artifact lock. They cannot deploy Pages. The master deployment
still requires the lock to match the version being published.

Generate the public plan using those final candidate commits. From a clean
checkout of the primary candidate whose `origin` is the canonical repository,
preview the exact tag set:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json
```

Run the same checks on GitHub without creating tags:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json --dispatch-preview
```

Check that preview run before requesting approval. Manual workflow dispatch
defaults to preview; creating tags requires `bootstrap_apply=true`.

After a maintainer reviews the candidates and supplies `REVIEWED_PLAN_ID`:

```bash
python scripts/release/bootstrap_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --approve-plan "$REVIEWED_PLAN_ID" --dispatch
```

The dispatcher validates the public plan and candidates locally before sending
the generated payload to GitHub. Do not submit raw private files as workflow
inputs. Dispatch queues the bootstrap job; it is not proof that tags were created.
Confirm the run completed and recheck the canonical refs.

The job requires that exact primary branch and commit, the complete public
plan, current candidate refs and PRs, successful current-head checks, expected
runtimes and versioned docs. It pushes all missing tag-family members in one
atomic operation and verifies them. Existing matching tags are preserved;
conflicting tags stop the entire operation.

The job uses `GITHUB_TOKEN`, whose tag writes do not trigger recursive push
workflows. Bootstrap does not merge PRs, start the normal port-PR chain or queue
package builds. The normal push workflow still enforces containment in `master`.
After successful bootstrap, use the same plan and publication ledger below.
Required tests run again on the exact tagged source before production uploads.

After publication, merge the unchanged primary candidate first, promptly, before
other automation changes. Tagged candidates are exempt from the usual PR-loop
rebase and not-behind gates: never use **Update branch**, amend, rebase, force-push
or add commits to them. If master changes conflict with a tagged candidate, the
release owner must use a separate reconciliation PR on master that preserves
the required changes and makes the unchanged candidate mergeable.

The new notes workflow is not dispatchable until it is registered on the
default branch. Retain the ledger locally. Merging the primary candidate also
integrates its automation. Reconcile any remaining automation PR afterward,
then regenerate fresh public evidence and publish notes. If integration cannot
be proved, leave notes unpublished, preserve the tags and artifacts, and have
the release owner resolve the source-integration issue. Do not move tags or
republish a coordinate to make notes pass.

### Version and launcher maintenance

Version bumps leave release protocol code, historical fixtures and existing
versioned documentation unchanged. New releases create new documentation
snapshots rather than rewriting old ones.
The new versioned installation guide omits the moving master-snapshot command.
Finalization preserves that example in the current source guide and does not
rewrite older documentation snapshots.
Spark 4.0 installation coordinates, notebook tags and Python examples keep
their last published version across default bumps. Website publication checks
that retained version against its lock while requiring the new Spark 4.1
version to be published. Preview mode does not relax production deployment.

If documentation generation fails after the source version has changed, follow
the stage-specific recovery commands printed by the bump tool. For an existing,
newly generated snapshot, repair the reported problem and finalize it without
repeating the version bump or `docs:version`:

```bash
python scripts/bump-version.py --finalize-docs --to 1.2.0 --dry-run
python scripts/bump-version.py --finalize-docs --to 1.2.0
```

Recovery requires complete Git history, the current source version, and a
snapshot not committed in the current candidate's ancestry. A sibling runtime's
same-version snapshot does not make this candidate historical. The dry run
validates without writing; finalization does not rerun notebook conversion or
Docusaurus and cannot repair missing generated inputs.
For a shallow canonical clone, run `git fetch --unshallow origin` before retrying.

Release preparation and PR validation check the downloaded SBT launcher against
`project/sbt-launch.sha256` before installing or running it. When changing the
SBT version in `project/build.properties`, review the launcher bytes and update
the checksum in the same PR. Missing or mismatched checksums stop bootstrap.

## 2. Bind the final source

Fetch the canonical tags from `microsoft/SynapseML`. Set `MASTER_SHA`
and `SPARK41_SHA` to the peeled commits of `v1.2.0` and `v1.2.0-spark4.1`.

```bash
python scripts/release/release_matrix.py \
  --version 1.2.0 --repositories oss --families maven \
  --oss-commit "master=$MASTER_SHA,spark4.1=$SPARK41_SHA" \
  --output ../release-runs/v1.2.0/oss/plan.json
```

Use final reviewed commits, not feature SHAs that a later merge may rewrite.
Never hand-edit coordinates, source bindings or the digest. Changing any of
them requires a regenerated plan and new approval. A legacy metadata-bearing
document is not made safe by relabeling it as an OSS plan.

New public plans use schema 4; saved schema-2 plans keep their original scope.
Old schema-1 production plans keep their original
identity when read locally, but cannot execute or enter public workflows.
Regenerate them, obtain approval for the new ID and start a new ledger.
Optional private operations require a separate plan, ledger and explicit local
configuration outside the checkout. No private destination is selected by default.

## 3. Preflight and publish

Use the existing authorized CLI login. Never copy tokens into plans or shell
commands recorded in public evidence.

```bash
python scripts/release/release_ops.py preflight \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json
```

Both commands queue nothing. Preflight reads source, policy, artifact inventory
and destination state. Missing new artifacts are expected; mismatched tags,
unknown source or failed service reads are not. Review the exact pending
operations before approval.

After a maintainer supplies `REVIEWED_PLAN_ID`:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" \
  --wait --poll-seconds 60 --timeout-seconds 3600
```

The driver queues selected missing work against exact tags and commits. It
records intent before submission, then records returned build IDs. Complete
the normal signing approvals when requested. Required style, unit, Python and
artifact gates remain enabled. Disabled optional tests are not dependencies;
enabled optional tests must succeed.

Release mode publishes Maven CDN artifacts, prepares signed Maven output, and
publishes the primary public PyPI wheel. Schema-4 plans also publish the approved
runtime-specific DBC archives; saved schema-2 plans do not upload notebooks.
Release mode does not upload generated docs, R packages, module wheels or badges.
Ordinary snapshot CI keeps its existing behavior.

For monitoring without queueing:

```bash
python scripts/release/release_ops.py status \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json --wait
```

Timeout and interruption do not cancel builds or discard IDs. Continue from
the original ledger. A read failure stops the command; restore service access
and rerun without inventing a retry or adoption.

## 4. Evidence and release notes

```bash
python scripts/release/verify_release.py \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json --json \
  > ../release-runs/v1.2.0/oss/evidence.json
```

Evidence requires fresh tag/artifact visibility, successful authoritative
producer runs, matching requests and source commits, and artifact-hash receipts.
All public Maven modules need their JAR and POM; Core also needs its tests JAR.
Verification checks the publisher's `https://mmlspark.blob.core.windows.net/maven`
repository and Maven Central separately.
The primary receipt includes the exact public PyPI wheel. Stale versions,
unexpected classifiers, missing modules and wrong source bindings fail.
Bound verification also requires that exact non-yanked wheel in PyPI's file
list and checks its public download. Version metadata alone cannot complete a
release after the wheel has been removed.

Inventory-only reports cannot approve a release. Neither can skipped required
jobs, old runs without receipts, stale evidence or user-supplied success claims.

For a read-only check of an older public release, use
`python scripts/release/verify_release.py --version 1.1.4 --skip ado,internal`.
This needs no private profile and checks public tags, Maven artifacts and PyPI.
It does not require historical DBCs or produce approval evidence. Use the bound
plan and state above to verify a new release, including its required DBCs.

After publication, merge the unchanged primary candidate before other
automation changes. Never update a candidate's head after it has been tagged.
Repository squash and rebase merges are supported without moving the release tag.
The notes guard requires either master ancestry or a merged canonical candidate
PR whose final head is the tagged commit and whose merge result is on master.
An open PR, a fork PR or an unmerged merge result cannot authorize notes.

When the notes workflow is available, run its read-only integration check from
a canonical checkout before dispatch. Regenerate public evidence immediately
before dispatch; it expires after one hour.

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

Only the public allowlisted plan and evidence may enter those inputs. The
workflow repeats public checks and leaves an existing GitHub Release unchanged.
Only a completed HTTP 404 lookup permits release creation. Authentication,
rate-limit, server and transport errors stop the workflow without generating
or publishing notes; retry after resolving the lookup failure.

Only after artifact and producer verification succeeds and the primary
candidate's versioned documentation has merged to master, update
`website/test/published-spark-ports.lock` to the verified Spark port versions in
a reviewed documentation follow-up. Do not bump this publication lock during
source preparation or open the lock follow-up against older master metadata.
Until that follow-up lands, master Website Deploy fails its strict lock check
and the existing public site stays in place. Verify the successful deployment
after the follow-up, not just the package publication.

Local candidate website checks use `SYNAPSEML_DOCS_PREVIEW=true npm test`.
Leaving the variable unset runs the strict production lock check.

### Python distribution readiness

Before approving production tags, validate the planned wheel packaging and
consumer behavior from the pinned candidates on every advertised runtime.
Port CI using that branch's own generated wrappers does not prove compatibility
of the primary PyPI wheel. If a port needs different Python code, resolve its
distribution and include its artifacts in the approved inventory first.
After the production build, verify the final wheel contents and hashes as well.
Preinstalled wrappers or a source checkout are not proof that the published
wheel supplies that code.

## Recovery and limits

Keep one authoritative directory and state filename per approved plan.
`.release-plan-<plan_id>.json` binds that filename. State and plan locks prevent
local concurrent updates; they are not a global lock across copied directories
or multiple machines. Back up the ledger and its claim in trusted storage.

For an ambiguous submission, inspect Azure and explicitly adopt a matching run:

```bash
python scripts/release/release_ops.py resume \
  --plan ../release-runs/v1.2.0/oss/plan.json \
  --state ../release-runs/v1.2.0/oss/state.json \
  --apply --approve-plan "$REVIEWED_PLAN_ID" \
  --adopt "maven.oss.master=$KNOWN_BUILD_ID"
```

Adoption validates source and request identity and queues no unrelated work.
Never delete the ledger to manufacture a fresh attempt.

Same-coordinate Maven retries are unsupported. Missing required artifacts do
not prove every signature, checksum or optional JAR is absent. Partial or bad
publication requires a new patch version and newly approved plan. Tags and
released packages cannot be reverted by this tooling. A source revert is not a
package rollback; issue a corrected version and communicate the affected one.

If a port merge event has no `merge_commit_sha`, do not infer the release commit
from the current branch tip or push tags directly. Fetch the canonical target,
independently verify the merged commit and its reviewed release contents, and
check it out detached. Run `release_guard.py full-release --repo . --version
<version>`, adding `--include-spark40 true` for that optional port. Create only
missing local tags with `git tag <tag> <sha>`; verify that any existing tags
match the same commit and never replace them. Run
`release_guard.py push-tags --repo . --commit <sha>` with both
`--tag v<version>-<target>` and `--tag v<version>-python<python-version>`.
This revalidates the committed notebooks before atomically pushing the selected
local tags, and rejects conflicting remote tags.

Use `status --inspect-lock` for bounded local lock metadata without Azure reads
or lock deletion. Confirm the original owner is gone, coordinate exclusive
recovery, inspect known submissions and preserve the records. Only then remove
the exact unchanged dead locks. Never remove the ledger or persistent claim.

Exit `0` means the requested operation succeeded, not necessarily that a
read-only inspection completed a release. Exit `1` means incomplete work;
exit `2` means invalid approval, source, state, policy or transport data.

## Validation

```bash
python -m pytest scripts/release tools/ci/tests/test_pipeline_yaml.py -q
SYNAPSEML_TEST_RELEASE_SBT=1 python -m pytest \
  scripts/release/test_release_version.py -q
```

Select the branch's JDK for SBT. Local regression tests and no-run pipeline
previews do not prove production publication. Verify the actual released
artifacts with consumers on each target runtime before calling the release
complete.
