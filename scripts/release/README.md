# Public SynapseML releases

Prepare, approve, publish and recover public SynapseML releases with one
source-bound plan and one durable ledger. Downstream private integrations are
not prerequisites and their deployment procedures do not belong in this guide.

**Consumer-wheel gate, before any production tag:** validate the planned Python
wheel packaging and consumer behavior from the candidate sources on every
advertised runtime. Port CI using different wrappers does not prove the primary
wheel works there. Resolve the distribution strategy and approved outputs before
requesting approval or running any tag-creating operation below.

The public release contains Maven CDN and Maven Central artifacts for `master`,
`spark4.0` and `spark4.1`, plus the primary public PyPI wheel. For `1.2.0`, the
Maven versions are `1.2.0`, `1.2.0-spark4.0` and `1.2.0-spark4.1`; PyPI receives
`synapseml==1.2.0`.

Merging the automation does not publish a release. Workflow dispatch, reviewed
source, exact-plan approval and signing approvals are separate steps.

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

Spark 4.1 PR compatibility replay is advisory. That does not waive validation of
the actual Spark 4.1 release candidate. Every selected release target still
requires its own successful build, tests and producer evidence.

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

The primary release workflow checks that the release commit is on `master`,
creates the primary derivative tags, and opens port release PRs. Review and
merge the port PRs in order. Preserve the target's Spark, Scala, Python and JDK
settings and resolve staging/pipeline differences together.

Do not rebase or force-push a shared port branch. A maintainer merging an exact
same-repository port release PR authorizes tagging its recorded merge SHA.
Manual conflict-resolution PRs use the same boundary. Fork PRs do not authorize
the callback. Tags alone do not approve package publication.

Full releases require all supported targets. `SKIP_SPARK40` must not be enabled.
Failure to read that policy is an error, not evidence that it is disabled.

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
Prepare same-repository branches named `release-candidate/v1.2.0-master`,
`release-candidate/v1.2.0-spark4.0` and `release-candidate/v1.2.0-spark4.1`.
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

Fetch the canonical tags from `microsoft/SynapseML`. Set `MASTER_SHA`,
`SPARK40_SHA` and `SPARK41_SHA` to the peeled commits of `v1.2.0`,
`v1.2.0-spark4.0` and `v1.2.0-spark4.1`.

```bash
python scripts/release/release_matrix.py \
  --version 1.2.0 --repositories oss --families maven \
  --oss-commit "master=$MASTER_SHA,spark4.0=$SPARK40_SHA,spark4.1=$SPARK41_SHA" \
  --output ../release-runs/v1.2.0/oss/plan.json
```

Use final reviewed commits, not feature SHAs that a later merge may rewrite.
Never hand-edit coordinates, source bindings or the digest. Changing any of
them requires a regenerated plan and new approval. A legacy metadata-bearing
document is not made safe by relabeling it as an OSS plan.

Public plans use schema 2. Old schema-1 production plans keep their original
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
publishes the primary public PyPI wheel. It does not upload notebooks, generated
docs, R packages, module wheels or badges. Ordinary snapshot CI keeps its
existing behavior.

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
The primary receipt includes the exact public PyPI wheel. Stale versions,
unexpected classifiers, missing modules and wrong source bindings fail.

Inventory-only reports cannot approve a release. Neither can skipped required
jobs, old runs without receipts, stale evidence or user-supplied success claims.

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
