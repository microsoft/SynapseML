# Public release automation boundaries

| Phase | Automation | Human boundary |
| --- | --- | --- |
| Matrix | Derives source bindings, coordinates and plan identity | Review the exact public plan |
| Preflight/status | Reads source, policy, runs and artifacts; saves local state | No remote writes |
| Preparation | Creates version/docs and port PRs | Explicit dispatch and reviewed merges |
| Tags | Uses recorded approved commits; verifies exact remote refs | Source approval; no moving published tags |
| Publication | Queues only approved missing work and records IDs | Exact-plan approval and signing gates |
| Recovery | Adopts an exact matching run | Explicit approved adoption; no blind retries |
| Evidence | Revalidates runs, receipts and public inventory | Inventory alone is not release approval |
| Consumer qualification | Producer receipts identify the published wheel | Qualify the source/version-bound candidate on every selected runtime; compare final payload/metadata and sign off |
| Release notes | Consumes public allowlisted evidence | Explicit notes dispatch |

Normal CI is snapshot-only. A tagged checkout alone does not authorize release
coordinates. Producer admission checks source, plan and approval before uploads.

The public Maven track includes CDN, Maven Central, primary PyPI and schema-4
source-bound DBC archives. Primary API documentation is generated from the
approved source and uploaded and checked before `publishBlob`; ports skip that
publication. The guard downloads the three version-specific API entry points
with bounded requests and rejects redirects, missing, empty or oversized pages.
API docs remain mutable, not an immutable package receipt or a separate lock.
R, module-wheel, badge and ordinary snapshot notebook uploads remain outside
release mode. Mandatory and selected tests must succeed.
Removing the old compatibility replay does not waive target validation or
qualification of the actual primary wheel on every selected runtime.
The rebuilt published wheel must match the qualified candidate's payload and
relevant metadata. ZIP metadata can change archive hashes; hashes alone do not
establish equivalence. A mismatch or absent qualification blocks readiness and
notes, even if automated producer verification succeeds.
Public Maven receipts bind CDN and ESRP output separately. Notes verify public
downloads against each destination's own producer hashes. An old receipt
without CDN proof blocks approval; it does not permit republishing.

Public warning-only producer builds require the task-level proof described in
the [release guide](../../../../scripts/release/README.md#recover-a-warning-only-azure-release-build).
An aggregate partial-success status is never sufficient. Unknown or failed
publication tasks remain blockers, and raw Azure outcomes stay in the evidence.

PR creation requires an approved, repository-scoped GitHub App. Follow the
[one-time setup](../../../../scripts/release/README.md#one-time-github-app-setup);
do not bypass organization policy with an unapproved personal token or infer
that merging the automation configures its credentials.

The normal primary-tag workflow requires containment in `master`. An explicit
pre-merge release must use a separately reviewed bootstrap path and preserve
the ordinary ancestry guard.

Do not send raw private or combined plans to a public workflow. Keep profiles,
credentials, operator records and private deployment procedures outside the
public checkout. No encoding makes a private document public-safe.
