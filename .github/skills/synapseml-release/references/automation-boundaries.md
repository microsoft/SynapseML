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
| Release notes | Consumes public allowlisted evidence | Explicit notes dispatch |

Normal CI is snapshot-only. A tagged checkout alone does not authorize release
coordinates. Producer admission checks source, plan and approval before uploads.

The public Maven track includes CDN, Maven Central, primary PyPI and schema-4
source-bound DBC archives. Unrelated documentation, R, module-wheel and badge
uploads remain outside release mode. Mandatory and selected tests must succeed.
Removing the old compatibility replay does not waive target validation or
qualification of the actual primary wheel on every advertised runtime.

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
