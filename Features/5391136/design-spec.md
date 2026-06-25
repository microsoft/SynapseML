# Design Spec - Feature 5391136 (SynapseML)

> Area-specific design for SynapseML's contribution to Feature 5391136.
> See the [overall design](https://msdata.visualstudio.com/A365/_git/FeatureRegistry?path=/Features/active/5391136/overalldesign-spec.md) in the Feature Registry for system-level context.

## Scope

SynapseML owns the repository-level cleanup for the Acrolinx retirement request:

- Remove the stale `.acrolinx-config.edn` file from source control.
- Remove the active repo-level GitHub webhook that sends pull request events to Acrolinx.
- Verify no operational source/config files outside this `Features/5391136/` audit folder or repo-level webhook settings still reference Acrolinx.

## Design

The source change is intentionally minimal. Delete `.acrolinx-config.edn` because the Acrolinx contract expires on June 30 and the repo should no longer advertise or configure an integration that cannot authorize users after that date.

The repository setting change is performed through GitHub administration, not code. The active webhook before cleanup was:

- Hook id: `376909528`
- Event: `pull_request`
- URL: `https://microsoft-ce-csi.acrolinx.cloud/githubhook/listen/`

After deletion, SynapseML pull request activity should no longer send events to Acrolinx.

## Dependencies

- GitHub admin permission on `microsoft/SynapseML`.
- ADO Feature 5391136 for traceability.
- FeatureRegistry specs in `Features/active/5391136/`.

## Testing Strategy

- Search tracked source for `Acrolinx` and `acrolinx`.
- Verify `.acrolinx-config.edn` is deleted in git.
- Query the specific GitHub hook id after deletion and verify it is absent.
- Query all repo hooks and verify no hook config URL contains `microsoft-ce-csi.acrolinx.cloud`.

## Tasks

Task files will be created here if this feature needs more implementation breakdown. The immediate cleanup is tracked by the FeatureRegistry process tasks:

- PM Spec: AB#5391145
- Design Spec: AB#5391146
- Deployment: AB#5391147
