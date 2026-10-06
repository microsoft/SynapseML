# PR 2628 pre-commit review, round 3: edge cases and robustness

## Review summary

| Field | Value |
| --- | --- |
| Round and theme | 3, edge cases and robustness. Error handling, boundary conditions, concurrency and failure modes |
| Model | claude-opus-5.5 |
| Mode | Sequential direct-contract, one round, no delegation |
| HEAD | `3ce916902329c20d5c37de43d7c43d805f28e748` with the staged nine-file correction |
| Merge base | `9d51ad1acd765b3246bec15517abf6b62e1d5d70` |
| Frozen patch fingerprint | `aec338560f920db95ebf109703d7e08bed51a5306b02cd984032bc7e2fe214f8` |
| Scope | The complete 55-path merge-base-to-index diff, excluding `reviews/` |
| Issue count | 1, Medium |
| Verdict | ISSUES_FOUND |

The pending correction works. `validate_archive` now checks every ZIP member path before it skips directory entries, and I couldn't find a way around it. The DBC build, handoff, retry and schema-2 paths held up in every failure case I traced.

The one issue isn't in the correction. It comes from this PR's strict release evidence meeting older pipeline steps. The pipeline still runs Publish and Release after a job finishes with issues, but the ledger and the evidence checks accept only a clean `succeeded` build. A cache or Codecov failure during a release build can therefore leave a correct, fully published release that the automation marks failed and can never complete.

## Evidence checklist

- [x] The staged diff from the merge base, excluding `reviews/`, hashes to the prompt's full-diff SHA256 `832791850b1fa268da9660b638177772de41a3b64ce8e67abdb197dee39d70f5`. The working tree matched the index. The prompt doesn't say how the frozen fingerprint is computed, so I report it as supplied.
- [x] I read the surrounding source directly. That covers all of `release_dbc.py`, `release_guard.py`, `verify_release.py`, `release_config.py`, `release_matrix.py`, `bootstrap_release.py` and `tools/esrp/prepare_jar.py`. In `release_ops.py` I read the ledger, build validation, refresh, adoption, retry and evidence code. I also read the Publish, Release and test jobs in `pipeline.yaml`, the cache, conda, Key Vault and Codecov templates, the upload guards in `project/build.scala`, the changed workflows, and the recovery sections of `scripts/release/README.md` and the release skill.
- [x] `validate_archive` at `scripts/release/release_dbc.py:169-232` rejects members outside the versioned root, backslashes and `..` components before the directory skip, so directory entries get the same checks as files. It accepts the bare root directory entry and requires files to end in `.python`. A name like `<root>//x.python` passes the path checks. The inventory comparison rejects it on the build path, and the publish and receipt paths compare exact bytes and hashes with the Publish-built archive, so nothing ever extracts it.
- [x] ZIP limits hold. Empty input, archives over 10 MB, more than 10,000 entries, declared expansion over 100 MB, duplicate names, CRC failures, encrypted or unsupported compression, and zlib or EOF errors all end in `ValueError`. Declared sizes bound every read.
- [x] The handoff matches the contract. `buildDbc` and `PublishPipelineArtifact@1` run in Publish inside the `publishRelease` compile-time block at `pipeline.yaml:258-281`. Both are gated on `releaseDbc`, run after the source guard and before Maven authentication and `publishBlob`, and neither uses `continueOnError`. Release reads `dependencies.Publish.outputs['buildDbc.artifactName']` at line 617. At lines 671-680 it checks the name against `^release-dbc-[1-9][0-9]*$`, so an empty, unexpanded or malformed value fails before `DownloadPipelineArtifact@2` runs with `buildType: current` at lines 681-687. A Publish rerun publishes a new attempt-suffixed artifact. A Release rerun downloads the same artifact and never contacts Databricks.
- [x] Snapshot CI compiles out both new Publish steps and the Release job. Schema-2 plans leave `releaseDbc` false, so validation, download and upload are skipped, and `maven_receipt` rejects `--dbc-directory`.
- [x] Retries are idempotent where they need to be. `publish_archive` accepts an existing blob only when the bytes, plan ID and source digest all match. It tolerates an upload error only if the public bytes match afterwards, and it never overwrites a conflict. `build_archive` reuses a public archive only for the same plan and source, and round-trips it again. If workspace cleanup fails after a successful round trip, Publish fails before the Maven upload and a rerun fixes it. If cleanup fails after an earlier error, the original error is what gets reported.
- [x] `release_dbc.py` turns HTTP errors other than 404, timeouts, `OSError` and `http.client.HTTPException` into `ValueError` with sanitized messages, and it refuses redirects.
- [x] The strict PyPI check wants exactly one non-yanked `bdist_wheel` with the expected filename, an HTTPS `files.pythonhosted.org/packages/` URL, and a download that answers. Unbound `--skip ado,internal` runs fall back to a schema-2 public plan with no DBC rows, no private profile and non-strict checks.
- [x] The ledger uses `O_EXCL` plan and state locks, compare-before-replace saves with claim and fingerprint checks, and fsync with atomic replace. `_queue` saves intent before it submits a build.
- [x] I ran no tests, SBT, Black, Azure, Databricks, GitHub or PyPI commands, and no Azure YAML preview. I changed no source. The local test results in the prompt come from the coordinator, and I didn't reproduce them.

Limits of this review:

- I worked out Databricks naming and import behavior from the code, not a live workspace. Two things are unconfirmed. One is whether an imported notebook keeps `.ipynb` in its name. The other is whether importing a DBC into a new folder maps the archive root to that folder. If either assumption is wrong, Publish fails closed before the Maven upload. That still happens after the release tags exist, so the fix would need new tagged source.
- Azure DevOps result aggregation and rerun behavior come from platform documentation. I didn't run a build.
- Remote CI for the pending correction hasn't run.

## Issues

### 1. Medium: a failed non-gating step strands a fully published release

Where the two gates disagree:

- `scripts/release/release_ops.py:2185-2191`. `_refresh_group` marks every Maven action in the group `failed`, with "Azure build did not succeed; automatic retry is forbidden.", when a completed build has any result other than `succeeded`. `_validate_build` accepts `partiallySucceeded` as a terminal result at line 1903, so this branch is reachable.
- `scripts/release/release_ops.py:1937` and `:1979`. `_jobs` rejects job results other than `succeeded` and `skipped`, and `_public_jobs` requires a single `succeeded` Release job. `validate_producer_evidence` at `:3034` and `verified_evidence` at `:3096` also require a `succeeded` build.
- `pipeline.yaml:219` and `:615`. Publish and Release run on `succeeded()`. For jobs, Azure DevOps treats that as true when dependencies succeeded or finished `succeededWithIssues`.
- Release builds still contain `continueOnError: true` steps. Cache@2 in `templates/sbt_cache.yml:46,56,66` and `templates/conda.yml:18` runs in Publish at `pipeline.yaml:242-244` and in the test jobs. The Release job has its own conda cache at `pipeline.yaml:651-653`. UnitTests loads the Codecov token at `pipeline.yaml:1146-1150`, and `templates/codecov.yml:24` uploads coverage. The pipeline includes the Codecov template for `refs/tags/` builds at `pipeline.yaml:852`, `:918`, `:955` and `:1155`, and `release_ops` queues release builds on the release tag.

These `continueOnError` steps predate this PR. The strict evidence gate is new here, and the mismatch between the two is what strands the release.

Trigger. Queue a schema-4 release and have any of those steps fail in any job. A cache service error, a Codecov CLI download failure or a Key Vault read error for the Codecov token is enough. That job ends `succeededWithIssues`, and job-level `succeeded()` still passes. Publish uploads Maven, and Release uploads the DBC archive, the PyPI wheel and the ESRP payload. The build ends `partiallySucceeded`.

Consequence. The next `release_ops status` or `resume` marks the target's Maven actions `failed`. `verified_evidence` then returns an incomplete report, `validate_evidence` at `scripts/release/verify_release.py:905-916` refuses anything short of producer-verified evidence, and `.github/workflows/release-notes.yml` has no `--github-evidence` input to run with. I found no recovery path:

- `_refresh` at `release_ops.py:2234-2243` re-reads the build on every call, so only a change in the Azure result could help. The Azure DevOps rerun option retries failed jobs, and `succeededWithIssues` doesn't count as failed.
- Rerunning every job repeats the Maven upload. `ReleaseVersion.mayOverwrite` returns false for release coordinates, so `refuseExistingReleaseBlob` at `project/build.scala:149-175` fails Publish, and the build ends `failed`.
- A new build gets a new build ID, and `_adopt` refuses to replace a recorded one at `release_ops.py:2273-2276`.
- `_retry` blocks Maven outright at `release_ops.py:2577-2580` and needs a `failed` result at `:2595-2597` anyway.

What's left is a new patch version for a publication that was correct. Neither `scripts/release/README.md` nor the recovery reference mentions this state. The prompt's requirement to describe partial releases honestly doesn't cover it, because nothing here is partial.

Minimal fix. Make the evidence gate agree with the pipeline gate for the known non-publishing steps. `_refresh_group`, `_jobs`, `_public_jobs`, `verified_evidence` and `validate_producer_evidence` should accept `succeededWithIssues` and `partiallySucceeded` only when the build timeline's task records show that every task with issues is on an allowlist of non-publishing tasks, meaning the Cache@2 steps and the Codecov token and upload steps, and every publication task succeeded. Add a contract test that fails if a release build gains a `continueOnError` step outside that allowlist, and describe the state in `scripts/release/README.md`.

A smaller interim step is to leave the Codecov token and upload steps out of `publishRelease` builds, since coverage doesn't gate a release. That removes the likeliest trigger but leaves the cache steps. Simply dropping `continueOnError` from caches isn't enough on its own. A cache failure in Publish or Release after an upload would turn into a job failure, and a rerun of that job then hits the no-overwrite guards.

## Driver disposition

Confirmed independently with a network-isolated reproduction using the real
release CLI and existing fake-remote fixtures. A clean completed build produces
complete evidence. The same modeled publication, with every public inventory
row present and a non-publishing warning in UnitTests, Publish or Release,
produces a failed Maven action and no complete evidence. The reproduction has
three failing cases and one passing control. This is modeled driver coverage,
not a live Azure or production publication experiment.

Microsoft's [expression documentation](https://learn.microsoft.com/en-us/azure/devops/pipelines/process/expressions?view=azure-devops#succeeded)
confirms that job-level `succeeded()` accepts partially successful dependencies.
The checked-in cache and Codecov templates use `continueOnError: true`; the
new strict completion gate rejects the resulting `partiallySucceeded` build.
The mismatch therefore remains an automation blocker.

No additional product change has been made for this finding. The bounded loop
has consumed one integration and four correction cycles. Further correction
requires an explicit extension, followed by a fresh complete pre-commit pass
and current-head CI. The reviewed ordering fix remains intact; this pass is
not a clean completion.
